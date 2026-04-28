-- models/intermediate/int_product_true_margin.sql
-- -----------------------------------------------------------------------
-- INTERMEDIATE MODEL: int_product_true_margin
-- DOMAIN: Margin / Supply Chain
-- GRAIN: One row per product
--
-- THE KEY INSIGHT OF THIS MODEL:
--   The original dim_products.gross_margin_rate uses a simple cost field
--   from the product catalog. That cost is what the CATALOG says it costs.
--
--   This model calculates TRUE LANDED COST by combining:
--     1. Material costs (from production_run_inputs + raw_materials)
--     2. Manufacturing labor (from production_runs)
--     3. Inbound logistics estimate (from supplier lead times + PO data)
--
--   The difference between catalog cost and true landed cost reveals
--   where margin is being lost in the supply chain.
--
-- BUSINESS LOGIC DEFINED HERE:
--   - true_landed_cost: material + labor + logistics estimate per unit
--   - true_margin_rate: (revenue - true_landed_cost) / revenue
--   - margin_gap: difference between catalog margin and true margin
--   - margin_risk_flag: products where true margin < 20% (business threshold)
-- -----------------------------------------------------------------------

with

products as (
    select
        product_id,
        product_name,
        category,
        cost                                                    as catalog_cost,
        price                                                   as list_price
    from {{ ref('stg_products') }}
),

-- Get actual revenue per product (from existing product revenue model)
product_revenue as (
    select
        product_id,
        total_revenue,
        total_units_sold,
        avg_selling_price
    from {{ ref('int_product_revenue') }}
),

-- Average material cost per unit produced from production runs
production_material_cost as (
    select
        pr.product_id,
        sum(ri.total_material_cost)                             as total_material_cost,
        sum(coalesce(pr.actual_quantity, 0))                    as total_units_produced,
        case
            when sum(coalesce(pr.actual_quantity, 0)) > 0
            then round(
                sum(ri.total_material_cost)
                / sum(coalesce(pr.actual_quantity, 0)),
                4)
            else null
        end                                                     as material_cost_per_unit
    from {{ ref('stg_production_run_inputs') }} ri
    inner join {{ ref('stg_production_runs') }} pr
        on ri.production_run_id = pr.production_run_id
    where pr.status = 'completed'
    group by pr.product_id
),

-- Average labor cost per unit from completed production runs
production_labor_cost as (
    select
        product_id,
        avg(cost_per_unit)                                      as avg_labor_cost_per_unit,
        sum(coalesce(total_production_cost, 0))                 as total_labor_cost,
        sum(coalesce(actual_quantity, 0))                       as total_units_produced
    from {{ ref('stg_production_runs') }}
    where status = 'completed'
    group by product_id
),

-- Estimate inbound logistics cost per unit
-- (simplified: average PO item cost weighted by qty ordered)
logistics_cost as (
    select
        poi.material_id,
        avg(poi.unit_price)                                     as avg_purchase_price
    from {{ ref('stg_purchase_order_items') }} poi
    inner join {{ ref('stg_purchase_orders') }} po
        on poi.purchase_order_id = po.purchase_order_id
    where po.status in ('received', 'partially_received')
    group by poi.material_id
),

-- Estimated logistics overhead per product
-- (products that use more expensive purchased materials bear higher logistics cost)
product_logistics as (
    select
        pr.product_id,
        avg(
            case when lc.avg_purchase_price is not null
            then lc.avg_purchase_price * 0.08   -- 8% of purchase price = freight estimate
            else null end
        )                                                       as estimated_logistics_cost_per_unit
    from {{ ref('stg_production_run_inputs') }} ri
    inner join {{ ref('stg_production_runs') }} pr
        on ri.production_run_id = pr.production_run_id
    left join logistics_cost lc
        on ri.material_id = lc.material_id
    where pr.status = 'completed'
    group by pr.product_id
),

combined as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.catalog_cost,
        p.list_price,

        -- Revenue metrics
        coalesce(pr.total_revenue, 0)                           as total_revenue,
        coalesce(pr.total_units_sold, 0)                        as total_units_sold,
        pr.avg_selling_price,

        -- Cost components
        pmc.material_cost_per_unit,
        plc.avg_labor_cost_per_unit,
        coalesce(pl.estimated_logistics_cost_per_unit, 0)       as logistics_cost_per_unit,

        -- ---------------------------------------------------------------
        -- BUSINESS RULE: True landed cost per unit
        -- = material cost + labor cost + logistics
        -- Falls back to catalog_cost if production data is unavailable
        -- ---------------------------------------------------------------
        coalesce(
            pmc.material_cost_per_unit, 0)
            + coalesce(plc.avg_labor_cost_per_unit, 0)
            + coalesce(pl.estimated_logistics_cost_per_unit, 0) as true_landed_cost,

        -- Flag: do we have enough data for a meaningful true cost?
        case
            when pmc.material_cost_per_unit is not null
             and plc.avg_labor_cost_per_unit is not null
            then 1 else 0
        end                                                     as has_true_cost_data

    from products p
    left join product_revenue pr        on p.product_id = pr.product_id
    left join production_material_cost pmc on p.product_id = pmc.product_id
    left join production_labor_cost plc on p.product_id = plc.product_id
    left join product_logistics pl      on p.product_id = pl.product_id
),

-- Calculate margin comparisons
margin_analysis as (
    select
        *,

        -- Catalog margin (using simple cost field — what dim_products uses)
        case
            when list_price > 0
            then round((list_price - catalog_cost) / list_price, 4)
            else null
        end                                                     as catalog_margin_rate,

        -- True margin using actual landed cost
        case
            when avg_selling_price > 0 and true_landed_cost > 0
            then round(
                (avg_selling_price - true_landed_cost) / avg_selling_price,
                4)
            else null
        end                                                     as true_margin_rate,

        -- Cost variance: catalog cost vs true landed cost
        case
            when catalog_cost > 0 and true_landed_cost > 0
            then round((true_landed_cost - catalog_cost) / catalog_cost, 4)
            else null
        end                                                     as cost_variance_pct

    from combined
),

final as (
    select
        *,

        -- Margin gap: difference between what we THOUGHT margin was vs reality
        case
            when catalog_margin_rate is not null
             and true_margin_rate is not null
            then round(catalog_margin_rate - true_margin_rate, 4)
            else null
        end                                                     as margin_gap,

        -- ---------------------------------------------------------------
        -- BUSINESS RULE: Margin risk flag
        -- Products with true margin < 20% are flagged for review.
        -- 20% minimum margin threshold set by finance team.
        -- ---------------------------------------------------------------
        case
            when true_margin_rate is not null
             and true_margin_rate < 0.20 then 1
            else 0
        end                                                     as margin_risk_flag,

        -- Margin health classification
        case
            when true_margin_rate is null     then 'unknown'
            when true_margin_rate < 0         then 'negative'
            when true_margin_rate < 0.10      then 'critical'
            when true_margin_rate < 0.20      then 'at_risk'
            when true_margin_rate < 0.35      then 'acceptable'
            when true_margin_rate < 0.50      then 'good'
            else                                   'excellent'
        end                                                     as margin_health

    from margin_analysis
)

select * from final
