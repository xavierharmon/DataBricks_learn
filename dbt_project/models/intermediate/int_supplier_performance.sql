-- models/intermediate/int_supplier_performance.sql
-- -----------------------------------------------------------------------
-- INTERMEDIATE MODEL: int_supplier_performance
-- -----------------------------------------------------------------------
-- DOMAIN: Suppliers
-- GRAIN: One row per supplier (collapsed from many purchase orders)
--
-- BUSINESS LOGIC DEFINED HERE:
--   - on_time_delivery_rate: actual vs expected delivery date
--   - actual_fill_rate: qty_received / qty_ordered across all POs
--   - cost_variance_pct: actual paid vs quoted price (overpay/underpay)
--   - supplier_score: composite performance metric (0-100)
--   - performance_tier: gold/silver/bronze/probation classification
-- -----------------------------------------------------------------------

with

suppliers as (
    select * from {{ ref('stg_suppliers') }}
),

purchase_orders as (
    select * from {{ ref('stg_purchase_orders') }}
),

po_items as (
    select * from {{ ref('stg_purchase_order_items') }}
),

supplier_materials as (
    select * from {{ ref('stg_supplier_materials') }}
),

-- Aggregate PO-level delivery performance per supplier
po_delivery as (
    select
        supplier_id,

        count(purchase_order_id)                                as total_pos,

        count(case when status = 'received' then 1 end)         as received_pos,

        count(case when status = 'cancelled' then 1 end)        as cancelled_pos,

        -- On-time: actual delivery <= expected delivery
        count(
            case when actual_delivery_date is not null
                  and actual_delivery_date <= expected_delivery_date
            then 1 end
        )                                                       as on_time_deliveries,

        -- Late deliveries and how late
        count(
            case when actual_delivery_date > expected_delivery_date
            then 1 end
        )                                                       as late_deliveries,

        avg(
            case when actual_delivery_date is not null
                  and actual_delivery_date > expected_delivery_date
            then {{ datediff_fn('expected_delivery_date', 'actual_delivery_date') }}
            end
        )                                                       as avg_days_late,

        -- Actual lead time vs quoted
        avg(
            case when actual_delivery_date is not null
            then {{ datediff_fn('order_date', 'actual_delivery_date') }}
            end
        )                                                       as avg_actual_lead_time_days,

        min(order_date)                                         as first_po_date,
        max(order_date)                                         as most_recent_po_date,

        sum(
            case when status in ('received','partially_received')
            then 1 else 0 end
        )                                                       as fulfilled_po_count

    from purchase_orders
    group by supplier_id
),

-- Aggregate line-item fill rates per supplier
po_fill_rates as (
    select
        po.supplier_id,
        sum(poi.qty_ordered)                                    as total_qty_ordered,
        sum(coalesce(poi.qty_received, 0))                      as total_qty_received,
        sum(poi.total_cost)                                     as total_po_spend,

        -- Cost variance: compare actual paid to standard material cost
        avg(
            case when rm.standard_cost > 0
            then (poi.unit_price - rm.standard_cost) / rm.standard_cost
            end
        )                                                       as avg_cost_variance_pct

    from {{ ref('stg_purchase_order_items') }} poi
    inner join {{ ref('stg_purchase_orders') }} po
        on poi.purchase_order_id = po.purchase_order_id
    left join {{ ref('stg_raw_materials') }} rm
        on poi.material_id = rm.material_id
    where po.status in ('received', 'partially_received')
    group by po.supplier_id
),

-- Count distinct materials this supplier provides
supplier_material_counts as (
    select
        supplier_id,
        count(distinct material_id)                             as materials_supplied,
        count(case when is_primary_supplier = TRUE then 1 end)     as primary_material_count,
        avg(quoted_unit_price)                                  as avg_quoted_price
    from supplier_materials
    group by supplier_id
),

-- Join everything together
combined as (
    select
        s.supplier_id,
        s.supplier_name,
        s.country,
        s.state,
        s.supplier_type,
        s.avg_lead_time_days                                    as quoted_lead_time_days,
        s.quoted_fill_rate,
        s.payment_terms_days,
        s.is_preferred,
        s.quality_rating,
        s.onboarded_at,

        -- PO volume metrics
        coalesce(pd.total_pos, 0)                               as total_pos,
        coalesce(pd.received_pos, 0)                            as received_pos,
        coalesce(pd.cancelled_pos, 0)                           as cancelled_pos,
        pd.first_po_date,
        pd.most_recent_po_date,

        -- Delivery performance
        coalesce(pd.on_time_deliveries, 0)                      as on_time_deliveries,
        coalesce(pd.late_deliveries, 0)                         as late_deliveries,
        pd.avg_days_late,
        pd.avg_actual_lead_time_days,

        -- On-time rate (of received POs)
        case
            when coalesce(pd.received_pos, 0) > 0
            then round(
                cast(coalesce(pd.on_time_deliveries, 0) as decimal(10,4))
                / cast(pd.received_pos as decimal(10,4)),
                4)
            else null
        end                                                     as on_time_delivery_rate,

        -- Actual fill rate
        case
            when coalesce(fr.total_qty_ordered, 0) > 0
            then round(
                cast(coalesce(fr.total_qty_received, 0) as decimal(14,4))
                / cast(fr.total_qty_ordered as decimal(14,4)),
                4)
            else null
        end                                                     as actual_fill_rate,

        -- Cost metrics
        coalesce(fr.total_po_spend, 0)                          as total_po_spend,
        fr.avg_cost_variance_pct,

        -- Material diversity
        coalesce(smc.materials_supplied, 0)                     as materials_supplied,
        coalesce(smc.primary_material_count, 0)                 as primary_material_count

    from suppliers s
    left join po_delivery pd          on s.supplier_id = pd.supplier_id
    left join po_fill_rates fr        on s.supplier_id = fr.supplier_id
    left join supplier_material_counts smc on s.supplier_id = smc.supplier_id
),

-- Apply scoring and tier classification
scored as (
    select
        *,

        -- ---------------------------------------------------------------
        -- BUSINESS RULE: Composite supplier score (0-100)
        -- Weighted components:
        --   40% on-time delivery rate
        --   35% actual fill rate
        --   15% quality rating (normalized to 0-1 scale from 0-5)
        --   10% cost variance (lower cost = higher score)
        -- ---------------------------------------------------------------
        round(
            coalesce(on_time_delivery_rate, quoted_fill_rate) * 40
            + coalesce(actual_fill_rate, quoted_fill_rate) * 35
            + (quality_rating / 5.0) * 15
            + case
                when avg_cost_variance_pct is null  then 10 * 0.5
                when avg_cost_variance_pct <= 0     then 10       -- below or at quoted price
                when avg_cost_variance_pct <= 0.05  then 10 * 0.8 -- up to 5% over
                when avg_cost_variance_pct <= 0.15  then 10 * 0.5 -- 5-15% over
                else 10 * 0.2                                      -- >15% over
              end,
            1
        )                                                       as supplier_score,

        -- ---------------------------------------------------------------
        -- BUSINESS RULE: Performance tier
        --   gold:       score >= 85 (top partners)
        --   silver:     score >= 70 (reliable, room to improve)
        --   bronze:     score >= 55 (adequate, needs monitoring)
        --   probation:  score < 55 (at risk, improvement plan needed)
        -- ---------------------------------------------------------------
        case
            when total_pos = 0 then 'unrated'
            when round(
                coalesce(on_time_delivery_rate, quoted_fill_rate) * 40
                + coalesce(actual_fill_rate, quoted_fill_rate) * 35
                + (quality_rating / 5.0) * 15
                + case
                    when avg_cost_variance_pct is null  then 5
                    when avg_cost_variance_pct <= 0     then 10
                    when avg_cost_variance_pct <= 0.05  then 8
                    when avg_cost_variance_pct <= 0.15  then 5
                    else 2
                  end,
                1) >= 85 then 'gold'
            when round(
                coalesce(on_time_delivery_rate, quoted_fill_rate) * 40
                + coalesce(actual_fill_rate, quoted_fill_rate) * 35
                + (quality_rating / 5.0) * 15
                + case
                    when avg_cost_variance_pct is null  then 5
                    when avg_cost_variance_pct <= 0     then 10
                    when avg_cost_variance_pct <= 0.05  then 8
                    when avg_cost_variance_pct <= 0.15  then 5
                    else 2
                  end,
                1) >= 70 then 'silver'
            when round(
                coalesce(on_time_delivery_rate, quoted_fill_rate) * 40
                + coalesce(actual_fill_rate, quoted_fill_rate) * 35
                + (quality_rating / 5.0) * 15
                + case
                    when avg_cost_variance_pct is null  then 5
                    when avg_cost_variance_pct <= 0     then 10
                    when avg_cost_variance_pct <= 0.05  then 8
                    when avg_cost_variance_pct <= 0.15  then 5
                    else 2
                  end,
                1) >= 55 then 'bronze'
            else 'probation'
        end                                                     as performance_tier

    from combined
)

select * from scored
