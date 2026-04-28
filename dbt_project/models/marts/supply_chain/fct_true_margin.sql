-- models/marts/supply_chain/fct_true_margin.sql
-- MART MODEL: fct_true_margin
-- GRAIN: One row per product
-- PURPOSE: True landed cost vs catalog cost vs selling price margin analysis
-- This is the payoff model — reveals where catalog margins are misleading

with source as (
    select * from {{ ref('int_product_true_margin') }}
)

select
    -- Keys
    product_id,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }}      as product_sk,

    -- Product identity
    product_name,
    category,

    -- Pricing
    list_price,
    avg_selling_price,

    -- Revenue
    total_revenue,
    total_units_sold,

    -- Cost breakdown (the new insight layer)
    catalog_cost,
    material_cost_per_unit,
    avg_labor_cost_per_unit,
    logistics_cost_per_unit,
    true_landed_cost,
    has_true_cost_data,

    -- Cost variance
    cost_variance_pct,

    -- Margin comparison
    catalog_margin_rate,
    true_margin_rate,
    margin_gap,
    margin_health,

    -- Flags
    margin_risk_flag,
    case when cost_variance_pct > 0.10 then 1 else 0 end        as has_significant_cost_overrun,
    case when margin_gap > 0.05 then 1 else 0 end               as catalog_overstates_margin,

    {{ current_timestamp_fn() }}                                 as dbt_updated_at

from source
