-- models/marts/supply_chain/dim_manufacturers.sql
-- MART MODEL: dim_manufacturers
-- GRAIN: One row per manufacturer (current state)

with source as (
    select * from {{ ref('int_manufacturer_performance') }}
)

select
    -- Keys
    manufacturer_id,
    {{ dbt_utils.generate_surrogate_key(['manufacturer_id']) }} as manufacturer_sk,

    -- Identity
    manufacturer_name,
    country,
    state,
    quality_certification,
    onboarded_at,

    -- Capacity
    production_capacity_units,
    lead_time_days,

    -- Quoted benchmarks
    quoted_yield_rate,
    quoted_defect_rate,
    cost_per_unit_labor,

    -- Production history
    total_runs,
    completed_runs,
    cancelled_runs,
    in_progress_runs,
    distinct_products_made,
    first_run_date,
    most_recent_run_date,

    -- Output metrics
    total_planned_units,
    total_actual_units,
    total_defects,
    overall_yield_rate,
    actual_defect_rate,
    avg_run_duration_days,
    yield_variance,

    -- Cost metrics
    total_labor_cost,
    total_material_cost,
    total_manufacturing_cost,
    avg_labor_cost_per_unit,
    true_cost_per_unit,
    distinct_materials_used,

    -- Scoring
    efficiency_score,
    manufacturer_tier,

    -- Derived flags
    case when manufacturer_tier in ('tier_1', 'tier_2') then 1 else 0 end
                                                                as is_preferred_manufacturer,
    case when actual_defect_rate > quoted_defect_rate * 1.5 then 1 else 0 end
                                                                as has_elevated_defects,
    case when yield_variance < -0.05 then 1 else 0 end          as has_yield_shortfall,

    {{ current_timestamp_fn() }}                                as dbt_updated_at

from source
