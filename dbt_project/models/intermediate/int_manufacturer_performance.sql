-- models/intermediate/int_manufacturer_performance.sql
-- -----------------------------------------------------------------------
-- INTERMEDIATE MODEL: int_manufacturer_performance
-- DOMAIN: Manufacturers
-- GRAIN: One row per manufacturer
--
-- BUSINESS LOGIC DEFINED HERE:
--   - actual_yield_rate: actual vs planned production output
--   - defect_rate_actual: defects found / units produced
--   - true_cost_per_unit: labor + material inputs
--   - efficiency_score: composite manufacturing performance metric
-- -----------------------------------------------------------------------

with

manufacturers as (
    select * from {{ ref('stg_manufacturers') }}
),

production_runs as (
    select * from {{ ref('stg_production_runs') }}
),

run_inputs as (
    select * from {{ ref('stg_production_run_inputs') }}
),

-- Aggregate production run metrics per manufacturer
run_summary as (
    select
        manufacturer_id,

        count(production_run_id)                                as total_runs,
        count(case when status = 'completed' then 1 end)        as completed_runs,
        count(case when status = 'cancelled' then 1 end)        as cancelled_runs,
        count(case when status = 'in_progress' then 1 end)      as in_progress_runs,

        sum(planned_quantity)                                   as total_planned_units,
        sum(coalesce(actual_quantity, 0))                       as total_actual_units,
        sum(coalesce(defects_found, 0))                         as total_defects,

        -- Yield: what fraction of planned units were actually produced
        case
            when sum(planned_quantity) > 0
            then round(
                cast(sum(coalesce(actual_quantity, 0)) as decimal(14,4))
                / cast(sum(planned_quantity) as decimal(14,4)),
                4)
            else null
        end                                                     as overall_yield_rate,

        -- Defect rate: defects per unit produced
        case
            when sum(coalesce(actual_quantity, 0)) > 0
            then round(
                cast(sum(coalesce(defects_found, 0)) as decimal(14,6))
                / cast(sum(coalesce(actual_quantity, 0)) as decimal(14,6)),
                6)
            else null
        end                                                     as actual_defect_rate,

        -- Average production run duration in days
        avg(
            case when actual_end_date is not null
                  and planned_start_date is not null
            then {{ datediff_fn('planned_start_date', 'actual_end_date') }}
            end
        )                                                       as avg_run_duration_days,

        -- Total labor cost
        sum(coalesce(total_production_cost, 0))                 as total_labor_cost,

        -- Average cost per unit produced
        case
            when sum(coalesce(actual_quantity, 0)) > 0
            then round(
                sum(coalesce(total_production_cost, 0))
                / sum(coalesce(actual_quantity, 0)),
                4)
            else null
        end                                                     as avg_labor_cost_per_unit,

        min(planned_start_date)                                 as first_run_date,
        max(coalesce(actual_end_date, planned_start_date))      as most_recent_run_date,

        count(distinct product_id)                              as distinct_products_made

    from production_runs
    group by manufacturer_id
),

-- Aggregate material input costs per manufacturer
material_costs as (
    select
        pr.manufacturer_id,
        sum(ri.total_material_cost)                             as total_material_cost,
        count(distinct ri.material_id)                          as distinct_materials_used
    from run_inputs ri
    inner join production_runs pr
        on ri.production_run_id = pr.production_run_id
    group by pr.manufacturer_id
),

combined as (
    select
        m.manufacturer_id,
        m.manufacturer_name,
        m.country,
        m.state,
        m.avg_yield_rate                                        as quoted_yield_rate,
        m.production_capacity_units,
        m.lead_time_days,
        m.quality_certification,
        m.defect_rate                                           as quoted_defect_rate,
        m.cost_per_unit_labor,
        m.onboarded_at,

        -- Run volume
        coalesce(rs.total_runs, 0)                              as total_runs,
        coalesce(rs.completed_runs, 0)                          as completed_runs,
        coalesce(rs.cancelled_runs, 0)                          as cancelled_runs,
        coalesce(rs.in_progress_runs, 0)                        as in_progress_runs,

        -- Output metrics
        coalesce(rs.total_planned_units, 0)                     as total_planned_units,
        coalesce(rs.total_actual_units, 0)                      as total_actual_units,
        coalesce(rs.total_defects, 0)                           as total_defects,

        -- Rates
        rs.overall_yield_rate,
        rs.actual_defect_rate,
        rs.avg_run_duration_days,
        rs.avg_labor_cost_per_unit,
        rs.distinct_products_made,
        rs.first_run_date,
        rs.most_recent_run_date,

        -- Cost breakdown
        coalesce(rs.total_labor_cost, 0)                        as total_labor_cost,
        coalesce(mc.total_material_cost, 0)                     as total_material_cost,
        coalesce(rs.total_labor_cost, 0)
            + coalesce(mc.total_material_cost, 0)               as total_manufacturing_cost,
        coalesce(mc.distinct_materials_used, 0)                 as distinct_materials_used,

        -- True cost per unit = (labor + materials) / units produced
        case
            when coalesce(rs.total_actual_units, 0) > 0
            then round(
                (coalesce(rs.total_labor_cost, 0)
                 + coalesce(mc.total_material_cost, 0))
                / rs.total_actual_units,
                4)
            else null
        end                                                     as true_cost_per_unit,

        -- Yield variance vs quoted
        case
            when rs.overall_yield_rate is not null
            then round(rs.overall_yield_rate - m.avg_yield_rate, 4)
            else null
        end                                                     as yield_variance

    from manufacturers m
    left join run_summary rs    on m.manufacturer_id = rs.manufacturer_id
    left join material_costs mc on m.manufacturer_id = mc.manufacturer_id
),

-- Efficiency scoring
scored as (
    select
        *,

        -- ---------------------------------------------------------------
        -- BUSINESS RULE: Manufacturer efficiency score (0-100)
        --   40% yield performance (actual vs quoted)
        --   30% defect performance (lower = better)
        --   20% cost performance (true cost vs quoted labor cost)
        --   10% completion rate (completed / total runs)
        -- ---------------------------------------------------------------
        round(
            -- Yield component (40 pts)
            case
                when overall_yield_rate is null then 40 * 0.5
                when overall_yield_rate >= quoted_yield_rate then 40
                when overall_yield_rate >= quoted_yield_rate - 0.05 then 40 * 0.8
                when overall_yield_rate >= quoted_yield_rate - 0.10 then 40 * 0.6
                else 40 * 0.3
            end
            +
            -- Defect component (30 pts, lower defect rate = better)
            case
                when actual_defect_rate is null then 30 * 0.5
                when actual_defect_rate <= quoted_defect_rate then 30
                when actual_defect_rate <= quoted_defect_rate * 1.5 then 30 * 0.7
                when actual_defect_rate <= quoted_defect_rate * 2.0 then 30 * 0.4
                else 30 * 0.1
            end
            +
            -- Cost component (20 pts)
            case
                when true_cost_per_unit is null then 20 * 0.5
                when true_cost_per_unit <= cost_per_unit_labor then 20
                when true_cost_per_unit <= cost_per_unit_labor * 1.1 then 20 * 0.8
                when true_cost_per_unit <= cost_per_unit_labor * 1.25 then 20 * 0.5
                else 20 * 0.2
            end
            +
            -- Completion rate component (10 pts)
            case
                when total_runs = 0 then 5
                else (cast(completed_runs as decimal) / cast(total_runs as decimal)) * 10
            end,
            1
        )                                                       as efficiency_score,

        case
            when total_runs = 0              then 'unrated'
            when overall_yield_rate >= 0.92
             and actual_defect_rate <= 0.02  then 'tier_1'
            when overall_yield_rate >= 0.85
             and actual_defect_rate <= 0.03  then 'tier_2'
            when overall_yield_rate >= 0.75  then 'tier_3'
            else 'under_review'
        end                                                     as manufacturer_tier

    from combined
)

select * from scored
