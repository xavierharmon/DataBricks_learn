-- models/staging/stg_production_runs.sql
-- STAGING MODEL: stg_production_runs
-- GRAIN: One row per production batch at a manufacturer

with source as (
    select * from {{ source('ecommerce_raw', 'production_runs') }}
),

renamed as (
    select
        cast(production_run_id as varchar(50))                  as production_run_id,
        cast(manufacturer_id as varchar(50))                    as manufacturer_id,
        cast(product_id as varchar(50))                         as product_id,
        cast(planned_quantity as int)                           as planned_quantity,
        cast(actual_quantity as int)                            as actual_quantity,
        cast(planned_start_date as date)                        as planned_start_date,
        cast(actual_end_date as date)                           as actual_end_date,
        lower(trim(cast(status as varchar(50))))                as status,
        cast(cost_per_unit as decimal(10,2))                    as cost_per_unit,
        cast(defects_found as int)                              as defects_found,
        {{ bool_to_bit('cast(quality_passed as boolean)') }}    as quality_passed,

        -- Derived in staging: yield rate (actual / planned)
        case
            when cast(planned_quantity as int) > 0
             and cast(actual_quantity as int) is not null
            then round(
                cast(actual_quantity as decimal(10,4))
                / cast(planned_quantity as decimal(10,4)),
                4)
            else null
        end                                                     as actual_yield_rate,

        -- Total production cost for this run
        case
            when cast(actual_quantity as int) is not null
            then round(
                cast(actual_quantity as decimal(14,2))
                * cast(cost_per_unit as decimal(10,2)),
                2)
            else null
        end                                                     as total_production_cost,

        cast(_loaded_at as timestamp)                           as _loaded_at
    from source
)

select * from renamed
