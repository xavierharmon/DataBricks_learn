-- models/staging/stg_manufacturers.sql
-- STAGING MODEL: stg_manufacturers
-- GRAIN: One row per manufacturer

with source as (
    select * from {{ source('ecommerce_raw', 'manufacturers') }}
),

renamed as (
    select
        cast(manufacturer_id as varchar(50))                    as manufacturer_id,
        cast(manufacturer_name as varchar(255))                 as manufacturer_name,
        cast(country as varchar(50))                            as country,
        cast(state as varchar(10))                              as state,
        cast(avg_yield_rate as decimal(6,4))                    as avg_yield_rate,
        cast(production_capacity_units as int)                  as production_capacity_units,
        cast(lead_time_days as int)                             as lead_time_days,
        cast(quality_certification as varchar(50))              as quality_certification,
        cast(defect_rate as decimal(6,4))                       as defect_rate,
        cast(cost_per_unit_labor as decimal(10,2))              as cost_per_unit_labor,
        cast(onboarded_at as timestamp)                         as onboarded_at,
        cast(_loaded_at as timestamp)                           as _loaded_at
    from source
)

select * from renamed
