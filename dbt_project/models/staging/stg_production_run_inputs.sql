-- models/staging/stg_production_run_inputs.sql
-- STAGING MODEL: stg_production_run_inputs
-- GRAIN: One row per raw material used in a production run

with source as (
    select * from {{ source('ecommerce_raw', 'production_run_inputs') }}
),

renamed as (
    select
        cast(run_input_id as varchar(50))                       as run_input_id,
        cast(production_run_id as varchar(50))                  as production_run_id,
        cast(material_id as varchar(50))                        as material_id,
        cast(qty_used as decimal(12,4))                         as qty_used,
        cast(unit_cost as decimal(10,4))                        as unit_cost,
        cast(total_material_cost as decimal(14,2))              as total_material_cost,
        cast(_loaded_at as timestamp)                           as _loaded_at
    from source
)

select * from renamed
