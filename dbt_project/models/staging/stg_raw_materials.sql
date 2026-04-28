-- models/staging/stg_raw_materials.sql
-- STAGING MODEL: stg_raw_materials
-- GRAIN: One row per raw material type

with source as (
    select * from {{ source('ecommerce_raw', 'raw_materials') }}
),

renamed as (
    select
        cast(material_id as varchar(50))                        as material_id,
        cast(material_name as varchar(255))                     as material_name,
        lower(trim(cast(material_category as varchar(100))))    as material_category,
        cast(unit_of_measure as varchar(50))                    as unit_of_measure,
        cast(standard_cost as decimal(10,4))                    as standard_cost,
        cast(reorder_point as int)                              as reorder_point,
        cast(current_stock as int)                              as current_stock,
        cast(lead_time_days as int)                             as lead_time_days,
        cast(created_at as timestamp)                           as created_at,
        cast(_loaded_at as timestamp)                           as _loaded_at
    from source
)

select * from renamed
