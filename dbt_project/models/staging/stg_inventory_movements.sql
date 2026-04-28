-- models/staging/stg_inventory_movements.sql
-- STAGING MODEL: stg_inventory_movements
-- GRAIN: One row per inventory movement event

with source as (
    select * from {{ source('ecommerce_raw', 'inventory_movements') }}
),

renamed as (
    select
        cast(movement_id as varchar(50))                        as movement_id,
        cast(product_id as varchar(50))                         as product_id,
        lower(trim(cast(movement_type as varchar(50))))         as movement_type,
        cast(quantity_change as int)                            as quantity_change,
        cast(reference_id as varchar(50))                       as reference_id,
        cast(movement_date as date)                             as movement_date,
        cast(unit_cost as decimal(10,2))                        as unit_cost,
        cast(notes as varchar(500))                             as notes,
        cast(_loaded_at as timestamp)                           as _loaded_at
    from source
)

select * from renamed
