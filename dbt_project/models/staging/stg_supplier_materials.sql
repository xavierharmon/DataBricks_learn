-- models/staging/stg_supplier_materials.sql
-- STAGING MODEL: stg_supplier_materials
-- GRAIN: One row per supplier-material combination (junction table)
-- PURPOSE: Maps which suppliers provide which materials at what price

with source as (
    select * from {{ source('ecommerce_raw', 'supplier_materials') }}
),

renamed as (
    select
        cast(supplier_material_id as varchar(50))               as supplier_material_id,
        cast(supplier_id as varchar(50))                        as supplier_id,
        cast(material_id as varchar(50))                        as material_id,
        cast(quoted_unit_price as decimal(10,4))                as quoted_unit_price,
        cast(min_order_quantity as int)                         as min_order_quantity,
        cast(lead_time_days as int)                             as lead_time_days,
        {{ bool_to_bit('cast(is_primary_supplier as boolean)') }} as is_primary_supplier,
        cast(contract_start_date as date)                       as contract_start_date,
        cast(contract_end_date as date)                         as contract_end_date,
        cast(_loaded_at as timestamp)                           as _loaded_at
    from source
)

select * from renamed
