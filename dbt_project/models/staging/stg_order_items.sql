-- models/staging/stg_order_items.sql
-- PLATFORM SUPPORT: Databricks + Azure Fabric
-- GRAIN: one row = one product line in one order

with

source as (
    select * from {{ source('ecommerce_raw', 'order_items') }}
),

renamed as (
    select
        cast(order_item_id as varchar(50))      as order_item_id,
        cast(order_id as varchar(50))           as order_id,
        cast(product_id as varchar(50))         as product_id,
        cast(quantity as int)                   as quantity,
        cast(unit_price as decimal(18,2))       as unit_price,
        cast(discount_amount as decimal(18,2))  as discount_amount,

        -- Derived: line total — pure arithmetic, safe in staging
        (cast(quantity as decimal(18,2)) * cast(unit_price as decimal(18,2)))
            - cast(discount_amount as decimal(18,2))    as line_total,

        cast(_loaded_at as timestamp)           as _loaded_at
    from source
)

select * from renamed
