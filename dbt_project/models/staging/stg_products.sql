-- models/staging/stg_products.sql
-- PLATFORM SUPPORT: Databricks + Azure Fabric

with

source as (
    select * from {{ source('ecommerce_raw', 'products') }}
),

renamed as (
    select
        cast(product_id as varchar(50))         as product_id,
        cast(product_name as varchar(255))      as product_name,
        lower(trim(cast(category as varchar(100))))     as category,
        lower(trim(cast(subcategory as varchar(100))))  as subcategory,
        cast(sku as varchar(50))                as sku,
        cast(price as decimal(18,2))            as price,
        cast(cost as decimal(18,2))             as cost,
        cast(stock_quantity as int)             as stock_quantity,
        lower(trim(cast(status as varchar(50)))) as status,
        cast(created_at as timestamp)           as created_at,
        cast(updated_at as timestamp)           as updated_at,
        cast(_loaded_at as timestamp)           as _loaded_at
    from source
)

select * from renamed
