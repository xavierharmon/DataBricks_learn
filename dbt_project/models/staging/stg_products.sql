-- models/staging/stg_products.sql
-- -----------------------------------------------------------------------
-- STAGING MODEL: stg_products
-- -----------------------------------------------------------------------

with

source as (
    select * from {{ source('ecommerce_raw', 'products') }}
),

renamed as (
    select
        product_id::string              as product_id,
        product_name,
        lower(trim(category))           as category,
        lower(trim(subcategory))        as subcategory,
        sku,

        -- Price — always decimal for money
        price::decimal(18, 2)           as price,
        cost::decimal(18, 2)            as cost,

        -- Inventory
        stock_quantity::int             as stock_quantity,

        -- Product status
        lower(trim(status))             as status,   -- 'active', 'discontinued', 'draft'

        created_at::timestamp           as created_at,
        updated_at::timestamp           as updated_at,
        _loaded_at::timestamp           as _loaded_at

    from source
)

select * from renamed
