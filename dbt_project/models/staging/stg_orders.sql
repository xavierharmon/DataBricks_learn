-- models/staging/stg_orders.sql
-- -----------------------------------------------------------------------
-- STAGING MODEL: stg_orders
-- -----------------------------------------------------------------------
-- PLATFORM SUPPORT: Databricks + Azure Fabric
--
-- KEY CHANGE from original: uses cast() instead of :: syntax everywhere.
-- Databricks supports both cast() and :: (e.g. order_id::string)
-- Fabric only supports cast() — so we standardize on cast() for portability.
--
-- Uses platform_adapter macros for any function that differs by dialect.
-- -----------------------------------------------------------------------

with

source as (
    select * from {{ source('ecommerce_raw', 'orders') }}
),

renamed as (
    select
        -- Primary key
        cast(order_id as varchar(50))               as order_id,

        -- Foreign keys
        cast(customer_id as varchar(50))            as customer_id,

        -- Dates
        cast(order_date as date)                    as order_date,
        cast(updated_at as timestamp)               as updated_at,

        -- Status — lowercase for consistency across platforms
        lower(trim(cast(status as varchar(50))))    as status,

        -- Financials
        cast(total_amount as decimal(18,2))         as total_amount,
        cast(shipping_amount as decimal(18,2))      as shipping_amount,
        cast(discount_amount as decimal(18,2))      as discount_amount,

        -- Metadata
        cast(_loaded_at as timestamp)               as _loaded_at

    from source
)

select * from renamed
