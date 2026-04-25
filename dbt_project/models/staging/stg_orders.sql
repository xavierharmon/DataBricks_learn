-- models/staging/stg_orders.sql
-- -----------------------------------------------------------------------
-- STAGING MODEL: stg_orders
-- -----------------------------------------------------------------------
-- PURPOSE: Clean and standardize the raw orders table.
--
-- THE GOLDEN RULES OF STAGING MODELS:
--   ✅ Rename columns to consistent naming conventions (snake_case)
--   ✅ Cast columns to correct data types
--   ✅ Standardize values (e.g., lowercase status)
--   ✅ Simple derived columns (e.g., extracting year from a date)
--   ❌ NO joins to other tables
--   ❌ NO aggregations
--   ❌ NO complex business logic
--
-- Think of staging as "what does this table MEAN" not "what can I DO with it"
-- -----------------------------------------------------------------------

with

-- Step 1: Pull directly from the raw source.
-- Notice we use source() not ref() here — that's how dbt knows this
-- is an external table it doesn't own.
source as (
    select * from {{ source('ecommerce_raw', 'orders') }}
),

-- Step 2: Rename and recast columns.
-- This is where you fix inconsistent naming from the source system.
-- e.g., "OrderId" → "order_id", "CUST_NO" → "customer_id"
renamed as (
    select
        -- Primary key
        order_id::string               as order_id,

        -- Foreign keys
        customer_id::string            as customer_id,

        -- Dates — always cast to DATE or TIMESTAMP explicitly
        -- Raw systems often store dates as VARCHAR or INT
        order_date::date               as order_date,
        updated_at::timestamp          as updated_at,

        -- Status — lowercase for consistency
        -- Source system may have 'SHIPPED', 'Shipped', 'shipped' — normalize it
        lower(trim(status))            as status,

        -- Financials — be explicit about precision
        total_amount::decimal(18, 2)   as total_amount,
        shipping_amount::decimal(18, 2) as shipping_amount,
        discount_amount::decimal(18, 2) as discount_amount,

        -- Metadata columns (useful for debugging data issues)
        -- _loaded_at tells you when this row landed in the warehouse
        _loaded_at::timestamp          as _loaded_at

    from source
)

-- Final select — staging models just return the cleaned CTE
select * from renamed

-- -----------------------------------------------------------------------
-- 💡 LEARNING NOTE:
-- After running `dbt run --select stg_orders`, go look at this in Databricks.
-- You'll see it as a VIEW in your dev schema (e.g., dev_yourname.stg_orders).
-- It has no data stored — it's a live query over the bronze table.
-- -----------------------------------------------------------------------
