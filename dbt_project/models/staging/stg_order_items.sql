-- models/staging/stg_order_items.sql
-- -----------------------------------------------------------------------
-- STAGING MODEL: stg_order_items
-- -----------------------------------------------------------------------
-- One row per line item within an order.
-- This is the BRIDGE between orders and products.
--
-- GRAIN: one row = one product in one order
-- -----------------------------------------------------------------------

with

source as (
    select * from {{ source('ecommerce_raw', 'order_items') }}
),

renamed as (
    select
        order_item_id::string           as order_item_id,
        order_id::string                as order_id,
        product_id::string              as product_id,

        quantity::int                   as quantity,
        unit_price::decimal(18, 2)      as unit_price,
        discount_amount::decimal(18, 2) as discount_amount,

        -- Derived: calculate line total in staging since it's pure arithmetic
        -- This is acceptable in staging — it's not a business rule, it's math
        (quantity * unit_price) - discount_amount as line_total,

        _loaded_at::timestamp           as _loaded_at

    from source
)

select * from renamed

-- -----------------------------------------------------------------------
-- 💡 LEARNING NOTE — GRAIN:
-- Always define the GRAIN of every model you write.
-- Grain = "what does one row represent?"
--
-- stg_orders:      one row = one order
-- stg_customers:   one row = one customer account
-- stg_products:    one row = one product SKU
-- stg_order_items: one row = one product in one order
--
-- If you ever JOIN two models with different grains, you risk FANOUT
-- (rows multiplying unexpectedly). Always think about grain first.
-- -----------------------------------------------------------------------
