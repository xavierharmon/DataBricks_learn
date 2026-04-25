-- models/marts/products/dim_products.sql
-- -----------------------------------------------------------------------
-- MART MODEL: dim_products
-- -----------------------------------------------------------------------
-- PURPOSE: The PRODUCTS DIMENSION TABLE.
-- GRAIN: one row = one product (current state)
-- -----------------------------------------------------------------------

with

product_revenue as (
    select * from {{ ref('int_product_revenue') }}
),

final as (
    select
        -- Keys
        product_id,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
        sku,

        -- Product attributes
        product_name,
        category,
        subcategory,
        status,

        -- Pricing
        list_price,
        cost,
        round(list_price - cost, 2)         as list_margin_dollars,
        round((list_price - cost) / nullif(list_price, 0), 4)
                                            as list_margin_rate,

        -- Inventory
        stock_quantity,
        stock_quantity > 0                  as is_in_stock,

        -- Sales performance
        total_units_sold,
        total_revenue,
        avg_selling_price,
        total_discounts_given,
        gross_margin_rate,
        performance_tier,
        first_sold_date,
        last_sold_date,
        orders_last_30_days,
        units_last_30_days,

        -- Is the product currently active and sellable?
        status = 'active' and stock_quantity > 0
                                            as is_available,

        current_timestamp()                 as dbt_updated_at

    from product_revenue
)

select * from final
