-- models/marts/products/dim_products.sql
-- PLATFORM SUPPORT: Databricks + Azure Fabric

with

product_revenue as (
    select * from {{ ref('int_product_revenue') }}
),

final as (
    select
        product_id,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
        sku,
        product_name,
        category,
        subcategory,
        status,

        -- Pricing
        list_price,
        cost,
        round(list_price - cost, 2)                         as list_margin_dollars,
        round((list_price - cost) / nullif(list_price, 0), 4) as list_margin_rate,

        -- Inventory
        stock_quantity,
        case when stock_quantity > 0 then 1 else 0 end      as is_in_stock,

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

        -- Availability flag — 1/0 for cross-platform compatibility
        case
            when status = 'active' and stock_quantity > 0 then 1
            else 0
        end                                                  as is_available,

        {{ current_timestamp_fn() }}                         as dbt_updated_at

    from product_revenue
)

select * from final
