-- models/intermediate/int_product_revenue.sql
-- -----------------------------------------------------------------------
-- INTERMEDIATE MODEL: int_product_revenue
-- -----------------------------------------------------------------------
-- PURPOSE: Enrich each product with its sales performance metrics.
--
-- This is the PRODUCTS DOMAIN intermediate model.
-- GRAIN: one row = one product
-- -----------------------------------------------------------------------

with

products as (
    select * from {{ ref('stg_products') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    -- We only want to count sales from revenue-generating orders
    select order_id, is_revenue_order, order_date
    from {{ ref('int_orders_enriched') }}
),

-- Join order items to their parent order to filter for revenue orders only
revenue_order_items as (
    select
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.line_total,
        oi.discount_amount,
        o.order_date

    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
        and o.is_revenue_order = true   -- Only count items from valid orders
),

-- Aggregate sales per product
product_sales as (
    select
        product_id,

        count(*)                                    as total_line_items,
        sum(quantity)                               as total_units_sold,
        sum(line_total)                             as total_revenue,
        avg(unit_price)                             as avg_selling_price,
        sum(discount_amount)                        as total_discounts_given,

        -- First and last sold dates
        min(order_date)                             as first_sold_date,
        max(order_date)                             as last_sold_date,

        -- Sales in last 30/90 days (useful for "trending products")
        count(case when order_date >= dateadd(day, -30, current_date()) then 1 end)
                                                    as orders_last_30_days,
        sum(case when order_date >= dateadd(day, -30, current_date()) then quantity else 0 end)
                                                    as units_last_30_days

    from revenue_order_items
    group by product_id
),

joined as (
    select
        -- Product catalog fields
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.sku,
        p.price                                     as list_price,
        p.cost,
        p.stock_quantity,
        p.status,

        -- Sales performance
        coalesce(ps.total_units_sold, 0)            as total_units_sold,
        coalesce(ps.total_revenue, 0)               as total_revenue,
        ps.avg_selling_price,
        coalesce(ps.total_discounts_given, 0)       as total_discounts_given,
        ps.first_sold_date,
        ps.last_sold_date,
        coalesce(ps.orders_last_30_days, 0)         as orders_last_30_days,
        coalesce(ps.units_last_30_days, 0)          as units_last_30_days,

        -- ---------------------------------------------------------------
        -- BUSINESS LOGIC: Margin calculation
        -- Gross margin = (revenue - cost of goods) / revenue
        -- Only calculable for products that have been sold
        -- ---------------------------------------------------------------
        case
            when coalesce(ps.total_revenue, 0) > 0
            then round(
                (coalesce(ps.total_revenue, 0) - (coalesce(ps.total_units_sold, 0) * p.cost))
                / coalesce(ps.total_revenue, 0),
                4
            )
            else null
        end                                         as gross_margin_rate,

        -- ---------------------------------------------------------------
        -- BUSINESS LOGIC: Product performance tier
        -- ---------------------------------------------------------------
        case
            when coalesce(ps.total_revenue, 0) = 0     then 'no_sales'
            when ps.units_last_30_days >= 50            then 'top_seller'
            when ps.units_last_30_days >= 10            then 'steady_seller'
            when ps.units_last_30_days > 0              then 'slow_mover'
            else 'stale'
        end                                         as performance_tier

    from products p
    left join product_sales ps
        on p.product_id = ps.product_id
)

select * from joined
