-- models/intermediate/int_customer_orders.sql
-- -----------------------------------------------------------------------
-- INTERMEDIATE MODEL: int_customer_orders
-- -----------------------------------------------------------------------
-- PURPOSE: Build a complete picture of each customer's order history.
--
-- This is the CUSTOMERS DOMAIN intermediate model.
-- We're answering: "For each customer, what is their purchasing behavior?"
--
-- GRAIN: one row = one customer (collapsed from many orders)
--
-- BUSINESS CONCEPTS DEFINED HERE:
--   - "First order" — acquisition event
--   - "Most recent order" — recency (used in RFM analysis)
--   - "Lifetime value" — sum of all revenue orders
--   - "Customer segment" — derived from purchase behavior
-- -----------------------------------------------------------------------

with

customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    -- Use the enriched orders model — it already has our business logic baked in
    -- This is the power of layering: we build on top of previous work
    select * from {{ ref('int_orders_enriched') }}
),

-- Aggregate all orders per customer
customer_order_history as (
    select
        customer_id,

        -- Order counts
        count(order_id)                                             as total_orders,
        count(case when is_revenue_order then order_id end)         as revenue_orders,
        count(case when status = 'cancelled' then order_id end)     as cancelled_orders,
        count(case when status = 'refunded' then order_id end)      as refunded_orders,

        -- Revenue metrics (only from revenue-generating orders)
        sum(case when is_revenue_order then net_revenue else 0 end) as lifetime_value,
        avg(case when is_revenue_order then net_revenue end)        as avg_order_value,
        max(case when is_revenue_order then net_revenue else null end) as max_order_value,

        -- Time metrics
        min(order_date)                                             as first_order_date,
        max(order_date)                                             as most_recent_order_date,

        -- Days between first and most recent order (customer "lifespan")
        datediff(
            max(order_date),
            min(order_date)
        )                                                           as customer_lifespan_days,

        -- Days since most recent order (recency)
        datediff(current_date(), max(order_date))                   as days_since_last_order,

        -- Product variety
        sum(distinct_product_count)                                 as total_distinct_products_ordered

    from orders
    group by customer_id
),

-- Join customer profile with their order history
joined as (
    select
        -- Customer profile
        c.customer_id,
        c.email,
        c.first_name,
        c.last_name,
        c.city,
        c.state,
        c.country,
        c.acquisition_channel,
        c.created_at                                                as account_created_at,
        c.is_active,

        -- Order history (coalesce handles customers with zero orders)
        coalesce(h.total_orders, 0)                                 as total_orders,
        coalesce(h.revenue_orders, 0)                               as revenue_orders,
        coalesce(h.cancelled_orders, 0)                             as cancelled_orders,
        coalesce(h.lifetime_value, 0)                               as lifetime_value,
        h.avg_order_value,
        h.max_order_value,
        h.first_order_date,
        h.most_recent_order_date,
        h.days_since_last_order,
        h.customer_lifespan_days,
        coalesce(h.total_distinct_products_ordered, 0)              as total_distinct_products_ordered,

        -- ---------------------------------------------------------------
        -- BUSINESS LOGIC: Customer segmentation (RFM-inspired)
        -- RFM = Recency, Frequency, Monetary Value
        --
        -- This is a simplified version — real RFM uses quintile scoring.
        -- These segments are defined by the business / marketing team.
        -- ---------------------------------------------------------------
        case
            when h.revenue_orders is null or h.revenue_orders = 0
                then 'no_purchases'
            when h.days_since_last_order <= 30 and h.revenue_orders >= 3
                then 'champion'             -- Recent, frequent, high value
            when h.days_since_last_order <= 90 and h.revenue_orders >= 2
                then 'loyal'               -- Regular buyers
            when h.days_since_last_order <= 30
                then 'new_customer'        -- Just started
            when h.days_since_last_order between 91 and 180
                then 'at_risk'             -- Haven't bought recently
            when h.days_since_last_order > 180
                then 'churned'             -- Gone quiet
            else 'one_time'
        end                                                         as customer_segment,

        -- Is this a multi-purchase customer?
        h.revenue_orders >= 2                                       as is_repeat_customer

    from customers c
    left join customer_order_history h
        on c.customer_id = h.customer_id
)

select * from joined
