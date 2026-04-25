-- models/intermediate/int_customer_orders.sql
-- -----------------------------------------------------------------------
-- INTERMEDIATE MODEL: int_customer_orders
-- PLATFORM SUPPORT: Databricks + Azure Fabric
-- -----------------------------------------------------------------------

with

customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customer_order_history as (
    select
        customer_id,

        count(order_id)                                             as total_orders,
        count(case when is_revenue_order = 1 then order_id end)     as revenue_orders,
        count(case when status = 'cancelled' then order_id end)     as cancelled_orders,
        count(case when status = 'refunded' then order_id end)      as refunded_orders,

        sum(case when is_revenue_order = 1 then net_revenue else 0 end) as lifetime_value,
        avg(case when is_revenue_order = 1 then net_revenue end)        as avg_order_value,
        max(case when is_revenue_order = 1 then net_revenue end)        as max_order_value,

        min(order_date)                                             as first_order_date,
        max(order_date)                                             as most_recent_order_date,

        -- Days between first and most recent order
        -- Uses datediff_fn macro — argument order is FLIPPED between Databricks and Fabric
        {{ datediff_fn('min(order_date)', 'max(order_date)') }}     as customer_lifespan_days,

        -- Days since most recent order
        {{ datediff_fn('max(order_date)', current_date_fn()) }}     as days_since_last_order,

        sum(distinct_product_count)                                 as total_distinct_products_ordered

    from orders
    group by customer_id
),

joined as (
    select
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

        -- Customer segmentation
        case
            when h.revenue_orders is null or h.revenue_orders = 0
                then 'no_purchases'
            when h.days_since_last_order <= 30 and h.revenue_orders >= 3
                then 'champion'
            when h.days_since_last_order <= 90 and h.revenue_orders >= 2
                then 'loyal'
            when h.days_since_last_order <= 30
                then 'new_customer'
            when h.days_since_last_order between 91 and 180
                then 'at_risk'
            when h.days_since_last_order > 180
                then 'churned'
            else 'one_time'
        end                                                         as customer_segment,

        -- Using 1/0 for boolean — compatible with both Databricks and Fabric
        case when h.revenue_orders >= 2 then 1 else 0 end          as is_repeat_customer

    from customers c
    left join customer_order_history h
        on c.customer_id = h.customer_id
)

select * from joined
