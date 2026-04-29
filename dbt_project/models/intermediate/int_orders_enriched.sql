-- models/intermediate/int_orders_enriched.sql
-- -----------------------------------------------------------------------
-- INTERMEDIATE MODEL: int_orders_enriched
-- PLATFORM SUPPORT: Databricks + Azure Fabric

-- -----------------------------------------------------------------------

with

orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

order_item_summary as (
    select
        order_id,
        count(order_item_id)            as item_count,
        sum(quantity)                   as total_units,
        sum(line_total)                 as items_subtotal,
        sum(discount_amount)            as items_discount_total,
        count(distinct product_id)      as distinct_product_count

    from order_items
    group by order_id
),

enriched as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.updated_at,
        o.status,
        o.total_amount,
        o.shipping_amount,
        o.discount_amount                                   as header_discount_amount,

        ois.item_count,
        ois.total_units,
        ois.items_subtotal,
        ois.items_discount_total,
        ois.distinct_product_count,

        -- Financial calculations
        o.total_amount - o.shipping_amount                  as net_revenue,
        ois.items_subtotal + ois.items_discount_total       as gross_merchandise_value,

        -- Revenue flag
        case
            when o.status in ('delivered', 'shipped', 'processing') then 1
            else 0
        end                                                 as is_revenue_order,
        -- NOTE: Using 1/0 instead of true/false — works on both Databricks and Fabric
        -- Fabric uses BIT type; Databricks supports boolean but also accepts 1/0

        -- Order size segmentation
        case
            when o.total_amount >= 500  then 'large'
            when o.total_amount >= 100  then 'medium'
            else 'small'
        end                                                 as order_size_segment,

        -- Date parts — using platform adapter macros
        {{ date_trunc_fn('month', 'o.order_date') }}        as order_month,
        {{ date_trunc_fn('week', 'o.order_date') }}         as order_week,
        {{ year_fn('o.order_date') }}                       as order_year,
        {{ dayofweek_fn('o.order_date') }}                  as order_day_of_week

    from orders o
    left join order_item_summary ois
        on o.order_id = ois.order_id
)

select * from enriched
