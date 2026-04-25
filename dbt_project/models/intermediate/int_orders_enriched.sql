-- models/intermediate/int_orders_enriched.sql
-- -----------------------------------------------------------------------
-- INTERMEDIATE MODEL: int_orders_enriched
-- -----------------------------------------------------------------------
-- PURPOSE: Enrich orders with their line items and calculated financials.
--
-- This is the ORDERS DOMAIN intermediate model.
-- We're combining stg_orders + stg_order_items to get a complete picture
-- of each order — what was bought, how many items, total revenue breakdown.
--
-- GRAIN: one row = one order (same grain as stg_orders)
--
-- THIS is where business logic starts:
--   - What counts as "revenue"? (not cancelled/refunded orders)
--   - How do we categorize order size?
--   - What is "net revenue" vs "gross revenue"?
-- -----------------------------------------------------------------------

with

orders as (
    -- Notice: we use ref() to reference OUR OWN models
    -- ref() is for models you own, source() is for external raw tables
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

-- Step 1: Aggregate order_items up to the order level
-- We're computing per-order metrics from the line-item detail
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

-- Step 2: Join orders to their item summary
enriched as (
    select
        -- All order fields
        o.order_id,
        o.customer_id,
        o.order_date,
        o.updated_at,
        o.status,

        -- Original financial fields from the order header
        o.total_amount,
        o.shipping_amount,
        o.discount_amount                                   as header_discount_amount,

        -- Item-level metrics joined in
        ois.item_count,
        ois.total_units,
        ois.items_subtotal,
        ois.items_discount_total,
        ois.distinct_product_count,

        -- ---------------------------------------------------------------
        -- BUSINESS LOGIC: Financial calculations
        -- These are DECISIONS your team makes about how to define revenue.
        -- Document them here so future engineers understand the WHY.
        -- ---------------------------------------------------------------

        -- Net revenue = what the customer actually paid (ex. shipping)
        o.total_amount - o.shipping_amount                  as net_revenue,

        -- Gross merchandise value = pre-discount item value
        ois.items_subtotal + ois.items_discount_total       as gross_merchandise_value,

        -- ---------------------------------------------------------------
        -- BUSINESS LOGIC: Order classification
        -- "Is this order revenue-generating?" — critical for reporting.
        -- Cancelled and refunded orders are NOT counted as revenue.
        -- ---------------------------------------------------------------
        case
            when o.status in ('delivered', 'shipped', 'processing') then true
            else false
        end                                                 as is_revenue_order,

        -- ---------------------------------------------------------------
        -- BUSINESS LOGIC: Order size segmentation
        -- Used in downstream reporting for "basket size" analysis.
        -- These thresholds should be agreed with the business!
        -- ---------------------------------------------------------------
        case
            when o.total_amount >= 500  then 'large'
            when o.total_amount >= 100  then 'medium'
            when o.total_amount >= 0    then 'small'
        end                                                 as order_size_segment,

        -- Date parts — useful for BI tools and time-series analysis
        date_trunc('month', o.order_date)                   as order_month,
        date_trunc('week', o.order_date)                    as order_week,
        year(o.order_date)                                  as order_year,
        dayofweek(o.order_date)                             as order_day_of_week

    from orders o
    left join order_item_summary ois
        on o.order_id = ois.order_id
        -- LEFT JOIN because we want all orders, even if order_items is missing
        -- (data quality issues can cause orphaned orders)
)

select * from enriched

-- -----------------------------------------------------------------------
-- 💡 LEARNING NOTE — ref() and the DAG:
-- When you write {{ ref('stg_orders') }}, dbt:
--   1. Replaces it with the correct schema.table for your environment
--   2. Records a DEPENDENCY — stg_orders must run before this model
--   3. Builds a DAG (Directed Acyclic Graph) of your entire project
--
-- Run `dbt docs generate && dbt docs serve` to see the DAG visually.
-- It shows every model and how they connect — incredibly powerful.
-- -----------------------------------------------------------------------
