-- models/marts/orders/fct_orders.sql
-- -----------------------------------------------------------------------
-- MART MODEL: fct_orders
-- -----------------------------------------------------------------------
-- PURPOSE: The primary FACT TABLE for orders analysis.
--
-- WHAT IS A FACT TABLE?
--   A fact table records EVENTS or TRANSACTIONS — things that HAPPENED.
--   Each row is a measurable event: a sale, a click, a payment.
--   Fact tables are WIDE and FLAT — BI tools love them.
--
-- GRAIN: one row = one order
--
-- This model is materialized as a TABLE (not a view) because:
--   - Power BI / Azure Fabric queries this directly
--   - It needs to be FAST
--   - We're willing to use storage to get performance
--
-- HOW IT DIFFERS FROM INTERMEDIATE MODELS:
--   Intermediate = for engineers building other models
--   Mart = for business users and BI tools
--   The mart should be self-contained and easy to understand.
-- -----------------------------------------------------------------------

with

orders_enriched as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
    -- Bring in a few key customer fields to denormalize onto the fact
    -- In a star schema, you'd join to dim_customers in the BI tool.
    -- But for Azure Fabric semantic models, a pre-joined fact is often easier.
    select
        customer_id,
        first_name,
        last_name,
        email,
        city,
        state,
        country,
        acquisition_channel,
        customer_segment

    from {{ ref('int_customer_orders') }}
),

final as (
    select
        -- ---------------------------------------------------------------
        -- KEYS
        -- ---------------------------------------------------------------
        o.order_id,
        o.customer_id,

        -- Surrogate key pattern: a hashed primary key
        -- Used in some warehouses for join performance
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }}
            as order_sk,                -- SK = surrogate key

        -- ---------------------------------------------------------------
        -- DATE DIMENSION (denormalized for BI tools)
        -- Power BI can use its own date table, but including these
        -- columns makes filtering easier without a relationship
        -- ---------------------------------------------------------------
        o.order_date,
        o.order_month,
        o.order_week,
        o.order_year,
        o.order_day_of_week,

        -- ---------------------------------------------------------------
        -- STATUS & FLAGS
        -- ---------------------------------------------------------------
        o.status,
        o.is_revenue_order,
        o.order_size_segment,

        -- ---------------------------------------------------------------
        -- FINANCIAL MEASURES
        -- These are the METRICS you'll sum/avg in your BI tool
        -- ---------------------------------------------------------------
        o.total_amount,
        o.shipping_amount,
        o.net_revenue,
        o.gross_merchandise_value,
        o.header_discount_amount,
        o.items_discount_total,

        -- ---------------------------------------------------------------
        -- ORDER COMPOSITION
        -- ---------------------------------------------------------------
        o.item_count,
        o.total_units,
        o.distinct_product_count,

        -- ---------------------------------------------------------------
        -- CUSTOMER CONTEXT (denormalized)
        -- Snapshot of customer info at time of reporting
        -- Note: this is current state, not historical state
        -- ---------------------------------------------------------------
        c.first_name                    as customer_first_name,
        c.last_name                     as customer_last_name,
        c.city                          as customer_city,
        c.state                         as customer_state,
        c.country                       as customer_country,
        c.acquisition_channel,
        c.customer_segment,

        -- ---------------------------------------------------------------
        -- METADATA
        -- ---------------------------------------------------------------
        o.updated_at,
        current_timestamp()             as dbt_updated_at   -- When dbt last ran

    from orders_enriched o
    left join customers c
        on o.customer_id = c.customer_id
)

select * from final

-- -----------------------------------------------------------------------
-- 💡 LEARNING NOTE — Star Schema vs. Wide Tables:
--
-- STAR SCHEMA (traditional data warehouse):
--   fct_orders joins to dim_customers, dim_products, dim_date
--   BI tool handles the joins at query time
--   Pro: Less data duplication. Con: More complex BI setup.
--
-- WIDE / FLAT TABLE (modern approach):
--   fct_orders contains customer fields, date fields baked in
--   BI tool just queries one table
--   Pro: Simpler for analysts. Con: More storage, some duplication.
--
-- Azure Fabric semantic models support BOTH approaches.
-- This model leans toward wide/flat for simplicity.
-- -----------------------------------------------------------------------
