-- models/marts/orders/fct_orders.sql
-- -----------------------------------------------------------------------
-- MART MODEL: fct_orders
-- PLATFORM SUPPORT: Databricks + Azure Fabric
--
-- CHANGES FROM ORIGINAL:
--   current_timestamp() → {{ current_timestamp_fn() }}
--   true/false booleans → 1/0 integers (Fabric uses BIT type)
--   :: cast syntax      → cast() syntax throughout
-- -----------------------------------------------------------------------

with

orders_enriched as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
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
        -- Keys
        o.order_id,
        o.customer_id,
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_sk,

        -- Date dimension
        o.order_date,
        o.order_month,
        o.order_week,
        o.order_year,
        o.order_day_of_week,

        -- Status & flags
        o.status,
        o.is_revenue_order,             -- 1/0 integer, works on both platforms
        o.order_size_segment,

        -- Financial measures
        o.total_amount,
        o.shipping_amount,
        o.net_revenue,
        o.gross_merchandise_value,
        o.header_discount_amount,
        o.items_discount_total,

        -- Order composition
        o.item_count,
        o.total_units,
        o.distinct_product_count,

        -- Customer context (denormalized)
        c.first_name                    as customer_first_name,
        c.last_name                     as customer_last_name,
        c.city                          as customer_city,
        c.state                         as customer_state,
        c.country                       as customer_country,
        c.acquisition_channel,
        c.customer_segment,

        -- Metadata
        o.updated_at,
        {{ current_timestamp_fn() }}    as dbt_updated_at   -- macro handles Databricks vs Fabric

    from orders_enriched o
    left join customers c on o.customer_id = c.customer_id
)

select * from final
