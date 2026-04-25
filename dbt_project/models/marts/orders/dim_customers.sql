-- models/marts/customers/dim_customers.sql
-- -----------------------------------------------------------------------
-- MART MODEL: dim_customers
-- -----------------------------------------------------------------------
-- PURPOSE: The CUSTOMERS DIMENSION TABLE.
--
-- WHAT IS A DIMENSION TABLE?
--   A dimension table describes the "things" in your business — WHO, WHAT, WHERE.
--   It answers descriptive questions: Who is this customer? What is this product?
--   Dimension tables are joined to fact tables in BI tools.
--
-- GRAIN: one row = one customer (current state)
--
-- This model is the SOURCE OF TRUTH for customer attributes in your BI layer.
-- Power BI / Azure Fabric will join fct_orders → dim_customers on customer_id.
-- -----------------------------------------------------------------------

with

customer_orders as (
    select * from {{ ref('int_customer_orders') }}
),

final as (
    select
        -- ---------------------------------------------------------------
        -- KEYS
        -- ---------------------------------------------------------------
        customer_id,

        -- Surrogate key (used in star schema joins)
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }}
            as customer_sk,

        -- ---------------------------------------------------------------
        -- CUSTOMER IDENTITY
        -- ---------------------------------------------------------------
        email,
        first_name,
        last_name,
        first_name || ' ' || last_name  as full_name,

        -- ---------------------------------------------------------------
        -- LOCATION
        -- ---------------------------------------------------------------
        city,
        state,
        country,

        -- ---------------------------------------------------------------
        -- ACQUISITION
        -- ---------------------------------------------------------------
        acquisition_channel,
        account_created_at,
        is_active,

        -- ---------------------------------------------------------------
        -- BEHAVIORAL ATTRIBUTES
        -- These are calculated from orders but "live" on the dimension
        -- because they describe WHO the customer IS, not what happened
        -- ---------------------------------------------------------------
        customer_segment,
        is_repeat_customer,
        total_orders,
        revenue_orders,
        lifetime_value,
        avg_order_value,
        first_order_date,
        most_recent_order_date,
        days_since_last_order,
        customer_lifespan_days,

        -- ---------------------------------------------------------------
        -- DERIVED CLASSIFICATIONS
        -- ---------------------------------------------------------------

        -- Has this customer ever placed an order?
        total_orders > 0                as has_purchased,

        -- High value customer flag (top LTV threshold)
        lifetime_value >= 1000          as is_high_value,

        -- Tenure bucket
        case
            when account_created_at >= dateadd(day, -30, current_date())
                then 'new'              -- Account < 30 days old
            when account_created_at >= dateadd(day, -365, current_date())
                then 'established'      -- 30 days to 1 year
            else 'long_term'            -- Over a year
        end                             as account_tenure,

        -- ---------------------------------------------------------------
        -- METADATA
        -- ---------------------------------------------------------------
        current_timestamp()             as dbt_updated_at

    from customer_orders
)

select * from final
