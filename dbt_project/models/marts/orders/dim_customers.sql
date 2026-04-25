-- models/marts/customers/dim_customers.sql
-- PLATFORM SUPPORT: Databricks + Azure Fabric

with

customer_orders as (
    select * from {{ ref('int_customer_orders') }}
),

final as (
    select
        -- Keys
        customer_id,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,

        -- Identity
        email,
        first_name,
        last_name,
        concat(first_name , ' ' , last_name )  as full_name,
        -- NOTE: String concat uses + on both Databricks and Fabric (|| also works on Databricks)
        -- Using + here for Fabric compatibility

        -- Location
        city,
        state,
        country,

        -- Acquisition
        acquisition_channel,
        account_created_at,
        is_active,                      -- BIT on Fabric, boolean on Databricks — both work

        -- Behavioral attributes
        customer_segment,
        is_repeat_customer,             -- 1/0 integer
        total_orders,
        revenue_orders,
        lifetime_value,
        avg_order_value,
        first_order_date,
        most_recent_order_date,
        days_since_last_order,
        customer_lifespan_days,

        -- Derived flags — using 1/0 for cross-platform boolean compatibility
        case when total_orders > 0 then 1 else 0 end        as has_purchased,
        case when lifetime_value >= 1000 then 1 else 0 end  as is_high_value,

        -- Account tenure bucket
        case
            when account_created_at >= {{ dateadd_fn('day', -30, current_date_fn()) }}
                then 'new'
            when account_created_at >= {{ dateadd_fn('day', -365, current_date_fn()) }}
                then 'established'
            else 'long_term'
        end                                                  as account_tenure,

        {{ current_timestamp_fn() }}                         as dbt_updated_at

    from customer_orders
)

select * from final
