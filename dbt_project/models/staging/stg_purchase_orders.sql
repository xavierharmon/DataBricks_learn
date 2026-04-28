-- models/staging/stg_purchase_orders.sql
-- STAGING MODEL: stg_purchase_orders
-- GRAIN: One row per purchase order sent to a supplier

with source as (
    select * from {{ source('ecommerce_raw', 'purchase_orders') }}
),

renamed as (
    select
        cast(purchase_order_id as varchar(50))                  as purchase_order_id,
        cast(supplier_id as varchar(50))                        as supplier_id,
        cast(order_date as date)                                as order_date,
        cast(expected_delivery_date as date)                    as expected_delivery_date,
        cast(actual_delivery_date as date)                      as actual_delivery_date,
        lower(trim(cast(status as varchar(50))))                as status,
        cast(currency as varchar(10))                           as currency,
        cast(notes as varchar(500))                             as notes,
        cast(_loaded_at as timestamp)                           as _loaded_at
    from source
)

select * from renamed
