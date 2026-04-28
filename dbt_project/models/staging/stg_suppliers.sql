-- models/staging/stg_suppliers.sql
-- STAGING MODEL: stg_suppliers
-- GRAIN: One row per supplier company
-- PLATFORM: Databricks + Azure Fabric (cast() syntax throughout)

with source as (
    select * from {{ source('ecommerce_raw', 'suppliers') }}
),

renamed as (
    select
        cast(supplier_id as varchar(50))                        as supplier_id,
        cast(supplier_name as varchar(255))                     as supplier_name,
        cast(country as varchar(50))                            as country,
        cast(state as varchar(10))                              as state,
        lower(trim(cast(supplier_type as varchar(50))))         as supplier_type,
        cast(avg_lead_time_days as int)                         as avg_lead_time_days,
        cast(quoted_fill_rate as decimal(6,4))                  as quoted_fill_rate,
        cast(payment_terms_days as int)                         as payment_terms_days,
        {{ bool_to_bit('cast(is_preferred as boolean)') }}      as is_preferred,
        cast(quality_rating as decimal(3,1))                    as quality_rating,
        cast(onboarded_at as timestamp)                         as onboarded_at,
        cast(_loaded_at as timestamp)                           as _loaded_at
    from source
)

select * from renamed
