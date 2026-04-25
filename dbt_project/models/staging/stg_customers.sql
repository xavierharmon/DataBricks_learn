-- models/staging/stg_customers.sql
-- -----------------------------------------------------------------------
-- STAGING MODEL: stg_customers
-- PLATFORM SUPPORT: Databricks + Azure Fabric
-- Uses platform_adapter macros for split_part (not available in T-SQL)
-- -----------------------------------------------------------------------

with

source as (
    select * from {{ source('ecommerce_raw', 'customers') }}
),

renamed as (
    select
        cast(customer_id as varchar(50))                    as customer_id,

        -- Email — always lowercase, cross-platform trim
        lower(trim(cast(email as varchar(255))))            as email,

        -- Name splitting — uses platform_adapter macro
        -- Databricks: split_part()   Fabric: charindex + substring
        {{ split_first_name('full_name') }}                 as first_name,
        {{ split_last_name('full_name') }}                  as last_name,
        cast(full_name as varchar(255))                     as full_name,

        -- Location
        cast(city as varchar(100))                          as city,
        cast(state as varchar(50))                          as state,
        cast(country as varchar(50))                        as country,
        cast(postal_code as varchar(20))                    as postal_code,

        -- Account metadata
        cast(created_at as timestamp)                       as created_at,
        cast(updated_at as timestamp)                       as updated_at,

        -- Acquisition channel normalization
        case
            when lower(cast(acquisition_channel as varchar(100))) like '%organic%'  then 'organic_search'
            when lower(cast(acquisition_channel as varchar(100))) like '%paid%'     then 'paid_search'
            when lower(cast(acquisition_channel as varchar(100))) like '%social%'   then 'social_media'
            when lower(cast(acquisition_channel as varchar(100))) like '%email%'    then 'email'
            when lower(cast(acquisition_channel as varchar(100))) like '%referral%' then 'referral'
            else 'unknown'
        end                                                 as acquisition_channel,

        -- Boolean — uses bool_to_bit macro (Fabric uses BIT not BOOLEAN)
        {{ bool_to_bit('cast(is_active as boolean)') }}     as is_active,

        cast(_loaded_at as timestamp)                       as _loaded_at

    from source
)

select * from renamed
