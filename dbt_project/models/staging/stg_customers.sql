-- models/staging/stg_customers.sql
-- -----------------------------------------------------------------------
-- STAGING MODEL: stg_customers
-- -----------------------------------------------------------------------
-- PURPOSE: Clean and standardize the raw customers table.
--
-- Key decisions made here:
--   - Normalize email to lowercase (critical for joins later!)
--   - Parse first/last name from a full_name field
--   - Categorize customer acquisition channel
-- -----------------------------------------------------------------------

with

source as (
    select * from {{ source('ecommerce_raw', 'customers') }}
),

renamed as (
    select
        -- Primary key
        customer_id::string                     as customer_id,

        -- Contact info
        -- Always lowercase emails — 'Bob@Gmail.com' and 'bob@gmail.com'
        -- are the same customer but would appear as two rows without this!
        lower(trim(email))                      as email,

        -- Name handling — source has a single full_name column
        -- We split it here for downstream flexibility
        trim(split_part(full_name, ' ', 1))     as first_name,
        trim(split_part(full_name, ' ', 2))     as last_name,
        full_name,

        -- Location
        city,
        state,
        country,
        postal_code,

        -- Account metadata
        created_at::timestamp                   as created_at,
        updated_at::timestamp                   as updated_at,

        -- Acquisition channel — normalize inconsistent raw values
        -- Raw system has 'ORGANIC', 'organic', 'Organic Search' etc.
        case
            when lower(acquisition_channel) like '%organic%'  then 'organic_search'
            when lower(acquisition_channel) like '%paid%'     then 'paid_search'
            when lower(acquisition_channel) like '%social%'   then 'social_media'
            when lower(acquisition_channel) like '%email%'    then 'email'
            when lower(acquisition_channel) like '%referral%' then 'referral'
            else 'unknown'
        end                                     as acquisition_channel,

        -- Is the account still active?
        is_active::boolean                      as is_active,

        _loaded_at::timestamp                   as _loaded_at

    from source
)

select * from renamed

-- -----------------------------------------------------------------------
-- 💡 LEARNING NOTE — The CASE statement above:
-- This is "light" business logic that's acceptable in staging because it's
-- purely about CLEANING the source data, not defining business rules.
-- The rule "organic = organic_search" is about fixing the source system,
-- not defining what "a good customer" is.
-- -----------------------------------------------------------------------
