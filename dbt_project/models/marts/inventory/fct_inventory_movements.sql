-- models/marts/inventory/fct_inventory_movements.sql
-- MART MODEL: fct_inventory_movements
-- GRAIN: One row per inventory movement event
-- PURPOSE: Movement history fact table for trend and audit analysis

with

movements as (
    select * from {{ ref('stg_inventory_movements') }}
),

products as (
    select product_id, product_name, category, cost
    from {{ ref('stg_products') }}
)

select
    -- Keys
    m.movement_id,
    {{ dbt_utils.generate_surrogate_key(['m.movement_id']) }}   as movement_sk,
    m.product_id,

    -- Date
    m.movement_date,
    {{ date_trunc_fn('month', 'm.movement_date') }}             as movement_month,
    {{ year_fn('m.movement_date') }}                            as movement_year,

    -- Movement details
    m.movement_type,
    m.quantity_change,
    abs(m.quantity_change)                                      as quantity_absolute,
    case when m.quantity_change > 0 then 'inbound'
         when m.quantity_change < 0 then 'outbound'
         else 'neutral' end                                     as movement_direction,

    -- Financial impact
    m.unit_cost,
    round(abs(m.quantity_change) * m.unit_cost, 2)             as movement_value,

    -- Reference
    m.reference_id,
    m.notes,

    -- Product context (denormalized)
    p.product_name,
    p.category,

    {{ current_timestamp_fn() }}                                as dbt_updated_at

from movements m
left join products p on m.product_id = p.product_id
