-- models/staging/stg_purchase_order_items.sql
-- STAGING MODEL: stg_purchase_order_items
-- GRAIN: One row per material line within a purchase order

with source as (
    select * from {{ source('ecommerce_raw', 'purchase_order_items') }}
),

renamed as (
    select
        cast(po_item_id as varchar(50))                         as po_item_id,
        cast(purchase_order_id as varchar(50))                  as purchase_order_id,
        cast(material_id as varchar(50))                        as material_id,
        cast(qty_ordered as int)                                as qty_ordered,
        cast(qty_received as int)                               as qty_received,
        cast(unit_price as decimal(10,4))                       as unit_price,
        cast(total_cost as decimal(14,2))                       as total_cost,

        -- Fill rate for this line item (NULL if not yet received)
        case
            when cast(qty_ordered as int) > 0
             and cast(qty_received as int) is not null
            then round(
                cast(qty_received as decimal(10,4))
                / cast(qty_ordered as decimal(10,4)),
                4)
            else null
        end                                                     as line_fill_rate,

        cast(_loaded_at as timestamp)                           as _loaded_at
    from source
)

select * from renamed
