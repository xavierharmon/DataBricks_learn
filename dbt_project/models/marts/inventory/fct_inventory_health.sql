-- models/marts/inventory/fct_inventory_health.sql
-- MART MODEL: fct_inventory_health
-- GRAIN: One row per product (current inventory snapshot)
-- PURPOSE: Inventory health dashboard — stockouts, reorder alerts, overstock

with source as (
    select * from {{ ref('int_inventory_health') }}
)

select
    -- Keys
    product_id,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }}      as product_sk,

    -- Product context
    product_name,
    category,
    product_status,
    cost,

    -- Stock levels
    current_stock_on_hand,
    pending_units_in_production,
    current_stock_on_hand
        + pending_units_in_production                           as total_available_stock,
    inventory_value_at_cost,

    -- Movement history
    units_received_from_production,
    units_sold_all_time,
    units_returned,
    units_written_off,
    net_adjustments,
    total_movement_events,
    first_movement_date,
    last_movement_date,

    -- Velocity
    units_sold_last_30_days,
    avg_daily_sales_rate,

    -- Days of stock
    days_of_stock_remaining,

    -- Alerts and status
    reorder_alert,
    stock_health_status,

    -- Derived flags for dashboard filtering
    case when stock_health_status = 'stockout'  then 1 else 0 end  as is_stockout,
    case when stock_health_status = 'critical'  then 1 else 0 end  as is_critical,
    case when stock_health_status = 'overstock' then 1 else 0 end  as is_overstock,
    case when reorder_alert = 1
          and pending_units_in_production = 0   then 1 else 0 end  as needs_immediate_reorder,

    {{ current_timestamp_fn() }}                                as dbt_updated_at

from source
