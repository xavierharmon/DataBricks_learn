-- models/intermediate/int_inventory_health.sql
-- -----------------------------------------------------------------------
-- INTERMEDIATE MODEL: int_inventory_health
-- DOMAIN: Inventory
-- GRAIN: One row per product (current inventory state)
--
-- BUSINESS LOGIC DEFINED HERE:
--   - current_stock_on_hand: net of all inventory movements
--   - days_of_stock: how many days until stockout at current sales rate
--   - reorder_alert: whether stock has fallen below reorder threshold
--   - stock_health_status: healthy / low / critical / overstock / stockout
-- -----------------------------------------------------------------------

with

products as (
    select
        product_id,
        product_name,
        category,
        status,
        stock_quantity                                          as catalog_stock_quantity,
        cost
    from {{ ref('stg_products') }}
),

-- Calculate net stock from all movements
inventory_movements as (
    select * from {{ ref('stg_inventory_movements') }}
),

-- Running stock balance per product
stock_balance as (
    select
        product_id,
        sum(quantity_change)                                    as net_movement_total,

        -- Break down by movement type
        sum(case when movement_type = 'production_receipt'
            then quantity_change else 0 end)                   as units_received_from_production,
        sum(case when movement_type = 'sale'
            then abs(quantity_change) else 0 end)              as units_sold,
        sum(case when movement_type = 'return'
            then quantity_change else 0 end)                   as units_returned,
        sum(case when movement_type = 'damaged_write_off'
            then abs(quantity_change) else 0 end)              as units_written_off,
        sum(case when movement_type = 'adjustment'
            then quantity_change else 0 end)                   as units_adjusted,

        -- Most recent movement date
        max(movement_date)                                      as last_movement_date,
        min(movement_date)                                      as first_movement_date,
        count(movement_id)                                      as total_movement_events

    from inventory_movements
    group by product_id
),

-- Recent sales velocity (last 30 days) for days-of-stock calc
recent_sales as (
    select
        product_id,
        sum(abs(quantity_change))                               as units_sold_last_30_days,
        sum(abs(quantity_change)) / 30.0                        as avg_daily_sales_rate
    from inventory_movements
    where movement_type = 'sale'
      and movement_date >= {{ dateadd_fn('day', -30, current_date_fn()) }}
    group by product_id
),

-- Pending production receipts (in_progress runs = incoming stock)
pending_production as (
    select
        product_id,
        sum(planned_quantity)                                   as pending_units_in_production
    from {{ ref('stg_production_runs') }}
    where status = 'in_progress'
    group by product_id
),

combined as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.status                                                as product_status,
        p.cost,

        -- Stock on hand: use catalog quantity as base, adjusted by movements
        -- In a real system movements would be the sole source of truth
        -- Here we blend both for robustness with sample data
        coalesce(sb.net_movement_total, 0)
            + p.catalog_stock_quantity                          as current_stock_on_hand,

        -- Movement breakdown
        coalesce(sb.units_received_from_production, 0)         as units_received_from_production,
        coalesce(sb.units_sold, 0)                             as units_sold_all_time,
        coalesce(sb.units_returned, 0)                         as units_returned,
        coalesce(sb.units_written_off, 0)                      as units_written_off,
        coalesce(sb.units_adjusted, 0)                         as net_adjustments,
        coalesce(sb.total_movement_events, 0)                  as total_movement_events,
        sb.last_movement_date,
        sb.first_movement_date,

        -- Sales velocity
        coalesce(rs.units_sold_last_30_days, 0)                as units_sold_last_30_days,
        coalesce(rs.avg_daily_sales_rate, 0)                   as avg_daily_sales_rate,

        -- Pending supply
        coalesce(pp.pending_units_in_production, 0)            as pending_units_in_production,

        -- Inventory value
        round(
            (coalesce(sb.net_movement_total, 0) + p.catalog_stock_quantity)
            * p.cost,
            2
        )                                                       as inventory_value_at_cost

    from products p
    left join stock_balance sb      on p.product_id = sb.product_id
    left join recent_sales rs       on p.product_id = rs.product_id
    left join pending_production pp on p.product_id = pp.product_id
),

-- Days of stock and health classification
with_health as (
    select
        *,

        -- ---------------------------------------------------------------
        -- BUSINESS RULE: Days of stock remaining
        -- Formula: current_stock_on_hand / avg_daily_sales_rate
        -- NULL when no recent sales (can't estimate)
        -- ---------------------------------------------------------------
        case
            when avg_daily_sales_rate > 0
            then round(
                current_stock_on_hand / avg_daily_sales_rate,
                1)
            else null
        end                                                     as days_of_stock_remaining,

        -- ---------------------------------------------------------------
        -- BUSINESS RULE: Reorder alert
        -- Trigger when days_of_stock < 30 (approximately 1 month of cover)
        -- or when current_stock_on_hand = 0
        -- ---------------------------------------------------------------
        case
            when current_stock_on_hand <= 0 then 1
            when avg_daily_sales_rate > 0
             and (current_stock_on_hand / avg_daily_sales_rate) < 30 then 1
            else 0
        end                                                     as reorder_alert,

        -- ---------------------------------------------------------------
        -- BUSINESS RULE: Stock health status
        --   stockout:   0 units on hand
        --   critical:   < 7 days of stock
        --   low:        7-30 days of stock
        --   healthy:    30-120 days of stock
        --   overstock:  > 120 days of stock (capital tied up unnecessarily)
        --   no_velocity: product has stock but no recent sales to compare
        -- ---------------------------------------------------------------
        case
            when current_stock_on_hand <= 0
                then 'stockout'
            when avg_daily_sales_rate > 0
             and (current_stock_on_hand / avg_daily_sales_rate) < 7
                then 'critical'
            when avg_daily_sales_rate > 0
             and (current_stock_on_hand / avg_daily_sales_rate) < 30
                then 'low'
            when avg_daily_sales_rate > 0
             and (current_stock_on_hand / avg_daily_sales_rate) > 120
                then 'overstock'
            when avg_daily_sales_rate > 0
                then 'healthy'
            else 'no_velocity'
        end                                                     as stock_health_status

    from combined
)

select * from with_health
