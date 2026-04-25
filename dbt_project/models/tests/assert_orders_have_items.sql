-- dbt_project/tests/assert_orders_have_items.sql
-- -----------------------------------------------------------------------
-- CUSTOM DATA TEST: Every revenue order should have at least one item
-- -----------------------------------------------------------------------
-- HOW CUSTOM dbt TESTS WORK:
--   Write a SQL query that RETURNS ROWS when the test FAILS.
--   dbt runs this query and checks if any rows come back.
--   0 rows = test passes ✅
--   Any rows = test fails ❌ (the rows show you the bad data)
--
-- This test finds revenue orders that have no matching order_items.
-- This would indicate a data pipeline bug — orders recorded but
-- their items were lost in transit.
-- -----------------------------------------------------------------------

select
    o.order_id,
    o.order_date,
    o.status,
    o.total_amount,
    'Missing order items for revenue order' as failure_reason

from {{ ref('fct_orders') }} o
left join {{ ref('stg_order_items') }} oi
    on o.order_id = oi.order_id

where
    o.is_revenue_order = 1       -- Only check revenue orders
    and oi.order_item_id is null    -- No matching items found

-- If this query returns rows, those orders have missing item data.
-- Investigate whether the issue is in the source system or the pipeline.
