-- dbt_project/tests/assert_no_orphaned_order_items.sql
-- -----------------------------------------------------------------------
-- CUSTOM TEST: Every order item must belong to a known order
-- -----------------------------------------------------------------------
-- WHAT THIS TESTS:
--   Every row in stg_order_items must have a matching order_id in
--   stg_orders. Orphaned line items indicate a pipeline failure where
--   items were loaded but their parent order was not.
--
-- WHY THIS MATTERS:
--   Orphaned items inflate product revenue metrics (int_product_revenue
--   joins to order_items directly). Even one orphaned high-value item
--   can meaningfully skew performance_tier classification.
--
-- RETURNS ROWS ON FAILURE.
-- -----------------------------------------------------------------------

select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.line_total,
    'Order item references an order_id that does not exist in stg_orders'
        as failure_reason

from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_orders') }} o
    on oi.order_id = o.order_id

where o.order_id is null
