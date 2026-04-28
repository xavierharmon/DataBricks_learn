-- dbt_project/tests/assert_revenue_customers_have_orders.sql
-- -----------------------------------------------------------------------
-- CUSTOM TEST: Customers marked as revenue customers must have orders
-- -----------------------------------------------------------------------
-- WHAT THIS TESTS:
--   Any customer in dim_customers with revenue_orders > 0 must have
--   at least one matching row in fct_orders with is_revenue_order = 1.
--
--   If this test fails, it means:
--     a) A customer's order history was lost in the pipeline, OR
--     b) The revenue_orders count in dim_customers is miscalculated
--
-- RETURNS ROWS ON FAILURE — zero rows means the test passes.
-- -----------------------------------------------------------------------

select
    c.customer_id,
    c.email,
    c.revenue_orders          as dim_says_revenue_orders,
    count(o.order_id)         as fct_actual_revenue_orders,
    'Customer has revenue_orders > 0 in dim but no matching fct rows'
        as failure_reason

from {{ ref('dim_customers') }} c
left join {{ ref('fct_orders') }} o
    on  c.customer_id = o.customer_id
    and o.is_revenue_order = 1

where c.revenue_orders > 0

group by
    c.customer_id,
    c.email,
    c.revenue_orders

having count(o.order_id) = 0   -- Customers dim says bought, but fct has no revenue orders
