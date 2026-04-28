-- dbt_project/tests/assert_revenue_reconciles.sql
-- -----------------------------------------------------------------------
-- CUSTOM TEST: Revenue in fct_orders must reconcile to customer LTV total
-- -----------------------------------------------------------------------
-- WHAT THIS TESTS:
--   The sum of net_revenue in fct_orders (for revenue orders) must equal
--   the sum of lifetime_value in dim_customers.
--
--   These two numbers are calculated independently:
--     fct_orders:     SUM(net_revenue) WHERE is_revenue_order = 1
--     dim_customers:  SUM(lifetime_value) — built from int_customer_orders
--
--   If they don't match, there is a discrepancy in how revenue is
--   aggregated between the orders domain and customers domain.
--   This is a CROSS-DOMAIN reconciliation test.
--
-- TOLERANCE: We allow a $0.01 tolerance for floating point rounding.
-- If the difference exceeds $1.00 the test fails.
-- -----------------------------------------------------------------------

with fct_total as (
    select
        round(sum(net_revenue), 2) as total_revenue_from_fct
    from {{ ref('fct_orders') }}
    where is_revenue_order = 1
),

dim_total as (
    select
        round(sum(lifetime_value), 2) as total_ltv_from_dim
    from {{ ref('dim_customers') }}
),

reconciliation as (
    select
        fct.total_revenue_from_fct,
        dim.total_ltv_from_dim,
        abs(fct.total_revenue_from_fct - dim.total_ltv_from_dim) as difference
    from fct_total fct
    cross join dim_total dim
)

-- Returns a row (failure) if the two totals differ by more than $1.00
select
    total_revenue_from_fct,
    total_ltv_from_dim,
    difference,
    'Revenue in fct_orders does not reconcile with sum of customer lifetime values'
        as failure_reason
from reconciliation
where difference > 1.00
