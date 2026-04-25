"""
pipeline/domains/orders_domain.py
-----------------------------------------------------------------------
ORDERS DOMAIN

This is where the Orders domain declares its:
  - Entities (what it tracks)
  - Metrics (what it measures)

This Python file is the CANONICAL DEFINITION of the orders domain.
If a new analyst wants to know "what does 'revenue' mean in our system?"
they come here first.
-----------------------------------------------------------------------
"""

from .base_domain import BaseDomain, MetricDefinition, DomainEntity


class OrdersDomain(BaseDomain):
    """
    The Orders Domain owns everything related to purchase transactions.

    Source tables:
      - bronze_ecommerce.orders
      - bronze_ecommerce.order_items

    Mart models produced:
      - fct_orders (the primary fact table)
    """

    def get_domain_name(self) -> str:
        return "orders"

    def get_description(self) -> str:
        return (
            "The Orders domain owns all data related to customer purchase transactions. "
            "It covers the full order lifecycle from placement through delivery, "
            "refunds, and cancellations. Revenue metrics in this domain EXCLUDE "
            "cancelled and refunded orders unless otherwise specified."
        )

    def register_entities(self) -> None:
        """Define the core entities this domain tracks."""

        self.add_entity(DomainEntity(
            name="Order",
            plural="Orders",
            primary_key="order_id",
            mart_model="fct_orders",
            description=(
                "A single purchase transaction. One order can contain multiple "
                "line items (order_items). The order is the atomic unit of revenue."
            )
        ))

        self.add_entity(DomainEntity(
            name="OrderItem",
            plural="OrderItems",
            primary_key="order_item_id",
            mart_model="fct_orders",  # Embedded in fct_orders
            description=(
                "A single product line within an order. "
                "One order has one or more order items."
            )
        ))

    def register_metrics(self) -> None:
        """
        Define all metrics owned by the Orders domain.

        ⚠️  IMPORTANT: Metric definitions are CONTRACTS.
        Changing the definition of 'total_revenue' affects every dashboard
        that uses it. Always communicate changes to stakeholders first.
        """

        # ----------------------------------------------------------
        # VOLUME METRICS
        # ----------------------------------------------------------

        self.add_metric(MetricDefinition(
            name="total_orders",
            label="Total Orders",
            description=(
                "Count of ALL orders regardless of status. "
                "Use 'revenue_orders' if you want to exclude cancellations."
            ),
            sql_expression="count(order_id)",
            aggregation_type="count",
            grain="order",
            unit="orders",
            owner="growth_team"
        ))

        self.add_metric(MetricDefinition(
            name="revenue_orders",
            label="Revenue Orders",
            description=(
                "Count of orders that generated revenue. "
                "EXCLUDES cancelled and refunded orders. "
                "This is the standard order count for revenue reporting."
            ),
            sql_expression="count(case when is_revenue_order then order_id end)",
            aggregation_type="count",
            grain="order",
            filters=["is_revenue_order = true"],
            unit="orders",
            owner="finance_team"
        ))

        # ----------------------------------------------------------
        # REVENUE METRICS
        # ----------------------------------------------------------

        self.add_metric(MetricDefinition(
            name="gross_revenue",
            label="Gross Revenue",
            description=(
                "Total order value including shipping, before any discounts. "
                "Only includes revenue orders (excludes cancelled/refunded). "
                "NOT the same as net revenue — use net_revenue for P&L reporting."
            ),
            sql_expression="sum(case when is_revenue_order then total_amount else 0 end)",
            aggregation_type="sum",
            grain="order",
            filters=["is_revenue_order = true"],
            unit="$",
            owner="finance_team"
        ))

        self.add_metric(MetricDefinition(
            name="net_revenue",
            label="Net Revenue",
            description=(
                "Revenue excluding shipping charges. "
                "Formula: total_amount - shipping_amount. "
                "This is the primary revenue metric for the business. "
                "Shipping is a pass-through cost, not product revenue."
            ),
            sql_expression="sum(case when is_revenue_order then net_revenue else 0 end)",
            aggregation_type="sum",
            grain="order",
            filters=["is_revenue_order = true"],
            unit="$",
            owner="finance_team"
        ))

        self.add_metric(MetricDefinition(
            name="average_order_value",
            label="Average Order Value (AOV)",
            description=(
                "Average net revenue per revenue order. "
                "Key e-commerce KPI for measuring transaction size. "
                "Increasing AOV = customers buying more per visit."
            ),
            sql_expression=(
                "sum(case when is_revenue_order then net_revenue else 0 end) "
                "/ nullif(count(case when is_revenue_order then order_id end), 0)"
            ),
            aggregation_type="average",
            grain="order",
            filters=["is_revenue_order = true"],
            unit="$",
            owner="growth_team"
        ))

        # ----------------------------------------------------------
        # CANCELLATION / QUALITY METRICS
        # ----------------------------------------------------------

        self.add_metric(MetricDefinition(
            name="cancellation_rate",
            label="Cancellation Rate",
            description=(
                "Percentage of orders that were cancelled. "
                "High cancellation rate may indicate checkout or product issues."
            ),
            sql_expression=(
                "count(case when status = 'cancelled' then order_id end) * 1.0 "
                "/ nullif(count(order_id), 0)"
            ),
            aggregation_type="average",
            grain="order",
            unit="%",
            owner="operations_team"
        ))
