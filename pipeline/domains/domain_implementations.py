"""
pipeline/domains/customers_domain.py
pipeline/domains/products_domain.py
-----------------------------------------------------------------------
CUSTOMERS & PRODUCTS DOMAIN IMPLEMENTATIONS
-----------------------------------------------------------------------
"""

from .base_domain import BaseDomain, MetricDefinition, DomainEntity


# ======================================================================
# CUSTOMERS DOMAIN
# ======================================================================

class CustomersDomain(BaseDomain):
    """
    The Customers Domain owns all data about customer identity,
    behavior, and lifecycle.

    Source tables:
      - bronze_ecommerce.customers

    Mart models produced:
      - dim_customers
    """

    def get_domain_name(self) -> str:
        return "customers"

    def get_description(self) -> str:
        return (
            "The Customers domain owns all data about customer accounts and their "
            "behavioral attributes. It defines customer segmentation, lifetime value, "
            "and acquisition metrics. All customer-level aggregations (e.g., total "
            "spend per customer) are computed here using data from the Orders domain."
        )

    def register_entities(self) -> None:
        self.add_entity(DomainEntity(
            name="Customer",
            plural="Customers",
            primary_key="customer_id",
            mart_model="dim_customers",
            description=(
                "A registered user account. Note: one person may have multiple accounts "
                "(deduplication is a separate process). customer_id is our system ID."
            )
        ))

    def register_metrics(self) -> None:

        self.add_metric(MetricDefinition(
            name="total_customers",
            label="Total Customers",
            description="Count of all registered customer accounts, active or inactive.",
            sql_expression="count(customer_id)",
            aggregation_type="count",
            grain="customer",
            unit="customers",
            owner="growth_team"
        ))

        self.add_metric(MetricDefinition(
            name="active_customers",
            label="Active Customers",
            description=(
                "Customers who have placed at least one revenue order in the last 90 days. "
                "'Active' threshold of 90 days was agreed with marketing in Q2 2024."
            ),
            sql_expression="count(case when days_since_last_order <= 90 and revenue_orders > 0 then customer_id end)",
            aggregation_type="count",
            grain="customer",
            filters=["days_since_last_order <= 90", "revenue_orders > 0"],
            unit="customers",
            owner="growth_team"
        ))

        self.add_metric(MetricDefinition(
            name="repeat_customer_rate",
            label="Repeat Customer Rate",
            description=(
                "Percentage of customers who have placed 2+ revenue orders. "
                "Key retention metric — higher = better customer loyalty."
            ),
            sql_expression=(
                "count(case when is_repeat_customer then customer_id end) * 1.0 "
                "/ nullif(count(case when revenue_orders > 0 then customer_id end), 0)"
            ),
            aggregation_type="average",
            grain="customer",
            unit="%",
            owner="growth_team"
        ))

        self.add_metric(MetricDefinition(
            name="avg_customer_ltv",
            label="Average Customer Lifetime Value",
            description=(
                "Average total net revenue per customer who has made at least one purchase. "
                "LTV = lifetime value. Excludes customers with zero purchases."
            ),
            sql_expression="avg(case when lifetime_value > 0 then lifetime_value end)",
            aggregation_type="average",
            grain="customer",
            unit="$",
            owner="finance_team"
        ))

        self.add_metric(MetricDefinition(
            name="new_customers",
            label="New Customers",
            description=(
                "Customers whose FIRST revenue order fell within the reporting period. "
                "Different from 'new accounts' — account creation date is not used."
            ),
            sql_expression="count(case when first_order_date >= [period_start] then customer_id end)",
            aggregation_type="count",
            grain="customer",
            unit="customers",
            owner="growth_team"
        ))


# ======================================================================
# PRODUCTS DOMAIN
# ======================================================================

class ProductsDomain(BaseDomain):
    """
    The Products Domain owns all data about the product catalog
    and sales performance per product.

    Source tables:
      - bronze_ecommerce.products

    Mart models produced:
      - dim_products
    """

    def get_domain_name(self) -> str:
        return "products"

    def get_description(self) -> str:
        return (
            "The Products domain owns catalog data (name, category, price, cost) "
            "and sales performance metrics per product. Margin calculations use "
            "the product cost field from the source system — ensure this is kept "
            "up to date by the merchandising team."
        )

    def register_entities(self) -> None:
        self.add_entity(DomainEntity(
            name="Product",
            plural="Products",
            primary_key="product_id",
            mart_model="dim_products",
            description=(
                "A single SKU (Stock Keeping Unit) in the product catalog. "
                "Variants (size, color) are separate product_ids."
            )
        ))

    def register_metrics(self) -> None:

        self.add_metric(MetricDefinition(
            name="total_products",
            label="Total Products",
            description="Count of all products in the catalog regardless of status.",
            sql_expression="count(product_id)",
            aggregation_type="count",
            grain="product",
            unit="products"
        ))

        self.add_metric(MetricDefinition(
            name="active_products",
            label="Active Products",
            description="Products with status='active' and stock > 0.",
            sql_expression="count(case when is_available then product_id end)",
            aggregation_type="count",
            grain="product",
            filters=["is_available = true"],
            unit="products"
        ))

        self.add_metric(MetricDefinition(
            name="avg_gross_margin",
            label="Average Gross Margin",
            description=(
                "Average gross margin rate across sold products. "
                "Formula: (revenue - COGS) / revenue. "
                "Only includes products with recorded sales."
            ),
            sql_expression="avg(case when gross_margin_rate is not null then gross_margin_rate end)",
            aggregation_type="average",
            grain="product",
            unit="%"
        ))

        self.add_metric(MetricDefinition(
            name="top_seller_count",
            label="Top Seller Products",
            description="Products classified as 'top_seller' performance tier (50+ units in last 30 days).",
            sql_expression="count(case when performance_tier = 'top_seller' then product_id end)",
            aggregation_type="count",
            grain="product",
            unit="products"
        ))
