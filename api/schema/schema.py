"""
api/schema/schema.py
-----------------------------------------------------------------------
GRAPHQL SCHEMA — wires types to resolvers

This file defines the root Query class that GraphQL clients see.
Every method on the Query class becomes a queryable field in the API.

HOW STRAWBERRY BUILDS THE SCHEMA:
    1. You define a Query class with @strawberry.type
    2. Each method decorated with @strawberry.field is a GraphQL query
    3. Strawberry reads the return type hints → builds the SDL
    4. Callers can run `query { orders { orderId netRevenue } }`

THE DEPENDENCY INJECTION PATTERN:
    Resolvers need the database connector and settings.
    We use FastAPI's dependency injection (via strawberry's context)
    to pass these in at request time — not hardcoded.

    connector = info.context["connector"]

    This means resolvers are testable in isolation — just pass a
    mock connector in tests without spinning up a real database.

WHAT GETS EXPOSED:
    orders          → list of orders with filters + pagination
    orderSummary    → aggregate KPI metrics (total revenue, AOV, etc.)
    revenueBy       → revenue broken down by any valid dimension
    customers       → list of customers with filters + pagination
    customerSegments → customers grouped by segment
    products        → list of products with filters + pagination
    health          → simple health check (useful for monitoring)
-----------------------------------------------------------------------
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info

from api.schema.types import (
    Order, OrderSummary, RevenueByDimension,
    Customer, CustomerSegmentSummary,
    Product,
    OrderFilters, CustomerFilters, ProductFilters,
    PaginationInput
)
from api.resolvers.resolvers import (
    resolve_orders, resolve_order_summary, resolve_revenue_by_dimension,
    resolve_customers, resolve_customer_segments,
    resolve_products,
)


@strawberry.type
class Query:
    """
    The root GraphQL query type.
    Every field here is a top-level query the API exposes.
    """

    # ------------------------------------------------------------------
    # ORDERS
    # ------------------------------------------------------------------

    @strawberry.field(
        description="List orders with optional filtering and pagination."
    )
    def orders(
        self,
        info: Info,
        filters: Optional[OrderFilters] = None,
        pagination: Optional[PaginationInput] = None
    ) -> List[Order]:
        """
        Fetch orders from fct_orders.

        Example queries:
            # All recent orders
            { orders { orderId status netRevenue } }

            # Delivered orders over $200
            { orders(filters: {status: "delivered", minRevenue: 200}) {
                orderId netRevenue customerSegment
              }
            }

            # Paginated
            { orders(pagination: {limit: 10, offset: 20}) { orderId } }
        """
        connector = info.context["connector"]
        settings = info.context["settings"]
        return resolve_orders(connector, filters, pagination, settings)

    @strawberry.field(
        description="Aggregate order KPIs: total revenue, AOV, cancellation rate, etc."
    )
    def order_summary(self, info: Info) -> OrderSummary:
        """
        Returns dashboard-level metrics across all orders.

        Example:
            {
              orderSummary {
                totalOrders
                totalRevenue
                avgOrderValue
                cancellationRate
              }
            }
        """
        connector = info.context["connector"]
        return resolve_order_summary(connector)

    @strawberry.field(
        description=(
            "Revenue broken down by a dimension. "
            "Valid dimensions: customer_segment, acquisition_channel, "
            "order_size_segment, customer_state, customer_country, "
            "order_year, status"
        )
    )
    def revenue_by(
        self,
        info: Info,
        dimension: str
    ) -> List[RevenueByDimension]:
        """
        Flexible revenue breakdown by any valid dimension column.

        Example:
            # Revenue by customer segment
            { revenueBy(dimension: "customer_segment") {
                dimensionValue totalRevenue orderCount
              }
            }

            # Revenue by year
            { revenueBy(dimension: "order_year") {
                dimensionValue totalRevenue
              }
            }
        """
        connector = info.context["connector"]
        return resolve_revenue_by_dimension(connector, dimension)

    # ------------------------------------------------------------------
    # CUSTOMERS
    # ------------------------------------------------------------------

    @strawberry.field(
        description="List customers with optional filtering and pagination."
    )
    def customers(
        self,
        info: Info,
        filters: Optional[CustomerFilters] = None,
        pagination: Optional[PaginationInput] = None
    ) -> List[Customer]:
        """
        Fetch customers from dim_customers.

        Example:
            # High-value champion customers
            { customers(filters: {customerSegment: "champion", isHighValue: 1}) {
                customerId email lifetimeValue
              }
            }
        """
        connector = info.context["connector"]
        settings = info.context["settings"]
        return resolve_customers(connector, filters, pagination, settings)

    @strawberry.field(
        description="Customer counts and LTV metrics grouped by segment."
    )
    def customer_segments(self, info: Info) -> List[CustomerSegmentSummary]:
        """
        Segment analysis — great for a customer health dashboard.

        Example:
            {
              customerSegments {
                segment
                customerCount
                avgLifetimeValue
                repeatCustomerRate
              }
            }
        """
        connector = info.context["connector"]
        return resolve_customer_segments(connector)

    # ------------------------------------------------------------------
    # PRODUCTS
    # ------------------------------------------------------------------

    @strawberry.field(
        description="List products with optional filtering and pagination."
    )
    def products(
        self,
        info: Info,
        filters: Optional[ProductFilters] = None,
        pagination: Optional[PaginationInput] = None
    ) -> List[Product]:
        """
        Fetch products from dim_products.

        Example:
            # Top sellers in Electronics under $200
            { products(filters: {category: "electronics", performanceTier: "top_seller"}) {
                productName totalRevenue grossMarginRate
              }
            }
        """
        connector = info.context["connector"]
        settings = info.context["settings"]
        return resolve_products(connector, filters, pagination, settings)

    # ------------------------------------------------------------------
    # UTILITY
    # ------------------------------------------------------------------

    @strawberry.field(description="Health check — returns backend connection info.")
    def health(self, info: Info) -> str:
        """
        Useful for monitoring and confirming which backend is active.

        Example:
            { health }
            → "OK | DuckDB (local: data/bronze)"
        """
        connector = info.context["connector"]
        return f"OK | {connector.get_backend_name()}"


# Build the schema — Strawberry reads the Query class and generates SDL
schema = strawberry.Schema(query=Query)
