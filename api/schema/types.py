"""
api/schema/types.py
-----------------------------------------------------------------------
GRAPHQL TYPE DEFINITIONS

This is where your data model becomes a GraphQL schema.

HOW STRAWBERRY WORKS:
    Strawberry uses Python dataclasses with @strawberry.type decorator
    to define GraphQL types. It reads your Python type hints and
    automatically generates the GraphQL SDL (Schema Definition Language).

    Python:                     GraphQL SDL (auto-generated):
    -------                     ------------------------------
    @strawberry.type            type Order {
    class Order:                  orderId: String!
      order_id: str               status: String!
      status: str                 netRevenue: Float
      net_revenue: float          isRevenueOrder: Int!
      is_revenue_order: int     }

    Notice: Python snake_case → GraphQL camelCase automatically.

WHY DEFINE TYPES SEPARATELY FROM RESOLVERS?
    Types = what the data looks like (the shape)
    Resolvers = how to fetch the data (the logic)

    Keeping them separate means you can change HOW you fetch data
    without changing WHAT the API returns. The contract with consumers
    stays stable even as your backend changes.

NULLABLE vs NON-NULLABLE:
    In GraphQL, fields are non-null by default in Strawberry (String!)
    Use Optional[str] to make a field nullable (String)
    Use this when a field might not always have a value in your data.
-----------------------------------------------------------------------
"""

import strawberry
from typing import Optional, List
from datetime import date


# ======================================================================
# ORDER TYPES
# ======================================================================

@strawberry.type
class Order:
    """
    Represents a single order from fct_orders.
    Maps directly to the columns in your mart table.

    GRAIN: one Order = one purchase transaction
    """
    order_id: str
    customer_id: str
    order_date: Optional[date]
    order_year: Optional[int]
    status: str
    is_revenue_order: int           # 1 = revenue, 0 = cancelled/refunded
    order_size_segment: Optional[str]
    total_amount: Optional[float]
    shipping_amount: Optional[float]
    net_revenue: Optional[float]
    item_count: Optional[int]
    total_units: Optional[int]
    distinct_product_count: Optional[int]
    customer_segment: Optional[str]
    acquisition_channel: Optional[str]
    customer_city: Optional[str]
    customer_state: Optional[str]
    customer_country: Optional[str]


@strawberry.type
class OrderSummary:
    """
    Aggregated order metrics — returned by summary queries.
    Used for dashboard-style queries that don't need row-level data.
    """
    total_orders: int
    revenue_orders: int
    total_revenue: float
    avg_order_value: float
    total_units_sold: int
    cancellation_rate: float


@strawberry.type
class RevenueByDimension:
    """
    Revenue broken down by a single dimension.
    Used for 'revenue by segment', 'revenue by channel', etc.
    """
    dimension_value: str
    order_count: int
    total_revenue: float
    avg_order_value: float


# ======================================================================
# CUSTOMER TYPES
# ======================================================================

@strawberry.type
class Customer:
    """
    Represents a single customer from dim_customers.
    GRAIN: one Customer = one registered account
    """
    customer_id: str
    email: str
    full_name: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    acquisition_channel: Optional[str]
    customer_segment: Optional[str]
    total_orders: int
    revenue_orders: int
    lifetime_value: float
    days_since_last_order: Optional[int]
    first_order_date: Optional[date]
    most_recent_order_date: Optional[date]
    is_repeat_customer: int         # 1 = has 2+ orders
    is_high_value: int              # 1 = LTV >= $1000


@strawberry.type
class CustomerSegmentSummary:
    """Customers grouped by segment — for segment analysis queries."""
    segment: str
    customer_count: int
    avg_lifetime_value: float
    repeat_customer_rate: float


# ======================================================================
# PRODUCT TYPES
# ======================================================================

@strawberry.type
class Product:
    """
    Represents a single product from dim_products.
    GRAIN: one Product = one SKU in the catalog
    """
    product_id: str
    product_name: str
    category: Optional[str]
    subcategory: Optional[str]
    sku: Optional[str]
    list_price: Optional[float]
    cost: Optional[float]
    stock_quantity: Optional[int]
    status: Optional[str]
    total_units_sold: int
    total_revenue: float
    gross_margin_rate: Optional[float]
    performance_tier: Optional[str]
    is_available: int               # 1 = active and in stock


# ======================================================================
# FILTER INPUT TYPES
# ======================================================================
# GraphQL input types define what filters callers can pass to queries.
# Using @strawberry.input keeps filters typed and self-documenting.

@strawberry.input
class OrderFilters:
    """
    Optional filters for order queries.
    All fields are optional — only supplied filters are applied.

    Example GraphQL query using filters:
        query {
          orders(filters: {status: "delivered", minRevenue: 100.0}) {
            orderId
            netRevenue
          }
        }
    """
    status: Optional[str] = None
    customer_segment: Optional[str] = None
    acquisition_channel: Optional[str] = None
    order_size_segment: Optional[str] = None
    min_revenue: Optional[float] = None
    max_revenue: Optional[float] = None
    order_year: Optional[int] = None
    is_revenue_order: Optional[int] = None


@strawberry.input
class CustomerFilters:
    """Optional filters for customer queries."""
    customer_segment: Optional[str] = None
    acquisition_channel: Optional[str] = None
    is_repeat_customer: Optional[int] = None
    is_high_value: Optional[int] = None
    min_lifetime_value: Optional[float] = None
    country: Optional[str] = None
    state: Optional[str] = None


@strawberry.input
class ProductFilters:
    """Optional filters for product queries."""
    category: Optional[str] = None
    performance_tier: Optional[str] = None
    status: Optional[str] = None
    is_available: Optional[int] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None


# ======================================================================
# PAGINATION INPUT
# ======================================================================

@strawberry.input
class PaginationInput:
    """
    Standard pagination arguments.
    limit: max rows to return
    offset: how many rows to skip (for page 2, 3, etc.)
    """
    limit: int = 100
    offset: int = 0
