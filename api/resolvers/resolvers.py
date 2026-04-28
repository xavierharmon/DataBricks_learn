"""
api/resolvers/resolvers.py
-----------------------------------------------------------------------
GRAPHQL RESOLVERS

Each resolver:
  1. Receives filter arguments from the GraphQL query
  2. Builds a safe parameterized SQL query (? placeholders only)
  3. Passes a flat list of params — never f-strings with user input
  4. Maps result rows to typed Strawberry objects
-----------------------------------------------------------------------
"""

from typing import List, Optional, TYPE_CHECKING
from api.schema.types import (
    Order, Customer, Product,
    OrderSummary, RevenueByDimension, CustomerSegmentSummary,
    OrderFilters, CustomerFilters, ProductFilters, PaginationInput
)

if TYPE_CHECKING:
    from api.connectors.base_connector import BaseConnector


def _build_where(conditions: list, params: list) -> str:
    """
    Build a WHERE clause from (sql_fragment, value) tuples.
    Values are appended to params list — never interpolated into SQL.
    """
    if not conditions:
        return ""
    for _, value in conditions:
        params.append(value)
    clauses = [sql for sql, _ in conditions]
    return "WHERE " + " AND ".join(clauses)


# ======================================================================
# ORDER RESOLVERS
# ======================================================================

def resolve_orders(
    connector: "BaseConnector",
    filters: Optional[OrderFilters],
    pagination: Optional[PaginationInput],
    settings
) -> List[Order]:
    conditions, params = [], []

    if filters:
        if filters.status is not None:
            conditions.append(("status = ?", filters.status))
        if filters.customer_segment is not None:
            conditions.append(("customer_segment = ?", filters.customer_segment))
        if filters.acquisition_channel is not None:
            conditions.append(("acquisition_channel = ?", filters.acquisition_channel))
        if filters.order_size_segment is not None:
            conditions.append(("order_size_segment = ?", filters.order_size_segment))
        if filters.min_revenue is not None:
            conditions.append(("net_revenue >= ?", filters.min_revenue))
        if filters.max_revenue is not None:
            conditions.append(("net_revenue <= ?", filters.max_revenue))
        if filters.order_year is not None:
            conditions.append(("order_year = ?", filters.order_year))
        if filters.is_revenue_order is not None:
            conditions.append(("is_revenue_order = ?", filters.is_revenue_order))

    where = _build_where(conditions, params)
    limit = min(
        pagination.limit if pagination else settings.default_limit,
        settings.max_limit
    )
    offset = pagination.offset if pagination else 0
    params += [limit, offset]

    sql = f"""
        SELECT
            order_id, customer_id, order_date, order_year, status,
            is_revenue_order, order_size_segment, total_amount,
            shipping_amount, net_revenue, item_count, total_units,
            distinct_product_count, customer_segment, acquisition_channel,
            customer_city, customer_state, customer_country
        FROM fct_orders
        {where}
        ORDER BY order_date DESC
        LIMIT ? OFFSET ?
    """

    rows = connector.execute_query_to_dicts(sql, params)
    return [
        Order(
            order_id=str(row["order_id"]),
            customer_id=str(row["customer_id"]),
            order_date=row.get("order_date"),
            order_year=row.get("order_year"),
            status=str(row.get("status", "")),
            is_revenue_order=int(row.get("is_revenue_order", 0)),
            order_size_segment=row.get("order_size_segment"),
            total_amount=row.get("total_amount"),
            shipping_amount=row.get("shipping_amount"),
            net_revenue=row.get("net_revenue"),
            item_count=row.get("item_count"),
            total_units=row.get("total_units"),
            distinct_product_count=row.get("distinct_product_count"),
            customer_segment=row.get("customer_segment"),
            acquisition_channel=row.get("acquisition_channel"),
            customer_city=row.get("customer_city"),
            customer_state=row.get("customer_state"),
            customer_country=row.get("customer_country"),
        )
        for row in rows
    ]


def resolve_order_summary(connector: "BaseConnector") -> OrderSummary:
    sql = """
        SELECT
            COUNT(*)                                                AS total_orders,
            SUM(CASE WHEN is_revenue_order = 1 THEN 1 ELSE 0 END) AS revenue_orders,
            COALESCE(SUM(CASE WHEN is_revenue_order = 1
                         THEN net_revenue ELSE 0 END), 0)          AS total_revenue,
            COALESCE(AVG(CASE WHEN is_revenue_order = 1
                         THEN net_revenue END), 0)                 AS avg_order_value,
            COALESCE(SUM(CASE WHEN is_revenue_order = 1
                         THEN total_units ELSE 0 END), 0)          AS total_units_sold,
            COALESCE(
                SUM(CASE WHEN status = 'cancelled' THEN 1.0 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 0
            )                                                      AS cancellation_rate
        FROM fct_orders
    """
    rows = connector.execute_query_to_dicts(sql)
    row = rows[0] if rows else {}
    return OrderSummary(
        total_orders=int(row.get("total_orders", 0)),
        revenue_orders=int(row.get("revenue_orders", 0)),
        total_revenue=float(row.get("total_revenue", 0)),
        avg_order_value=float(row.get("avg_order_value", 0)),
        total_units_sold=int(row.get("total_units_sold", 0)),
        cancellation_rate=float(row.get("cancellation_rate", 0)),
    )


def resolve_revenue_by_dimension(
    connector: "BaseConnector",
    dimension: str
) -> List[RevenueByDimension]:
    # Whitelist — NEVER skip this. Prevents SQL injection via column name.
    valid_dimensions = {
        "customer_segment", "acquisition_channel",
        "order_size_segment", "customer_state",
        "customer_country", "order_year", "status"
    }
    if dimension not in valid_dimensions:
        raise ValueError(
            f"Invalid dimension: '{dimension}'. "
            f"Valid options: {sorted(valid_dimensions)}"
        )

    sql = f"""
        SELECT
            COALESCE(CAST({dimension} AS VARCHAR), 'unknown') AS dimension_value,
            COUNT(*)                                           AS order_count,
            COALESCE(SUM(CASE WHEN is_revenue_order = 1
                         THEN net_revenue ELSE 0 END), 0)     AS total_revenue,
            COALESCE(AVG(CASE WHEN is_revenue_order = 1
                         THEN net_revenue END), 0)            AS avg_order_value
        FROM fct_orders
        GROUP BY {dimension}
        ORDER BY total_revenue DESC
    """
    rows = connector.execute_query_to_dicts(sql)
    return [
        RevenueByDimension(
            dimension_value=str(row.get("dimension_value", "")),
            order_count=int(row.get("order_count", 0)),
            total_revenue=float(row.get("total_revenue", 0)),
            avg_order_value=float(row.get("avg_order_value", 0)),
        )
        for row in rows
    ]


# ======================================================================
# CUSTOMER RESOLVERS
# ======================================================================

def resolve_customers(
    connector: "BaseConnector",
    filters: Optional[CustomerFilters],
    pagination: Optional[PaginationInput],
    settings
) -> List[Customer]:
    conditions, params = [], []

    if filters:
        if filters.customer_segment is not None:
            conditions.append(("customer_segment = ?", filters.customer_segment))
        if filters.acquisition_channel is not None:
            conditions.append(("acquisition_channel = ?", filters.acquisition_channel))
        if filters.is_repeat_customer is not None:
            conditions.append(("is_repeat_customer = ?", filters.is_repeat_customer))
        if filters.is_high_value is not None:
            conditions.append(("is_high_value = ?", filters.is_high_value))
        if filters.min_lifetime_value is not None:
            conditions.append(("lifetime_value >= ?", filters.min_lifetime_value))
        if filters.country is not None:
            conditions.append(("country = ?", filters.country))
        if filters.state is not None:
            conditions.append(("state = ?", filters.state))

    where = _build_where(conditions, params)
    limit = min(
        pagination.limit if pagination else settings.default_limit,
        settings.max_limit
    )
    offset = pagination.offset if pagination else 0
    params += [limit, offset]

    sql = f"""
        SELECT
            customer_id, email, full_name, city, state, country,
            acquisition_channel, customer_segment, total_orders,
            revenue_orders, lifetime_value, days_since_last_order,
            first_order_date, most_recent_order_date,
            is_repeat_customer, is_high_value
        FROM dim_customers
        {where}
        ORDER BY lifetime_value DESC
        LIMIT ? OFFSET ?
    """
    rows = connector.execute_query_to_dicts(sql, params)
    return [
        Customer(
            customer_id=str(row["customer_id"]),
            email=str(row.get("email", "")),
            full_name=row.get("full_name"),
            city=row.get("city"),
            state=row.get("state"),
            country=row.get("country"),
            acquisition_channel=row.get("acquisition_channel"),
            customer_segment=row.get("customer_segment"),
            total_orders=int(row.get("total_orders", 0)),
            revenue_orders=int(row.get("revenue_orders", 0)),
            lifetime_value=float(row.get("lifetime_value", 0)),
            days_since_last_order=row.get("days_since_last_order"),
            first_order_date=row.get("first_order_date"),
            most_recent_order_date=row.get("most_recent_order_date"),
            is_repeat_customer=int(row.get("is_repeat_customer", 0)),
            is_high_value=int(row.get("is_high_value", 0)),
        )
        for row in rows
    ]


def resolve_customer_segments(
    connector: "BaseConnector"
) -> List[CustomerSegmentSummary]:
    sql = """
        SELECT
            COALESCE(customer_segment, 'unknown')   AS segment,
            COUNT(*)                                 AS customer_count,
            COALESCE(AVG(CASE WHEN lifetime_value > 0
                         THEN lifetime_value END), 0) AS avg_lifetime_value,
            COALESCE(
                SUM(CASE WHEN is_repeat_customer = 1 THEN 1.0 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN total_orders > 0 THEN 1 ELSE 0 END), 0),
            0)                                       AS repeat_customer_rate
        FROM dim_customers
        GROUP BY customer_segment
        ORDER BY customer_count DESC
    """
    rows = connector.execute_query_to_dicts(sql)
    return [
        CustomerSegmentSummary(
            segment=str(row.get("segment", "")),
            customer_count=int(row.get("customer_count", 0)),
            avg_lifetime_value=float(row.get("avg_lifetime_value", 0)),
            repeat_customer_rate=float(row.get("repeat_customer_rate", 0)),
        )
        for row in rows
    ]


# ======================================================================
# PRODUCT RESOLVERS
# ======================================================================

def resolve_products(
    connector: "BaseConnector",
    filters: Optional[ProductFilters],
    pagination: Optional[PaginationInput],
    settings
) -> List[Product]:
    conditions, params = [], []

    if filters:
        if filters.category is not None:
            conditions.append(("category = ?", filters.category))
        if filters.performance_tier is not None:
            conditions.append(("performance_tier = ?", filters.performance_tier))
        if filters.status is not None:
            conditions.append(("status = ?", filters.status))
        if filters.is_available is not None:
            conditions.append(("is_available = ?", filters.is_available))
        if filters.min_price is not None:
            conditions.append(("list_price >= ?", filters.min_price))
        if filters.max_price is not None:
            conditions.append(("list_price <= ?", filters.max_price))

    where = _build_where(conditions, params)
    limit = min(
        pagination.limit if pagination else settings.default_limit,
        settings.max_limit
    )
    offset = pagination.offset if pagination else 0
    params += [limit, offset]

    sql = f"""
        SELECT
            product_id, product_name, category, subcategory, sku,
            list_price, cost, stock_quantity, status,
            total_units_sold, total_revenue, gross_margin_rate,
            performance_tier, is_available
        FROM dim_products
        {where}
        ORDER BY total_revenue DESC
        LIMIT ? OFFSET ?
    """
    rows = connector.execute_query_to_dicts(sql, params)
    return [
        Product(
            product_id=str(row["product_id"]),
            product_name=str(row.get("product_name", "")),
            category=row.get("category"),
            subcategory=row.get("subcategory"),
            sku=row.get("sku"),
            list_price=row.get("list_price"),
            cost=row.get("cost"),
            stock_quantity=row.get("stock_quantity"),
            status=row.get("status"),
            total_units_sold=int(row.get("total_units_sold", 0)),
            total_revenue=float(row.get("total_revenue", 0)),
            gross_margin_rate=row.get("gross_margin_rate"),
            performance_tier=row.get("performance_tier"),
            is_available=int(row.get("is_available", 0)),
        )
        for row in rows
    ]
