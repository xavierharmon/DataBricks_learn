"""
api/connectors/base_connector.py
-----------------------------------------------------------------------
DATA CONNECTORS — Abstract base + DuckDB implementation

ARCHITECTURE PATTERN:
    BaseConnector defines the CONTRACT all backends must follow.
    DuckDBConnector implements it for local dev (reads CSVs, no cloud).
    DatabricksConnector (databricks_connector.py) implements it for prod.

    Resolvers always call: connector.execute_query(sql, params)
    They never know which backend is running underneath.
-----------------------------------------------------------------------
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd


class BaseConnector(ABC):
    """Abstract base class — all connectors must implement these methods."""

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass

    @abstractmethod
    def execute_query(self, sql: str, params: Optional[List] = None) -> pd.DataFrame:
        """
        Execute SQL and return a DataFrame.
        params is a flat LIST of values matching ? placeholders in sql.
        Always use ? placeholders — never f-string user input into SQL.
        """
        pass

    @abstractmethod
    def get_backend_name(self) -> str:
        pass

    def execute_query_to_dicts(
        self, sql: str, params: Optional[List] = None
    ) -> List[Dict[str, Any]]:
        """Execute query and return list of dicts (GraphQL resolvers use this)."""
        df = self.execute_query(sql, params)
        return df.where(pd.notna(df), None).to_dict(orient="records")


# ======================================================================
# DUCKDB CONNECTOR
# ======================================================================

class DuckDBConnector(BaseConnector):
    """
    Local DuckDB connector — reads CSVs from data/bronze/.
    Creates mart-like views that mirror your dbt models so the
    GraphQL API can run without any cloud connection.

    Run generate_sample_data.py first to create the CSV files.
    """

    def __init__(self, data_path: str = "data/bronze"):
        self.data_path = data_path
        self._conn = None

    def connect(self) -> None:
        try:
            import duckdb
            self._conn = duckdb.connect(database=":memory:")
            self._register_tables()
            print(f"✅ DuckDB connected — reading from: {self.data_path}")
        except ImportError:
            raise RuntimeError("duckdb not installed. Run: pip install duckdb")

    def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def get_backend_name(self) -> str:
        return f"DuckDB (local: {self.data_path})"

    def execute_query(self, sql: str, params: Optional[List] = None) -> pd.DataFrame:
        if not self._conn:
            self.connect()
        if params:
            return self._conn.execute(sql, params).df()
        return self._conn.execute(sql).df()

    def _register_tables(self) -> None:
        """Register CSV files as views, then build mart-like views on top."""
        import os
        for table in ["orders", "customers", "products", "order_items"]:
            path = os.path.join(self.data_path, f"{table}.csv")
            if os.path.exists(path):
                self._conn.execute(f"""
                    CREATE OR REPLACE VIEW raw_{table} AS
                    SELECT * FROM read_csv_auto('{path}', header=true)
                """)
            else:
                print(f"  ⚠️  {path} not found — run generate_sample_data.py first")

        self._create_fct_orders_view()
        self._create_dim_customers_view()
        self._create_dim_products_view()

    def _create_fct_orders_view(self) -> None:
        """Approximates fct_orders — mirrors dbt staging + intermediate + mart logic."""
        self._conn.execute("""
            CREATE OR REPLACE VIEW fct_orders AS
            WITH order_items_summary AS (
                SELECT
                    order_id,
                    COUNT(*)                    AS item_count,
                    SUM(quantity)               AS total_units,
                    SUM(quantity * unit_price)  AS items_subtotal,
                    COUNT(DISTINCT product_id)  AS distinct_product_count
                FROM raw_order_items
                GROUP BY order_id
            ),
            customer_history AS (
                SELECT
                    customer_id,
                    COUNT(CASE WHEN LOWER(TRIM(status))
                        IN ('delivered','shipped','processing')
                        THEN 1 END)                                     AS revenue_orders,
                    DATEDIFF('day', MAX(TRY_CAST(order_date AS DATE)),
                             CURRENT_DATE)                              AS days_since_last_order
                FROM raw_orders
                GROUP BY customer_id
            )
            SELECT
                o.order_id,
                o.customer_id,
                TRY_CAST(o.order_date AS DATE)              AS order_date,
                DATE_TRUNC('month', TRY_CAST(o.order_date AS DATE)) AS order_month,
                YEAR(TRY_CAST(o.order_date AS DATE))        AS order_year,
                LOWER(TRIM(o.status))                       AS status,

                CASE
                    WHEN LOWER(TRIM(o.status)) IN ('delivered','shipped','processing')
                    THEN 1 ELSE 0
                END                                         AS is_revenue_order,

                TRY_CAST(o.total_amount AS DOUBLE)          AS total_amount,
                TRY_CAST(o.shipping_amount AS DOUBLE)       AS shipping_amount,
                TRY_CAST(o.total_amount AS DOUBLE)
                    - TRY_CAST(o.shipping_amount AS DOUBLE) AS net_revenue,

                CASE
                    WHEN TRY_CAST(o.total_amount AS DOUBLE) >= 500 THEN 'large'
                    WHEN TRY_CAST(o.total_amount AS DOUBLE) >= 100 THEN 'medium'
                    ELSE 'small'
                END                                         AS order_size_segment,

                COALESCE(ois.item_count, 0)                 AS item_count,
                COALESCE(ois.total_units, 0)                AS total_units,
                COALESCE(ois.distinct_product_count, 0)     AS distinct_product_count,

                LOWER(TRIM(
                    CASE
                        WHEN LOWER(c.acquisition_channel) LIKE '%organic%' THEN 'organic_search'
                        WHEN LOWER(c.acquisition_channel) LIKE '%paid%'    THEN 'paid_search'
                        WHEN LOWER(c.acquisition_channel) LIKE '%social%'  THEN 'social_media'
                        WHEN LOWER(c.acquisition_channel) LIKE '%email%'   THEN 'email'
                        WHEN LOWER(c.acquisition_channel) LIKE '%referral%' THEN 'referral'
                        ELSE 'unknown'
                    END
                ))                                          AS acquisition_channel,
                c.city                                      AS customer_city,
                c.state                                     AS customer_state,
                c.country                                   AS customer_country,

                CASE
                    WHEN ch.revenue_orders IS NULL OR ch.revenue_orders = 0
                        THEN 'no_purchases'
                    WHEN ch.days_since_last_order <= 30 AND ch.revenue_orders >= 3
                        THEN 'champion'
                    WHEN ch.days_since_last_order <= 90 AND ch.revenue_orders >= 2
                        THEN 'loyal'
                    WHEN ch.days_since_last_order <= 30
                        THEN 'new_customer'
                    WHEN ch.days_since_last_order BETWEEN 91 AND 180
                        THEN 'at_risk'
                    WHEN ch.days_since_last_order > 180
                        THEN 'churned'
                    ELSE 'one_time'
                END                                         AS customer_segment

            FROM raw_orders o
            LEFT JOIN order_items_summary ois ON o.order_id = ois.order_id
            LEFT JOIN raw_customers c         ON o.customer_id = c.customer_id
            LEFT JOIN customer_history ch     ON o.customer_id = ch.customer_id
        """)

    def _create_dim_customers_view(self) -> None:
        """Approximates dim_customers — mirrors int_customer_orders + dim_customers."""
        self._conn.execute("""
            CREATE OR REPLACE VIEW dim_customers AS
            WITH customer_history AS (
                SELECT
                    customer_id,
                    COUNT(*)                                            AS total_orders,
                    COUNT(CASE WHEN LOWER(TRIM(status))
                        IN ('delivered','shipped','processing')
                        THEN 1 END)                                     AS revenue_orders,
                    SUM(CASE WHEN LOWER(TRIM(status))
                        IN ('delivered','shipped','processing')
                        THEN TRY_CAST(total_amount AS DOUBLE)
                             - TRY_CAST(shipping_amount AS DOUBLE)
                        ELSE 0 END)                                     AS lifetime_value,
                    MIN(TRY_CAST(order_date AS DATE))                   AS first_order_date,
                    MAX(TRY_CAST(order_date AS DATE))                   AS most_recent_order_date,
                    DATEDIFF('day', MAX(TRY_CAST(order_date AS DATE)),
                             CURRENT_DATE)                              AS days_since_last_order
                FROM raw_orders
                GROUP BY customer_id
            )
            SELECT
                c.customer_id,
                LOWER(TRIM(c.email))                        AS email,
                c.full_name,
                c.city,
                c.state,
                c.country,
                CASE
                    WHEN LOWER(c.acquisition_channel) LIKE '%organic%' THEN 'organic_search'
                    WHEN LOWER(c.acquisition_channel) LIKE '%paid%'    THEN 'paid_search'
                    WHEN LOWER(c.acquisition_channel) LIKE '%social%'  THEN 'social_media'
                    WHEN LOWER(c.acquisition_channel) LIKE '%email%'   THEN 'email'
                    WHEN LOWER(c.acquisition_channel) LIKE '%referral%' THEN 'referral'
                    ELSE 'unknown'
                END                                         AS acquisition_channel,

                COALESCE(h.total_orders, 0)                 AS total_orders,
                COALESCE(h.revenue_orders, 0)               AS revenue_orders,
                COALESCE(h.lifetime_value, 0)               AS lifetime_value,
                h.first_order_date,
                h.most_recent_order_date,
                h.days_since_last_order,

                CASE
                    WHEN h.revenue_orders IS NULL OR h.revenue_orders = 0
                        THEN 'no_purchases'
                    WHEN h.days_since_last_order <= 30 AND h.revenue_orders >= 3
                        THEN 'champion'
                    WHEN h.days_since_last_order <= 90 AND h.revenue_orders >= 2
                        THEN 'loyal'
                    WHEN h.days_since_last_order <= 30
                        THEN 'new_customer'
                    WHEN h.days_since_last_order BETWEEN 91 AND 180
                        THEN 'at_risk'
                    WHEN h.days_since_last_order > 180
                        THEN 'churned'
                    ELSE 'one_time'
                END                                         AS customer_segment,

                CASE WHEN COALESCE(h.revenue_orders, 0) >= 2
                     THEN 1 ELSE 0 END                      AS is_repeat_customer,
                CASE WHEN COALESCE(h.lifetime_value, 0) >= 1000
                     THEN 1 ELSE 0 END                      AS is_high_value

            FROM raw_customers c
            LEFT JOIN customer_history h ON c.customer_id = h.customer_id
        """)

    def _create_dim_products_view(self) -> None:
        """Approximates dim_products — mirrors int_product_revenue + dim_products."""
        self._conn.execute("""
            CREATE OR REPLACE VIEW dim_products AS
            WITH product_sales AS (
                SELECT
                    oi.product_id,
                    SUM(oi.quantity)                AS total_units_sold,
                    SUM(oi.quantity * oi.unit_price) AS total_revenue
                FROM raw_order_items oi
                INNER JOIN raw_orders o ON oi.order_id = o.order_id
                WHERE LOWER(TRIM(o.status)) IN ('delivered','shipped','processing')
                GROUP BY oi.product_id
            )
            SELECT
                p.product_id,
                p.product_name,
                LOWER(TRIM(p.category))             AS category,
                LOWER(TRIM(p.subcategory))          AS subcategory,
                p.sku,
                TRY_CAST(p.price AS DOUBLE)         AS list_price,
                TRY_CAST(p.cost AS DOUBLE)          AS cost,
                TRY_CAST(p.stock_quantity AS INT)   AS stock_quantity,
                LOWER(TRIM(p.status))               AS status,
                COALESCE(ps.total_units_sold, 0)    AS total_units_sold,
                COALESCE(ps.total_revenue, 0)       AS total_revenue,
                CASE WHEN COALESCE(ps.total_revenue, 0) > 0
                THEN ROUND(
                    (COALESCE(ps.total_revenue, 0)
                     - COALESCE(ps.total_units_sold, 0) * TRY_CAST(p.cost AS DOUBLE))
                    / ps.total_revenue, 4)
                ELSE NULL END                       AS gross_margin_rate,
                CASE
                    WHEN COALESCE(ps.total_units_sold, 0) = 0  THEN 'no_sales'
                    WHEN ps.total_units_sold >= 50             THEN 'top_seller'
                    WHEN ps.total_units_sold >= 10             THEN 'steady_seller'
                    WHEN ps.total_units_sold > 0               THEN 'slow_mover'
                    ELSE 'stale'
                END                                 AS performance_tier,
                CASE WHEN LOWER(TRIM(p.status)) = 'active'
                      AND TRY_CAST(p.stock_quantity AS INT) > 0
                     THEN 1 ELSE 0 END              AS is_available
            FROM raw_products p
            LEFT JOIN product_sales ps ON p.product_id = ps.product_id
        """)
