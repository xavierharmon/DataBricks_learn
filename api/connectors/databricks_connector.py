"""
api/connectors/databricks_connector.py
-----------------------------------------------------------------------
DATABRICKS CONNECTOR — production data backend

Connects to your live Databricks mart tables (fct_orders, dim_customers,
dim_products) via the Databricks SQL connector.

This is the same warehouse your dbt models write to — so the data here
is the fully transformed, tested, production-quality mart layer.

SWITCHING FROM DUCKDB TO DATABRICKS:
    In api/.env, change:
        DATA_BACKEND=duckdb
    to:
        DATA_BACKEND=databricks
    And fill in the DATABRICKS_* variables.
    No code changes needed anywhere else.
-----------------------------------------------------------------------
"""

from typing import Optional, Dict, Any
import pandas as pd
from .base_connector import BaseConnector


class DatabricksConnector(BaseConnector):
    """
    Connects the GraphQL API to live Databricks mart tables.

    Uses the same connection details as your dbt profiles.yml.
    The API reads from the mart schema — it never touches staging
    or intermediate tables directly.

    Args:
        host: Databricks workspace host
        http_path: SQL warehouse HTTP path (/sql/1.0/warehouses/...)
        token: Personal access token
        catalog: Unity catalog name (hive_metastore for Community Edition)
        schema: Schema containing the mart tables (usually 'marts')
    """

    def __init__(
        self,
        host: str,
        http_path: str,
        token: str,
        catalog: str = "hive_metastore",
        schema: str = "marts"
    ):
        self.host = host
        self.http_path = http_path
        self.token = token
        self.catalog = catalog
        self.schema = schema
        self._conn = None

    def connect(self) -> None:
        try:
            from databricks import sql as databricks_sql
            self._conn = databricks_sql.connect(
                server_hostname=self.host,
                http_path=self.http_path,
                access_token=self.token,
                catalog=self.catalog,
                schema=self.schema
            )
            print(f"✅ Databricks connected — {self.host} / {self.schema}")
        except ImportError:
            raise RuntimeError(
                "databricks-sql-connector not installed.\n"
                "Run: pip install databricks-sql-connector"
            )
        except Exception as e:
            raise RuntimeError(f"Databricks connection failed: {e}")

    def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def get_backend_name(self) -> str:
        return f"Databricks ({self.host} / {self.schema})"

    def execute_query(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Execute SQL against Databricks and return a DataFrame.

        The SQL you write here queries your actual mart tables:
            fct_orders, dim_customers, dim_products

        These are the tables dbt built and tested — production quality.
        """
        if not self._conn:
            self.connect()

        with self._conn.cursor() as cursor:
            if params:
                cursor.execute(sql, list(params.values()))
            else:
                cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

        return pd.DataFrame(rows, columns=columns)


# -----------------------------------------------------------------------
# CONNECTOR FACTORY
# -----------------------------------------------------------------------

def get_connector(settings) -> BaseConnector:
    """
    Factory function — returns the correct connector based on settings.

    This is the ONE place in the codebase where the backend choice
    is made. Everything else (resolvers, schema) uses BaseConnector
    and stays completely backend-agnostic.

    Called once at API startup. The connector is then injected into
    all resolvers via FastAPI dependency injection.
    """
    from api.config import DataBackend

    if settings.data_backend == DataBackend.DATABRICKS:
        if not all([
            settings.databricks_host,
            settings.databricks_http_path,
            settings.databricks_token
        ]):
            raise ValueError(
                "DATA_BACKEND=databricks requires DATABRICKS_HOST, "
                "DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN in api/.env"
            )
        connector = DatabricksConnector(
            host=settings.databricks_host,
            http_path=settings.databricks_http_path,
            token=settings.databricks_token,
            catalog=settings.databricks_catalog,
            schema=settings.databricks_schema
        )
    else:
        from .base_connector import DuckDBConnector
        connector = DuckDBConnector(data_path=settings.duckdb_data_path)

    connector.connect()
    return connector
