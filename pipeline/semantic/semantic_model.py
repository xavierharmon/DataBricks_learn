"""
pipeline/semantic/semantic_model.py
-----------------------------------------------------------------------
SEMANTIC MODEL

WHAT IS A SEMANTIC MODEL?
    A semantic model sits ON TOP of your mart tables and answers:
      "What metrics can BI tools query, and how are they calculated?"

    It's the TRANSLATION LAYER between:
      Technical layer:  fct_orders (table with columns)
      Business layer:   "Total Revenue for Q3 by Customer Segment"

    In the modern data stack, semantic layers are implemented by:
      - dbt Semantic Layer / MetricFlow (open source)
      - Azure Fabric semantic models (Power BI datasets)
      - Looker LookML
      - Cube.dev

    This Python class DEFINES the semantic model as code.
    It can be used to generate the YAML/config for any of the above tools.

HOW IT FITS IN THE ARCHITECTURE:
    Bronze (raw) → Silver (staged) → Gold (marts) → Semantic Model → BI Tool

    The semantic model is the FINAL LAYER before business users touch the data.
    It defines dimensions, measures, and relationships — not raw SQL.
-----------------------------------------------------------------------
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from .metrics import SemanticMetric, SemanticDimension


@dataclass
class SemanticModelTable:
    """
    Represents a single table in the semantic model.

    In Azure Fabric / Power BI terms, this is a "table" in the dataset.
    In dbt MetricFlow terms, this is a "semantic model".

    Args:
        name: Logical name used in the BI tool
        physical_table: The actual mart table (e.g., "marts.fct_orders")
        primary_key: Column that uniquely identifies each row
        description: What this table represents
        dimensions: Columns used to FILTER and GROUP BY
        measures: Columns used to AGGREGATE (sum, count, avg)
    """
    name: str
    physical_table: str
    primary_key: str
    description: str
    grain: str
    dimensions: List[SemanticDimension] = field(default_factory=list)
    measures: List[SemanticMetric] = field(default_factory=list)

    def get_dimension(self, name: str) -> Optional[SemanticDimension]:
        return next((d for d in self.dimensions if d.name == name), None)

    def get_measure(self, name: str) -> Optional[SemanticMetric]:
        return next((m for m in self.measures if m.name == name), None)

    def to_metricflow_yaml(self) -> dict:
        """
        Generate dbt MetricFlow YAML for this semantic model.
        MetricFlow is the open-source semantic layer built into dbt Cloud.
        You'd write this output to a .yml file in your dbt project.
        """
        return {
            "semantic_models": [{
                "name": self.name,
                "description": self.description,
                "model": f"ref('{self.physical_table.split('.')[-1]}')",
                "entities": [{
                    "name": self.name,
                    "type": "primary",
                    "expr": self.primary_key
                }],
                "dimensions": [d.to_metricflow_yaml() for d in self.dimensions],
                "measures": [m.to_metricflow_yaml() for m in self.measures]
            }]
        }


class EcommerceSemanticModel:
    """
    The complete semantic model for the e-commerce analytics project.

    This class assembles ALL tables, dimensions, and measures into
    a single cohesive semantic model that Azure Fabric / Power BI
    can consume.

    Think of this as the "data model" tab in Power BI Desktop —
    but defined as code so it's version-controlled and reviewable.
    """

    def __init__(self):
        self._tables: Dict[str, SemanticModelTable] = {}
        self._relationships: List[Dict] = []
        self._build()

    def _build(self) -> None:
        """Construct the full semantic model."""
        self._register_orders_table()
        self._register_customers_table()
        self._register_products_table()
        self._register_relationships()

    def _register_orders_table(self) -> None:
        """Define the fct_orders semantic table."""

        orders_table = SemanticModelTable(
            name="orders",
            physical_table="marts.fct_orders",
            primary_key="order_id",
            grain="One row per order",
            description="All customer orders with revenue metrics and customer context.",
            dimensions=[
                SemanticDimension("order_date",     "time",        "Order Date"),
                SemanticDimension("order_month",    "time",        "Order Month"),
                SemanticDimension("order_year",     "categorical", "Order Year"),
                SemanticDimension("status",         "categorical", "Order Status"),
                SemanticDimension("order_size_segment", "categorical", "Order Size"),
                SemanticDimension("customer_segment",   "categorical", "Customer Segment"),
                SemanticDimension("acquisition_channel","categorical", "Acquisition Channel"),
                SemanticDimension("customer_state", "categorical", "Customer State"),
                SemanticDimension("customer_country","categorical", "Customer Country"),
            ],
            measures=[
                SemanticMetric(
                    name="total_revenue",
                    label="Total Revenue",
                    sql="sum(case when is_revenue_order then net_revenue else 0 end)",
                    aggregation="sum",
                    description="Net revenue from non-cancelled, non-refunded orders.",
                    format="currency"
                ),
                SemanticMetric(
                    name="order_count",
                    label="Number of Orders",
                    sql="count(order_id)",
                    aggregation="count",
                    description="Total order count including all statuses.",
                    format="number"
                ),
                SemanticMetric(
                    name="avg_order_value",
                    label="Average Order Value",
                    sql="avg(case when is_revenue_order then net_revenue end)",
                    aggregation="average",
                    description="Average net revenue per revenue order (AOV).",
                    format="currency"
                ),
                SemanticMetric(
                    name="total_units_sold",
                    label="Total Units Sold",
                    sql="sum(total_units)",
                    aggregation="sum",
                    description="Total product units sold across all revenue orders.",
                    format="number"
                ),
            ]
        )
        self._tables["orders"] = orders_table

    def _register_customers_table(self) -> None:
        """Define the dim_customers semantic table."""

        customers_table = SemanticModelTable(
            name="customers",
            physical_table="marts.dim_customers",
            primary_key="customer_id",
            grain="One row per customer (current state)",
            description="Customer dimension with behavioral segments and lifetime value.",
            dimensions=[
                SemanticDimension("customer_segment",  "categorical", "Customer Segment"),
                SemanticDimension("acquisition_channel","categorical","Acquisition Channel"),
                SemanticDimension("country",           "categorical", "Country"),
                SemanticDimension("state",             "categorical", "State"),
                SemanticDimension("account_tenure",    "categorical", "Account Tenure"),
                SemanticDimension("is_repeat_customer","boolean",     "Is Repeat Customer"),
                SemanticDimension("is_high_value",     "boolean",     "Is High Value"),
            ],
            measures=[
                SemanticMetric(
                    name="customer_count",
                    label="Number of Customers",
                    sql="count(customer_id)",
                    aggregation="count",
                    description="Total distinct customer count.",
                    format="number"
                ),
                SemanticMetric(
                    name="avg_lifetime_value",
                    label="Avg Customer Lifetime Value",
                    sql="avg(lifetime_value)",
                    aggregation="average",
                    description="Average total revenue per customer.",
                    format="currency"
                ),
            ]
        )
        self._tables["customers"] = customers_table

    def _register_relationships(self) -> None:
        """
        Define relationships between tables.
        This is what Power BI uses to know how to JOIN tables.
        In Azure Fabric, these relationships are set up in the semantic model.
        """
        self._relationships = [
            {
                "from_table": "orders",
                "from_column": "customer_id",
                "to_table": "customers",
                "to_column": "customer_id",
                "cardinality": "many_to_one",   # Many orders → one customer
                "active": True
            }
        ]

    def get_table(self, name: str) -> Optional[SemanticModelTable]:
        return self._tables.get(name)

    def list_all_metrics(self) -> List[Dict]:
        """Return every metric across all tables — useful for a data catalog."""
        all_metrics = []
        for table_name, table in self._tables.items():
            for metric in table.measures:
                all_metrics.append({
                    "table": table_name,
                    "metric_name": metric.name,
                    "label": metric.label,
                    "description": metric.description,
                    "format": metric.format
                })
        return all_metrics

    def to_fabric_semantic_model(self) -> dict:
        """
        Generate an Azure Fabric / Power BI semantic model definition.
        In real usage, this would generate TMDL (Tabular Model Definition Language)
        or PBIX-compatible JSON that Azure Fabric can import.
        """
        return {
            "semantic_model": {
                "name": "ecommerce_analytics",
                "tables": [
                    {
                        "name": t.name,
                        "source": t.physical_table,
                        "columns": [
                            {"name": d.name, "type": d.dimension_type}
                            for d in t.dimensions
                        ],
                        "measures": [
                            {
                                "name": m.label,
                                "expression": m.sql,
                                "format_string": m.format
                            }
                            for m in t.measures
                        ]
                    }
                    for t in self._tables.values()
                ],
                "relationships": self._relationships
            }
        }
