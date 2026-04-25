"""
pipeline/semantic/metrics.py
-----------------------------------------------------------------------
SEMANTIC METRICS & DIMENSIONS

These dataclasses are the building blocks of the semantic model.
They represent the BUSINESS VOCABULARY of your analytics system.

KEY DISTINCTION:
    Dimension  = something you FILTER or GROUP BY  (date, country, status)
    Measure    = something you AGGREGATE           (revenue, count, avg)

    In SQL terms:
      SELECT  [dimension], [dimension],    ← GROUP BY these
              sum([measure])               ← Aggregate these
      FROM    fct_orders
      WHERE   [dimension filter]
      GROUP BY [dimension], [dimension]
-----------------------------------------------------------------------
"""

from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class SemanticDimension:
    """
    A dimension is a column used to SLICE, FILTER, or GROUP your data.

    Types:
      categorical  → text categories (status, country, segment)
      time         → date/timestamp columns (order_date, created_at)
      boolean      → true/false flags (is_active, is_repeat_customer)
      numeric      → numeric buckets used as categories (order_year)

    Args:
        name: Column name in the physical table
        dimension_type: One of categorical, time, boolean, numeric
        label: Human-friendly name shown in BI tool
        description: What this dimension represents
    """
    name: str
    dimension_type: str       # categorical, time, boolean, numeric
    label: str
    description: Optional[str] = None

    def __post_init__(self):
        valid_types = {"categorical", "time", "boolean", "numeric"}
        if self.dimension_type not in valid_types:
            raise ValueError(
                f"Dimension '{self.name}' has invalid type '{self.dimension_type}'. "
                f"Must be one of: {valid_types}"
            )

    def to_metricflow_yaml(self) -> dict:
        """Generate dbt MetricFlow YAML representation."""
        result = {
            "name": self.name,
            "type": "time" if self.dimension_type == "time" else "categorical",
            "label": self.label,
        }
        if self.dimension_type == "time":
            result["type_params"] = {"time_granularity": "day"}
        if self.description:
            result["description"] = self.description
        return result


@dataclass
class SemanticMetric:
    """
    A metric (measure) is a column that gets AGGREGATED.

    In Power BI / Azure Fabric, metrics become DAX measures.
    In dbt MetricFlow, they're defined in metrics.yml.
    In Looker, they're "measures" in LookML.

    Args:
        name: Programmatic name (used in code/API)
        label: Human-friendly name (shown in BI tool)
        sql: The SQL aggregation expression
        aggregation: The aggregation type (sum, count, average, max, min)
        description: What this metric means and how it's calculated
        format: Display format (currency, percent, number)
        filters: SQL WHERE conditions always applied to this metric
    """
    name: str
    label: str
    sql: str
    aggregation: str
    description: str
    format: str = "number"     # number, currency, percent
    filters: List[str] = field(default_factory=list)

    def __post_init__(self):
        valid_aggregations = {"sum", "count", "count_distinct", "average", "max", "min"}
        if self.aggregation not in valid_aggregations:
            raise ValueError(
                f"Metric '{self.name}' has invalid aggregation '{self.aggregation}'. "
                f"Must be one of: {valid_aggregations}"
            )

        valid_formats = {"number", "currency", "percent"}
        if self.format not in valid_formats:
            raise ValueError(
                f"Metric '{self.name}' has invalid format '{self.format}'. "
                f"Must be one of: {valid_formats}"
            )

    def to_metricflow_yaml(self) -> dict:
        """Generate dbt MetricFlow YAML representation."""
        result = {
            "name": self.name,
            "description": self.description,
            "label": self.label,
            "agg": self.aggregation,
            "expr": self.sql,
        }
        if self.filters:
            result["agg_params"] = {"where": " AND ".join(self.filters)}
        return result

    def to_dax_measure(self, table_name: str) -> str:
        """
        Generate a DAX measure definition for Azure Fabric / Power BI.

        DAX (Data Analysis Expressions) is the formula language used
        by Power BI and Azure Fabric for calculated measures.

        This is a simplified DAX generator — real DAX is more complex
        but this gives you the pattern to start from.
        """
        filter_str = ""
        if self.filters:
            filter_str = ", ".join(
                f"FILTER({table_name}, {table_name}[{f}])"
                for f in self.filters
            )

        dax_map = {
            "sum":           f"SUM({table_name}[{self.name}])",
            "count":         f"COUNTROWS({table_name})",
            "count_distinct":f"DISTINCTCOUNT({table_name}[{self.name}])",
            "average":       f"AVERAGE({table_name}[{self.name}])",
            "max":           f"MAX({table_name}[{self.name}])",
            "min":           f"MIN({table_name}[{self.name}])",
        }

        base_expr = dax_map.get(self.aggregation, f"SUM({table_name}[{self.name}])")

        if filter_str:
            base_expr = f"CALCULATE({base_expr}, {filter_str})"

        return f"{self.label} = {base_expr}"
