"""
pipeline/domains/base_domain.py
-----------------------------------------------------------------------
BASE DOMAIN — Abstract class for business domains

WHAT IS A DOMAIN IN ANALYTICS ENGINEERING?
    A domain is a BUSINESS CONCEPT that owns a set of related data.
    In e-commerce, the domains are:
      - Orders Domain    → everything about what was bought
      - Customers Domain → everything about who bought it
      - Products Domain  → everything about what's for sale

    Domains are the bridge between raw data (tables) and business
    language (metrics, KPIs). They define:
      1. Which source tables belong to them
      2. What business entities they expose (orders, customers)
      3. What metrics are defined within the domain

WHY MODEL DOMAINS IN PYTHON?
    The Python classes here serve as DOCUMENTATION and CONTRACTS.
    They force you to explicitly answer:
      "What does this domain own?"
      "What metrics does it expose?"
      "What does the business MEAN by X?"

    In a real team, this Python layer might also:
      - Generate dbt YAML documentation automatically
      - Push metric definitions to a data catalog (e.g., Azure Purview)
      - Validate that all required models exist before deploying
-----------------------------------------------------------------------
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass
class MetricDefinition:
    """
    Represents a single business metric owned by a domain.

    A metric is MORE than just a SQL formula — it has:
      - A business owner (who is responsible for this number?)
      - A precise definition (what exactly is included/excluded?)
      - A grain (is this per-day? per-customer? overall?)
      - The SQL to compute it

    This is the foundation of a SEMANTIC LAYER.
    """
    name: str                          # e.g., "total_revenue"
    label: str                         # Human-friendly: "Total Revenue"
    description: str                   # What is included/excluded and why
    sql_expression: str                # The actual calculation
    aggregation_type: str              # sum, count, average, max, min
    grain: str                         # What level this is measured at
    filters: List[str] = field(default_factory=list)  # e.g., ["is_revenue_order = true"]
    owner: Optional[str] = None        # Business owner (team or person)
    unit: Optional[str] = None         # "$", "%", "orders", etc.

    def to_dbt_metric(self) -> dict:
        """
        Convert to dbt metrics YAML format.
        This could be written to a metrics.yml file automatically.
        """
        return {
            "name": self.name,
            "label": self.label,
            "description": self.description,
            "type": self.aggregation_type,
            "sql": self.sql_expression,
            "filters": [{"field": f} for f in self.filters],
        }


@dataclass
class DomainEntity:
    """
    An entity is a core THING that a domain tracks.
    e.g., the Orders domain tracks "Order" entities.
    """
    name: str                    # e.g., "Order"
    plural: str                  # e.g., "Orders"
    primary_key: str             # e.g., "order_id"
    mart_model: str              # The final dbt mart model for this entity
    description: str


class BaseDomain(ABC):
    """
    Abstract base class for business domains.

    Every domain must declare:
      1. Its name and description
      2. The source tables it owns
      3. The entities it exposes
      4. The metrics it defines
    """

    def __init__(self):
        self._metrics: List[MetricDefinition] = []
        self._entities: List[DomainEntity] = []
        self._source_tables: List[str] = []

    # ------------------------------------------------------------------
    # ABSTRACT — must be implemented by each domain
    # ------------------------------------------------------------------

    @abstractmethod
    def get_domain_name(self) -> str:
        """e.g., 'orders', 'customers', 'products'"""
        pass

    @abstractmethod
    def get_description(self) -> str:
        """What does this domain represent?"""
        pass

    @abstractmethod
    def register_metrics(self) -> None:
        """
        Define all metrics owned by this domain.
        Called during initialization to populate self._metrics.
        """
        pass

    @abstractmethod
    def register_entities(self) -> None:
        """
        Define all entities owned by this domain.
        Called during initialization to populate self._entities.
        """
        pass

    # ------------------------------------------------------------------
    # CONCRETE — shared behavior
    # ------------------------------------------------------------------

    def initialize(self) -> None:
        """Set up the domain by registering all metrics and entities."""
        self.register_entities()
        self.register_metrics()
        print(f"✅ Domain '{self.get_domain_name()}' initialized: "
              f"{len(self._entities)} entities, {len(self._metrics)} metrics")

    def add_metric(self, metric: MetricDefinition) -> None:
        """Register a metric with this domain."""
        self._metrics.append(metric)

    def add_entity(self, entity: DomainEntity) -> None:
        """Register an entity with this domain."""
        self._entities.append(entity)

    def get_metrics(self) -> List[MetricDefinition]:
        return self._metrics.copy()

    def get_entities(self) -> List[DomainEntity]:
        return self._entities.copy()

    def get_metric(self, name: str) -> Optional[MetricDefinition]:
        """Look up a metric by name."""
        return next((m for m in self._metrics if m.name == name), None)

    def catalog(self) -> Dict:
        """Return a full catalog of this domain's contents."""
        return {
            "domain": self.get_domain_name(),
            "description": self.get_description(),
            "entities": [
                {"name": e.name, "primary_key": e.primary_key, "mart": e.mart_model}
                for e in self._entities
            ],
            "metrics": [
                {
                    "name": m.name,
                    "label": m.label,
                    "aggregation": m.aggregation_type,
                    "grain": m.grain,
                    "unit": m.unit
                }
                for m in self._metrics
            ]
        }
