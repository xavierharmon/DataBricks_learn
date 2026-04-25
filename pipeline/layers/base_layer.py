"""
pipeline/layers/base_layer.py
-----------------------------------------------------------------------
BASE LAYER — Abstract Base Class for all Medallion Architecture layers

WHAT IS AN ABSTRACT BASE CLASS (ABC)?
    An ABC defines a CONTRACT — a set of methods that every subclass
    MUST implement. It can't be instantiated directly; it's a blueprint.

    Think of it like an interface in Java/TypeScript, but in Python.

WHY USE OOP HERE?
    Each layer (Staging, Intermediate, Mart) shares common behaviors:
      - They all have models (SQL files)
      - They all have a schema they write to
      - They all can be validated and documented

    OOP lets us define that shared contract ONCE and enforce it everywhere.
    When you add a new layer, Python will tell you if you've forgotten
    to implement a required method.
-----------------------------------------------------------------------
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum


class Materialization(Enum):
    """
    How dbt materializes a model in the warehouse.
    Using an Enum instead of plain strings prevents typos and
    makes valid values self-documenting.
    """
    VIEW = "view"           # SQL view — no data stored, always fresh
    TABLE = "table"         # Physical table — fast reads, uses storage
    INCREMENTAL = "incremental"  # Only processes new rows — efficient for large tables
    EPHEMERAL = "ephemeral"  # CTE only — never written to warehouse


class LayerType(Enum):
    """The three layers of the medallion architecture."""
    STAGING = "staging"
    INTERMEDIATE = "intermediate"
    MART = "mart"


@dataclass
class ModelMetadata:
    """
    Metadata about a single dbt model.

    Using @dataclass means Python auto-generates __init__, __repr__,
    and __eq__ for us — less boilerplate.
    """
    name: str                                    # e.g., "stg_orders"
    layer: LayerType                             # Which layer it belongs to
    materialization: Materialization             # How it's stored
    grain: str                                   # What does one row represent?
    description: str                             # Human-readable purpose
    depends_on: List[str] = field(default_factory=list)  # Upstream models/sources
    tags: List[str] = field(default_factory=list)

    def __post_init__(self):
        """Validate the model metadata after initialization."""
        if not self.grain:
            raise ValueError(f"Model '{self.name}' must define its grain. "
                             "Ask: 'What does one row represent?'")
        if not self.description:
            raise ValueError(f"Model '{self.name}' must have a description.")


class BaseLayer(ABC):
    """
    Abstract base class for all medallion architecture layers.

    Every concrete layer (StagingLayer, IntermediateLayer, MartLayer)
    must implement the abstract methods defined here.

    Args:
        schema_name: The database schema this layer writes to
        warehouse_catalog: Unity Catalog name in Databricks
    """

    def __init__(self, schema_name: str, warehouse_catalog: str = "main"):
        self.schema_name = schema_name
        self.warehouse_catalog = warehouse_catalog
        self._models: List[ModelMetadata] = []

    # ------------------------------------------------------------------
    # ABSTRACT METHODS — subclasses MUST implement these
    # If you don't, Python raises TypeError when you try to instantiate
    # ------------------------------------------------------------------

    @abstractmethod
    def get_layer_type(self) -> LayerType:
        """Return the LayerType enum value for this layer."""
        pass

    @abstractmethod
    def get_default_materialization(self) -> Materialization:
        """
        Return the default materialization strategy for this layer.
        Staging → VIEW, Marts → TABLE, etc.
        """
        pass

    @abstractmethod
    def validate_model(self, model: ModelMetadata) -> List[str]:
        """
        Validate that a model follows this layer's conventions.
        Returns a list of validation errors (empty = valid).
        """
        pass

    # ------------------------------------------------------------------
    # CONCRETE METHODS — shared behavior all layers inherit
    # ------------------------------------------------------------------

    def register_model(self, model: ModelMetadata) -> None:
        """
        Register a model with this layer.
        Validates it first and raises ValueError if invalid.
        """
        errors = self.validate_model(model)
        if errors:
            raise ValueError(
                f"Model '{model.name}' failed validation for "
                f"{self.get_layer_type().value} layer:\n" +
                "\n".join(f"  - {e}" for e in errors)
            )
        self._models.append(model)
        print(f"✅ Registered model: {model.name} → {self.schema_name}")

    def get_models(self) -> List[ModelMetadata]:
        """Return all registered models for this layer."""
        return self._models.copy()  # Return a copy to prevent mutation

    def get_full_schema_path(self) -> str:
        """Return the fully qualified schema path for Databricks."""
        return f"{self.warehouse_catalog}.{self.schema_name}"

    def summary(self) -> dict:
        """Return a summary dict useful for documentation and debugging."""
        return {
            "layer": self.get_layer_type().value,
            "schema": self.get_full_schema_path(),
            "model_count": len(self._models),
            "models": [
                {
                    "name": m.name,
                    "materialization": m.materialization.value,
                    "grain": m.grain,
                    "depends_on": m.depends_on
                }
                for m in self._models
            ]
        }

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"schema='{self.schema_name}', "
            f"models={len(self._models)})"
        )
