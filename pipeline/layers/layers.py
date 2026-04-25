"""
pipeline/layers/staging_layer.py
pipeline/layers/intermediate_layer.py
pipeline/layers/mart_layer.py
-----------------------------------------------------------------------
CONCRETE LAYER IMPLEMENTATIONS

These classes INHERIT from BaseLayer and implement the abstract methods.
Each enforces the conventions of its layer.
-----------------------------------------------------------------------
"""

from typing import List
from .base_layer import BaseLayer, LayerType, Materialization, ModelMetadata


# ======================================================================
# STAGING LAYER
# ======================================================================

class StagingLayer(BaseLayer):
    """
    The STAGING layer (Bronze → Silver).

    Conventions this layer ENFORCES:
      - Model names must start with 'stg_'
      - Default materialization is VIEW (no data stored)
      - Models may NOT have more than one source in depends_on
        (staging = 1:1 with source tables, no joins!)
      - Grain must be defined

    Inheritance: StagingLayer → BaseLayer → ABC
    """

    def get_layer_type(self) -> LayerType:
        return LayerType.STAGING

    def get_default_materialization(self) -> Materialization:
        # Staging models are views — they're lenses over raw data
        return Materialization.VIEW

    def validate_model(self, model: ModelMetadata) -> List[str]:
        """
        Enforce staging layer conventions.
        Returns a list of error messages. Empty list = valid.
        """
        errors = []

        # Convention: staging model names must start with 'stg_'
        if not model.name.startswith("stg_"):
            errors.append(
                f"Staging models must be named 'stg_<source_table>'. "
                f"Got: '{model.name}'"
            )

        # Convention: staging models should only read from ONE source
        # If you're joining, it belongs in intermediate, not staging
        source_deps = [d for d in model.depends_on if d.startswith("source:")]
        model_deps = [d for d in model.depends_on if not d.startswith("source:")]

        if len(source_deps) > 1:
            errors.append(
                f"Staging models should have exactly 1 source dependency. "
                f"Found {len(source_deps)}: {source_deps}. "
                f"If you need to join, use an intermediate model."
            )

        if len(model_deps) > 0:
            errors.append(
                f"Staging models should not reference other dbt models. "
                f"Found: {model_deps}. Move joins to intermediate layer."
            )

        # Convention: materialization should be VIEW in staging
        if model.materialization not in (Materialization.VIEW, Materialization.EPHEMERAL):
            errors.append(
                f"Staging models should be materialized as VIEW or EPHEMERAL. "
                f"Got: {model.materialization.value}. "
                f"Tables in staging waste storage for no benefit."
            )

        return errors


# ======================================================================
# INTERMEDIATE LAYER
# ======================================================================

class IntermediateLayer(BaseLayer):
    """
    The INTERMEDIATE layer (Silver → Gold). Domain logic lives here.

    Conventions this layer ENFORCES:
      - Model names must start with 'int_'
      - Default materialization is VIEW
      - Models MUST reference at least one upstream dbt model (not just sources)
      - This is where joins, aggregations, and business logic belong
    """

    def get_layer_type(self) -> LayerType:
        return LayerType.INTERMEDIATE

    def get_default_materialization(self) -> Materialization:
        return Materialization.VIEW

    def validate_model(self, model: ModelMetadata) -> List[str]:
        errors = []

        # Convention: intermediate model names must start with 'int_'
        if not model.name.startswith("int_"):
            errors.append(
                f"Intermediate models must be named 'int_<description>'. "
                f"Got: '{model.name}'"
            )

        # Convention: intermediate models should reference staging models
        model_deps = [d for d in model.depends_on if not d.startswith("source:")]
        if len(model_deps) == 0:
            errors.append(
                f"Intermediate models should reference at least one staging or "
                f"other intermediate model. Got no model dependencies. "
                f"Are you sure this belongs in intermediate?"
            )

        return errors


# ======================================================================
# MART LAYER
# ======================================================================

class MartLayer(BaseLayer):
    """
    The MART layer (Gold → Semantic). BI-ready tables live here.

    Conventions this layer ENFORCES:
      - Fact tables must start with 'fct_'
      - Dimension tables must start with 'dim_'
      - Default materialization is TABLE (BI tools query this directly)
      - Models must reference intermediate models (not staging directly)
    """

    def get_layer_type(self) -> LayerType:
        return LayerType.MART

    def get_default_materialization(self) -> Materialization:
        # Marts are TABLES — BI tools need fast query performance
        # We pay the storage cost to get the speed benefit
        return Materialization.TABLE

    def validate_model(self, model: ModelMetadata) -> List[str]:
        errors = []

        # Convention: mart models must be named fct_ or dim_
        valid_prefixes = ("fct_", "dim_", "rpt_")  # rpt_ for report-specific models
        if not any(model.name.startswith(p) for p in valid_prefixes):
            errors.append(
                f"Mart models must start with 'fct_' (facts), 'dim_' (dimensions), "
                f"or 'rpt_' (reports). Got: '{model.name}'"
            )

        # Convention: marts should reference intermediate models, not staging
        stg_deps = [d for d in model.depends_on if d.startswith("stg_")]
        if stg_deps:
            errors.append(
                f"Mart models should reference intermediate models, not staging directly. "
                f"Found staging dependencies: {stg_deps}. "
                f"Add an intermediate model between staging and this mart."
            )

        # Convention: fact tables should be materialized as TABLE or INCREMENTAL
        if model.name.startswith("fct_"):
            if model.materialization == Materialization.VIEW:
                errors.append(
                    f"Fact tables should not be VIEWs — BI tools query them constantly "
                    f"and views will be slow. Use TABLE or INCREMENTAL."
                )

        return errors
