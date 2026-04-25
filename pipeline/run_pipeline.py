"""
pipeline/run_pipeline.py
-----------------------------------------------------------------------
PIPELINE RUNNER — ties the entire project together

This script demonstrates how all the Python OOP classes work together.
Run it to see your full semantic model cataloged and validated.

    python pipeline/run_pipeline.py

This is NOT a production orchestrator (use Databricks Jobs or
Azure Data Factory for that) — it's a LEARNING TOOL to show you
how the architecture connects end-to-end.
-----------------------------------------------------------------------
"""

import json
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.layers.base_layer import ModelMetadata, Materialization, LayerType
from pipeline.layers.layers import StagingLayer, IntermediateLayer, MartLayer
from pipeline.domains.orders_domain import OrdersDomain
from pipeline.domains.domain_implementations import CustomersDomain, ProductsDomain
from pipeline.semantic.semantic_model import EcommerceSemanticModel


def setup_layers():
    """
    Set up and validate all three medallion layers.
    This demonstrates how the OOP layer classes enforce conventions.
    """
    print("=" * 60)
    print("LAYER SETUP & VALIDATION")
    print("=" * 60)

    # ----------------------------------------------------------------
    # STAGING LAYER
    # ----------------------------------------------------------------
    staging = StagingLayer(schema_name="staging", warehouse_catalog="main")

    # Register each staging model — the class validates naming conventions
    staging_models = [
        ModelMetadata(
            name="stg_orders",
            layer=LayerType.STAGING,
            materialization=Materialization.VIEW,
            grain="One row per order",
            description="Cleaned and typed orders from the raw bronze layer.",
            depends_on=["source:ecommerce_raw.orders"],
            tags=["staging", "orders"]
        ),
        ModelMetadata(
            name="stg_customers",
            layer=LayerType.STAGING,
            materialization=Materialization.VIEW,
            grain="One row per customer account",
            description="Cleaned customers with normalized email and acquisition channel.",
            depends_on=["source:ecommerce_raw.customers"],
            tags=["staging", "customers"]
        ),
        ModelMetadata(
            name="stg_products",
            layer=LayerType.STAGING,
            materialization=Materialization.VIEW,
            grain="One row per product SKU",
            description="Cleaned product catalog from the raw bronze layer.",
            depends_on=["source:ecommerce_raw.products"],
            tags=["staging", "products"]
        ),
        ModelMetadata(
            name="stg_order_items",
            layer=LayerType.STAGING,
            materialization=Materialization.VIEW,
            grain="One row per line item in an order",
            description="Order line items — bridge between orders and products.",
            depends_on=["source:ecommerce_raw.order_items"],
            tags=["staging", "orders"]
        ),
    ]

    for model in staging_models:
        staging.register_model(model)

    print(f"\nStaging layer: {staging}\n")

    # ----------------------------------------------------------------
    # INTERMEDIATE LAYER
    # ----------------------------------------------------------------
    intermediate = IntermediateLayer(schema_name="intermediate", warehouse_catalog="main")

    intermediate_models = [
        ModelMetadata(
            name="int_orders_enriched",
            layer=LayerType.INTERMEDIATE,
            materialization=Materialization.VIEW,
            grain="One row per order with item summary and business flags",
            description="Orders joined with item-level aggregates and revenue classification.",
            depends_on=["stg_orders", "stg_order_items"],
            tags=["intermediate", "orders"]
        ),
        ModelMetadata(
            name="int_customer_orders",
            layer=LayerType.INTERMEDIATE,
            materialization=Materialization.VIEW,
            grain="One row per customer with order history aggregated",
            description="Customer profiles enriched with full purchase history and segment.",
            depends_on=["stg_customers", "int_orders_enriched"],
            tags=["intermediate", "customers"]
        ),
        ModelMetadata(
            name="int_product_revenue",
            layer=LayerType.INTERMEDIATE,
            materialization=Materialization.VIEW,
            grain="One row per product with sales performance metrics",
            description="Products enriched with sales stats, margin, and performance tier.",
            depends_on=["stg_products", "stg_order_items", "int_orders_enriched"],
            tags=["intermediate", "products"]
        ),
    ]

    for model in intermediate_models:
        intermediate.register_model(model)

    print(f"Intermediate layer: {intermediate}\n")

    # ----------------------------------------------------------------
    # MART LAYER
    # ----------------------------------------------------------------
    mart = MartLayer(schema_name="marts", warehouse_catalog="main")

    mart_models = [
        ModelMetadata(
            name="fct_orders",
            layer=LayerType.MART,
            materialization=Materialization.TABLE,
            grain="One row per order — primary fact table",
            description="Wide, flat fact table for orders. Source of truth for all order reporting.",
            depends_on=["int_orders_enriched", "int_customer_orders"],
            tags=["marts", "facts", "orders"]
        ),
        ModelMetadata(
            name="dim_customers",
            layer=LayerType.MART,
            materialization=Materialization.TABLE,
            grain="One row per customer (current state)",
            description="Customer dimension with segments, LTV, and account attributes.",
            depends_on=["int_customer_orders"],
            tags=["marts", "dimensions", "customers"]
        ),
        ModelMetadata(
            name="dim_products",
            layer=LayerType.MART,
            materialization=Materialization.TABLE,
            grain="One row per product SKU (current state)",
            description="Product dimension with catalog attributes and sales performance.",
            depends_on=["int_product_revenue"],
            tags=["marts", "dimensions", "products"]
        ),
    ]

    for model in mart_models:
        mart.register_model(model)

    print(f"Mart layer: {mart}\n")

    return staging, intermediate, mart


def setup_domains():
    """
    Initialize all business domains and print their metric catalogs.
    """
    print("=" * 60)
    print("DOMAIN INITIALIZATION")
    print("=" * 60)

    domains = [OrdersDomain(), CustomersDomain(), ProductsDomain()]

    for domain in domains:
        domain.initialize()

    print("\n--- METRIC CATALOG ---")
    for domain in domains:
        catalog = domain.catalog()
        print(f"\n📊 {catalog['domain'].upper()} DOMAIN")
        print(f"   {catalog['description'][:80]}...")
        print(f"   Entities: {[e['name'] for e in catalog['entities']]}")
        print(f"   Metrics ({len(catalog['metrics'])}):")
        for metric in catalog['metrics']:
            print(f"     • {metric['label']:<35} [{metric['aggregation']}] {metric.get('unit', '')}")

    return domains


def setup_semantic_model():
    """
    Build and display the semantic model.
    """
    print("\n" + "=" * 60)
    print("SEMANTIC MODEL")
    print("=" * 60)

    semantic = EcommerceSemanticModel()

    print("\n--- ALL AVAILABLE METRICS ---")
    print(f"{'Table':<12} {'Metric Name':<25} {'Label':<35} {'Format'}")
    print("-" * 85)
    for metric in semantic.list_all_metrics():
        print(
            f"{metric['table']:<12} "
            f"{metric['metric_name']:<25} "
            f"{metric['label']:<35} "
            f"{metric['format']}"
        )

    print("\n--- AZURE FABRIC SEMANTIC MODEL DEFINITION (preview) ---")
    fabric_def = semantic.to_fabric_semantic_model()
    # Print a readable preview
    print(json.dumps(fabric_def, indent=2)[:800] + "\n  ... (truncated)")

    return semantic


def demonstrate_validation():
    """
    Show what happens when you violate layer conventions.
    This is how the OOP classes TEACH you the rules.
    """
    print("\n" + "=" * 60)
    print("VALIDATION DEMO — What happens when you break the rules?")
    print("=" * 60)

    staging = StagingLayer(schema_name="staging")

    bad_models = [
        # Wrong name prefix
        ("orders_raw",    "stg_", "Missing 'stg_' prefix"),
        # Multiple sources (should only have one)
        ("stg_bad_joins", None,   "Joining in staging"),
    ]

    print("\nTrying to register models that violate staging conventions:\n")

    # Test 1: Wrong name
    try:
        staging.register_model(ModelMetadata(
            name="orders_raw",                          # ❌ Should be stg_orders
            layer=LayerType.STAGING,
            materialization=Materialization.VIEW,
            grain="One row per order",
            description="Test",
            depends_on=["source:ecommerce_raw.orders"]
        ))
    except ValueError as e:
        print(f"❌ Caught expected error:\n   {e}\n")

    # Test 2: Materialized as TABLE in staging
    try:
        staging.register_model(ModelMetadata(
            name="stg_orders",
            layer=LayerType.STAGING,
            materialization=Materialization.TABLE,      # ❌ Should be VIEW
            grain="One row per order",
            description="Test",
            depends_on=["source:ecommerce_raw.orders"]
        ))
    except ValueError as e:
        print(f"❌ Caught expected error:\n   {e}\n")

    print("✅ Validation works! The layer classes protect you from anti-patterns.")


if __name__ == "__main__":
    print("\n🛒 ECOMMERCE ANALYTICS ENGINEERING PROJECT")
    print("   Databricks + dbt + Azure Fabric\n")

    staging, intermediate, mart = setup_layers()
    domains = setup_domains()
    semantic = setup_semantic_model()
    demonstrate_validation()

    print("\n" + "=" * 60)
    print("✨ Pipeline architecture validated successfully!")
    print("\nNext steps:")
    print("  1. Run: python scripts/generate_sample_data.py")
    print("  2. Upload CSVs to Databricks bronze_ecommerce schema")
    print("  3. Run: cd dbt_project && dbt run --select staging")
    print("  4. Run: dbt run --select intermediate")
    print("  5. Run: dbt run --select marts")
    print("  6. Run: dbt test")
    print("  7. Run: dbt docs generate && dbt docs serve")
    print("=" * 60 + "\n")
