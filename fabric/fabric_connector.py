"""
pipeline/fabric/fabric_connector.py
-----------------------------------------------------------------------
FABRIC CONNECTOR — Python class for Azure Fabric integration

This class provides a programmatic interface for:
  1. Validating your Fabric connection
  2. Checking that dbt mart tables exist in Fabric
  3. Generating the semantic model definition
  4. Exporting DAX measures to a file for import into Fabric

WHY THIS CLASS EXISTS:
  The rest of the Python pipeline layer defines the architecture
  conceptually (domains, layers, metrics). This class connects
  those definitions to the actual Fabric environment — it's the
  bridge between "what we want" and "what Fabric has."

USAGE:
  python pipeline/fabric/fabric_connector.py

DEPENDENCIES:
  pip install pyodbc azure-identity
-----------------------------------------------------------------------
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
import os
import json


@dataclass
class FabricConfig:
    """
    Configuration for connecting to Azure Fabric.
    Reads from environment variables — never hardcode credentials.
    """
    server: str = field(
        default_factory=lambda: os.getenv("FABRIC_SERVER", "")
    )
    database: str = field(
        default_factory=lambda: os.getenv("FABRIC_DATABASE", "ecommerce_lakehouse")
    )
    schema: str = field(
        default_factory=lambda: os.getenv("FABRIC_SCHEMA", "marts")
    )
    driver: str = "ODBC Driver 18 for SQL Server"

    def __post_init__(self):
        if not self.server:
            raise ValueError(
                "FABRIC_SERVER environment variable not set.\n"
                "export FABRIC_SERVER='your-endpoint.datawarehouse.fabric.microsoft.com'"
            )

    @property
    def connection_string(self) -> str:
        """Build the ODBC connection string for Fabric."""
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server},1433;"
            f"DATABASE={self.database};"
            f"Authentication=ActiveDirectoryInteractive;"  # Uses az login session
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
        )


class FabricValidator:
    """
    Validates that your Fabric environment matches what dbt built.

    Checks:
      - Can we connect to Fabric?
      - Do the expected mart tables exist?
      - Do they have the expected columns?
      - Do they have data (row count > 0)?
    """

    # The tables dbt should have created in Fabric
    EXPECTED_TABLES = {
        "marts.fct_orders": [
            "order_id", "customer_id", "order_date", "status",
            "is_revenue_order", "net_revenue", "customer_segment"
        ],
        "marts.dim_customers": [
            "customer_id", "email", "customer_segment",
            "lifetime_value", "is_repeat_customer"
        ],
        "marts.dim_products": [
            "product_id", "category", "performance_tier",
            "total_revenue", "gross_margin_rate"
        ]
    }

    def __init__(self, config: FabricConfig):
        self.config = config
        self._connection = None

    def connect(self) -> bool:
        """
        Attempt to connect to Fabric.
        Returns True if successful, False with error details if not.
        """
        try:
            import pyodbc
            self._connection = pyodbc.connect(self.config.connection_string)
            print(f"✅ Connected to Fabric: {self.config.server}")
            return True
        except ImportError:
            print("❌ pyodbc not installed. Run: pip install pyodbc")
            return False
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            print("\n  Troubleshooting:")
            print("  1. Run 'az login' to refresh your Azure session")
            print("  2. Verify FABRIC_SERVER environment variable")
            print("  3. Check ODBC driver: brew install msodbcsql18")
            return False

    def validate_tables(self) -> Dict[str, dict]:
        """
        Check that all expected mart tables exist and have data.
        Returns a report of findings.
        """
        if not self._connection:
            if not self.connect():
                return {}

        results = {}
        cursor = self._connection.cursor()

        for table_path, expected_columns in self.EXPECTED_TABLES.items():
            schema, table = table_path.split(".")
            result = {"exists": False, "row_count": 0, "missing_columns": []}

            # Check if table exists and get row count
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
                row_count = cursor.fetchone()[0]
                result["exists"] = True
                result["row_count"] = row_count

                # Check for expected columns
                cursor.execute(f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{schema}'
                      AND TABLE_NAME = '{table}'
                """)
                actual_columns = {row[0].lower() for row in cursor.fetchall()}
                result["missing_columns"] = [
                    col for col in expected_columns
                    if col.lower() not in actual_columns
                ]

                status = "✅" if row_count > 0 and not result["missing_columns"] else "⚠️"
                print(f"{status} {table_path}: {row_count:,} rows")
                if result["missing_columns"]:
                    print(f"   Missing columns: {result['missing_columns']}")

            except Exception as e:
                print(f"❌ {table_path}: {e}")
                result["error"] = str(e)

            results[table_path] = result

        return results

    def close(self):
        if self._connection:
            self._connection.close()


class FabricSemanticModelExporter:
    """
    Exports the semantic model definition and DAX measures
    to files that can be imported into Azure Fabric.

    This bridges the Python semantic model definition
    (in pipeline/semantic/semantic_model.py) with the
    actual Fabric UI configuration.
    """

    def __init__(self, output_dir: str = "fabric/semantic_model"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def export_dax_measures(self, output_file: str = "fabric/dax/generated_measures.dax"):
        """
        Generate a DAX measures file from the Python metric definitions.
        This creates a .dax file you can copy-paste into Fabric.
        """
        # Import the semantic model from the pipeline layer
        try:
            import sys
            sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            from pipeline.semantic.semantic_model import EcommerceSemanticModel
            from pipeline.semantic.metrics import SemanticMetric

            model = EcommerceSemanticModel()

            lines = [
                "-- AUTO-GENERATED DAX MEASURES",
                "-- Generated from pipeline/semantic/semantic_model.py",
                "-- Copy each measure into Fabric Semantic Model → New Measure",
                "",
            ]

            for table_name, table in model._tables.items():
                lines.append(f"-- {'='*60}")
                lines.append(f"-- TABLE: {table_name}")
                lines.append(f"-- {'='*60}")
                lines.append("")

                for measure in table.measures:
                    dax = measure.to_dax_measure(table_name)
                    lines.append(f"-- {measure.description}")
                    lines.append(dax)
                    lines.append("")

            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, "w") as f:
                f.write("\n".join(lines))

            print(f"✅ DAX measures exported to: {output_file}")

        except ImportError as e:
            print(f"⚠️  Could not import semantic model: {e}")
            print("   Using static DAX file at fabric/dax/all_measures.dax instead")

    def export_model_definition(self):
        """Export the semantic model JSON definition."""
        definition_file = os.path.join(
            self.output_dir, "ecommerce_semantic_model.json"
        )
        if os.path.exists(definition_file):
            print(f"✅ Semantic model definition: {definition_file}")
            with open(definition_file) as f:
                model = json.load(f)
            print(f"   Tables: {[t['name'] for t in model.get('tables', [])]}")
            print(f"   Relationships: {len(model.get('relationships', []))}")
        else:
            print(f"⚠️  Model definition not found: {definition_file}")


def main():
    """
    Run all Fabric validation and export tasks.
    Usage: python pipeline/fabric/fabric_connector.py
    """
    print("\n🔷 AZURE FABRIC CONNECTOR")
    print("=" * 50)

    # Export semantic model artifacts
    print("\n📤 Exporting semantic model artifacts...")
    exporter = FabricSemanticModelExporter()
    exporter.export_dax_measures()
    exporter.export_model_definition()

    # Validate Fabric connection (only if FABRIC_SERVER is set)
    fabric_server = os.getenv("FABRIC_SERVER")
    if fabric_server:
        print(f"\n🔌 Validating Fabric connection ({fabric_server})...")
        try:
            config = FabricConfig()
            validator = FabricValidator(config)

            if validator.connect():
                print("\n📊 Checking mart tables...")
                validator.validate_tables()
            validator.close()
        except ValueError as e:
            print(f"⚠️  {e}")
    else:
        print("\nℹ️  FABRIC_SERVER not set — skipping connection validation.")
        print("   Set it to validate your Fabric environment:")
        print("   export FABRIC_SERVER='your-endpoint.datawarehouse.fabric.microsoft.com'")

    print("\n" + "=" * 50)
    print("📖 Next steps:")
    print("  1. Run: ./switch_target.sh fabric")
    print("  2. Run: az login")
    print("  3. Run: cd dbt_project && dbt run --profile ecommerce_fabric")
    print("  4. Follow: fabric/setup/FABRIC_SETUP_GUIDE.md")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()
