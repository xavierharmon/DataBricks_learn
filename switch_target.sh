#!/bin/bash
# switch_target.sh
# -----------------------------------------------------------------------
# ONE-COMMAND PLATFORM SWITCHER
# Switches the entire project between Databricks and Azure Fabric.
#
# USAGE:
#   ./switch_target.sh databricks
#   ./switch_target.sh fabric
#
# WHAT IT DOES:
#   1. Swaps the active _sources.yml for the correct platform
#   2. Updates dbt_project.yml to use the correct profile
#   3. Sets the correct environment variable reminders
#   4. Confirms what you need to run next
#
# WHY A SCRIPT AND NOT JUST A FLAG?
#   The _sources.yml file needs to point at different databases depending
#   on the platform. dbt doesn't support conditional sources natively,
#   so we swap the file. Everything else is handled by the profiles.yml
#   and the platform_adapter macros automatically.
# -----------------------------------------------------------------------

set -e  # Exit on any error

PLATFORM=$1
PROJECT_DIR="$(cd "$(dirname "$0")/dbt_project" && pwd)"
STAGING_DIR="$PROJECT_DIR/models/staging"

# -----------------------------------------------------------------------
# Validate input
# -----------------------------------------------------------------------
if [ -z "$PLATFORM" ]; then
    echo ""
    echo "❌  Missing argument. Usage:"
    echo "    ./switch_target.sh databricks"
    echo "    ./switch_target.sh fabric"
    echo ""
    exit 1
fi

if [ "$PLATFORM" != "databricks" ] && [ "$PLATFORM" != "fabric" ]; then
    echo ""
    echo "❌  Unknown platform: '$PLATFORM'"
    echo "    Valid options: databricks, fabric"
    echo ""
    exit 1
fi

# -----------------------------------------------------------------------
# Switch sources file
# -----------------------------------------------------------------------
echo ""
echo "🔄  Switching to: $PLATFORM"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$PLATFORM" = "databricks" ]; then
    cp "$STAGING_DIR/_sources_databricks.yml" "$STAGING_DIR/_sources.yml"
    echo "✅  Sources   → _sources_databricks.yml (hive_metastore / Unity Catalog)"
else
    cp "$STAGING_DIR/_sources_fabric.yml" "$STAGING_DIR/_sources.yml"
    echo "✅  Sources   → _sources_fabric.yml (Fabric Lakehouse)"
fi

# -----------------------------------------------------------------------
# Show the correct dbt commands for this platform
# -----------------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋  Commands to run against $PLATFORM:"
echo ""

if [ "$PLATFORM" = "databricks" ]; then
    echo "    # Test connection"
    echo "    cd dbt_project"
    echo "    dbt debug --profile ecommerce_databricks"
    echo ""
    echo "    # Run all models"
    echo "    dbt run --profile ecommerce_databricks"
    echo ""
    echo "    # Run by layer"
    echo "    dbt run --profile ecommerce_databricks --select staging"
    echo "    dbt run --profile ecommerce_databricks --select intermediate"
    echo "    dbt run --profile ecommerce_databricks --select marts"
    echo ""
    echo "    # Run tests"
    echo "    dbt test --profile ecommerce_databricks"
    echo ""
    echo "🔑  Required environment variable:"
    echo "    export DATABRICKS_TOKEN='dapi...'"
else
    echo "    # Test connection"
    echo "    cd dbt_project"
    echo "    dbt debug --profile ecommerce_fabric"
    echo ""
    echo "    # Run all models"
    echo "    dbt run --profile ecommerce_fabric"
    echo ""
    echo "    # Run by layer"
    echo "    dbt run --profile ecommerce_fabric --select staging"
    echo "    dbt run --profile ecommerce_fabric --select intermediate"
    echo "    dbt run --profile ecommerce_fabric --select marts"
    echo ""
    echo "    # Run tests"
    echo "    dbt test --profile ecommerce_fabric"
    echo ""
    echo "🔑  Required setup:"
    echo "    brew install azure-cli          # if not installed"
    echo "    az login                        # authenticate with Azure"
    echo "    pip install dbt-fabric          # if not installed"
    echo ""
    echo "📦  Also install the ODBC driver for Mac:"
    echo "    https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✨  Platform switched to: $PLATFORM"
echo ""