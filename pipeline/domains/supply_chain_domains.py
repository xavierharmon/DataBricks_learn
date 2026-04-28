"""
pipeline/domains/supply_chain_domains.py
-----------------------------------------------------------------------
SUPPLY CHAIN DOMAIN IMPLEMENTATIONS

Four new domains added in the supply chain expansion:
  1. SuppliersDomain    — supplier performance, fill rates, lead times
  2. ManufacturersDomain — production efficiency, yield, defect rates
  3. InventoryDomain   — stock health, reorder alerts, movement tracking
  4. MarginDomain      — true landed cost vs catalog cost vs selling price

Each domain:
  - Declares its entities (what it tracks)
  - Defines its metrics (what it measures, with precise definitions)
  - Is registered in run_pipeline.py alongside the existing domains

WHY FOUR SEPARATE DOMAINS?
  Each domain has a distinct business owner and a separate set of
  questions it answers:
    Suppliers    → Procurement team: "Are our suppliers reliable?"
    Manufacturers → Operations team: "Are our factories efficient?"
    Inventory    → Warehouse team:   "Do we have the right stock levels?"
    Margin       → Finance team:     "What is our true profitability?"

  Keeping them separate means ownership is clear and metric definitions
  don't bleed across business boundaries.
-----------------------------------------------------------------------
"""

from .base_domain import BaseDomain, MetricDefinition, DomainEntity


# ======================================================================
# SUPPLIERS DOMAIN
# ======================================================================

class SuppliersDomain(BaseDomain):
    """
    The Suppliers Domain owns all data about supplier companies,
    their contracts, and their purchase order performance.

    Source tables:
      - bronze_ecommerce.suppliers
      - bronze_ecommerce.supplier_materials
      - bronze_ecommerce.purchase_orders
      - bronze_ecommerce.purchase_order_items

    Mart models produced:
      - dim_suppliers
    """

    def get_domain_name(self) -> str:
        return "suppliers"

    def get_description(self) -> str:
        return (
            "The Suppliers Domain owns all data about the companies we purchase "
            "raw materials from. It tracks purchase order volume, delivery "
            "performance (on-time rate, fill rate), cost variance vs quoted prices, "
            "and overall supplier scoring. Supplier tier classification (gold through "
            "probation) is the primary procurement tool for vendor management decisions."
        )

    def register_entities(self) -> None:
        self.add_entity(DomainEntity(
            name="Supplier",
            plural="Suppliers",
            primary_key="supplier_id",
            mart_model="dim_suppliers",
            description=(
                "A company that provides raw materials to us via purchase orders. "
                "One supplier can provide multiple materials. "
                "Suppliers are classified into tiers based on composite performance scores."
            )
        ))

        self.add_entity(DomainEntity(
            name="PurchaseOrder",
            plural="PurchaseOrders",
            primary_key="purchase_order_id",
            mart_model="dim_suppliers",
            description=(
                "A formal order sent to a supplier for one or more raw materials. "
                "Purchase orders are the unit of delivery performance measurement."
            )
        ))

    def register_metrics(self) -> None:

        self.add_metric(MetricDefinition(
            name="on_time_delivery_rate",
            label="On-Time Delivery Rate",
            description=(
                "Percentage of received purchase orders delivered on or before "
                "the expected delivery date. Calculated per supplier. "
                "Gold tier threshold: >= 95% OTD."
            ),
            sql_expression=(
                "count(case when actual_delivery_date <= expected_delivery_date "
                "          and actual_delivery_date is not null then 1 end) "
                "/ nullif(count(case when status = 'received' then 1 end), 0)"
            ),
            aggregation_type="average",
            grain="purchase_order",
            unit="%",
            owner="procurement_team"
        ))

        self.add_metric(MetricDefinition(
            name="actual_fill_rate",
            label="Actual Fill Rate",
            description=(
                "Total quantity received / total quantity ordered across all "
                "received purchase orders. A fill rate of 1.0 means the supplier "
                "shipped exactly what was ordered. Below 0.90 triggers supplier review. "
                "Note: this is ACTUAL fill rate from received POs, not the quoted rate."
            ),
            sql_expression=(
                "sum(qty_received) / nullif(sum(qty_ordered), 0)"
            ),
            aggregation_type="average",
            grain="purchase_order_item",
            unit="%",
            owner="procurement_team"
        ))

        self.add_metric(MetricDefinition(
            name="supplier_cost_variance",
            label="Cost Variance vs Standard",
            description=(
                "Average percentage difference between actual purchase prices paid "
                "and standard material cost. Positive = paying above standard (unfavorable). "
                "Negative = paying below standard (favorable). "
                "Target: within ±5% of standard cost."
            ),
            sql_expression=(
                "avg((unit_price - standard_cost) / nullif(standard_cost, 0))"
            ),
            aggregation_type="average",
            grain="purchase_order_item",
            unit="%",
            owner="procurement_team"
        ))

        self.add_metric(MetricDefinition(
            name="total_po_spend",
            label="Total PO Spend",
            description=(
                "Total dollars spent across all received purchase orders "
                "for a given supplier or time period."
            ),
            sql_expression="sum(total_cost)",
            aggregation_type="sum",
            grain="purchase_order_item",
            unit="$",
            owner="procurement_team"
        ))

        self.add_metric(MetricDefinition(
            name="avg_days_late",
            label="Average Days Late",
            description=(
                "Average number of days late across all late deliveries. "
                "Only counts POs where actual_delivery_date > expected_delivery_date. "
                "Zero means all deliveries were on time. "
                "Alert threshold: > 5 days triggers 'chronically late' flag."
            ),
            sql_expression=(
                "avg(case when actual_delivery_date > expected_delivery_date "
                "    then datediff(actual_delivery_date, expected_delivery_date) end)"
            ),
            aggregation_type="average",
            grain="purchase_order",
            unit="days",
            owner="procurement_team"
        ))


# ======================================================================
# MANUFACTURERS DOMAIN
# ======================================================================

class ManufacturersDomain(BaseDomain):
    """
    The Manufacturers Domain owns all data about the factories that
    produce our finished goods from raw materials.

    Source tables:
      - bronze_ecommerce.manufacturers
      - bronze_ecommerce.production_runs
      - bronze_ecommerce.production_run_inputs

    Mart models produced:
      - dim_manufacturers
    """

    def get_domain_name(self) -> str:
        return "manufacturers"

    def get_description(self) -> str:
        return (
            "The Manufacturers Domain owns all data about the factories that "
            "produce our finished goods. It tracks production run volume, yield "
            "rates (planned vs actual output), defect rates, and true cost per unit "
            "including both labor and material inputs. Manufacturer tier classification "
            "(tier_1 through under_review) drives sourcing decisions."
        )

    def register_entities(self) -> None:
        self.add_entity(DomainEntity(
            name="Manufacturer",
            plural="Manufacturers",
            primary_key="manufacturer_id",
            mart_model="dim_manufacturers",
            description=(
                "A factory that produces finished goods for us from raw materials. "
                "Each manufacturer has a quoted yield rate and defect rate — "
                "actual performance is tracked against these benchmarks."
            )
        ))

        self.add_entity(DomainEntity(
            name="ProductionRun",
            plural="ProductionRuns",
            primary_key="production_run_id",
            mart_model="dim_manufacturers",
            description=(
                "A single manufacturing batch producing a quantity of one product. "
                "Production runs consume raw material inputs and produce finished units. "
                "The unit of efficiency measurement."
            )
        ))

    def register_metrics(self) -> None:

        self.add_metric(MetricDefinition(
            name="overall_yield_rate",
            label="Overall Yield Rate",
            description=(
                "Actual units produced / planned units across all completed runs. "
                "Measures how efficiently a manufacturer converts planned production "
                "into actual finished goods. 1.0 = perfect yield, no waste. "
                "Tier 1 threshold: >= 0.92 (92% yield)."
            ),
            sql_expression=(
                "sum(actual_quantity) / nullif(sum(planned_quantity), 0)"
            ),
            aggregation_type="average",
            grain="production_run",
            unit="%",
            owner="operations_team"
        ))

        self.add_metric(MetricDefinition(
            name="actual_defect_rate",
            label="Actual Defect Rate",
            description=(
                "Total units found defective / total units produced across all "
                "completed runs. Lower is better. "
                "Industry alert threshold: > 3% for most product categories. "
                "Tier 1 threshold: <= 2%."
            ),
            sql_expression=(
                "sum(defects_found) / nullif(sum(actual_quantity), 0)"
            ),
            aggregation_type="average",
            grain="production_run",
            unit="%",
            owner="quality_team"
        ))

        self.add_metric(MetricDefinition(
            name="true_cost_per_unit",
            label="True Cost Per Unit",
            description=(
                "(Total labor cost + total material input cost) / total units produced. "
                "This is the most accurate cost metric — reflects what we actually "
                "spend per finished unit including all inputs. "
                "Compare to quoted cost_per_unit_labor to understand material cost impact."
            ),
            sql_expression=(
                "(sum(total_production_cost) + sum(total_material_cost)) "
                "/ nullif(sum(actual_quantity), 0)"
            ),
            aggregation_type="average",
            grain="production_run",
            unit="$",
            owner="finance_team"
        ))

        self.add_metric(MetricDefinition(
            name="production_completion_rate",
            label="Run Completion Rate",
            description=(
                "Percentage of production runs that reached 'completed' status. "
                "Cancelled runs indicate planning failures or quality issues. "
                "Target: > 90% completion rate."
            ),
            sql_expression=(
                "count(case when status = 'completed' then 1 end) "
                "/ nullif(count(production_run_id), 0)"
            ),
            aggregation_type="average",
            grain="production_run",
            unit="%",
            owner="operations_team"
        ))

        self.add_metric(MetricDefinition(
            name="yield_variance",
            label="Yield Variance vs Quoted",
            description=(
                "actual_yield_rate minus quoted (avg_yield_rate). "
                "Negative = underperforming vs what manufacturer quoted us. "
                "Positive = exceeding expectations. "
                "Alert threshold: < -0.05 (more than 5 points below quoted yield)."
            ),
            sql_expression=(
                "avg(actual_yield_rate) - avg(avg_yield_rate)"
            ),
            aggregation_type="average",
            grain="production_run",
            unit="%",
            owner="operations_team"
        ))


# ======================================================================
# INVENTORY DOMAIN
# ======================================================================

class InventoryDomain(BaseDomain):
    """
    The Inventory Domain owns all data about product stock levels,
    movement history, and replenishment health.

    Source tables:
      - bronze_ecommerce.inventory_movements
      - bronze_ecommerce.production_runs (for pending supply)

    Mart models produced:
      - fct_inventory_health
      - fct_inventory_movements
    """

    def get_domain_name(self) -> str:
        return "inventory"

    def get_description(self) -> str:
        return (
            "The Inventory Domain owns all data about product stock levels "
            "and movement history. It tracks current stock on hand, sales "
            "velocity, days of stock remaining, and health classifications "
            "(stockout through overstock). The reorder_alert flag is the "
            "primary operational output — it drives daily replenishment decisions."
        )

    def register_entities(self) -> None:
        self.add_entity(DomainEntity(
            name="InventoryPosition",
            plural="InventoryPositions",
            primary_key="product_id",
            mart_model="fct_inventory_health",
            description=(
                "The current stock position for a single product. "
                "One row per product in fct_inventory_health. "
                "Refreshed on every dbt run — must run daily for actionable alerts."
            )
        ))

        self.add_entity(DomainEntity(
            name="InventoryMovement",
            plural="InventoryMovements",
            primary_key="movement_id",
            mart_model="fct_inventory_movements",
            description=(
                "A single event that changed stock level for a product. "
                "Types: production_receipt, sale, return, adjustment, damaged_write_off."
            )
        ))

    def register_metrics(self) -> None:

        self.add_metric(MetricDefinition(
            name="total_inventory_value",
            label="Total Inventory Value at Cost",
            description=(
                "Sum of (current_stock_on_hand × product_cost) across all products. "
                "Represents total working capital tied up in inventory. "
                "High overstock inventory_value = capital inefficiency."
            ),
            sql_expression="sum(inventory_value_at_cost)",
            aggregation_type="sum",
            grain="product",
            unit="$",
            owner="warehouse_team"
        ))

        self.add_metric(MetricDefinition(
            name="stockout_count",
            label="Products in Stockout",
            description=(
                "Count of products with current_stock_on_hand <= 0. "
                "These products cannot be sold and represent lost revenue. "
                "Target: 0 stockouts for active products."
            ),
            sql_expression="count(case when is_stockout = 1 then product_id end)",
            aggregation_type="count",
            grain="product",
            unit="products",
            owner="warehouse_team"
        ))

        self.add_metric(MetricDefinition(
            name="reorder_alert_count",
            label="Products Needing Reorder",
            description=(
                "Count of products with reorder_alert = 1 "
                "(stock < 30 days or stockout). "
                "Reviewed daily by warehouse team."
            ),
            sql_expression="count(case when reorder_alert = 1 then product_id end)",
            aggregation_type="count",
            grain="product",
            unit="products",
            owner="warehouse_team"
        ))

        self.add_metric(MetricDefinition(
            name="avg_days_of_stock",
            label="Average Days of Stock",
            description=(
                "Average days_of_stock_remaining across products with velocity. "
                "Target range: 30-90 days across the active catalog. "
                "< 30 days = supply risk. > 120 days = overstock risk."
            ),
            sql_expression=(
                "avg(case when days_of_stock_remaining is not null "
                "    then days_of_stock_remaining end)"
            ),
            aggregation_type="average",
            grain="product",
            unit="days",
            owner="warehouse_team"
        ))

        self.add_metric(MetricDefinition(
            name="overstock_value",
            label="Overstock Inventory Value",
            description=(
                "Total inventory_value_at_cost for products classified as 'overstock' "
                "(> 120 days of stock). Represents capital that could be freed through "
                "markdowns or demand stimulation."
            ),
            sql_expression=(
                "sum(case when stock_health_status = 'overstock' "
                "    then inventory_value_at_cost else 0 end)"
            ),
            aggregation_type="sum",
            grain="product",
            unit="$",
            owner="warehouse_team"
        ))


# ======================================================================
# MARGIN DOMAIN
# ======================================================================

class MarginDomain(BaseDomain):
    """
    The Margin Domain owns the true profitability analysis — the bridge
    between the e-commerce layer and the supply chain layer.

    Source tables / upstream models:
      - int_product_true_margin (combines supply chain + sales data)

    Mart models produced:
      - fct_true_margin
    """

    def get_domain_name(self) -> str:
        return "margin"

    def get_description(self) -> str:
        return (
            "The Margin Domain is the analytical bridge between the e-commerce "
            "and supply chain layers. It defines TRUE profitability metrics that "
            "account for actual production costs (materials + labor + logistics), "
            "not just the catalog cost field. The key insight: catalog_margin_rate "
            "often overstates true margin by 5-15 percentage points when production "
            "costs are accurately captured. The margin_gap metric quantifies this "
            "overstatement product by product."
        )

    def register_entities(self) -> None:
        self.add_entity(DomainEntity(
            name="ProductMargin",
            plural="ProductMargins",
            primary_key="product_id",
            mart_model="fct_true_margin",
            description=(
                "The true margin profile of a single product. "
                "Combines actual selling prices, true landed cost, and "
                "catalog cost for a complete profitability picture."
            )
        ))

    def register_metrics(self) -> None:

        self.add_metric(MetricDefinition(
            name="avg_true_margin_rate",
            label="Average True Margin Rate",
            description=(
                "Average true_margin_rate across products with sufficient data. "
                "(avg_selling_price - true_landed_cost) / avg_selling_price. "
                "This is the most accurate margin metric in the entire platform. "
                "Finance minimum threshold: 20% true margin. "
                "Only includes products with has_true_cost_data = 1."
            ),
            sql_expression=(
                "avg(case when has_true_cost_data = 1 and true_margin_rate is not null "
                "    then true_margin_rate end)"
            ),
            aggregation_type="average",
            grain="product",
            filters=["has_true_cost_data = 1"],
            unit="%",
            owner="finance_team"
        ))

        self.add_metric(MetricDefinition(
            name="margin_at_risk_count",
            label="Products at Margin Risk",
            description=(
                "Count of products with margin_risk_flag = 1 "
                "(true_margin_rate < 20%). "
                "These products need pricing review, cost renegotiation, "
                "or discontinuation decisions."
            ),
            sql_expression="count(case when margin_risk_flag = 1 then product_id end)",
            aggregation_type="count",
            grain="product",
            unit="products",
            owner="finance_team"
        ))

        self.add_metric(MetricDefinition(
            name="avg_catalog_margin_rate",
            label="Average Catalog Margin Rate",
            description=(
                "Average (list_price - catalog_cost) / list_price. "
                "This is the THEORETICAL margin — what we would earn at full "
                "price using catalog costs. Compare to avg_true_margin_rate "
                "to see the overall margin gap."
            ),
            sql_expression=(
                "avg(case when catalog_margin_rate is not null "
                "    then catalog_margin_rate end)"
            ),
            aggregation_type="average",
            grain="product",
            unit="%",
            owner="finance_team"
        ))

        self.add_metric(MetricDefinition(
            name="avg_margin_gap",
            label="Average Margin Gap",
            description=(
                "Average (catalog_margin_rate - true_margin_rate). "
                "Positive = catalog analysis overstates our true profitability. "
                "A gap > 0.05 (5 points) across the catalog is a material "
                "misstatement that affects pricing and product decisions. "
                "This metric quantifies how much catalog analysis misleads us."
            ),
            sql_expression=(
                "avg(case when margin_gap is not null then margin_gap end)"
            ),
            aggregation_type="average",
            grain="product",
            unit="%",
            owner="finance_team"
        ))

        self.add_metric(MetricDefinition(
            name="catalog_overstating_count",
            label="Products Where Catalog Overstates Margin",
            description=(
                "Count of products where margin_gap > 5 percentage points. "
                "These are products where relying on catalog margin for "
                "pricing decisions would lead to unprofitable choices."
            ),
            sql_expression=(
                "count(case when catalog_overstates_margin = 1 then product_id end)"
            ),
            aggregation_type="count",
            grain="product",
            unit="products",
            owner="finance_team"
        ))
