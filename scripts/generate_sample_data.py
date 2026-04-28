"""
scripts/generate_sample_data.py
-----------------------------------------------------------------------
SAMPLE DATA GENERATOR

This script creates realistic fake e-commerce data you can load into
Databricks to practice your transformations on.

It generates four CSV files that represent your Bronze (raw) layer:
  - orders.csv
  - customers.csv
  - products.csv
  - order_items.csv

HOW TO USE:
    python scripts/generate_sample_data.py

    Then upload the CSVs to your Databricks workspace:
    1. Databricks → Data → Create Table → Upload File
    2. Set catalog: main, schema: bronze_ecommerce

DEPENDENCIES:
    pip install faker pandas

WHY FAKER?
    Faker generates realistic fake data (names, emails, addresses).
    This lets you practice with data that LOOKS real without using
    any actual customer information.
-----------------------------------------------------------------------
"""

import random
import csv
import os
from datetime import datetime, timedelta

# Try to import faker; guide the user if not installed
try:
    from faker import Faker
except ImportError:
    print("❌ Missing dependency. Run: pip install faker")
    exit(1)

try:
    import pandas as pd
    USE_PANDAS = True
except ImportError:
    USE_PANDAS = False
    print("ℹ️  pandas not found — will write raw CSV instead")

# -----------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------

SEED = 42                   # Random seed for reproducibility
N_CUSTOMERS = 500
N_PRODUCTS = 80
N_ORDERS = 2000
N_ORDER_ITEMS = 5000        # Total line items across all orders

OUTPUT_DIR = "data/bronze"  # Where to save the CSV files

random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

# -----------------------------------------------------------------------
# REFERENCE DATA
# -----------------------------------------------------------------------

PRODUCT_CATEGORIES = {
    "Electronics":    ["Headphones", "Phone Case", "USB Cable", "Power Bank", "Smart Watch"],
    "Clothing":       ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Hat"],
    "Home & Kitchen": ["Coffee Mug", "Cutting Board", "Water Bottle", "Candle", "Throw Pillow"],
    "Books":          ["Fiction Novel", "Cookbook", "Self-Help Book", "Tech Guide", "Biography"],
    "Sports":         ["Yoga Mat", "Resistance Bands", "Running Socks", "Water Bottle", "Jump Rope"],
}

ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "refunded"]
# Weighted so most orders are delivered (realistic distribution)
STATUS_WEIGHTS = [0.05, 0.08, 0.12, 0.60, 0.10, 0.05]

ACQUISITION_CHANNELS = [
    "ORGANIC SEARCH", "Paid Search", "social media",    # Intentionally inconsistent
    "EMAIL", "Referral", "organic",                      # to practice staging cleanup
    "PAID_SEARCH", "Social Media"
]

# -----------------------------------------------------------------------
# GENERATORS
# -----------------------------------------------------------------------

def generate_customers(n: int) -> list:
    """Generate n customer records with intentional data quality issues."""
    customers = []
    for i in range(1, n + 1):
        # Intentionally mix email case — staging will normalize this
        email = fake.email()
        if i % 3 == 0:
            email = email.upper()  # Some emails are uppercase in the source

        customers.append({
            "customer_id": f"CUST-{i:05d}",
            "full_name": fake.name(),
            "email": email,
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "US",
            "postal_code": fake.zipcode(),
            "acquisition_channel": random.choice(ACQUISITION_CHANNELS),
            "is_active": random.random() > 0.05,  # 95% active
            "created_at": fake.date_time_between(
                start_date="-3y", end_date="-1m"
            ).isoformat(),
            "updated_at": fake.date_time_between(
                start_date="-1m", end_date="now"
            ).isoformat(),
            "_loaded_at": datetime.now().isoformat(),
        })
    return customers


def generate_products(n: int) -> list:
    """Generate n product records."""
    products = []
    product_id = 1

    for category, items in PRODUCT_CATEGORIES.items():
        for item in items:
            if product_id > n:
                break
            cost = round(random.uniform(5, 150), 2)
            # Price is cost + 40-120% markup
            price = round(cost * random.uniform(1.4, 2.2), 2)

            products.append({
                "product_id": f"PROD-{product_id:04d}",
                "product_name": f"{fake.color_name()} {item}",
                "category": category,
                "subcategory": item,
                "sku": f"SKU-{fake.bothify('??###').upper()}",
                "price": price,
                "cost": cost,
                "stock_quantity": random.randint(0, 500),
                "status": random.choices(
                    ["active", "active", "active", "discontinued", "draft"],
                    weights=[70, 10, 10, 7, 3]
                )[0],
                "created_at": fake.date_time_between(
                    start_date="-2y", end_date="-3m"
                ).isoformat(),
                "updated_at": fake.date_time_between(
                    start_date="-3m", end_date="now"
                ).isoformat(),
                "_loaded_at": datetime.now().isoformat(),
            })
            product_id += 1

    return products


def generate_orders(n: int, customer_ids: list) -> list:
    """Generate n order records."""
    orders = []
    for i in range(1, n + 1):
        order_date = fake.date_time_between(start_date="-2y", end_date="now")
        status = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0]

        base_amount = round(random.uniform(15, 800), 2)
        shipping = round(random.uniform(0, 25), 2)
        discount = round(base_amount * random.uniform(0, 0.2), 2) if random.random() > 0.7 else 0

        orders.append({
            "order_id": f"ORD-{i:06d}",
            "customer_id": random.choice(customer_ids),
            "order_date": order_date.date().isoformat(),
            "status": status,
            "total_amount": round(base_amount + shipping - discount, 2),
            "shipping_amount": shipping,
            "discount_amount": discount,
            "updated_at": (order_date + timedelta(days=random.randint(0, 14))).isoformat(),
            "_loaded_at": datetime.now().isoformat(),
        })
    return orders


def generate_order_items(n: int, order_ids: list, product_ids: list, products: list) -> list:
    """Generate n order item records."""
    # Build a product lookup for pricing
    product_lookup = {p["product_id"]: p for p in products}
    items = []

    for i in range(1, n + 1):
        product_id = random.choice(product_ids)
        product = product_lookup[product_id]
        quantity = random.randint(1, 4)
        unit_price = product["price"]
        discount = round(unit_price * random.uniform(0, 0.15), 2) if random.random() > 0.8 else 0

        items.append({
            "order_item_id": f"ITEM-{i:07d}",
            "order_id": random.choice(order_ids),
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_amount": discount,
            "_loaded_at": datetime.now().isoformat(),
        })
    return items


# -----------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------

def main():
    print("🛒 Generating e-commerce sample data...\n")

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Generate each dataset
    print(f"  👤 Generating {N_CUSTOMERS} customers...")
    customers = generate_customers(N_CUSTOMERS)

    print(f"  📦 Generating {N_PRODUCTS} products...")
    products = generate_products(N_PRODUCTS)

    print(f"  🛍️  Generating {N_ORDERS} orders...")
    customer_ids = [c["customer_id"] for c in customers]
    orders = generate_orders(N_ORDERS, customer_ids)

    print(f"  📋 Generating {N_ORDER_ITEMS} order items...")
    order_ids = [o["order_id"] for o in orders]
    product_ids = [p["product_id"] for p in products]
    order_items = generate_order_items(N_ORDER_ITEMS, order_ids, product_ids, products)

    # Write to CSV
    datasets = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }

    for name, data in datasets.items():
        filepath = os.path.join(OUTPUT_DIR, f"{name}.csv")
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"  ✅ Wrote {len(data):,} rows → {filepath}")

    print(f"\n✨ Done! Files saved to ./{OUTPUT_DIR}/")
    print("\n📤 Next steps:")
    print("  1. Upload CSVs to Databricks: Data → Create Table → Upload File")
    print("  2. Set catalog=main, schema=bronze_ecommerce for each table")
    print("  3. Run: cd dbt_project && dbt run --select staging")
    print("  4. Inspect the views created in your dev schema")
    print("\n💡 Tip: Notice the messy email casing and inconsistent acquisition_channel values.")
    print("   Your stg_customers.sql will clean those up!")


if __name__ == "__main__":
    main()


# =============================================================================
# SUPPLY CHAIN DATA GENERATORS
# Added to extend the base e-commerce dataset with manufacturing/supplier data
# =============================================================================

def generate_raw_materials(n: int = 30) -> list:
    """
    Generate raw materials that go into finished products.
    Examples: fabric, plastic pellets, electronic components, cardboard.
    """
    material_types = [
        ("Cotton Fabric",        "textile",    "meters",    2.50,  8.00),
        ("Polyester Fabric",     "textile",    "meters",    1.20,  4.50),
        ("ABS Plastic Pellets",  "polymer",    "kg",        1.80,  3.20),
        ("Silicone Rubber",      "polymer",    "kg",        4.50, 12.00),
        ("Copper Wire",          "metal",      "meters",    0.80,  2.10),
        ("Aluminum Sheet",       "metal",      "kg",        3.20,  6.80),
        ("Stainless Steel",      "metal",      "kg",        2.90,  5.40),
        ("Cardboard Sheet",      "packaging",  "units",     0.15,  0.45),
        ("Bubble Wrap Roll",     "packaging",  "meters",    0.40,  1.20),
        ("Foam Padding",         "packaging",  "kg",        1.10,  3.30),
        ("Cotton Thread",        "textile",    "spools",    0.90,  2.80),
        ("Elastic Band",         "textile",    "meters",    0.20,  0.65),
        ("Rubber Sole Material", "polymer",    "kg",        3.80,  9.50),
        ("PP Plastic Sheet",     "polymer",    "kg",        1.60,  3.90),
        ("Tempered Glass",       "glass",      "units",     4.20, 11.00),
        ("PCB Components",       "electronic", "units",     8.50, 24.00),
        ("Lithium Battery Cell", "electronic", "units",    12.00, 28.00),
        ("LED Components",       "electronic", "units",     2.30,  7.50),
        ("Nylon Fiber",          "textile",    "kg",        3.10,  7.20),
        ("Paper Pulp",           "cellulose",  "kg",        0.60,  1.80),
        ("Dye Concentrate",      "chemical",   "liters",    5.50, 14.00),
        ("Adhesive Resin",       "chemical",   "liters",    3.20,  8.40),
        ("Zipper Components",    "hardware",   "units",     0.45,  1.30),
        ("Metal Buckles",        "hardware",   "units",     0.30,  0.95),
        ("Velcro Strip",         "hardware",   "meters",    0.55,  1.60),
        ("Ink Cartridge",        "chemical",   "units",     6.80, 18.00),
        ("Soy Wax",              "natural",    "kg",        2.20,  5.50),
        ("Essential Oils",       "natural",    "ml",        8.00, 22.00),
        ("Ceramic Clay",         "natural",    "kg",        1.40,  3.70),
        ("Bamboo Fiber",         "natural",    "kg",        2.80,  6.90),
    ]
    materials = []
    for i, (name, category, unit, min_cost, max_cost) in enumerate(material_types[:n], 1):
        materials.append({
            "material_id":       f"MAT-{i:04d}",
            "material_name":     name,
            "material_category": category,
            "unit_of_measure":   unit,
            "standard_cost":     round(random.uniform(min_cost, max_cost), 2),
            "reorder_point":     random.randint(50, 500),
            "current_stock":     random.randint(0, 2000),
            "lead_time_days":    random.randint(3, 45),
            "created_at":        fake.date_time_between(start_date="-3y", end_date="-1y").isoformat(),
            "_loaded_at":        datetime.now().isoformat(),
        })
    return materials


def generate_suppliers(n: int = 12) -> list:
    """
    Generate supplier companies. Each supplier provides specific materials.
    Mix of domestic and international to enable lead time analysis.
    """
    suppliers_data = [
        ("TechFab Industries",      "US",  "CA", "domestic",      8,  15, 0.96),
        ("GlobalWeave Co",          "CN",  None, "international", 25,  45, 0.94),
        ("PolyMat Solutions",       "US",  "TX", "domestic",      5,  12, 0.98),
        ("EuroComponent GmbH",      "DE",  None, "international", 18,  30, 0.97),
        ("PacificRaw Materials",    "VN",  None, "international", 30,  55, 0.91),
        ("NaturaSupply Corp",       "US",  "OR", "domestic",     10,  20, 0.95),
        ("MetalWorks International","KR",  None, "international", 20,  38, 0.93),
        ("PackRight Solutions",     "US",  "OH", "domestic",      4,   9, 0.99),
        ("ChemSource Ltd",          "IN",  None, "international", 22,  40, 0.92),
        ("ElectroParts Asia",       "TW",  None, "international", 15,  28, 0.96),
        ("FiberFirst Textiles",     "BD",  None, "international", 28,  50, 0.90),
        ("AlpineNatural Goods",     "CH",  None, "international", 12,  22, 0.98),
    ]
    suppliers = []
    for i, (name, country, state, s_type, min_lt, max_lt, fill_rate) in \
            enumerate(suppliers_data[:n], 1):
        suppliers.append({
            "supplier_id":              f"SUP-{i:04d}",
            "supplier_name":            name,
            "country":                  country,
            "state":                    state,
            "supplier_type":            s_type,
            "avg_lead_time_days":       random.randint(min_lt, max_lt),
            "quoted_fill_rate":         fill_rate,
            "payment_terms_days":       random.choice([15, 30, 45, 60]),
            "is_preferred":             random.random() > 0.4,
            "quality_rating":           round(random.uniform(3.5, 5.0), 1),
            "onboarded_at":             fake.date_time_between(
                                            start_date="-4y", end_date="-1y").isoformat(),
            "_loaded_at":               datetime.now().isoformat(),
        })
    return suppliers


def generate_supplier_materials(suppliers: list, materials: list) -> list:
    """
    Junction table: which supplier provides which materials, at what price.
    A material can have multiple suppliers (enables cheapest-source analysis).
    """
    records = []
    rec_id = 1
    supplier_ids = [s["supplier_id"] for s in suppliers]

    for mat in materials:
        # Each material gets 1-3 suppliers
        n_suppliers = random.randint(1, 3)
        chosen = random.sample(supplier_ids, min(n_suppliers, len(supplier_ids)))
        base_cost = mat["standard_cost"]

        for sup_id in chosen:
            # Each supplier quotes slightly different prices
            variance = random.uniform(-0.15, 0.25)
            quoted_price = round(base_cost * (1 + variance), 2)
            records.append({
                "supplier_material_id": f"SM-{rec_id:05d}",
                "supplier_id":          sup_id,
                "material_id":          mat["material_id"],
                "quoted_unit_price":    quoted_price,
                "min_order_quantity":   random.choice([10, 25, 50, 100, 250, 500]),
                "lead_time_days":       random.randint(3, 55),
                "is_primary_supplier":  chosen.index(sup_id) == 0,
                "contract_start_date":  fake.date_between(
                                            start_date="-2y", end_date="-3m").isoformat(),
                "contract_end_date":    fake.date_between(
                                            start_date="today", end_date="+1y").isoformat(),
                "_loaded_at":           datetime.now().isoformat(),
            })
            rec_id += 1
    return records


def generate_manufacturers(n: int = 8) -> list:
    """
    Generate manufacturer companies. They take raw materials and produce
    finished goods (our products).
    """
    mfr_data = [
        ("PrecisionMake Co",     "US", "MI", 85, 95, 200),
        ("AsiaFab Solutions",    "CN", None, 80, 92, 500),
        ("EuroQuality GmbH",     "DE", None, 90, 98, 150),
        ("NorthTex Manufacturing","US","NC", 82, 94, 300),
        ("VietProd Industries",  "VN", None, 78, 91, 400),
        ("TechAssemble Ltd",     "TW", None, 88, 96, 250),
        ("MexicoMfg SA",         "MX", None, 83, 93, 350),
        ("DomesticCraft Inc",    "US", "WI", 87, 97, 100),
    ]
    manufacturers = []
    for i, (name, country, state, min_yield, max_yield, capacity) in \
            enumerate(mfr_data[:n], 1):
        manufacturers.append({
            "manufacturer_id":          f"MFR-{i:04d}",
            "manufacturer_name":        name,
            "country":                  country,
            "state":                    state,
            "avg_yield_rate":           round(random.uniform(min_yield, max_yield) / 100, 4),
            "production_capacity_units": capacity,  # daily capacity
            "lead_time_days":           random.randint(7, max(8, min(45, capacity))),  # manufacturing lead time
            "quality_certification":    random.choice(
                                            ["ISO9001", "ISO9001", "ISO14001",
                                             "ISO9001+14001", "none"]),
            "defect_rate":              round(random.uniform(0.005, 0.04), 4),
            "cost_per_unit_labor":      round(random.uniform(1.50, 18.00), 2),
            "onboarded_at":             fake.date_time_between(
                                            start_date="-4y", end_date="-1y").isoformat(),
            "_loaded_at":               datetime.now().isoformat(),
        })
    return manufacturers


def generate_purchase_orders(suppliers: list, materials: list,
                              supplier_materials: list, n: int = 300) -> tuple:
    """
    Generate purchase orders sent to suppliers for raw materials.
    Returns (purchase_orders, purchase_order_items).
    """
    pos, po_items = [], []
    supplier_ids = [s["supplier_id"] for s in suppliers]

    # Build supplier→material lookup
    sm_lookup = {}
    for sm in supplier_materials:
        sm_lookup.setdefault(sm["supplier_id"], []).append(sm)

    po_statuses = ["submitted", "acknowledged", "shipped", "received",
                   "received", "received", "partially_received", "cancelled"]

    for i in range(1, n + 1):
        sup_id      = random.choice(supplier_ids)
        order_date  = fake.date_between(start_date="-2y", end_date="-7d")
        status      = random.choice(po_statuses)
        sup_mats    = sm_lookup.get(sup_id, [])
        if not sup_mats:
            continue

        # Expected delivery based on supplier lead time
        supplier    = next(s for s in suppliers if s["supplier_id"] == sup_id)
        exp_days    = supplier["avg_lead_time_days"] + random.randint(-3, 5)
        exp_deliver = order_date + timedelta(days=max(1, exp_days))

        # Actual delivery for received orders
        actual_deliver = None
        if status in ("received", "partially_received"):
            delay = random.choice([-2, -1, 0, 0, 1, 2, 3, 5, 7])
            actual_deliver = (exp_deliver + timedelta(days=delay)).isoformat()

        pos.append({
            "purchase_order_id":        f"PO-{i:06d}",
            "supplier_id":              sup_id,
            "order_date":               order_date.isoformat(),
            "expected_delivery_date":   exp_deliver.isoformat(),
            "actual_delivery_date":     actual_deliver,
            "status":                   status,
            "currency":                 "USD",
            "notes":                    None,
            "_loaded_at":               datetime.now().isoformat(),
        })

        # 1-5 line items per PO
        chosen_mats = random.sample(sup_mats, min(random.randint(1, 5), len(sup_mats)))
        for j, sm in enumerate(chosen_mats, 1):
            qty_ordered  = random.randint(50, 1000)
            qty_received = None
            if status == "received":
                # Sometimes receive slightly less than ordered (fill rate)
                fill        = random.uniform(0.88, 1.0)
                qty_received = int(qty_ordered * fill)
            elif status == "partially_received":
                qty_received = int(qty_ordered * random.uniform(0.4, 0.75))

            po_items.append({
                "po_item_id":         f"POI-{i:06d}-{j:02d}",
                "purchase_order_id":  f"PO-{i:06d}",
                "material_id":        sm["material_id"],
                "qty_ordered":        qty_ordered,
                "qty_received":       qty_received,
                "unit_price":         sm["quoted_unit_price"],
                "total_cost":         round(qty_ordered * sm["quoted_unit_price"], 2),
                "_loaded_at":         datetime.now().isoformat(),
            })

    return pos, po_items


def generate_production_runs(manufacturers: list, products: list,
                              materials: list, n: int = 200) -> tuple:
    """
    Generate production runs — batches of finished goods manufactured.
    Returns (production_runs, production_run_inputs).
    """
    runs, inputs = [], []
    mfr_ids  = [m["manufacturer_id"] for m in manufacturers]
    prod_ids = [p["product_id"]       for p in products]
    mat_ids  = [m["material_id"]      for m in materials]

    statuses = ["planned", "in_progress", "completed",
                "completed", "completed", "completed", "cancelled"]

    for i in range(1, n + 1):
        mfr        = random.choice(manufacturers)
        prod_id    = random.choice(prod_ids)
        start_date = fake.date_between(start_date="-18M", end_date="-7d")
        status     = random.choice(statuses)

        planned_qty = random.randint(50, 500)
        actual_qty  = None
        end_date    = None

        if status == "completed":
            yield_rate  = mfr["avg_yield_rate"] + random.uniform(-0.05, 0.05)
            actual_qty  = int(planned_qty * max(0.7, min(1.0, yield_rate)))
            duration    = random.randint(2, 21)
            end_date    = (start_date + timedelta(days=duration)).isoformat()
        elif status == "cancelled":
            actual_qty = 0

        # Cost per unit = labor + overhead
        labor_cost   = mfr["cost_per_unit_labor"]
        overhead_pct = random.uniform(0.15, 0.35)
        cost_per_unit = round(labor_cost * (1 + overhead_pct), 2)

        runs.append({
            "production_run_id":    f"PRN-{i:05d}",
            "manufacturer_id":      mfr["manufacturer_id"],
            "product_id":           prod_id,
            "planned_quantity":     planned_qty,
            "actual_quantity":      actual_qty,
            "planned_start_date":   start_date.isoformat(),
            "actual_end_date":      end_date,
            "status":               status,
            "cost_per_unit":        cost_per_unit,
            "defects_found":        int((actual_qty or 0) * mfr["defect_rate"]
                                        * random.uniform(0.5, 1.5))
                                    if actual_qty else 0,
            "quality_passed":       status == "completed",
            "_loaded_at":           datetime.now().isoformat(),
        })

        # 2-4 raw material inputs per production run
        chosen_mats = random.sample(mat_ids, min(random.randint(2, 4), len(mat_ids)))
        for mat_id in chosen_mats:
            mat = next(m for m in materials if m["material_id"] == mat_id)
            qty_used = round(random.uniform(0.5, 20.0) * (planned_qty / 100), 2)
            inputs.append({
                "run_input_id":       f"RNI-{i:05d}-{chosen_mats.index(mat_id)+1:02d}",
                "production_run_id":  f"PRN-{i:05d}",
                "material_id":        mat_id,
                "qty_used":           qty_used,
                "unit_cost":          mat["standard_cost"],
                "total_material_cost":round(qty_used * mat["standard_cost"], 2),
                "_loaded_at":         datetime.now().isoformat(),
            })

    return runs, inputs


def generate_inventory_movements(products: list, production_runs: list,
                                  orders: list) -> list:
    """
    Generate inventory movement records — every time stock changes.
    Movement types:
      production_receipt  — stock added from a completed production run
      sale                — stock removed by a customer order
      adjustment          — manual stock correction
      return              — stock added from a customer return
      damaged_write_off   — stock removed due to damage/expiry
    """
    movements = []
    mov_id    = 1
    prod_ids  = {p["product_id"]: p for p in products}

    # Production receipts — stock in from completed runs
    for run in production_runs:
        if run["status"] == "completed" and run["actual_quantity"]:
            movements.append({
                "movement_id":        f"INV-{mov_id:07d}",
                "product_id":         run["product_id"],
                "movement_type":      "production_receipt",
                "quantity_change":    run["actual_quantity"],
                "reference_id":       run["production_run_id"],
                "movement_date":      run["actual_end_date"],
                "unit_cost":          run["cost_per_unit"],
                "notes":              f"Receipt from production run {run['production_run_id']}",
                "_loaded_at":         datetime.now().isoformat(),
            })
            mov_id += 1

    # Sales — stock out from customer orders
    delivered_orders = [o for o in orders
                        if o["status"] in ("delivered", "shipped", "processing")]
    for order in random.sample(delivered_orders,
                                min(len(delivered_orders), 800)):
        prod_id = random.choice(list(prod_ids.keys()))
        prod    = prod_ids[prod_id]
        qty     = random.randint(1, 5)
        movements.append({
            "movement_id":    f"INV-{mov_id:07d}",
            "product_id":     prod_id,
            "movement_type":  "sale",
            "quantity_change": -qty,
            "reference_id":   order["order_id"],
            "movement_date":  order["order_date"],
            "unit_cost":      round(float(prod.get("cost", 10.0)), 2),
            "notes":          f"Sale from order {order['order_id']}",
            "_loaded_at":     datetime.now().isoformat(),
        })
        mov_id += 1

    # Random adjustments, returns, and write-offs
    adj_types = [
        ("adjustment",       0.5,  -50,  50,  "Cycle count adjustment"),
        ("return",           1.0,    1,   5,  "Customer return"),
        ("damaged_write_off",-1.0,  -20, -1,  "Damaged goods write-off"),
    ]
    for _ in range(120):
        adj_type, sign_mult, min_q, max_q, note = random.choice(adj_types)
        prod_id = random.choice(list(prod_ids.keys()))
        qty     = random.randint(min(abs(min_q), abs(max_q)), max(abs(min_q), abs(max_q)))
        qty     = qty if min_q >= 0 else -qty
        movements.append({
            "movement_id":    f"INV-{mov_id:07d}",
            "product_id":     prod_id,
            "movement_type":  adj_type,
            "quantity_change": qty,
            "reference_id":   None,
            "movement_date":  fake.date_between(
                                  start_date="-18M", end_date="today").isoformat(),
            "unit_cost":      round(float(prod_ids[prod_id].get("cost", 10.0)), 2),
            "notes":          note,
            "_loaded_at":     datetime.now().isoformat(),
        })
        mov_id += 1

    return movements


def main_supply_chain():
    """Generate and save all supply chain CSV files."""
    import os
    print("\n🏭 Generating supply chain data...\n")
    os.makedirs("data/bronze", exist_ok=True)

    # Load existing orders for inventory movements
    import csv
    orders = []
    orders_path = "data/bronze/orders.csv"
    if os.path.exists(orders_path):
        with open(orders_path) as f:
            orders = list(csv.DictReader(f))
    else:
        print("  ⚠️  orders.csv not found — run generate_sample_data.py first")
        return

    # Load existing products for production/inventory linkage
    products_raw = []
    products_path = "data/bronze/products.csv"
    if os.path.exists(products_path):
        with open(products_path) as f:
            products_raw = list(csv.DictReader(f))
    else:
        print("  ⚠️  products.csv not found — run generate_sample_data.py first")
        return

    print("  🧱 Generating raw materials...")
    materials    = generate_raw_materials(30)

    print("  🏢 Generating suppliers...")
    suppliers    = generate_suppliers(12)

    print("  🔗 Generating supplier-material relationships...")
    sup_mats     = generate_supplier_materials(suppliers, materials)

    print("  🏭 Generating manufacturers...")
    manufacturers = generate_manufacturers(8)

    print("  📋 Generating purchase orders...")
    pos, po_items = generate_purchase_orders(suppliers, materials, sup_mats, 300)

    print("  ⚙️  Generating production runs...")
    runs, run_inputs = generate_production_runs(
        manufacturers, products_raw, materials, 200)

    print("  📦 Generating inventory movements...")
    movements    = generate_inventory_movements(products_raw, runs, orders)

    # Write all files
    datasets = {
        "raw_materials":           materials,
        "suppliers":               suppliers,
        "supplier_materials":      sup_mats,
        "manufacturers":           manufacturers,
        "purchase_orders":         pos,
        "purchase_order_items":    po_items,
        "production_runs":         runs,
        "production_run_inputs":   run_inputs,
        "inventory_movements":     movements,
    }

    for name, data in datasets.items():
        if not data:
            print(f"  ⚠️  {name}: no data generated (skipping)")
            continue
        filepath = f"data/bronze/{name}.csv"
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"  ✅ {name}: {len(data):,} rows → {filepath}")

    print(f"\n✨ Supply chain data generated!")
    print("  Upload these CSVs to Databricks bronze_ecommerce schema:")
    for name in datasets:
        print(f"    • {name}.csv")


if __name__ == "__main__":
    main()
    main_supply_chain()
