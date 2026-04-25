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
