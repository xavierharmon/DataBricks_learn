# 🛒 E-Commerce Analytics Engineering Project
### Learning Path: Data Warehouse → Domains → Semantic Model

---

## 🎯 What You'll Learn

This project teaches you **Analytical Engineering** end-to-end using:
- **Databricks** (Delta Lake) as your data warehouse
- **dbt** (data build tool) for SQL transformations
- **Azure Fabric / OneLake** as your semantic serving layer
- **Python OOP** to understand the architecture programmatically

---

## 🏗️ The Medallion Architecture

The core concept you need to internalize. Every layer has ONE job:

```
RAW DATA (Bronze)
    ↓  "Clean it"
STAGING (Silver)        ← dbt staging models
    ↓  "Shape it"
INTERMEDIATE (Gold)     ← dbt intermediate models (your DOMAINS live here)
    ↓  "Serve it"
MARTS / SEMANTIC        ← dbt mart models (ready for Power BI / Fabric)
```

**Rule of thumb:** If you're fixing a typo → Staging. If you're joining tables → Intermediate. If you're defining a business metric → Mart.

---

## 📁 Project Structure

```
ecommerce_analytics/
│
├── dbt_project/                    # All SQL transformations
│   ├── dbt_project.yml             # dbt config (warehouse connection, paths)
│   ├── profiles.yml                # Databricks connection profile
│   ├── models/
│   │   ├── staging/                # LAYER 1: Bronze → Silver
│   │   │   ├── _sources.yml        # Declares your raw tables
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   └── stg_products.sql
│   │   ├── intermediate/           # LAYER 2: Silver → Gold (Domains)
│   │   │   ├── int_orders_enriched.sql
│   │   │   ├── int_customer_orders.sql
│   │   │   └── int_product_revenue.sql
│   │   └── marts/                  # LAYER 3: Gold → Semantic
│   │       ├── orders/
│   │       │   └── fct_orders.sql
│   │       ├── customers/
│   │       │   └── dim_customers.sql
│   │       └── products/
│   │           └── dim_products.sql
│   ├── tests/                      # Data quality tests
│   ├── macros/                     # Reusable SQL snippets
│   └── seeds/                      # Small static CSV reference data
│
├── pipeline/                       # Python OOP layer (learn the concepts)
│   ├── layers/                     # Base classes for each medallion layer
│   │   ├── base_layer.py
│   │   ├── staging_layer.py
│   │   ├── intermediate_layer.py
│   │   └── mart_layer.py
│   ├── domains/                    # Domain objects (Orders, Customers, Products)
│   │   ├── base_domain.py
│   │   ├── orders_domain.py
│   │   ├── customers_domain.py
│   │   └── products_domain.py
│   └── semantic/                   # Semantic model definitions
│       ├── semantic_model.py
│       └── metrics.py
│
├── scripts/
│   └── generate_sample_data.py     # Creates fake e-commerce data to practice with
│
└── docs/
    └── domain_glossary.md          # Business definitions for every field
```

---

## 🚀 Getting Started (Step by Step)

### Step 1 — Understand the raw data
Read `scripts/generate_sample_data.py` — it creates the Bronze tables you'll transform.

### Step 2 — Run the staging models
```bash
cd dbt_project
dbt run --select staging
```
These models clean and rename raw columns. Nothing fancy.

### Step 3 — Run the intermediate (domain) models
```bash
dbt run --select intermediate
```
This is where business logic lives. Orders get enriched with customer data, etc.

### Step 4 — Run the marts
```bash
dbt run --select marts
```
Wide, flat tables ready for Power BI or Azure Fabric semantic models.

### Step 5 — Run tests
```bash
dbt test
```
Validates uniqueness, not-null, referential integrity.

---

## 🧠 Key Concepts Glossary

| Term | What it means |
|------|--------------|
| **Source** | Raw table in your warehouse you didn't create |
| **Staging model** | 1:1 with a source, just cleaned |
| **Intermediate model** | Joins/aggregations, business logic |
| **Mart model** | Final table a business user queries |
| **Grain** | What does ONE row represent? Always ask this. |
| **Fact table** | Measures events (orders, clicks, payments) |
| **Dimension table** | Describes things (customers, products) |
| **Semantic model** | Defines metrics ON TOP of mart tables |


![Image description here](https://github.com/xavierharmon/DataBricks_learn/blob/main/dbt-dag.png)