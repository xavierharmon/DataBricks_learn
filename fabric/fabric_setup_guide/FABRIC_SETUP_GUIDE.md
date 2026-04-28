# Azure Fabric Setup Guide
### Connecting This Project to Azure Fabric + Power BI

---

## What You're Setting Up

By the end of this guide you will have:
- A free Azure Fabric trial workspace
- Your sample data loaded into a Fabric Lakehouse
- dbt connected to Fabric and running your models
- A Fabric Semantic Model built on top of your marts
- A basic Power BI report pulling from the semantic model

**Pre-requisite:** Complete the Databricks setup first. Your dbt models
should already be running successfully against Databricks before moving to Fabric.

**Time estimate:** 60-90 minutes

---

## How Fabric Differs From Databricks (Quick Recap)

| | Databricks | Azure Fabric |
|--|--|--|
| Compute engine | Apache Spark | Spark + SQL Analytics |
| Storage | Delta Lake | OneLake (Delta under the hood) |
| SQL dialect | Spark SQL | T-SQL (SQL Server style) |
| BI layer | External (Power BI) | Built-in (native Power BI) |
| Semantic model | Not included | First-class feature |
| dbt adapter | `dbt-databricks` | `dbt-fabric` |

The dbt SQL you've written works on both — the platform_adapter macros
handle the dialect differences automatically.

---

## Stage 1: Start Your Azure Fabric Trial

### 1.1 Create a Microsoft account (if needed)
If you don't have a Microsoft / Azure account:
- Go to: **https://account.microsoft.com**
- Create a free account with any email

### 1.2 Start the Fabric trial

1. Go to: **https://app.fabric.microsoft.com**
2. Sign in with your Microsoft account
3. You'll be prompted to start a **free 60-day trial**
4. Click **"Start trial"** — no credit card required
5. Choose a region close to you (e.g., East US)

You'll land in your **Fabric workspace** — think of this as your project folder
in the cloud.

### 1.3 Create a dedicated workspace

1. Click **Workspaces** in the left sidebar
2. Click **"New workspace"**
3. Name it: `ecommerce_analytics_dev`
4. Click **Apply**

Always work in your own workspace, not the default "My Workspace."

---

## Stage 2: Create a Lakehouse

A Lakehouse in Fabric is the equivalent of a Databricks schema with Delta tables.
It's where your data lives — both raw files and structured tables.

### 2.1 Create the Lakehouse

1. In your workspace, click **"New item"**
2. Select **"Lakehouse"**
3. Name it: `ecommerce_lakehouse`
4. Click **Create**

You'll see two sections:
- **Files** — raw file storage (like an S3 bucket or ADLS)
- **Tables** — Delta tables you can query with SQL

### 2.2 Get your SQL endpoint (save this for dbt config)

1. In your Lakehouse, click **"SQL endpoint"** in the top right
2. Copy the **connection string** — it looks like:
   `abc123xyz.datawarehouse.fabric.microsoft.com`
3. Save this — it's your Fabric host in profiles.yml

---

## Stage 3: Load Your Sample Data into Fabric

### 3.1 Upload your CSV files

1. In your Lakehouse, click on the **Files** section
2. Click **"Upload"** → **"Upload files"**
3. Upload all 4 CSVs from your `data/bronze/` folder:
   - `orders.csv`
   - `customers.csv`
   - `products.csv`
   - `order_items.csv`

### 3.2 Promote CSVs to Delta Tables

Fabric won't automatically recognize uploaded CSVs as queryable tables.
You need to load them into the Tables section using a Notebook.

1. In your workspace, click **"New item"** → **"Notebook"**
2. Make sure your Lakehouse is attached (click "Add Lakehouse" if needed)
3. Paste and run this PySpark code:

```python
# Load all Bronze CSV files into Delta tables in the Lakehouse
# Run each cell separately so you can see any errors clearly

# --- Orders ---
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("Files/orders.csv")
df.write.mode("overwrite").saveAsTable("orders")
print(f"✅ orders: {df.count()} rows")

# --- Customers ---
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("Files/customers.csv")
df.write.mode("overwrite").saveAsTable("customers")
print(f"✅ customers: {df.count()} rows")

# --- Products ---
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("Files/products.csv")
df.write.mode("overwrite").saveAsTable("products")
print(f"✅ products: {df.count()} rows")

# --- Order Items ---
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("Files/order_items.csv")
df.write.mode("overwrite").saveAsTable("order_items")
print(f"✅ order_items: {df.count()} rows")
```

4. After running, click **Tables** in your Lakehouse — you should see all 4 tables

### 3.3 Verify with SQL

1. Click **"SQL endpoint"** in your Lakehouse
2. Run this in the SQL query editor:

```sql
SELECT TOP 5 * FROM orders;
SELECT COUNT(*) as customer_count FROM customers;
```

If you see data — your Bronze layer is ready in Fabric.

---

## Stage 4: Install the Fabric dbt Adapter

Back on your Mac, with your virtual environment active:

```bash
source .venv/bin/activate

# Install the Fabric adapter (separate from dbt-databricks)
pip install dbt-fabric

# Install the Microsoft ODBC driver (required for Fabric connection)
# This is a one-time system install
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew install msodbcsql18
```

Verify the install:
```bash
dbt --version
# Should show dbt-fabric in the plugins list
```

---

## Stage 5: Configure Azure CLI Authentication

Fabric uses Azure Active Directory, not a simple token like Databricks.
The easiest approach for learning is Azure CLI authentication.

### 5.1 Install Azure CLI

```bash
brew install azure-cli
```

### 5.2 Log in to Azure

```bash
az login
```

This opens a browser window. Sign in with the same Microsoft account
you used to create your Fabric trial.

After signing in, your terminal will show your subscription details.
You're now authenticated — dbt will use this session automatically.

### 5.3 Verify authentication

```bash
az account show
```

You should see your account name and subscription ID.

---

## Stage 6: Update profiles.yml for Fabric

Open `~/.dbt/profiles.yml` and fill in the Fabric section:

```yaml
ecommerce_fabric:
  target: fabric_dev
  outputs:
    fabric_dev:
      type: fabric
      server: YOUR_SQL_ENDPOINT_HERE      # e.g. abc123.datawarehouse.fabric.microsoft.com
      port: 1433
      database: ecommerce_lakehouse       # Your Lakehouse name
      schema: dev_yourname                # Your personal dev schema
      driver: "ODBC Driver 18 for SQL Server"
      authentication: CLI                 # Uses your az login session
```

**Replace:**
- `YOUR_SQL_ENDPOINT_HERE` → the SQL endpoint from Stage 2.2

---

## Stage 7: Switch the Project to Fabric and Run dbt

### 7.1 Use the switch script

```bash
# From the project root folder
./switch_target.sh fabric
```

This swaps the sources file to point at your Fabric Lakehouse.

### 7.2 Test the connection

```bash
cd dbt_project
dbt debug --profile ecommerce_fabric
```

You should see: `All checks passed!`

Common issues:
- ODBC driver not found → make sure `brew install msodbcsql18` completed
- Auth error → run `az login` again
- Server not found → double-check the SQL endpoint URL in profiles.yml

### 7.3 Run the models

```bash
# Install packages (if not already done)
dbt deps

# Run staging
dbt run --profile ecommerce_fabric --select staging

# Run intermediate
dbt run --profile ecommerce_fabric --select intermediate

# Run marts
dbt run --profile ecommerce_fabric --select marts

# Test
dbt test --profile ecommerce_fabric
```

### 7.4 Verify in Fabric

1. Go back to your Fabric Lakehouse
2. Click **Tables** — you should see new schemas:
   - `staging` → stg_orders, stg_customers, stg_products, stg_order_items
   - `intermediate` → int_orders_enriched, int_customer_orders, int_product_revenue
   - `marts` → fct_orders, dim_customers, dim_products
3. Click **SQL endpoint** and query your mart:

```sql
SELECT TOP 20
    order_date,
    status,
    net_revenue,
    customer_segment,
    acquisition_channel
FROM marts.fct_orders
ORDER BY order_date DESC;
```

---

## Stage 8: Build the Fabric Semantic Model

This is the step that doesn't exist in Databricks — it's unique to Fabric.

### 8.1 Create a new Semantic Model

1. In your Fabric workspace, click **"New item"**
2. Select **"Semantic model"**
3. Name it: `ecommerce_semantic_model`
4. Select your Lakehouse as the data source
5. Check these tables:
   - `marts.fct_orders`
   - `marts.dim_customers`
   - `marts.dim_products`
6. Click **Confirm**

### 8.2 Create the relationship

1. Open the semantic model → click **"Model"** view (bottom of screen)
2. You'll see three table boxes
3. Drag `fct_orders[customer_id]` → `dim_customers[customer_id]`
4. A relationship line appears — this is how Power BI knows to join the tables

### 8.3 Add DAX measures

All measures are defined in `fabric/dax/all_measures.dax`.
Add them one at a time:

1. Click on the `fct_orders` table in the Model view
2. Click **"New measure"** in the top ribbon
3. Paste the first measure:

```dax
Total Revenue = CALCULATE(SUM(fct_orders[net_revenue]), fct_orders[is_revenue_order] = 1)
```

4. Set the format to **Currency**
5. Repeat for each measure in the DAX file

Start with these four to get a working report quickly:
- `Total Revenue` (from fct_orders)
- `Revenue Order Count` (from fct_orders)
- `Average Order Value` (from fct_orders)
- `Customer Count` (from dim_customers)

### 8.4 Save the semantic model

Click **Save**. Your semantic model is now ready for Power BI reports.

---

## Stage 9: Build Your First Power BI Report

1. In your Fabric workspace, click **"New item"** → **"Report"**
2. Select **"Use a published semantic model"**
3. Choose `ecommerce_semantic_model`
4. Click **Create**

You're now in the Power BI report editor. Try building:

**Revenue by Month chart:**
- Visualization: Line chart
- X-axis: `fct_orders[order_month]`
- Y-axis: `[Total Revenue]` (your DAX measure)

**Revenue by Customer Segment:**
- Visualization: Bar chart
- X-axis: `fct_orders[customer_segment]`
- Y-axis: `[Total Revenue]`

**KPI Cards (top of report):**
- One card for `[Total Revenue]`
- One card for `[Revenue Order Count]`
- One card for `[Average Order Value]`
- One card for `[Customer Count]`

This is the full stack working end to end:
```
CSV files → Fabric Lakehouse (Bronze)
    → dbt staging models (Silver)
    → dbt intermediate models (Gold)
    → dbt mart models (fct_orders, dim_customers, dim_products)
    → Fabric Semantic Model (dimensions + DAX measures)
    → Power BI Report (business users)
```

---

## Switching Between Platforms

```bash
# Switch to Databricks
./switch_target.sh databricks
cd dbt_project && dbt run --profile ecommerce_databricks

# Switch to Fabric
./switch_target.sh fabric
cd dbt_project && dbt run --profile ecommerce_fabric
```

The SQL models are identical — the platform_adapter macros handle
the Spark SQL vs T-SQL differences automatically.

---

## Troubleshooting

### "ODBC Driver not found"
```bash
# Verify the driver is installed
odbcinst -q -d
# Should show: [ODBC Driver 18 for SQL Server]

# If missing, reinstall:
brew install msodbcsql18
```

### "Login failed for user"
Your `az login` session may have expired.
```bash
az login
# Then retry dbt debug
```

### "Table not found" in Fabric
Make sure you ran the Notebook in Stage 3.2 to promote CSVs to Delta tables.
Check Fabric → Lakehouse → Tables to confirm they exist.

### "Schema does not exist"
Fabric may need the schema created manually first:
```sql
-- Run in Fabric SQL endpoint
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
```

### dbt test failures on Fabric
Some test failures are caused by type differences between platforms.
Fabric is strict about BIT vs boolean comparisons. The models in this
project use 1/0 integers throughout to avoid this, but if you
add new models, use integers for boolean-like columns.
