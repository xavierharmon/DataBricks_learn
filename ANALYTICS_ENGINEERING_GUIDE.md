# Analytics Engineering — A Complete Learning Guide
### Understanding What You're Building, Why It's Built This Way, and How It All Connects

---

## Before You Read Anything Else — The One Idea That Explains Everything

Imagine a restaurant kitchen. Raw ingredients arrive at the back door (raw data). A prep cook cleans and portions them (staging). A line cook combines them into dishes (intermediate). A plated meal goes to the customer (mart/semantic model).

Every decision in this project — every folder, every SQL file, every Python class — is just that kitchen, applied to data. Your job as an **Analytical Engineer** is to be the head chef: designing the kitchen so that anyone can reliably produce the same meal, every time, from whatever ingredients arrive.

---

## Part 1: What Problem Are We Actually Solving?

### The world before analytical engineering

In most companies, this is what happens:

1. A business analyst needs a revenue number for Monday's meeting
2. They write a SQL query against the production database
3. Another analyst needs the same number — they write a slightly different query
4. The numbers don't match
5. Nobody knows which one is right
6. The meeting becomes about the data, not the business

This is sometimes called the **"spreadsheet chaos"** problem, but it also happens in SQL. Everyone has their own version of "total revenue" and none of them agree.

### What analytical engineering fixes

Analytical engineering creates a **single, trusted, documented transformation pipeline** between raw data and business decisions. When someone asks "what was our revenue last quarter?", there is exactly one answer, and everyone knows exactly how it was calculated.

The discipline sits between traditional **data engineering** (moving data) and **data science** (analyzing data):

```
Data Engineer          Analytical Engineer       Data Scientist / Analyst
      │                        │                          │
Moves raw data          Transforms &               Queries trusted
into the warehouse      models it                  data to find insights
(ETL/ELT pipelines)    (THIS PROJECT)             (dashboards, models)
```

---

## Part 2: The Medallion Architecture — The Core Mental Model

This project uses the **Medallion Architecture**, sometimes called Bronze/Silver/Gold. It's the most widely adopted pattern for organizing data transformations in modern data platforms like Databricks.

The name comes from medals: you start with raw bronze and refine it to gold.

### Bronze — Raw Data (You Don't Own This)

Bronze is whatever arrived in your warehouse from the source systems. It could be:
- A Shopify export of orders
- A Salesforce sync of customers
- Event logs from your website
- A CSV someone uploaded

**Key characteristics of Bronze:**
- You never modify it. It's an immutable record of what the source sent you.
- It's often messy: wrong data types, inconsistent casing, null values, duplicate rows
- Column names may be cryptic (`CUST_NO`, `ord_dt`, `AMT`)
- Multiple tables may encode the same concept differently

In this project, Bronze lives in Databricks at `main.bronze_ecommerce`. The `generate_sample_data.py` script creates it.

### Silver — Staged Data (Your Staging Models)

Silver is Bronze that has been **cleaned and standardized** — but not yet combined or analyzed. Think of it as "the raw data, but trustworthy."

**What happens at this layer:**
- Column names become consistent (`CUST_NO` → `customer_id`)
- Data types are correct (`"2024-01-15"` the string → `2024-01-15` the date)
- Values are normalized (`"SHIPPED"`, `"Shipped"`, `"shipped"` → all become `"shipped"`)
- Simple derived values are added (`quantity * price` → `line_total`)

**What does NOT happen here:**
- No joining tables together
- No business logic (no deciding what "counts as revenue")
- No aggregations

In dbt, these are your `stg_` models. Each one maps 1:1 to a Bronze source table.

**Why keep this layer separate?**
Because cleaning data and interpreting data are different jobs. If you mix them, future engineers can't tell whether a transformation is fixing a data quality issue or making a business decision.

### Gold — Intermediate and Mart Models (Business Logic)

Gold is where the intelligence lives. This is split into two sub-layers in this project:

**Intermediate models (`int_`):** Business logic, joins, aggregations — but organized for other engineers to build on top of. These are the "prep cook" layer.

**Mart models (`fct_`, `dim_`):** The final, polished output. These are what business users and BI tools query. These are the "plated dish."

---

## Part 3: dbt — The Tool That Makes This Possible

### What dbt actually is

dbt (data build tool) is a command-line tool that lets you write SQL `SELECT` statements and handles everything else: running them in the right order, creating the tables/views in the warehouse, running tests, and generating documentation.

Before dbt, a SQL transformation pipeline looked like this:
- A Python script that ran SQL files in a specific order
- Manually managed dependencies ("run this after that")
- No built-in testing
- Documentation that was always out of date

dbt solved all of this.

### The single most important dbt concept: `ref()`

When you write a dbt model and want to reference another model, you write:

```sql
select * from {{ ref('stg_orders') }}
```

Instead of:

```sql
select * from main.staging.stg_orders
```

This looks like a small difference but it's massive. When you use `ref()`, dbt:

1. **Knows the dependency** — it won't run this model until `stg_orders` is done
2. **Builds a DAG** — a visual map of every model and how they connect
3. **Handles environments** — in dev it points to `dev_yourname.stg_orders`, in prod it points to `analytics_prod.stg_orders` — automatically

The DAG (Directed Acyclic Graph) is one of the most powerful things dbt produces. Run `dbt docs generate && dbt docs serve` and you'll see a web page showing every model, its upstream dependencies, and its downstream dependents. Click any model and see its SQL, its description, its tests, and its lineage.

### `source()` vs `ref()`

There are two referencing functions in dbt:

- `source('ecommerce_raw', 'orders')` — use this for Bronze tables you **don't own** (external sources)
- `ref('stg_orders')` — use this for dbt models **you do own**

This distinction matters because dbt tracks lineage differently for each. Sources show up as the "roots" of your DAG — the starting points.

### Materialization — a critical decision

Every dbt model has a **materialization strategy** — how it's stored in the warehouse:

| Strategy | What it creates | When to use |
|----------|----------------|-------------|
| `view` | A SQL view — no data stored, query runs every time | Staging and intermediate models |
| `table` | A physical table — data stored, fast queries | Mart models that BI tools hit frequently |
| `incremental` | Only processes new rows since the last run | Large fact tables (millions of rows) |
| `ephemeral` | Just a CTE, never written to the warehouse | Small helper logic |

**Why staging = view and marts = table?**

Staging models are views because they're just lenses over Bronze data. They cost almost nothing to rebuild. There's no point storing a copy.

Mart models are tables because Power BI / Azure Fabric queries them constantly, often with complex filters. If `fct_orders` is a view, every dashboard refresh reruns the entire SQL chain from Bronze. If it's a table, the data is already computed and waiting.

---

## Part 4: Domains — Organizing by Business Concept

### What a domain is

A domain is a **bounded business concept** that owns a set of related data. In this e-commerce project there are three:

- **Orders Domain** — everything about purchase transactions
- **Customers Domain** — everything about who bought something  
- **Products Domain** — everything about what's for sale

Domains are not a dbt concept — they're an **organizational concept** that you impose on your dbt models through folder structure and naming conventions.

### Why domains matter

Without domains, analytical engineering projects become a flat list of models with no clear ownership:

```
models/
  stg_orders.sql
  stg_customers.sql
  orders_with_customers.sql        ← who owns this?
  customer_revenue.sql             ← is this orders or customers?
  product_sales_by_customer.sql   ← ???
```

With domains, every model has a clear home:

```
models/
  staging/          ← source-aligned (one per source table)
  intermediate/     ← domain-aligned (int_orders_*, int_customers_*)
  marts/
    orders/         ← fct_orders
    customers/      ← dim_customers
    products/       ← dim_products
```

When a new analyst joins, they can immediately understand which SQL files are responsible for which business area.

### The Python domain classes in this project

The `pipeline/domains/` Python classes serve as **living documentation** of each domain. They force you to explicitly answer:

- What entities does this domain track? (Orders, Customers, Products)
- What metrics does it define? (revenue, AOV, LTV, cancellation rate)
- What does each metric *precisely* mean? (does "revenue" include shipping? exclude cancelled orders?)

This is more than documentation — it becomes the foundation of your semantic layer.

---

## Part 5: The Semantic Layer — The Final Frontier

### What a semantic model is

A semantic model sits on top of your mart tables and answers the question: **"What can a business user ask about this data, and how is the answer calculated?"**

It's the translation layer between:
- Technical: `sum(case when is_revenue_order then net_revenue else 0 end)`
- Business: "Total Revenue"

Think of it as a **dictionary** that tells your BI tool (Power BI, Tableau, Looker) what every number means and how to compute it.

### Why this matters in Azure Fabric

In Azure Fabric, the semantic model is a first-class citizen. When you create a Power BI dataset connected to your Databricks Gold tables, you're building a semantic model. It defines:

- Which columns are **dimensions** (things you filter and group by: date, country, product category)
- Which columns are **measures** (things you aggregate: revenue, order count, average order value)
- **Relationships** between tables (orders → customers via customer_id)
- **DAX measures** — custom calculations written in Fabric's formula language

Without a semantic model, every analyst writes their own DAX measure for "revenue." They make different choices. The numbers diverge. You're back to the original problem.

With a semantic model, "Total Revenue" is defined once. Every report that uses it gets the same number.

### Dimensions vs Measures — the fundamental distinction

This is the single most important concept in BI:

**Dimension** = something you use to *describe* or *filter* data
- Order date, customer country, product category, order status
- You GROUP BY dimensions
- You put dimensions on chart axes and in slicers/filters

**Measure** = something you *calculate* from data
- Total revenue, order count, average order value, cancellation rate
- You AGGREGATE measures (sum, count, average)
- You put measures on chart values

A common mistake is treating a numeric column as a measure when it's really a dimension. For example, `order_year` is a number (2024) but it's a dimension — you GROUP BY it, you don't SUM it.

The Python `SemanticDimension` and `SemanticMetric` classes in `pipeline/semantic/metrics.py` make this distinction explicit and enforceable.

---

## Part 6: Object-Oriented Python — Why It's Here

### The problem with just writing SQL

SQL is excellent at transforming data. It's not excellent at enforcing conventions. Nothing stops someone from:
- Creating a staging model named `orders_clean.sql` (should be `stg_orders.sql`)
- Building a mart that reads directly from Bronze, skipping staging entirely
- Defining "revenue" differently in two different models

The Python OOP layer in this project serves as a **convention enforcement engine and documentation system**.

### How the class hierarchy works

```
ABC (Python built-in abstract base class)
  └── BaseLayer          ← defines the CONTRACT all layers must follow
        ├── StagingLayer      ← enforces stg_ naming, view materialization, no joins
        ├── IntermediateLayer ← enforces int_ naming, must ref staging models
        └── MartLayer         ← enforces fct_/dim_ naming, table materialization

ABC
  └── BaseDomain         ← defines the CONTRACT all domains must follow
        ├── OrdersDomain      ← owns order metrics and entities
        ├── CustomersDomain   ← owns customer metrics and entities
        └── ProductsDomain    ← owns product metrics and entities
```

### Abstract Base Classes — the key OOP concept

An Abstract Base Class (ABC) is a class that **cannot be instantiated directly** — it only exists to be inherited from. It defines methods that every subclass *must* implement.

```python
# You cannot do this:
layer = BaseLayer()  # raises TypeError

# You must do this:
layer = StagingLayer(schema_name="staging")  # works — it implements all required methods
```

If you create a new domain class but forget to implement `register_metrics()`, Python immediately tells you with a clear error message. This is the OOP equivalent of a SQL `NOT NULL` constraint — it prevents incomplete implementations.

### Why model architecture in Python at all?

In a mature analytics engineering team, the Python layer does real work:

- **Generates dbt YAML** — instead of hand-writing `schema.yml` files for every model, the Python classes generate them automatically from the metric definitions
- **Pushes to data catalogs** — the domain catalog output can be sent directly to Azure Purview, so business users can discover metrics in a searchable catalog
- **Validates before deploying** — CI/CD pipelines run the Python validation before any SQL runs in production
- **Documents decisions** — the `description` fields in `MetricDefinition` become the canonical source of truth for what a metric means

In this project, you're learning the pattern. In production, it becomes the foundation of your entire documentation and governance strategy.

---

## Part 7: Data Testing — Why It's Not Optional

### What dbt tests are

dbt has a built-in testing framework. Tests are assertions about your data that run after your models are built. If a test fails, dbt tells you which rows are bad.

There are two types:

**Schema tests** (defined in YAML):
```yaml
- name: order_id
  tests:
    - unique        # no duplicate order_ids
    - not_null      # every row has an order_id
```

**Custom tests** (SQL files in the `tests/` folder):
```sql
-- This query returns rows ONLY when the test FAILS
select order_id from fct_orders
where is_revenue_order = true
  and order_id not in (select order_id from stg_order_items)
```

### Why testing matters more than you might think

Imagine your `fct_orders` mart has a bug that creates duplicate `order_id` rows. Every revenue dashboard now double-counts orders. The finance team reports a record quarter. Bonuses are calculated. Then someone discovers the error.

That's an extreme example, but data quality issues cause real business harm at every scale. dbt tests are your automated QA layer that runs every time you deploy.

The standard tests to always have:
- `unique` + `not_null` on every primary key
- `relationships` for every foreign key (referential integrity)
- `accepted_values` for status/category columns
- Custom tests for business logic invariants (revenue orders always have items)

---

## Part 8: How It All Fits Together — End to End

Here is the complete journey of a single order through this project:

### Step 1: Raw data arrives in Bronze
```
bronze_ecommerce.orders:
  order_id: "ORD-000001"
  customer_id: "CUST-00042"
  order_date: "2024-03-15"     ← stored as a string
  STATUS: "DELIVERED"          ← uppercase, inconsistent
  total_amount: 142.50
```

### Step 2: Staging model cleans it (Silver)
`stg_orders.sql` runs and produces a view:
```
staging.stg_orders:
  order_id: "ORD-000001"       ← same, just confirmed as string type
  customer_id: "CUST-00042"
  order_date: 2024-03-15       ← now a proper DATE type
  status: "delivered"          ← lowercase, normalized
  total_amount: 142.50
```

### Step 3: Intermediate model adds business logic (Gold)
`int_orders_enriched.sql` joins order items and adds flags:
```
intermediate.int_orders_enriched:
  order_id: "ORD-000001"
  status: "delivered"
  net_revenue: 130.50          ← total minus shipping
  is_revenue_order: true       ← delivered = counts as revenue
  order_size_segment: "medium" ← $100-499 = medium
  item_count: 3
  total_units: 4
```

### Step 4: Mart model creates the final wide table
`fct_orders.sql` denormalizes customer info and prepares for BI:
```
marts.fct_orders:
  order_id: "ORD-000001"
  order_date: 2024-03-15
  status: "delivered"
  net_revenue: 130.50
  is_revenue_order: true
  order_size_segment: "medium"
  customer_segment: "loyal"        ← from dim_customers, joined in
  acquisition_channel: "organic_search"
  customer_state: "TN"
  item_count: 3
```

### Step 5: Semantic model defines what Power BI can ask
The Azure Fabric semantic model on top of `fct_orders` defines:
- **Dimension:** `order_date` — "filter reports by date"
- **Dimension:** `customer_segment` — "filter by who bought"
- **Measure:** `Total Revenue` = `sum(net_revenue) where is_revenue_order = true`

A business user opens Power BI, drags `order_month` to the X axis, drags `Total Revenue` to the Y axis, and gets a monthly revenue chart — without writing a single line of SQL.

---

## Part 9: The Skills You're Building

By working through this project you are developing the following skills that are in high demand on analytics engineering teams:

**SQL skills:**
- Writing modular, layered SQL (not monolithic queries)
- Using CTEs to break complex logic into readable steps
- Understanding grain and how joining tables at different grains causes data problems
- Writing data quality tests

**dbt skills:**
- Configuring projects, profiles, and sources
- Understanding ref(), source(), and the DAG
- Choosing materialization strategies
- Writing schema tests and custom tests
- Using macros to avoid SQL repetition

**Databricks / Delta Lake skills:**
- Unity Catalog (catalog → schema → table hierarchy)
- Delta table format and why it's better than plain Parquet
- SQL Warehouses vs compute clusters

**Azure Fabric skills:**
- Connecting Databricks Gold layer to Fabric OneLake
- Building semantic models on top of Delta tables
- Defining DAX measures from metric definitions

**Data modeling skills:**
- Medallion architecture
- Star schema (facts and dimensions)
- Grain definition
- Slowly Changing Dimensions (the next concept to learn after this project)

**Software engineering skills:**
- Object-oriented design with abstract base classes
- Enforcing conventions through code
- Writing self-documenting code
- Separation of concerns (each layer has one job)

---

## Part 10: What to Do Next

### Immediate next steps (in order)

1. **Generate the sample data** and load it into Databricks. You need real data to run the models against.

2. **Run just the staging models** and look at them in Databricks. Compare `stg_orders` to the raw `bronze_ecommerce.orders` table. Notice what changed and what stayed the same.

3. **Run the intermediate models** and look at `int_orders_enriched`. Find the `is_revenue_order` column. Trace backwards: where does that logic live? Why is it in intermediate and not staging?

4. **Run the mart models** and query `fct_orders`. Try writing a SQL query against it: total revenue by month, by customer segment, by acquisition channel. Notice how easy it is — all the hard work was done upstream.

5. **Run `dbt test`** and read the output. Break something on purpose (add a duplicate `order_id` to the raw data) and see which test catches it.

6. **Run `dbt docs generate && dbt docs serve`** and explore the DAG. Click on `fct_orders` and trace its lineage all the way back to the Bronze sources.

7. **Run `python pipeline/run_pipeline.py`** and read the output. This demonstrates the Python OOP layer and shows what happens when you violate layer conventions.

### Concepts to study alongside this project

- **Slowly Changing Dimensions (SCD Type 2)** — what do you do when a customer changes their email? How do you keep history?
- **Incremental models in dbt** — how do you process only new orders instead of rebuilding the entire fact table every day?
- **The dbt Semantic Layer / MetricFlow** — the open-source version of what's described in Part 5
- **Data vault modeling** — an alternative to star schema for highly volatile source systems
- **Azure Fabric shortcuts** — how to connect OneLake directly to your Databricks Delta tables without copying data

---

## Glossary — Quick Reference

| Term | Plain English definition |
|------|--------------------------|
| **Analytical Engineering** | The discipline of building reliable, tested, documented data transformation pipelines |
| **Medallion Architecture** | Bronze (raw) → Silver (clean) → Gold (business-ready) layering pattern |
| **dbt** | The tool that runs your SQL transformations, tracks dependencies, and runs tests |
| **Staging model** | A dbt model that cleans one source table. Named `stg_`. Always a view. |
| **Intermediate model** | A dbt model with business logic and joins. Named `int_`. Organized by domain. |
| **Mart model** | The final dbt model. Named `fct_` (facts) or `dim_` (dimensions). Always a table. |
| **Grain** | What one row in a table represents. Always define this before writing any model. |
| **Fact table** | Records events/transactions (orders, payments). Has measures and foreign keys. |
| **Dimension table** | Describes things (customers, products). Has attributes and a primary key. |
| **Semantic model** | Defines dimensions and measures on top of mart tables for BI tools |
| **Dimension** | A column you filter or group by (date, country, segment) |
| **Measure** | A column you aggregate (revenue, count, average) |
| **DAG** | Directed Acyclic Graph — dbt's visual map of model dependencies |
| **ref()** | dbt function to reference another model — builds the DAG automatically |
| **source()** | dbt function to reference a raw/external table — appears as DAG root nodes |
| **Materialization** | How dbt stores a model: view, table, incremental, or ephemeral |
| **Domain** | A bounded business concept that owns related data (orders, customers, products) |
| **LTV** | Lifetime Value — total revenue from one customer across all their orders |
| **AOV** | Average Order Value — average revenue per order |
| **RFM** | Recency, Frequency, Monetary — framework for customer segmentation |
| **ABC** | Abstract Base Class — a Python class that defines a contract subclasses must fulfill |
| **DAX** | Data Analysis Expressions — the formula language used by Power BI and Azure Fabric |
