# GraphQL API — Setup & Usage Guide
### A queryable API layer over your e-commerce semantic model

---

## What This Is

The GraphQL API sits between your mart tables and any consumer that
wants to query your analytics data programmatically — a dashboard,
a mobile app, an external partner, or just you testing queries in
a browser.

```
Mart tables (fct_orders, dim_customers, dim_products)
        ↓
GraphQL API (this layer)
        ↓
Any consumer: browser playground, React app, Postman, curl
```

**Why GraphQL instead of REST?**

With REST you'd need separate endpoints for every use case:
`/api/revenue-by-segment`, `/api/top-customers`, `/api/product-performance`...

With GraphQL, there's one endpoint. The caller describes exactly what
they want and the API returns exactly that — no more, no less.

One request can fetch order KPIs + segment breakdown + top products
simultaneously. That's the `DashboardData` query in `example_queries.graphql`.

---

## Stage 1: Install Dependencies

From the project root with your virtual environment active:

```bash
source .venv/bin/activate
pip install -r api/requirements.txt
```

This installs:
- `strawberry-graphql` — the GraphQL framework
- `fastapi` + `uvicorn` — the web server
- `duckdb` — local data backend (no cloud needed for dev)
- `pydantic-settings` — config management

---

## Stage 2: Configure the API

Copy the example env file:

```bash
cp api/.env.example api/.env
```

The default config runs against local CSV files (no cloud needed):

```env
DATA_BACKEND=duckdb
DUCKDB_DATA_PATH=data/bronze
```

Make sure your sample data exists first:

```bash
python3 scripts/generate_sample_data.py
```

---

## Stage 3: Start the API

```bash
python -m uvicorn api.main:app --reload --port 8000
```

You'll see:

```
🚀 Starting E-Commerce Analytics GraphQL API
   Backend: duckdb
✅ DuckDB connected — reading from: data/bronze

INFO:     Uvicorn running on http://127.0.0.1:8000
```

`--reload` means the server restarts automatically when you change code.
Leave this terminal running while you use the API.

---

## Stage 4: Open the GraphQL Playground

Navigate to: **http://localhost:8000/graphql**

You'll see GraphiQL — a browser-based IDE for writing GraphQL queries.

**Left panel** — write your queries here
**Right panel** — results appear here
**Docs tab (top right)** — auto-generated schema documentation

Try your first query — paste this into the left panel and press the
play button (▶):

```graphql
{
  health
}
```

You should see:
```json
{
  "data": {
    "health": "OK | DuckDB (local: data/bronze)"
  }
}
```

---

## Stage 5: Run Example Queries

All example queries are in `api/example_queries.graphql`.
Here are the most useful ones to start with:

### Dashboard KPIs
```graphql
{
  orderSummary {
    totalOrders
    revenueOrders
    totalRevenue
    avgOrderValue
    cancellationRate
  }
}
```

### Revenue by Customer Segment
```graphql
{
  revenueBy(dimension: "customer_segment") {
    dimensionValue
    totalRevenue
    orderCount
    avgOrderValue
  }
}
```

### Recent Large Orders
```graphql
{
  orders(
    filters: { status: "delivered", minRevenue: 300.0 }
    pagination: { limit: 10 }
  ) {
    orderId
    orderDate
    netRevenue
    customerSegment
    acquisitionChannel
    customerState
  }
}
```

### At-Risk Customers (re-engagement list)
```graphql
{
  customers(
    filters: { customerSegment: "at_risk" }
    pagination: { limit: 50 }
  ) {
    customerId
    email
    fullName
    lifetimeValue
    daysSinceLastOrder
  }
}
```

### Full Dashboard in One Request
```graphql
{
  orderSummary {
    totalRevenue
    avgOrderValue
    cancellationRate
  }
  revenueBy(dimension: "acquisition_channel") {
    dimensionValue
    totalRevenue
  }
  customerSegments {
    segment
    customerCount
    avgLifetimeValue
  }
  products(
    filters: { performanceTier: "top_seller" }
    pagination: { limit: 5 }
  ) {
    productName
    totalRevenue
    grossMarginRate
  }
}
```

This last query fetches four different things in a single HTTP request —
that's one of GraphQL's core advantages over REST.

---

## Stage 6: Switch to Databricks Backend

Once your dbt models are running in Databricks, point the API at
your live mart tables instead of local CSVs.

In `api/.env`:

```env
DATA_BACKEND=databricks
DATABRICKS_HOST=community.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_TOKEN=dapi1234567890abcdef
DATABRICKS_CATALOG=hive_metastore
DATABRICKS_SCHEMA=marts
```

Restart the server:

```bash
python -m uvicorn api.main:app --reload --port 8000
```

The health check will confirm the switch:
```graphql
{ health }
# → "OK | Databricks (community.cloud.databricks.com / marts)"
```

No other changes needed. Every query works identically — the connector
swap is transparent to the schema and resolvers.

---

## How It All Connects to the Project

```
dbt models                        GraphQL API
──────────────────────────────    ──────────────────────────────
stg_orders.sql                    (staging — not exposed directly)
int_orders_enriched.sql           (intermediate — not exposed directly)
fct_orders.sql            ──────► fct_orders view → Order type
                                   → orders() query
                                   → orderSummary() query
                                   → revenueBy() query

int_customer_orders.sql           (not exposed directly)
dim_customers.sql         ──────► dim_customers view → Customer type
                                   → customers() query
                                   → customerSegments() query

int_product_revenue.sql           (not exposed directly)
dim_products.sql          ──────► dim_products view → Product type
                                   → products() query
```

The GraphQL API only exposes the **mart layer** — the final, tested,
business-ready tables. Staging and intermediate models are internal
to the transformation pipeline and never surface in the API.

---

## Understanding the Code Structure

```
api/
├── config.py               ← Settings (backend choice, limits, connection details)
├── main.py                 ← FastAPI app, server startup, endpoint mounting
├── requirements.txt        ← Python dependencies
├── .env.example            ← Copy to .env and fill in your values
├── example_queries.graphql ← 15 ready-to-run queries
│
├── connectors/
│   ├── base_connector.py       ← Abstract base + DuckDB implementation
│   └── databricks_connector.py ← Databricks implementation + factory
│
├── schema/
│   ├── types.py    ← GraphQL types (Order, Customer, Product + filter inputs)
│   └── schema.py   ← Root Query class — wires queries to resolvers
│
└── resolvers/
    └── resolvers.py ← SQL query functions, one per GraphQL query field
```

**The flow for every query:**

```
GraphQL query arrives at /graphql
    ↓
schema.py routes it to the right resolver function
    ↓
resolver builds parameterized SQL + filter conditions
    ↓
connector executes SQL against DuckDB or Databricks
    ↓
resolver maps rows → typed Python objects
    ↓
Strawberry serializes to JSON
    ↓
response returned to caller
```

---

## Key Concepts You're Learning Here

**GraphQL schema-first design** — you define the shape of data
(types.py) separately from how you fetch it (resolvers.py). This
mirrors the same separation of concerns as your dbt layers.

**Abstract base classes in practice** — BaseConnector defines the
contract. DuckDBConnector and DatabricksConnector both implement it.
Resolvers only ever call the abstract interface — they have no idea
which backend is running.

**Parameterized queries** — every resolver uses `?` placeholders
and a params list. User-supplied filter values are never interpolated
into SQL strings. This is non-negotiable for any real API.

**Whitelisting for dynamic SQL** — `revenueBy(dimension:)` lets
callers choose the GROUP BY column. Before using it in SQL the
resolver checks it against a hardcoded whitelist. This prevents
column-name injection even though the value is validated.

**Context injection** — the connector and settings are passed into
every resolver via `info.context`. This keeps resolvers testable
in isolation — just pass a mock connector in tests without needing
a real database.

---

## Useful URLs While the Server Is Running

| URL | What it shows |
|-----|--------------|
| `http://localhost:8000/graphql` | GraphiQL browser IDE |
| `http://localhost:8000/health` | JSON health check |
| `http://localhost:8000/schema` | Raw GraphQL SDL |
| `http://localhost:8000/docs` | FastAPI auto-generated REST docs |

---

## Troubleshooting

**"No module named api"**
Make sure you're running from the project root, not inside the api/ folder:
```bash
cd ecommerce_analytics   # project root
python -m uvicorn api.main:app --reload
```

**"No such file: data/bronze/orders.csv"**
Generate the sample data first:
```bash
python3 scripts/generate_sample_data.py
```

**"Connection refused" on Databricks backend**
Check that your serverless SQL warehouse is running in Databricks,
and that DATABRICKS_HTTP_PATH uses `/warehouses/` not `/clusters/`.

**GraphQL error: "Invalid dimension"**
`revenueBy()` only accepts these dimension values:
`customer_segment`, `acquisition_channel`, `order_size_segment`,
`customer_state`, `customer_country`, `order_year`, `status`
