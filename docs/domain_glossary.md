# Domain Glossary
### The Business Definition of Every Key Term

> **How to use this doc:** When you're writing a model and unsure what a
> field should include or exclude, look it up here first. If it's not here,
> ask the domain owner and add it when you have the answer.

---

## Orders Domain

| Term | Definition | Owner | Notes |
|------|-----------|-------|-------|
| **Order** | A single purchase transaction by one customer | `growth_team` | Contains 1+ order items |
| **Revenue Order** | An order with status `delivered`, `shipped`, or `processing` | `finance_team` | Excludes `cancelled` and `refunded` |
| **Gross Revenue** | Sum of `total_amount` for all revenue orders | `finance_team` | Includes shipping |
| **Net Revenue** | `total_amount - shipping_amount` for revenue orders | `finance_team` | Excludes shipping — use this for P&L |
| **GMV** | Gross Merchandise Value: pre-discount product value | `growth_team` | `items_subtotal + discount` |
| **AOV** | Average Order Value: `net_revenue / revenue_orders` | `growth_team` | Excludes cancelled/refunded |
| **Order Size Segment** | `small` (<$100), `medium` ($100-499), `large` (≥$500) | `analytics_team` | Based on `total_amount` |
| **Cancellation Rate** | `cancelled_orders / total_orders` | `operations_team` | All time, not windowed |

---

## Customers Domain

| Term | Definition | Owner | Notes |
|------|-----------|-------|-------|
| **Customer** | A registered account in the system | `growth_team` | One person may have multiple accounts |
| **Active Customer** | Made a revenue order in the last 90 days | `growth_team` | 90-day threshold agreed Q2 2024 |
| **New Customer** | First revenue order within reporting period | `growth_team` | Based on `first_order_date`, NOT account creation |
| **Repeat Customer** | Has placed 2+ revenue orders | `growth_team` | `revenue_orders >= 2` |
| **LTV** | Lifetime Value: sum of `net_revenue` across all revenue orders | `finance_team` | Current state, not predicted |
| **Champion** | Active (≤30 days), frequent (≥3 orders) customer | `marketing_team` | RFM segment |
| **Churned** | No revenue order in 180+ days | `marketing_team` | Reactivation campaign target |
| **Acquisition Channel** | How the customer originally found us | `marketing_team` | Normalized in `stg_customers` |

---

## Products Domain

| Term | Definition | Owner | Notes |
|------|-----------|-------|-------|
| **Product** | A single SKU in the product catalog | `merchandising_team` | Variants = separate product_ids |
| **List Price** | The published selling price | `merchandising_team` | May differ from avg selling price due to discounts |
| **Cost** | The cost of goods for this product | `finance_team` | Must be kept current by merchandising |
| **Gross Margin Rate** | `(revenue - COGS) / revenue` | `finance_team` | Per product, uses actual units sold |
| **Top Seller** | ≥50 units sold in last 30 days | `merchandising_team` | Threshold reviewed quarterly |
| **Slow Mover** | 1-9 units sold in last 30 days | `merchandising_team` | Candidate for markdown or discontinuation |
| **Stale** | No sales in last 30 days, but previously sold | `merchandising_team` | Investigate: out of stock? pricing? |
| **Available** | `status = active` AND `stock_quantity > 0` | `ops_team` | Used to filter catalog for customers |

---

## Architectural Conventions

| Convention | Rule |
|-----------|------|
| **Naming: Staging** | `stg_<source_table>` |
| **Naming: Intermediate** | `int_<description>` |
| **Naming: Fact** | `fct_<entity>` |
| **Naming: Dimension** | `dim_<entity>` |
| **Materialization: Staging** | `view` |
| **Materialization: Intermediate** | `view` |
| **Materialization: Mart** | `table` |
| **Money columns** | `decimal(18, 2)` — always |
| **Date columns** | Cast to `date` or `timestamp` — never store as string |
| **IDs** | Cast to `string` — even if they look like numbers |
| **Grain** | Every model must have a documented grain in its header comment |

---

## FAQ

**Q: Should I use `total_amount` or `net_revenue` for revenue reporting?**
A: Use `net_revenue` (excludes shipping). `total_amount` is the raw customer charge.

**Q: Why are cancelled orders not excluded at the staging layer?**
A: Staging is a faithful representation of the source. We want to preserve all data.
Business filtering (is_revenue_order) happens in the intermediate and mart layers where
the INTENT is clear from context.

**Q: A customer emailed us — their `customer_id` doesn't appear in `dim_customers`. Why?**
A: They may have a `is_active = false` account — `dim_customers` includes all accounts.
Check that their `customer_id` exists in `stg_customers` first.
