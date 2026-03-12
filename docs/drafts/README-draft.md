# Spindle by SQLLocks

**Schema-aware synthetic data for Microsoft Fabric.**

Spindle generates realistic, relationally-intact synthetic datasets — not random noise. It understands your schema, respects foreign keys, and produces data that looks like it came from a real system.

```bash
pip install sqllocks-spindle
```

```bash
spindle generate retail --scale small --seed 42
```

```
Spindle v0.1.0 — Generating retail (3nf) at scale 'small'

  customer           1,000 rows
  product_category      50 rows
  product              500 rows
  store                 25 rows
  promotion             40 rows
  order              5,000 rows
  order_line        12,800 rows
  return               400 rows

  Total: 19,815 rows across 8 tables in 0.2s
  Referential integrity: PASS (all FKs resolve)
```

---

## Why Spindle?

**Every Fabric demo you've ever seen uses garbage data.** Random strings, sequential IDs, uniform distributions. It doesn't look real, it doesn't behave real, and it doesn't test real.

Spindle gives you:

- **Relational integrity** — Foreign keys always resolve. Parent rows exist before child rows. No orphans, no dangling references.
- **Realistic distributions** — Prices follow log-normal. Order frequency follows Pareto (80/20). Seasonal patterns spike in November. Because that's how real data works.
- **Schema awareness** — Define your schema once. Spindle generates tables in dependency order, resolves cross-table relationships, and enforces business rules automatically.
- **Reproducible output** — Same seed, same data. Every time.

### What it's NOT

Spindle is not a privacy tool. It doesn't anonymize production data. It generates synthetic data from scratch — structurally realistic but entirely fictional.

---

## Quick Start

### Python API

```python
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.retail import RetailDomain

result = Spindle().generate(
    domain=RetailDomain(),
    scale="medium",   # 50K customers, 500K orders
    seed=42
)

# Dict of table_name → pandas DataFrame
result["customer"].head()
result["order"].shape          # (500000, 8)
result["order_line"].shape     # (~1.2M, 6)

# Built-in integrity check
errors = result.verify_integrity()
assert len(errors) == 0
```

### In a Fabric Notebook

```python
# Works in any Microsoft Fabric Notebook — all dependencies pre-installed
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.retail import RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="large", seed=42)

# Convert to Spark and write to Lakehouse (Phase 1 will have a native writer)
for name, df in result.tables.items():
    spark_df = spark.createDataFrame(df)
    spark_df.write.format("delta").saveAsTable(name)
```

### CLI

```bash
# Generate and inspect
spindle generate retail --scale small --seed 42

# Export to CSV
spindle generate retail --scale small --format csv --output ./data

# Export to Parquet
spindle generate retail --scale medium --format parquet --output ./data

# Describe a domain without generating
spindle describe retail

# Validate a custom schema file
spindle validate my_schema.spindle.json
```

---

## Domains

Domains are pre-built, statistically-profiled schemas that model real industries.

| Domain | Status | Tables | Description |
|--------|--------|--------|-------------|
| **Retail** | Available | 8 | Customers, products, orders, returns — 3NF normalized |
| Financial | Planned | — | Accounts, transactions, loans, risk ratings |
| Insurance | Planned | — | Policies, claims, agents, coverage |
| Healthcare | Planned | — | Patients, encounters, diagnoses, providers |
| Supply Chain | Planned | — | Suppliers, inventory, shipments, warehouses |
| Telecom | Planned | — | Subscribers, usage, plans, network events |
| Education | Planned | — | Students, courses, enrollments, grades |
| Energy | Planned | — | Meters, readings, billing, grid topology |
| HR | Planned | — | Employees, departments, payroll, reviews |
| Real Estate | Planned | — | Properties, listings, transactions, agents |
| Marketing | Planned | — | Campaigns, impressions, conversions, channels |
| IoT | Planned | — | Devices, sensors, telemetry, alerts |

Each domain ships with:
- A complete schema definition (`.spindle.json`)
- Curated reference data (product names, category hierarchies, etc.)
- Statistical profiles based on real-world distributions
- Multiple schema modes: 3NF, Star/Snowflake, Microsoft CDM

---

## Schema Definition Format

Spindle schemas are JSON files that describe tables, columns, relationships, and generation rules. They're designed to be human-readable — a data engineer should understand one without documentation.

```json
{
  "model": {
    "name": "retail_3nf",
    "domain": "retail",
    "schema_mode": "3nf"
  },
  "tables": {
    "customer": {
      "primary_key": ["customer_id"],
      "columns": {
        "customer_id": {
          "type": "integer",
          "generator": { "strategy": "sequence", "start": 1 }
        },
        "loyalty_tier": {
          "type": "string",
          "generator": {
            "strategy": "weighted_enum",
            "values": { "Basic": 0.80, "Silver": 0.12, "Gold": 0.06, "Platinum": 0.02 }
          }
        }
      }
    }
  },
  "relationships": [
    {
      "name": "customer_orders",
      "parent": "customer",
      "child": "order",
      "type": "one_to_many",
      "cardinality": { "distribution": "pareto", "alpha": 1.2 }
    }
  ]
}
```

### Generator Strategies

| Strategy | What it does | Example use |
|----------|-------------|-------------|
| `sequence` | Auto-incrementing integer | Primary keys |
| `faker` | Faker library provider | Names, emails, addresses |
| `weighted_enum` | Pick from weighted list | Status codes, loyalty tiers |
| `distribution` | Statistical distribution | Prices (log-normal), ages (normal) |
| `temporal` | Time-aware with seasonal profiles | Order dates with holiday spikes |
| `formula` | Computed from other columns | `quantity * unit_price` |
| `foreign_key` | Reference to parent table PK | Relationship columns |
| `lookup` | Copy value from related table | Line item price from product |
| `reference_data` | Pick from curated dataset | Product names, categories |
| `pattern` | Formatted string with tokens | SKU codes (`SKU-ELEC-000142`) |
| `computed` | Aggregated from child rows | Order total = sum of line totals |

### Supported Distributions

| Distribution | Shape | Use case |
|-------------|-------|----------|
| `uniform` | Flat | Equal probability ranges |
| `normal` | Bell curve | Ages, store sizes |
| `log_normal` | Right-skewed | Prices, transaction amounts |
| `pareto` | 80/20 power law | Customer order frequency |
| `zipf` | Steep power law | Product popularity |
| `geometric` | Decay | Quantities per line item |
| `bernoulli` | Binary | Return probability |
| `poisson` | Count | Events per interval |

---

## How It Works

```
Schema (.spindle.json or Domain class)
  → Parse & validate
  → Topological sort (dependency order)
  → Generate tables (parents before children)
  → Resolve foreign keys from PK pools
  → Enforce business rules
  → Back-fill computed columns
  → Output (DataFrames, CSV, Parquet)
```

Spindle's engine generates tables in dependency order. The ID Manager tracks every primary key value as it's generated, so foreign key columns can reference real parent rows — with configurable distributions (uniform, Pareto, Zipf) that model how real relationships cluster.

Business rules are enforced post-generation: orders can't predate customer signups, returns can't predate orders, promotions must be active on the order date.

---

## Roadmap

| Phase | What | Status |
|-------|------|--------|
| **0** | Core engine, SDF format, Retail domain, CLI | Done |
| **1** | Fabric integration — Lakehouse Delta writer, Notebook templates, scale testing | Next |
| **2** | Streaming — Eventstream/Event Hub producer, temporal arrival patterns | Planned |
| **3** | Domain expansion — remaining 11 industry domains | Planned |
| **4** | MCP companion — generate from Data Modeler schemas | Planned |
| **5** | Community release — PyPI, docs site, sample notebooks | Planned |

---

## Requirements

- Python 3.10+
- Dependencies: `faker`, `numpy`, `pandas`, `click`
- All four ship pre-installed in Microsoft Fabric Notebooks

---

## License

MIT — free for any use.

---

## Links

- **SQLBites** — [sqlbites.net](https://sqlbites.net) — Fabric content and tutorials
- **SQLLocks** — [sqllocks.com](https://sqllocks.com) — Data engineering consultancy

Built by [Jonathan Stewart](https://sqllocks.com) as part of the SQLLocks open-source ecosystem.
