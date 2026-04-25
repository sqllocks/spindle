# Spindle in 60 Seconds

**Spindle** generates realistic, multi-table synthetic data for Microsoft Fabric and beyond.

## Install

```bash
pip install sqllocks-spindle
```

## Generate

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
print(result)
# 9 tables: customer (1000), address (1500), product (500), order (5000), ...
```

## Use

```python
# DataFrames — ready for Pandas, Spark, or any pipeline
customers = result["customer"]
orders = result["order"]

# Write to files
result.to_parquet("./output/retail")
result.to_csv("./output/retail")

# Write SQL (Fabric Warehouse compatible)
sql = result.to_sql_inserts(dialect="tsql", include_ddl=True)
```

## CLI

```bash
spindle generate retail --scale small --format parquet --output ./output/
spindle generate healthcare --scale medium --format sql --sql-dialect tsql
spindle list                    # See all 13 domains
spindle describe retail         # Inspect tables, columns, relationships
spindle stream retail --table order --max-events 1000
```

## 13 Domains

Retail, Healthcare, Financial, HR, Education, Insurance, IoT, Manufacturing, Marketing, Real Estate, Supply Chain, Telecom, Capital Markets

## Key Features

- **Schema-aware** — Foreign keys, business rules, realistic distributions
- **13 calibrated domains** — 90+ tables with real-world ratios
- **Multiple scales** — `demo` to `xlarge` (5K to 100M+ rows)
- **Fabric-native** — Lakehouse, Warehouse, SQL Database, Eventstream, Semantic Model
- **Star schema + CDM** — Transform 3NF to analytics-ready or Common Data Model
- **Streaming** — Real-time event generation with Poisson timing
- **Chaos + Simulation** — File drops, corruption, validation gates
- **Reproducible** — Same seed = same data, every time

## Next Steps

- [Quickstart Guide](quickstart.md) — Full walkthrough
- [CLI Cheatsheet](cli-cheatsheet.md) — Every command at a glance
- [Fabric Notebooks](../guides/fabric-notebook.md) — Use in Fabric
