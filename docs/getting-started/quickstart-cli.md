# Quickstart (CLI)

Generate synthetic data without writing Python. This guide is for DBAs, DevOps engineers, and anyone who prefers the command line.

## Install

```bash
pip install sqllocks-spindle
```

## Generate Your First Dataset

```bash
# Generate retail data as CSV
spindle generate retail --scale small --seed 42 --format csv --output ./output/

# See what you got
ls ./output/
# customer.csv  address.csv  product_category.csv  product.csv  store.csv
# promotion.csv  order.csv  order_line.csv  return.csv
```

That's it. 9 tables, 21,000 rows, relationally correct, in ~2 seconds.

## Preview Before Generating

```bash
# Dry run — see row counts without writing files
spindle generate retail --scale medium --dry-run
```

## Change the Scale

```bash
spindle generate retail --scale fabric_demo --format csv --output ./demo/    # ~3,000 rows
spindle generate retail --scale medium --format csv --output ./medium/       # ~1,000,000 rows
spindle generate retail --scale warehouse --format parquet --output ./wh/    # ~20,000,000 rows
```

## Generate SQL Instead of Files

```bash
# SQL INSERT statements (T-SQL dialect, with CREATE TABLE DDL)
spindle generate retail --scale small --format sql --output ./sql/ \
  --sql-dialect tsql --sql-ddl --sql-drop

# PostgreSQL
spindle generate retail --scale small --format sql --output ./sql/ \
  --sql-dialect postgresql --sql-ddl
```

The `--sql-ddl` flag includes `CREATE TABLE` statements. `--sql-drop` adds `DROP TABLE IF EXISTS` before each create.

## Explore Available Domains

```bash
# List all 13 domains
spindle list

# Inspect a domain's schema
spindle describe retail
spindle describe healthcare
spindle describe capital_markets
```

## Generate a Star Schema

```bash
# Star schema (dim_* + fact_* tables) as Parquet
spindle to-star retail --scale small --output ./star/ --format parquet
```

## Stream Events

```bash
# Stream 100 order events to console
spindle stream retail --table order --max-events 100

# Stream to a file with rate limiting
spindle stream retail --table order --rate 50 --realtime --max-events 5000 \
  --sink file --output orders.jsonl
```

## What's Next?

- [CLI Cheatsheet](cli-cheatsheet.md) — every command and option
- [Tutorials](../tutorials/) — step-by-step learning paths
- [Troubleshooting](../guides/troubleshooting.md) — common issues and fixes
