# Incremental & Day 2 Generation

Generate realistic Day 2 data: CDC deltas (inserts, updates, deletes) and monthly point-in-time snapshots with configurable growth, churn, and seasonality.

---

## Continue: CDC Deltas

Generate incremental changes from existing data — tagged with `_delta_type` and `_delta_timestamp` for downstream merge logic.

### Quick Start

```bash
# Generate baseline
spindle generate retail --scale small --format csv --output ./day1/

# Generate Day 2 deltas
spindle continue retail --input ./day1/ --output ./deltas/ --inserts 100 --seed 42
```

```python
from sqllocks_spindle import Spindle, RetailDomain, ContinueEngine, ContinueConfig

# Day 1 baseline
result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

# Day 2 deltas
engine = ContinueEngine()
deltas = engine.continue_from(
    existing=result,
    config=ContinueConfig(
        insert_count=100,
        update_fraction=0.10,
        delete_fraction=0.02,
        state_transitions={
            "order.status": {
                "pending":   {"shipped": 0.7, "cancelled": 0.3},
                "shipped":   {"delivered": 0.9, "returned": 0.1},
            },
        },
        seed=42,
    ),
)

print(deltas.summary())
# Inserts: 100, Updates: 500, Deletes: 100

# Access combined deltas with metadata columns
combined = deltas.combined["order"]
print(combined[["order_id", "_delta_type", "_delta_timestamp"]].head())
```

### Delta Semantics

| Delta Type | Behavior |
| --- | --- |
| **INSERT** | New rows with PKs offset above existing max. FKs reference both existing and newly-inserted parents. |
| **UPDATE** | Existing rows with Markov state transitions applied first, then non-key columns perturbed (±10% numeric, shuffled categorical). |
| **DELETE** | Existing rows returned as-is (soft delete). Full row preserved with `_delta_type="DELETE"`. |

### Metadata Columns

Every delta row gets two columns:

| Column | Description |
| --- | --- |
| `_delta_type` | `"INSERT"`, `"UPDATE"`, or `"DELETE"` |
| `_delta_timestamp` | Batch-level timestamp (`pd.Timestamp.now()`) |

### State Transitions (Markov)

Model realistic business workflow evolution:

```python
state_transitions={
    "order.status": {
        "pending":   {"shipped": 0.7, "cancelled": 0.3},
        "shipped":   {"delivered": 0.9, "returned": 0.1},
    },
    "claim.status": {
        "submitted": {"under_review": 0.8, "rejected": 0.2},
        "under_review": {"approved": 0.7, "rejected": 0.3},
    },
}
```

Rows with no matching transition rule are left unchanged.

### ContinueConfig Options

```python
ContinueConfig(
    insert_count=100,               # New rows per anchor table
    update_fraction=0.1,            # 10% of existing rows updated
    delete_fraction=0.02,           # 2% of existing rows soft-deleted
    state_transitions={...},        # Markov state evolution
    timestamp_column="_delta_timestamp",  # Customizable column name
    delta_type_column="_delta_type",      # Customizable column name
    seed=42,                        # Reproducibility
)
```

### CLI Reference

```bash
spindle continue DOMAIN_NAME [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `DOMAIN_NAME` | — | Domain to continue (required) |
| `--input` | — | Directory with existing data files (required) |
| `--output, -o` | — | Output directory for delta files |
| `--format` | `csv` | Output format: `csv`, `parquet`, `jsonl` |
| `--inserts` | `100` | New rows per anchor table |
| `--update-fraction` | `0.1` | Fraction of rows to update (0.0–1.0) |
| `--delete-fraction` | `0.02` | Fraction of rows to soft-delete (0.0–1.0) |
| `--seed` | `42` | Random seed |

---

## Time-Travel: Monthly Snapshots

Generate N+1 monthly point-in-time snapshots with configurable growth, churn, and seasonality — ideal for testing Lakehouse temporal queries and partitioned storage.

### Quick Start

```bash
spindle time-travel retail --months 12 --output ./snapshots/ --format parquet \
  --growth-rate 0.08 --churn-rate 0.03
```

```python
from sqllocks_spindle import Spindle, RetailDomain, TimeTravelEngine, TimeTravelConfig

engine = TimeTravelEngine()
result = engine.generate(
    domain=RetailDomain(),
    config=TimeTravelConfig(
        months=12,
        start_date="2025-01-01",
        growth_rate=0.05,        # 5% net growth per month
        churn_rate=0.02,         # 2% soft-delete per month
        seasonality={11: 1.5, 12: 2.0},  # Holiday spike
        seed=42,
    ),
    scale="small",
)

print(result.summary())

# Access individual snapshots
jan = result.get_snapshot(0)
dec = result.get_snapshot(12)
print(f"Jan customers: {jan.row_counts['customer']}")
print(f"Dec customers: {dec.row_counts['customer']}")
```

### Monthly Evolution

Each month applies three transformations per table:

1. **Growth** — add `len(df) * growth_rate * seasonality_multiplier` new rows
2. **Churn** — remove `len(df) * churn_rate` rows
3. **Updates** — perturb `len(df) * update_fraction` numeric columns by ±10%

### Partitioned Output

Combine all snapshots into single DataFrames with a `_snapshot_date` partition column:

```python
partitioned = result.to_partitioned_dfs()

for table_name, df in partitioned.items():
    df.to_parquet(f"output/{table_name}.parquet")
    # Each row has _snapshot_date column for downstream partitioning
```

### TimeTravelConfig Options

```python
TimeTravelConfig(
    months=12,                      # Number of monthly snapshots (produces months+1 total)
    start_date="2025-01-01",        # Month 0 date
    growth_rate=0.05,               # 5% net growth per month
    seasonality={11: 1.5, 12: 2.0}, # Month → multiplier (default 1.0)
    churn_rate=0.02,                # 2% soft-delete per month
    update_fraction=0.1,            # 10% of rows modified per month
    seed=42,                        # Reproducibility
)
```

### CLI Reference

```bash
spindle time-travel DOMAIN_NAME [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `DOMAIN_NAME` | — | Domain to snapshot (required) |
| `--months` | `12` | Number of monthly snapshots |
| `--scale, -s` | `small` | Scale preset |
| `--output, -o` | — | Output directory (required) |
| `--format` | `parquet` | Output format: `csv`, `parquet` |
| `--growth-rate` | `0.05` | Monthly growth rate (0.0–1.0) |
| `--churn-rate` | `0.02` | Monthly churn rate (0.0–1.0) |
| `--seed` | `42` | Random seed |

### Output Structure (CLI)

```
./snapshots/
  month_0/
    customer.parquet
    order.parquet
  month_1/
    customer.parquet
    order.parquet
  ...
  month_12/
```
