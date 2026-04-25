# Tutorial 16: Day 2 Operations

Generate incremental CDC data, create time-travel snapshots spanning months, and apply state transitions to simulate how production data evolves after the initial load.

---

## Prerequisites

- Python 3.10 or later
- `pip install sqllocks-spindle`
- Completed [Tutorial 01: Hello, Spindle!](../beginner/01-hello-spindle.md)
- Basic understanding of Change Data Capture (CDC) concepts

## What You'll Learn

- How to generate incremental changes (inserts, updates, soft deletes) on top of an existing dataset using `ContinueEngine`
- How to inspect delta tags (`_delta_type`, `_delta_timestamp`) that CDC pipelines consume
- How to configure state transitions that model real business processes (e.g., order pending to shipped to delivered)
- How to produce multi-month time-travel snapshots with `TimeTravelEngine`
- How growth rate and churn rate shape the evolution of your data over time

---

## Why Incremental Generation?

Day 1 is generating your initial dataset. Day 2 is where it gets interesting: new rows arrive, existing records change, some get deleted. Static datasets cannot test CDC pipelines, state transitions, or late-arriving data. Spindle's incremental engine solves this.

| Static Dataset | Incremental Dataset |
|---|---|
| One snapshot in time | Evolves over time like production data |
| Cannot test CDC pipelines | Produces INSERT/UPDATE/DELETE deltas |
| No state transitions | Orders move from pending to shipped to delivered |
| No late-arriving data | Configurable late arrivals and out-of-order events |

## Step 1: Generate the Baseline Dataset

Every CDC pipeline needs a baseline -- this is your "Day 1" initial full load. The incremental engine needs this baseline to know which records exist, what their current states are, and what key ranges are already occupied.

```python
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.retail import RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

print("Day 1 Baseline -- Retail Domain")
print("=" * 45)
for table_name, df in result.tables.items():
    print(f"  {table_name:<20} {len(df):>8,} rows")

total = sum(len(df) for df in result.tables.values())
print(f"\nTotal rows: {total:,}")

# Show current order status distribution
if "order" in result.tables:
    orders = result.tables["order"]
    if "status" in orders.columns:
        print(f"\n=== Order Status Distribution (Day 1) ===")
        for status, count in orders["status"].value_counts().items():
            print(f"  {status:<15} {count:>6,}")
```

## Step 2: Generate Incremental Changes

Use `ContinueEngine` to produce a batch of incremental changes. The `ContinueConfig` controls exactly how many inserts, what fraction of records to update, and what fraction to soft-delete.

```python
from sqllocks_spindle.incremental import ContinueEngine, ContinueConfig

engine = ContinueEngine()
config = ContinueConfig(
    insert_count=50,          # 50 new records across tables
    update_fraction=0.1,      # update 10% of existing records
    delete_fraction=0.02,     # soft-delete 2% of existing records
    state_transitions={
        "order.status": {
            "pending":  {"shipped": 0.7, "cancelled": 0.3},
            "shipped":  {"delivered": 0.9, "returned": 0.1},
        }
    },
    seed=42,
)

delta = engine.continue_from(result, config=config)
print(delta.summary())
```

The `ContinueEngine` understands your schema's foreign keys: new orders reference existing customers, and updates modify realistic fields (not primary keys). The `state_transitions` parameter defines transition probabilities that mirror your real business process.

## Step 3: Inspect Delta Tags

Every changed record gets metadata columns that CDC pipelines consume directly:

- **`_delta_type`** -- `insert`, `update`, or `delete`
- **`_delta_timestamp`** -- when the change occurred (enables watermark-based incremental loading)

```python
print("=== Delta Breakdown by Table ===")
for table_name, df in delta.tables.items():
    if "_delta_type" in df.columns:
        counts = df["_delta_type"].value_counts()
        parts = [f"{dtype}: {count}" for dtype, count in counts.items()]
        print(f"  {table_name:<20} {len(df):>5} changes  |  {', '.join(parts)}")

# Show sample delta records
print("\n=== Sample Delta Records ===")
for table_name, df in delta.tables.items():
    if "_delta_type" in df.columns and len(df) > 0:
        print(f"\n--- {table_name} (first 3 changes) ---")
        display_cols = (
            [c for c in df.columns if not c.startswith("_")]
            + ["_delta_type", "_delta_timestamp"]
        )
        display_cols = [c for c in display_cols if c in df.columns]
        print(df[display_cols].head(3).to_string(index=False))
        break
```

A Fabric Data Pipeline or Spark notebook can filter on `_delta_type` to route inserts to append operations, updates to merge/upsert, and deletes to soft-delete flags.

## Step 4: State Transitions in Action

State machines are everywhere in business data -- order statuses, claim statuses, ticket workflows, employee lifecycle stages. Compare the status distribution before and after the incremental run to see transitions in action.

```python
if "order" in result.tables and "order" in delta.tables:
    before = result.tables["order"]
    after_changes = delta.tables["order"]

    print("=== Order Status -- Before (Day 1) ===")
    if "status" in before.columns:
        for status, count in before["status"].value_counts().items():
            print(f"  {status:<15} {count:>6,}")

    print("\n=== Status Changes in Delta ===")
    updates = after_changes[after_changes["_delta_type"] == "update"]
    if "status" in updates.columns and len(updates) > 0:
        print(f"  Updated orders: {len(updates)}")
        for status, count in updates["status"].value_counts().items():
            print(f"  -> {status:<15} {count:>6,}")

    print("\nState transitions move orders through their lifecycle:")
    print("  pending  -> shipped (70%) / cancelled (30%)")
    print("  shipped  -> delivered (90%) / returned (10%)")
```

## Step 5: Time-Travel Snapshots

`TimeTravelEngine` generates a complete multi-month history of a dataset -- not just one delta, but a full sequence of monthly snapshots showing how data evolves over time. This lets you test time-based queries, SCD Type 2 implementations, and month-over-month dashboards without waiting months for real data to accumulate.

```python
from sqllocks_spindle.incremental.time_travel import TimeTravelEngine, TimeTravelConfig

tt_engine = TimeTravelEngine()
tt_config = TimeTravelConfig(
    months=6,
    growth_rate=0.05,    # 5% month-over-month growth
    churn_rate=0.01,     # 1% monthly churn
    seed=42,
)

tt_result = tt_engine.generate(
    domain=RetailDomain(),
    config=tt_config,
    scale="small",
)

print(tt_result.summary())

# Explore month-over-month growth
if hasattr(tt_result, "snapshots"):
    print("\n=== Monthly Snapshot Summary ===")
    print(f"{'Month':<12} {'Tables':>8} {'Total Rows':>12} {'vs Prior':>10}")
    print("-" * 45)

    prev_rows = 0
    for snapshot in tt_result.snapshots:
        total_rows = sum(len(df) for df in snapshot.tables.values())
        change = f"+{total_rows - prev_rows:,}" if prev_rows > 0 else "baseline"
        print(f"  {snapshot.label:<10} {len(snapshot.tables):>8} "
              f"{total_rows:>12,} {change:>10}")
        prev_rows = total_rows
```

The growth rate controls how many new records appear each month, while the churn rate controls how many records are soft-deleted. Together they produce realistic data evolution patterns.

## Key Takeaways

- **`ContinueEngine`** generates CDC-style deltas (inserts, updates, deletes) from any baseline
- Every delta record is tagged with **`_delta_type`** and **`_delta_timestamp`** for pipeline consumption
- **`state_transitions`** model realistic business process flows (order lifecycle, claim adjudication, etc.)
- **`TimeTravelEngine`** produces multi-month snapshot histories with configurable growth and churn
- Deterministic seeds ensure reproducible deltas for regression testing

---

> **Run It Yourself**
>
> - Notebook: [`T17_day2_incremental.ipynb`](../../../examples/notebooks/intermediate/T17_day2_incremental.ipynb)

---

## Related

- [Simulation guide](../../guides/simulation.md) -- the condensed reference for incremental generation, time-travel, and CDC patterns

---

## Next Step

[Tutorial 17: CI Integration](17-ci-integration.md) -- use the Spindle CLI and GSL specs to integrate synthetic data generation into your CI/CD pipeline.
