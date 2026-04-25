# Chaos Engine

Spindle's chaos engine injects realistic data quality issues into generated datasets — schema drift, value corruption, orphaned FKs, temporal anomalies, volume spikes, and file-level corruption. Use it to test whether your pipelines handle real-world data problems.

## Quick Start

```python
from sqllocks_spindle.chaos import ChaosEngine, ChaosConfig

config = ChaosConfig(
    enabled=True,
    intensity="moderate",
    seed=42,
)
engine = ChaosEngine(config=config)

# Corrupt a DataFrame
corrupted_df = engine.corrupt_dataframe(df, day=10)

# Apply all applicable chaos categories
corrupted_df = engine.apply_all(df, day=15)
```

## Six Chaos Categories

| Category | What It Corrupts |
| --- | --- |
| `SCHEMA` | Column renames, type changes, missing columns, extra columns |
| `VALUE` | NULL injection, out-of-range values, type mismatches, encoding errors |
| `FILE` | Truncated files, corrupt headers, wrong formats, empty files |
| `REFERENTIAL` | Orphan FKs, duplicate PKs, broken references |
| `TEMPORAL` | Out-of-order timestamps, future dates, impossible date sequences |
| `VOLUME` | Row count spikes (10x), empty partitions, duplicate batches |

## Four Intensity Presets

| Preset | Multiplier | Description |
| --- | --- | --- |
| `calm` | 0.25x | Occasional issues — realistic production noise |
| `moderate` | 1.0x | Regular data quality problems |
| `stormy` | 2.5x | Frequent issues — stress testing |
| `hurricane` | 5.0x | Everything breaks — chaos testing |

## ChaosConfig

```python
from sqllocks_spindle.chaos import ChaosConfig, ChaosOverride

config = ChaosConfig(
    enabled=True,
    intensity="stormy",          # calm | moderate | stormy | hurricane
    seed=42,
    warmup_days=7,               # clean data for first N days
    chaos_start_day=8,           # chaos begins on this day
    escalation="gradual",        # gradual | random | front-loaded
    breaking_change_day=20,      # column drops/renames allowed after this day
    overrides=[                  # force specific chaos on specific days
        ChaosOverride(day=15, category="schema", params={"action": "drop_column"}),
    ],
)
```

| Param | Default | Description |
| --- | --- | --- |
| `enabled` | `False` | Master switch |
| `intensity` | `"moderate"` | Preset name or custom multiplier |
| `seed` | `42` | Random seed for reproducibility |
| `warmup_days` | `7` | Days of clean data before chaos starts |
| `chaos_start_day` | `8` | First day chaos can be injected |
| `escalation` | `"gradual"` | How injection probability increases over time |
| `breaking_change_day` | `20` | Day after which breaking schema changes are allowed |
| `overrides` | `[]` | List of `ChaosOverride` to force specific events |

## ChaosEngine Methods

```python
engine = ChaosEngine(config=config)

# Decision: should chaos be injected on this day for this category?
if engine.should_inject(day=10, category="value"):
    ...

# Per-category injection
df = engine.corrupt_dataframe(df, day=10)          # VALUE chaos
df = engine.drift_schema(df, day=10)               # SCHEMA chaos
bytes_ = engine.corrupt_file(file_bytes, day=10)   # FILE chaos

# Cross-table chaos
tables = engine.inject_referential_chaos(tables_dict, day=10)  # REFERENTIAL

# Temporal chaos (specify which columns are dates)
df = engine.inject_temporal_chaos(df, date_columns=["order_date"], day=10)

# Volume chaos
df = engine.inject_volume_chaos(df, day=10)        # VOLUME

# Apply all applicable categories at once
df = engine.apply_all(
    df, day=15,
    tables_dict=all_tables,
    date_columns=["order_date", "ship_date"],
)
```

## CLI Usage

```bash
# File-drop simulation with chaos
spindle simulate file-drop --domain retail --scale small \
  --start-date 2025-01-01 --end-date 2025-01-31 \
  --chaos-intensity stormy --output ./landing/
```

## Escalation Modes

- **`gradual`** — injection probability increases linearly from `chaos_start_day` to the end of the date range
- **`random`** — each day has an independent random probability based on intensity
- **`front-loaded`** — high probability early, decreasing over time (useful for testing recovery)

---

## See Also

- **Tutorial:** [07: Chaos Engineering](../tutorials/intermediate/07-chaos-engineering.md) — step-by-step walkthrough
- **Tutorial:** [13: Medallion Architecture](../tutorials/fabric/13-medallion.md) — step-by-step walkthrough
- **Example script:** [`13_chaos_injection.py`](https://github.com/sqllocks/spindle/blob/main/examples/scenarios/13_chaos_injection.py)
- **Notebook:** [`T14_chaos_engineering.ipynb`](https://github.com/sqllocks/spindle/blob/main/examples/notebooks/intermediate/T14_chaos_engineering.ipynb)
- **Notebook:** [`F05_chaos_pipeline.ipynb`](https://github.com/sqllocks/spindle/blob/main/examples/notebooks/fabric-scenarios/F05_chaos_pipeline.ipynb)
