# Tutorial 07: Chaos Engineering

Inject realistic data quality failures -- schema drift, null corruption, broken foreign keys -- to test your pipeline's resilience before production surprises you.

## Prerequisites

- Completed [Tutorial 06: Streaming](06-streaming.md) (or equivalent experience)
- Familiarity with `Spindle.generate()` and domain objects
- Basic understanding of data quality concerns in ETL pipelines

## What You'll Learn

- Why chaos engineering matters for data pipelines
- How to configure `ChaosEngine` with `ChaosConfig` and intensity presets
- How to apply value corruption (null injection, type mismatches)
- How to apply schema drift (added, removed, renamed columns)
- How to break referential integrity (orphaned foreign keys)
- How to inject temporal and volume chaos
- How to simulate day-by-day chaos escalation
- How to use `ChaosOverride` to force specific chaos on specific days

## Time Estimate

**~20 minutes**

---

## Why Inject Chaos?

Most data pipelines are tested with clean, well-formed data. But production data is messy:

- A source system deploys a schema change and suddenly a column is renamed or removed
- A bug in an upstream system starts writing nulls into a NOT NULL column
- A data migration corrupts foreign-key relationships
- A decimal field starts receiving string values

If you only test with clean data, these failures will surprise you in production. Chaos engineering means deliberately injecting failures so you can verify your pipeline handles them gracefully -- with clear error messages, proper quarantining, and no silent data loss.

## Step 1 -- Generate a Clean Baseline

Start with clean data so you can measure the impact of chaos by comparing before and after:

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

print(result.summary())
print(f"\nBaseline null counts:")
for name, df in result.tables.items():
    null_count = df.isnull().sum().sum()
    print(f"  {name}: {null_count} nulls across {len(df.columns)} columns")
```

Clean Spindle data has zero unexpected nulls and valid foreign keys throughout. This is your known-good baseline.

## Step 2 -- Understand the Intensity Scale

Spindle's chaos engine has four intensity presets that control probability multipliers across all chaos categories:

| Intensity | Multiplier | Description |
|-----------|-----------|-------------|
| `calm` | 0.25x | Minimal chaos -- a few nulls, one renamed column |
| `moderate` | 1.0x | Noticeable issues -- multiple null injections, some type mismatches |
| `stormy` | 2.5x | Significant breakage -- schema drift, broken FKs, widespread corruption |
| `hurricane` | 5.0x | Maximum chaos -- everything breaks at once |

## Step 3 -- Configure Chaos at Moderate Intensity

Create a `ChaosConfig` with `moderate` intensity. This injects a manageable amount of corruption -- enough to test your error handling without making the data completely unusable:

```python
from sqllocks_spindle.chaos import ChaosConfig, ChaosEngine

chaos_config = ChaosConfig(enabled=True, intensity="moderate", seed=99)
engine = ChaosEngine(chaos_config)

print(f"Chaos enabled: {chaos_config.enabled}")
print(f"Intensity: {chaos_config.intensity}")
print(f"Seed: {chaos_config.seed}")
```

## Step 4 -- Apply Value Corruption

Value corruption injects nulls into columns that should be NOT NULL, swaps data types, and introduces out-of-range values. The `day` parameter controls the chaos timeline (more on that in Step 8):

```python
customer_copy = result.tables["customers"].copy()

corrupted = engine.corrupt_dataframe(customer_copy, day=5)

original_nulls = result.tables["customers"].isnull().sum().sum()
corrupted_nulls = corrupted.isnull().sum().sum()

print(f"Original null count: {original_nulls}")
print(f"Corrupted null count: {corrupted_nulls}")
print(f"New nulls injected: {corrupted_nulls - original_nulls}")

# Show which columns gained nulls
for col in corrupted.columns:
    orig = result.tables["customers"][col].isnull().sum()
    corr = corrupted[col].isnull().sum()
    if corr > orig:
        print(f"  {col}: {orig} -> {corr} (+{corr - orig} nulls)")
```

## Step 5 -- Apply Schema Drift

Schema drift includes added columns (your pipeline does not expect them), removed columns (expected columns disappear), and renamed columns (e.g., `order_date` becomes `orderDate`):

```python
orders_copy = result.tables["orders"].copy()
original_columns = set(orders_copy.columns)

drifted = engine.apply_schema_drift(orders_copy, day=3)
drifted_columns = set(drifted.columns)

added = drifted_columns - original_columns
removed = original_columns - drifted_columns

print(f"Original columns ({len(original_columns)}): {sorted(original_columns)}")
print(f"Drifted columns ({len(drifted_columns)}): {sorted(drifted_columns)}")
if added:
    print(f"Added columns: {sorted(added)}")
if removed:
    print(f"Removed columns: {sorted(removed)}")
```

Schema drift is inevitable in any system with multiple teams or external data sources. Your pipeline needs to detect drift and either adapt or fail gracefully -- never silently drop data.

## Step 6 -- Break Referential Integrity

Broken foreign keys cause JOINs to silently drop rows. This chaos category corrupts key columns so that child records reference parent IDs that do not exist:

```python
orders_for_fk = result.tables["orders"].copy()
valid_customer_ids = set(result.tables["customers"]["customer_id"])

# Baseline: zero orphans
baseline_orphans = orders_for_fk[~orders_for_fk["customer_id"].isin(valid_customer_ids)]
print(f"Baseline orphaned orders: {len(baseline_orphans)}")

# Apply referential chaos
broken = engine.break_referential_integrity(orders_for_fk, column="customer_id", day=5)

chaos_orphans = broken[~broken["customer_id"].isin(valid_customer_ids)]
print(f"Orphaned orders after chaos: {len(chaos_orphans)}")
print(f"Orphan rate: {len(chaos_orphans) / len(broken) * 100:.1f}%")
```

## Step 7 -- Ramp Up to Hurricane

Hurricane intensity is your worst-case scenario test. Compare the damage across intensities:

```python
hurricane_config = ChaosConfig(enabled=True, intensity="hurricane", seed=99)
hurricane_engine = ChaosEngine(hurricane_config)

customer_hurricane = result.tables["customers"].copy()
wrecked = hurricane_engine.corrupt_dataframe(customer_hurricane, day=5)

original_nulls = result.tables["customers"].isnull().sum().sum()
hurricane_nulls = wrecked.isnull().sum().sum()

print(f"=== Intensity Comparison ===")
print(f"Original:  {original_nulls} nulls")
print(f"Moderate:  {corrupted_nulls} nulls")
print(f"Hurricane: {hurricane_nulls} nulls")
```

If your pipeline can survive hurricane intensity, it can handle anything production throws at it.

## Step 8 -- Day-by-Day Chaos Simulation

In real pipelines, chaos does not hit all at once. Spindle supports a day-by-day simulation model with warmup periods, gradual escalation, and breaking-change gates:

```python
from sqllocks_spindle.chaos.config import ChaosConfig, ChaosCategory

cfg = ChaosConfig(
    enabled=True,
    intensity="stormy",
    seed=99,
    warmup_days=7,            # no chaos before day 7
    chaos_start_day=8,
    escalation="gradual",     # ramp up probabilities over 30 days
    breaking_change_day=20,   # column drops/renames only after day 20
)

engine = ChaosEngine(cfg)

print("Day-by-day chaos decisions:")
for day in [1, 5, 8, 12, 20, 25, 30]:
    decisions = {
        cat.value: engine.should_inject(day, cat.value)
        for cat in ChaosCategory
    }
    active = [k for k, v in decisions.items() if v]
    print(f"  Day {day:2d}: {active if active else '(none)'}")
```

This simulates a realistic production timeline: the pipeline runs clean for a week, then chaos starts slowly and escalates, with destructive schema changes only appearing after day 20.

## Step 9 -- Force-Inject with ChaosOverride

Use `ChaosOverride` to guarantee specific chaos on specific days, regardless of probability rolls:

```python
from sqllocks_spindle.chaos.config import ChaosOverride

cfg_with_override = ChaosConfig(
    enabled=True,
    intensity="moderate",
    seed=42,
    overrides=[
        ChaosOverride(day=14, category="value",  params={"severity": "high"}),
        ChaosOverride(day=14, category="volume", params={}),
    ],
)

engine_override = ChaosEngine(cfg_with_override)
overrides_day14 = cfg_with_override.overrides_for_day(14)
print(f"Overrides on day 14: {[o.category for o in overrides_day14]}")
```

## Step 10 -- Apply All Chaos at Once

The `apply_all` convenience method runs every chaos category in a single call:

```python
date_cols = [c for c in orders.columns if "date" in c.lower()]

after_all = engine.apply_all(
    df=orders.copy(),
    day=12,
    tables_dict=result.tables,
    date_columns=date_cols,
)
print(f"Output shape: {after_all.shape}")
```

This applies value corruption, schema drift, temporal chaos, volume chaos, and referential breakage in one step -- useful when you want the full chaos experience without calling each method individually.

---

> **Run It Yourself**
>
> - Notebook: [`T14_chaos_engineering.ipynb`](../../../examples/notebooks/intermediate/T14_chaos_engineering.ipynb)
> - Script: [`13_chaos_injection.py`](../../../examples/scenarios/13_chaos_injection.py)

## Related

- [Chaos Guide](../../guides/chaos.md) -- full reference for all six chaos categories, escalation modes, and override configuration

## Next Step

Continue to [Tutorial 08: Validation Gates](08-validation-gates.md) to learn how to catch the chaos you just injected with automated quality checks and quarantine flows.
