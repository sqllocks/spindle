# Tutorial 18: Fidelity Reporting

**Duration:** ~20 minutes  
**Level:** Intermediate  
**Prerequisites:** Complete Tutorial 01 (Hello Spindle) and have a CSV or Parquet file of real data handy.  
**Extras required:** `pip install sqllocks-spindle[inference]`

---

## What You'll Build

A pipeline that:

1. Profiles a real CSV file with `DataProfiler`
2. Infers a Spindle schema with `SchemaBuilder`
3. Generates synthetic data with `Spindle.generate()`
4. Measures statistical fidelity with `FidelityReport`
5. Identifies failing columns and iterates on the schema

---

## Step 1: Set Up

```bash
pip install sqllocks-spindle[inference]
```

We'll use a sample orders CSV for this tutorial. If you don't have real data, create a synthetic one first:

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
real_orders = result.tables["order"]
real_orders.to_csv("real_orders.csv", index=False)
print(f"Created real_orders.csv: {len(real_orders):,} rows")
```

---

## Step 2: Profile the Real Data

```python
from sqllocks_spindle.inference import DataProfiler, ProfileIO

profiler = DataProfiler(
    fit_threshold=0.80,   # columns with KS fit < 0.80 get empirical strategy
    sample_rows=None,     # full scan for small files; use an int for large files
)

profile = DataProfiler.from_csv("real_orders.csv")

# Save the profile for reuse
ProfileIO.save(profile, "orders_profile.json")
print("Profile saved.")
```

---

## Step 3: Inspect the Profile

```python
from sqllocks_spindle.inference import ProfileIO

profile = ProfileIO.load("orders_profile.json")

# Inspect one table
table = profile.tables["real_orders"]  # key matches CSV filename stem
for col in table.columns:
    print(f"  {col.name}: null_rate={col.null_rate:.2%}, fit_score={col.fit_score}")
    if col.quantiles:
        print(f"    → empirical strategy queued (fit < threshold)")
```

---

## Step 4: Build a Schema

```python
from sqllocks_spindle.inference import SchemaBuilder

schema, registry = SchemaBuilder().build(
    profile,
    domain_name="orders",
    fit_threshold=0.80,
    correlation_threshold=0.5,
    include_anomaly_registry=True,
)

print("Schema built.")
print(f"Suggested registry anomaly types: {[type(a).__name__ for a in registry.anomalies]}")
```

---

## Step 5: Generate with Fidelity Scoring

```python
from sqllocks_spindle import Spindle

result, fidelity = Spindle().generate(
    schema,
    seed=42,
    fidelity_profile=profile,
)

print(f"Generated {result.total_rows:,} rows across {len(result.table_names)} tables")
```

---

## Step 6: Read the Fidelity Report

```python
# Print the full per-column table
fidelity.summary()

# Find columns below 85% threshold
failing = fidelity.failing_columns(threshold=85.0)
if failing:
    print("\nFailing columns:")
    for table, col, score in failing:
        print(f"  {table}.{col}: {score:.1f}/100")
else:
    print("\nAll columns above threshold.")

# Export as a DataFrame
df_scores = fidelity.to_dataframe()
print(df_scores.sort_values("score").head(10))
```

---

## Step 7: Iterate on Failing Columns

If any columns fail, the most common fixes are:

**A. Force empirical strategy** — lower `fit_threshold` so more columns use quantile interpolation:

```python
schema = SchemaBuilder().build(profile, domain_name="orders", fit_threshold=0.60)
result, fidelity = Spindle().generate(schema, seed=42, fidelity_profile=profile)
fidelity.summary()
```

**B. Check the column in the profile** — if it has an unusual distribution that the profiler doesn't capture well, inspect the `quantiles` field and ensure it's populated:

```python
table = profile.tables["real_orders"]
col = next(c for c in table.columns if c.name == "order_amount")
print(col.quantiles)  # should show p1..p99 values
```

**C. Manually set quantiles in the schema JSON** — for full control, open the inferred `.spindle.json` and adjust the strategy directly.

---

## Step 8: Save the Report

```python
import json
from pathlib import Path

Path("reports").mkdir(exist_ok=True)
json.dump(fidelity.to_dict(), open("reports/fidelity_report.json", "w"), indent=2)
fidelity.to_dataframe().to_csv("reports/fidelity_scores.csv", index=False)
print("Reports saved to reports/")
```

---

## Summary

You've completed the fidelity reporting loop:

| Step | Tool |
| --- | --- |
| Profile real data | `DataProfiler.from_csv()` |
| Infer schema | `SchemaBuilder().build()` |
| Generate + score | `Spindle().generate(..., fidelity_profile=...)` |
| Identify gaps | `fidelity.failing_columns()` |
| Iterate | Lower `fit_threshold`, force `empirical` strategy |

---

## Next Steps

- **[Lakehouse Profiling](../fabric/14-lakehouse-profiling.md)** — run this same pipeline against a Fabric Delta table
- **[Guide: Fidelity Scoring](../../guides/fidelity-scoring.md)** — deep dive on scoring dimensions
- **[Guide: Empirical Distributions](../../guides/empirical-distributions.md)** — how quantile interpolation works
