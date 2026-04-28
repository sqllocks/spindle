# Tutorial 14: Lakehouse Profiling

**Duration:** ~25 minutes  
**Level:** Intermediate (Fabric)  
**Prerequisites:** A Microsoft Fabric workspace with a Lakehouse containing Delta tables, and `az login` completed (or a service principal configured).  
**Extras required:** `pip install sqllocks-spindle[fabric-inference]`

---

## What You'll Build

A pipeline that:

1. Connects to a Fabric Lakehouse with `LakehouseProfiler`
2. Profiles a Delta table to capture distributions, correlations, and patterns
3. Builds a Spindle schema with `SchemaBuilder`
4. Generates synthetic data with correlation enforcement
5. Measures fidelity against the source profile

---

## Step 1: Install the Extra

```bash
pip install sqllocks-spindle[fabric-inference]
```

This installs `scipy`, `deltalake` (delta-rs), and `pyarrow`.

In a Fabric Notebook:

```python
%pip install sqllocks-spindle[fabric-inference]
```

---

## Step 2: Set Up Credentials

`LakehouseProfiler` uses `DefaultAzureCredential` by default.

**Local machine:** Make sure you are logged in:
```bash
az login
az account set --subscription "My Subscription"
```

**Fabric Notebook:** The managed identity is used automatically — no extra setup.

Find your workspace and lakehouse GUIDs in the Fabric portal URL:
```
https://app.fabric.microsoft.com/groups/{workspace_id}/lakehouses/{lakehouse_id}
```

---

## Step 3: Profile a Delta Table

```python
from sqllocks_spindle.inference import LakehouseProfiler

lp = LakehouseProfiler(
    workspace_id="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",  # replace
    lakehouse_id="yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",   # replace
)

# Profile with default 100,000 row sample
profile = lp.profile_table("sales_orders")
print(f"Profiled table: {list(profile.tables.keys())}")
```

---

## Step 4: Inspect the Profile

```python
table = list(profile.tables.values())[0]

for col in table.columns:
    print(f"{col.name}")
    print(f"  null_rate: {col.null_rate:.2%}")
    print(f"  fit_score: {col.fit_score}")
    if col.quantiles:
        print(f"  quantiles: {col.quantiles}")
    if col.hour_histogram:
        print(f"  has temporal histogram (hour)")

# Check detected correlations
if table.correlation_matrix:
    for col_a, pairs in table.correlation_matrix.items():
        for col_b, r in pairs.items():
            if abs(r) >= 0.5:
                print(f"  CORRELATION: {col_a} <-> {col_b}: r={r:.2f}")
```

---

## Step 5: Build a Schema with Correlation Enforcement

```python
from sqllocks_spindle.inference import SchemaBuilder

schema, registry = SchemaBuilder().build(
    profile,
    domain_name="sales_orders",
    fit_threshold=0.80,           # KS fit < 0.80 → empirical strategy
    correlation_threshold=0.5,    # |r| >= 0.5 → enforce via GaussianCopula
    include_anomaly_registry=True,
)

print("Schema built.")
```

---

## Step 6: Generate Synthetic Data

```python
from sqllocks_spindle import Spindle

result, fidelity = Spindle().generate(
    schema,
    seed=42,
    enforce_correlations=True,  # default; applies GaussianCopula post-pass
    fidelity_profile=profile,
)

print(f"Generated {result.total_rows:,} rows")
fidelity.summary()
```

---

## Step 7: Check Fidelity

```python
failing = fidelity.failing_columns(threshold=85.0)
if failing:
    print("Columns below fidelity threshold:")
    for table, col, score in failing:
        print(f"  {table}.{col}: {score:.1f}")
else:
    print("All columns meet fidelity threshold.")
```

---

## Step 8: Write Back to the Lakehouse

```python
# Write as Parquet to Files area for registration as Delta
result.to_parquet("/lakehouse/default/Files/spindle/sales_orders_synthetic")

# Or in a Fabric Notebook, write directly as Delta via Spark
for table_name, df in result.tables.items():
    sdf = spark.createDataFrame(df)
    sdf.write.format("delta").mode("overwrite").saveAsTable(f"spindle_{table_name}")

print("Synthetic data written to Lakehouse.")
```

---

## Step 9: Save Profile for Future Use

```python
from sqllocks_spindle.inference import ProfileIO

ProfileIO.save(profile, "sales_orders_profile.json")
print("Profile saved. Reuse with ProfileIO.load() to skip re-profiling.")
```

---

## Summary

| Step | Tool |
| --- | --- |
| Profile Lakehouse table | `LakehouseProfiler.profile_table()` |
| Detect correlations | Automatic in profiler, surfaced in `TableProfile.correlation_matrix` |
| Build schema | `SchemaBuilder().build(..., correlation_threshold=0.5)` |
| Enforce correlations | `Spindle().generate(..., enforce_correlations=True)` |
| Measure fidelity | `Spindle().generate(..., fidelity_profile=profile)` |
| Reuse profile | `ProfileIO.save()` / `ProfileIO.load()` |

---

## Next Steps

- **[Guide: Lakehouse Profiling](../../guides/lakehouse-profiling.md)** — full reference
- **[Tutorial 18: Fidelity Reporting](../intermediate/18-fidelity-reporting.md)** — deeper fidelity iteration
- **[Guide: Fidelity Scoring](../../guides/fidelity-scoring.md)** — scoring dimensions explained
