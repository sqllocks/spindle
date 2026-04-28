# Lakehouse Profiling

`LakehouseProfiler` reads Delta tables directly from a Microsoft Fabric Lakehouse over ABFSS and returns the same `DatasetProfile` as `DataProfiler`. No local Spark session required — it uses the `deltalake` (delta-rs) library.

## Requirements

```bash
pip install sqllocks-spindle[fabric-inference]
```

The `[fabric-inference]` extra installs `scipy`, `deltalake`, and `pyarrow`.

## Quick Start

```python
from sqllocks_spindle.inference import LakehouseProfiler

lp = LakehouseProfiler(
    workspace_id="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    lakehouse_id="yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",
    # token_provider=None → DefaultAzureCredential used automatically
)

profile = lp.profile_table("sales_orders")
```

## Constructor

```python
LakehouseProfiler(
    workspace_id: str,         # Fabric workspace GUID
    lakehouse_id: str,         # Fabric Lakehouse GUID
    token_provider=None,       # callable returning a bearer token string; None = DefaultAzureCredential
    default_sample_rows=100_000,  # default row cap per profile_table() call
)
```

## Profiling Methods

### Profile a Single Table

```python
# Default: sample 100,000 rows
profile = lp.profile_table("sales_orders")

# Explicit sample size
profile = lp.profile_table("sales_orders", sample_rows=50_000)

# Full table scan (may be slow for large tables)
profile = lp.profile_table("sales_orders", sample_rows=None)
```

### Profile All Tables

```python
# Returns dict[str, DatasetProfile] — one entry per table in the Lakehouse
profiles = lp.profile_all(sample_rows=100_000)

for table_name, profile in profiles.items():
    print(f"{table_name}: {len(profile.tables)} tables profiled")
```

## Save and Reuse Profiles

Profile once, reuse across sessions:

```python
from sqllocks_spindle.inference import ProfileIO

# Save
ProfileIO.save(profile, "sales_orders_profile.json")

# Load
profile = ProfileIO.load("sales_orders_profile.json")
```

## End-to-End: Lakehouse → Synthetic Data

```python
from sqllocks_spindle import Spindle
from sqllocks_spindle.inference import LakehouseProfiler, SchemaBuilder, ProfileIO

# 1. Profile a Lakehouse table
lp = LakehouseProfiler(workspace_id="...", lakehouse_id="...")
profile = lp.profile_table("sales_orders")
ProfileIO.save(profile, "sales_orders_profile.json")

# 2. Build a schema — automatically selects best strategy per column
schema, registry = SchemaBuilder().build(
    profile,
    domain_name="sales",
    fit_threshold=0.80,           # KS fit < 0.80 → empirical strategy
    correlation_threshold=0.5,    # |r| >= 0.5 → correlation enforcement
    include_anomaly_registry=True,
)

# 3. Generate synthetic data with fidelity scoring
result, fidelity = Spindle().generate(schema, seed=42, fidelity_profile=profile)
fidelity.summary()

# 4. Write the synthetic data back to a Lakehouse
result.to_parquet("/lakehouse/default/Files/spindle/sales_orders_synthetic")
```

## Authentication

By default, `LakehouseProfiler` uses `DefaultAzureCredential` from `azure-identity`, which works transparently in:

- Fabric Notebooks (managed identity)
- Local machines with `az login`
- Service principals via environment variables (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`)

To supply your own token:

```python
def my_token_provider() -> str:
    # return a valid Azure bearer token string
    return "eyJ0..."

lp = LakehouseProfiler(
    workspace_id="...",
    lakehouse_id="...",
    token_provider=my_token_provider,
)
```

## How It Works

1. Constructs the ABFSS path: `abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}`
2. Reads the Delta table using `deltalake.DeltaTable` + `to_pandas()` with optional row sampling
3. Passes the resulting DataFrame through `DataProfiler` to compute all statistics
4. Returns a `DatasetProfile` identical to the one you'd get from `DataProfiler.profile_dataset()`

---

## See Also

- **Guide:** [Fabric Lakehouse](fabric-lakehouse.md) — write generated data to a Lakehouse
- **Guide:** [Schema Learning](schema-learning.md) — full inference pipeline
- **Guide:** [Fidelity Scoring](fidelity-scoring.md) — measure statistical similarity
- **Tutorial:** [14: Lakehouse Profiling](../tutorials/fabric/14-lakehouse-profiling.md) — end-to-end walkthrough
