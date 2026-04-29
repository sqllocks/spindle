# Profile Registry

The **Profile Registry** lets you save named, tagged statistical profiles of any dataset, then reuse them for fidelity validation and comparison across runs, environments, and schema versions.

## Concepts

| Term | Description |
|------|-------------|
| **System** | The source system (e.g. `salesforce`, `sap`, `fabric-dw`) |
| **Table** | The table or entity name |
| **Name** | A version or environment label (`prod-2026Q2`, `uat-baseline`) |
| **Identity** | Fully-qualified key: `system/table/name` |

## Quick Start

```python
from sqllocks_spindle import Spindle, ProfileRegistry
from sqllocks_spindle.inference import DataProfiler, DatasetProfile

spindle = Spindle()
reg = ProfileRegistry()  # default root: ~/.spindle/profiles

# 1. Generate reference data
result = spindle.generate(domain=RetailDomain(), scale="medium", seed=42)

# 2. Profile and save
profiler = DataProfiler(sample_rows=1000)
table_profiles = {
    name: profiler.profile(df, table_name=name)
    for name, df in result.tables.items()
}
saved = reg.save_from_dataset_profile(
    DatasetProfile(tables=table_profiles),
    system="retail",
    name="prod-2026Q2",
    tags=["prod", "baseline"],
)
print(f"Saved {len(saved)} profiles")

# 3. Later — validate new generation
new_result = spindle.generate(domain=RetailDomain(), scale="medium", seed=99)
report = reg.validate("retail/customer/prod-2026Q2", new_result)
print(report.summary())
```

## CLI

```bash
# List all profiles
spindle profile registry list

# Filter by system
spindle profile registry list --system salesforce

# Save from domain generation
spindle profile registry save retail --system retail --name baseline-2026

# Validate
spindle profile registry validate retail/customer/baseline-2026 --domain retail --output report.html

# Tag a profile
spindle profile registry tag retail/customer/baseline-2026 pii sensitive

# Diff two versions
spindle profile registry diff retail/customer/v1 retail/customer/v2

# Rebuild index from disk
spindle profile registry reindex
```

## Changing Column Variables

When the data schema changes (new column, renamed field, adjusted distribution), update the profile:

```python
profile = reg.load("salesforce/account/prod-2026")
# Update a column's tracked statistics
profile.columns["annual_revenue"]["mean"] = 285000.0
profile.columns["annual_revenue"]["std"] = 92000.0
reg.save(profile)

# Or add a new column to the tracked set
profile.columns["tier"] = {
    "dtype": "object",
    "null_rate": 0.0,
    "cardinality": 4,
    "top_values": {"Enterprise": 0.35, "Commercial": 0.40, "SMB": 0.20, "Free": 0.05},
}
reg.save(profile)
```

The next `reg.validate()` call will include the new column in the fidelity comparison.

## Bulk Import

```python
# Import all profiles from a directory tree
imported = reg.import_from_dir(Path("/data/profiles/"), overwrite=False)
print(f"Imported {len(imported)} profiles")
```

## Profile Diff

```python
diff = reg.diff("retail/customer/v1", "retail/customer/v2")
print("Added:", diff["added"])
print("Removed:", diff["removed"])
print("Changed:", diff["changed"])
```
