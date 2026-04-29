# Changing Column Variables in Profiles

This guide explains how to update the statistical variables tracked for each column in a stored profile — useful when the real data distribution changes or when you want to constrain a specific column.

## Profile Column Structure

Each column in a `RegistryProfile` is stored as a dict with these keys:

| Key | Type | Applies to |
|-----|------|-----------|
| `dtype` | str | All |
| `null_rate` | float (0–1) | All |
| `cardinality` | int | All |
| `mean` | float | Numeric |
| `std` | float | Numeric |
| `min` | float | Numeric |
| `max` | float | Numeric |
| `top_values` | dict str→float | Categorical |

## Loading and Updating a Column

```python
from sqllocks_spindle import ProfileRegistry

reg = ProfileRegistry()
profile = reg.load("salesforce/account/prod-2026Q2")

# Update mean and std
profile.columns["annual_revenue"]["mean"] = 285000.0
profile.columns["annual_revenue"]["std"] = 92000.0

# Tighten the range
profile.columns["annual_revenue"]["min"] = 5000.0
profile.columns["annual_revenue"]["max"] = 5000000.0

reg.save(profile)
```

## Updating Categorical Frequencies

```python
profile = reg.load("retail/customer/baseline")

# Update segment distribution
profile.columns["loyalty_tier"]["top_values"] = {
    "Basic": 0.35,
    "Silver": 0.30,
    "Gold": 0.25,
    "Platinum": 0.10,
}
reg.save(profile)
```

## Adding a New Column

```python
profile.columns["new_flag"] = {
    "dtype": "object",
    "null_rate": 0.02,
    "cardinality": 2,
    "top_values": {"Y": 0.80, "N": 0.20},
}
reg.save(profile)
```

## Removing a Column

```python
profile.columns.pop("deprecated_column", None)
reg.save(profile)
```

## Bulk Update via diff

```python
diff = reg.diff("retail/customer/v1", "retail/customer/v2")
print("Changed columns:", list(diff["changed"].keys()))
# Review changes, then accept v2 as the new baseline:
# (no action needed — v2 is already saved)
```

## Verifying Changes

After updating, run a validation to confirm the new profile produces expected fidelity:

```bash
spindle profile registry validate retail/customer/prod-2026Q2 --domain retail --output /tmp/fidelity.html
```
