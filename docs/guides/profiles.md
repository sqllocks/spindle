# Distribution Profiles

Every Spindle domain ships with a `default` profile — a set of distribution weights calibrated from real-world data. You can override any weight at runtime or create named profiles.

## Using Profiles

```python
from sqllocks_spindle import RetailDomain, HealthcareDomain

# Default profile (calibrated from NRF, Census, CMS data)
domain = RetailDomain()

# Override specific distributions
domain = RetailDomain(overrides={
    "customer.loyalty_tier": {"Basic": 0.40, "Silver": 0.30, "Gold": 0.20, "Platinum": 0.10},
    "order.status": {"completed": 0.85, "shipped": 0.05, "processing": 0.02, "cancelled": 0.03, "returned": 0.05},
})

# Use a named profile (if available)
domain = HealthcareDomain(profile="medicare")
```

## How Profiles Work

Each domain's `profiles/` directory contains JSON files with distribution weights:

```
domains/retail/profiles/
  default.json     # Ships with Spindle — calibrated from real data
  holiday_heavy.json  # Custom — heavier Nov/Dec weighting
```

### Profile JSON Structure

A profile is a flat dict of `"table.column"` keys mapping to weight dicts or ratio values:

```json
{
  "customer.gender": {"M": 0.49, "F": 0.51},
  "customer.loyalty_tier": {"Basic": 0.55, "Silver": 0.25, "Gold": 0.13, "Platinum": 0.07},
  "customer.active_rate": 0.85,
  "order.status": {"completed": 0.77, "shipped": 0.08, "processing": 0.02, "cancelled": 0.04, "returned": 0.09},
  "order.items_per_order_mean": 2.5,
  "address.per_customer_ratio": 1.5
}
```

### Accessing Profile Values in Domain Code

Domain classes use two helper methods to read profile values:

- `self._dist("table.column")` — returns a distribution dict (e.g., `{"M": 0.49, "F": 0.51}`)
- `self._ratio("table.column_ratio", default=1.0)` — returns a float ratio

## Listing Available Profiles

```python
domain = RetailDomain()
print(domain.available_profiles)   # ['default']
print(domain.profile_name)         # 'default'
```

```bash
spindle list   # Shows all domains with their available profiles
```

## Override Precedence

1. **Profile JSON** provides the base weights
2. **`overrides={}` dict** at construction time overrides any profile key
3. Runtime overrides take priority over profile values

```python
# Profile says loyalty_tier is 55/25/13/7
# Override changes it to 40/30/20/10
domain = RetailDomain(overrides={
    "customer.loyalty_tier": {"Basic": 0.40, "Silver": 0.30, "Gold": 0.20, "Platinum": 0.10},
})
```

## Calibration Sources

All default profile weights are documented with real-world sources in the [Methodology](../methodology/calibration.md). Sources include:

- **Retail:** NRF, US Census, Statista, Shopify, CapitalOne Shopping
- **Healthcare:** CMS, CDC, AAMC, KFF, BLS, AHA, MEPS
- **Other domains:** Industry-specific authoritative sources

See [Methodology & Calibration](../methodology/calibration.md) for the full citation trail.

---

## See Also

- **Tutorial:** [01: Hello Spindle](../tutorials/beginner/01-hello-spindle.md) — step-by-step walkthrough
- **Example script:** [`05_distribution_overrides.py`](https://github.com/sqllocks/spindle/blob/main/examples/scenarios/05_distribution_overrides.py)
- **Notebook:** [`T05_distribution_overrides.ipynb`](https://github.com/sqllocks/spindle/blob/main/examples/notebooks/quickstart/T05_distribution_overrides.ipynb)
