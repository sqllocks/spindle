# Drift Monitoring

Track whether your synthetic data's distribution has drifted from the reference profile over time.

## What is Drift?

Drift occurs when the statistical properties of a dataset change — means shift, frequencies change, new values appear, or old values disappear. In synthetic data, drift indicates that the generation parameters no longer match the original target distribution.

## Using DriftMonitor

```python
from sqllocks_spindle.inference.tier3_research import DriftMonitor

monitor = DriftMonitor(
    pvalue_threshold=0.05,  # KS/Chi2 p-value below this → drifted
    psi_threshold=0.2,      # PSI above this → drifted
)

report = monitor.compare(reference_df, current_df)
print(f"Drifted columns: {report.drifted_columns}")
print(f"Drift fraction:  {report.drift_fraction:.1%}")
```

## Thresholds

| Metric | Low (no concern) | Medium | High (action needed) |
|--------|-----------------|--------|---------------------|
| KS statistic | < 0.05 | 0.05–0.15 | > 0.15 |
| PSI | < 0.1 | 0.1–0.2 | > 0.2 |
| Chi2 p-value | > 0.05 | 0.01–0.05 | < 0.01 |

## Automated Drift Detection in CI

```python
monitor = DriftMonitor()
report = monitor.compare(reference_df, new_batch_df)

if report.drift_fraction > 0.2:
    raise RuntimeError(
        f"20%+ of columns drifted: {report.drifted_columns}"
    )
```

## Connecting to Profile Registry

```python
from sqllocks_spindle import ProfileRegistry
from sqllocks_spindle.inference.tier3_research import DriftMonitor

reg = ProfileRegistry()
profile = reg.load("retail/customer/baseline-2026")
ref_df = ProfileRegistry._reconstruct_reference(profile, n_rows=1000)

monitor = DriftMonitor()
report = monitor.compare(ref_df, new_customer_df)
```
