# Fidelity Validation

Spindle measures how statistically similar synthetic data is to real data using **KS tests** (numeric) and **Chi-squared tests** (categorical). Results are scored 0-100.

## Basic Usage

```python
from sqllocks_spindle.inference.comparator import FidelityReport

report = FidelityReport.score(real_df, synthetic_df, table_name="customer")
print(f"Score: {report.overall_score:.1f}/100")
print(report.summary())

# Export as HTML
html = report.to_html(title="Customer Fidelity Report")
Path("report.html").write_text(html)

# Get failing columns
failing = report.failing_columns(threshold=85.0)
for table, col, score in failing:
    print(f"  {table}/{col}: {score:.1f}")
```

## Score Bands

| Score | Colour | Meaning |
|-------|--------|---------|
| ≥ 85 | Green | Production-quality synthetic data |
| 70–84 | Amber | Acceptable for most purposes |
| < 70 | Red | Investigate distribution mismatch |

## What the Tests Measure

- **KS statistic** (numeric columns): Measures the maximum difference between CDFs. KS = 0 means identical distributions.
- **Chi-squared** (categorical columns): Tests whether value frequencies are consistent.
- **Null rate delta**: Difference in missing-value rates.
- **Cardinality ratio**: `synth_unique / real_unique` — should be close to 1.0.

## Multi-Table Comparison

```python
from sqllocks_spindle.inference.comparator import FidelityComparator

comparator = FidelityComparator()
report = comparator.compare(
    real={"customer": real_customer_df, "order": real_order_df},
    synthetic={"customer": synth_customer_df, "order": synth_order_df},
)
print(f"Overall: {report.overall_score:.1f}/100")
```

## Realistic Ceiling

A fidelity score of 100/100 is intentionally unachievable — Spindle anonymises PII by design. Typical ceilings by table type:

| Table type | Expected ceiling |
|------------|-----------------|
| OLTP transactional | 88–94 |
| Analytics aggregates | 85–92 |
| PII-heavy (customer) | 78–88 |
| Time series | 82–90 |
