# Fidelity Scoring

Fidelity scoring measures how closely Spindle-generated data matches the statistical properties of your source data. A high fidelity score means the synthetic data is statistically indistinguishable from real data across distribution shape, null rates, cardinality, pattern match rates, and correlations.

## Why Fidelity Matters

Synthetic data is only useful if it behaves like real data under analysis. A fidelity report answers:

- Does the `amount` column have the right distribution shape? (not just roughly normal — the real shape)
- Is the null rate in `discount_code` the same as production?
- Are the correlated columns (`unit_price` and `cost`) still correlated?
- Do detected patterns (emails, SSNs, IBANs) appear at the same rate?

## Quick Start

```python
from sqllocks_spindle.inference.comparator import FidelityReport

# Score generated df against real df
report = FidelityReport.score(real_df, synthetic_df, table_name="orders")

report.summary()                              # print per-column score table
failing = report.failing_columns(threshold=85.0)  # list of (table, col, score) below threshold
df_scores = report.to_dataframe()             # pandas DataFrame for analysis
report_dict = report.to_dict()                # serializable dict
```

## Score Dimensions

Each column is scored 0–100 across multiple dimensions, then averaged into a composite score:

| Dimension | Metric | Applies To |
| --- | --- | --- |
| **dtype match** | Column type matches (int/float/string/datetime) | All |
| **Null rate** | `1 - abs(real_null_rate - synth_null_rate)` | All |
| **Cardinality** | `min(synth_unique / real_unique, 1.0)` | All |
| **Mean** | Normalized mean deviation | Numeric |
| **Std** | Ratio of standard deviations | Numeric |
| **KS test** | Kolmogorov-Smirnov statistic | Numeric |
| **Value overlap** | Fraction of real values present in synthetic | Categorical |
| **Chi-squared** | Distribution shape similarity | Categorical |

Composite score = weighted average (equal weights by default).

### Interpreting Scores

| Score | Interpretation |
| --- | --- |
| 90–100 | Excellent — statistically indistinguishable |
| 80–90 | Good — minor distributional differences |
| 70–80 | Acceptable — suitable for most tests |
| < 70 | Investigate — distribution mismatch, consider schema tuning |

## Inline Scoring During Generation

The most convenient path: pass `fidelity_profile` to `Spindle.generate()` and get the report in the same call.

```python
from sqllocks_spindle import Spindle
from sqllocks_spindle.inference import DataProfiler, SchemaBuilder, ProfileIO

# Step 1: profile real data (once)
profiler = DataProfiler()
profile = profiler.profile_dataset({"orders": real_orders_df})
ProfileIO.save(profile, "orders_profile.json")

# Step 2: build schema from profile
profile = ProfileIO.load("orders_profile.json")
schema = SchemaBuilder().build(profile, domain_name="orders")

# Step 3: generate + score in one call
result, fidelity = Spindle().generate(schema, seed=42, fidelity_profile=profile)

fidelity.summary()
print(f"Failing columns: {fidelity.failing_columns(threshold=85.0)}")
```

When `fidelity_profile` is `None` (default), `Spindle.generate()` returns the original single `GenerationResult`. When supplied, it returns a `(GenerationResult, FidelityReport)` tuple.

## Configuration

```python
FidelityReport.score(
    real_df,
    synthetic_df,
    table_name="orders",    # label used in reports
    threshold=85.0,         # failing_columns() uses this as the cutoff
)
```

## Iterating on Fidelity

A typical tuning loop:

1. Profile → `DataProfiler.from_csv("real_data.csv")`
2. Build schema → `SchemaBuilder().build(profile, fit_threshold=0.80)`
3. Generate → `Spindle().generate(schema, fidelity_profile=profile)`
4. Check report → `fidelity.failing_columns()`
5. Tune schema: lower `fit_threshold` to force more columns to use empirical strategy, add explicit `quantiles` overrides, or adjust distribution params
6. Repeat

For columns with highly unusual distributions, explicitly switch to `empirical` strategy in the schema JSON and supply the quantile fingerprint from the profiler output.

## Exporting Results

```python
# Save scores as CSV for reporting
fidelity.to_dataframe().to_csv("fidelity_scores.csv", index=False)

# Serialize as JSON
import json
json.dump(fidelity.to_dict(), open("fidelity_report.json", "w"), indent=2)
```

---

## See Also

- **Guide:** [Schema Learning](schema-learning.md) — profile real data and infer schemas
- **Guide:** [Empirical Distributions](empirical-distributions.md) — quantile-fingerprint interpolation
- **Guide:** [Validation Gates](validation.md) — structural quality checks (FK integrity, nulls, uniqueness)
- **Tutorial:** [18: Fidelity Reporting](../tutorials/intermediate/18-fidelity-reporting.md) — end-to-end walkthrough
