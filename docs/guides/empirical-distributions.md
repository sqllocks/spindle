# Empirical Distributions

The `empirical` strategy generates numeric values by interpolating a quantile fingerprint captured from real data. Use it when no standard distribution (normal, log-normal, Pareto, etc.) fits your source data well.

## When to Use Empirical

`SchemaBuilder` automatically selects the `empirical` strategy when the KS test fit score for a column falls below `fit_threshold` (default 0.80). You can also use it explicitly in any schema.

Common cases where empirical outperforms parametric:

- Multi-modal distributions (e.g., a price column with two distinct product tiers)
- Heavy-tailed or irregularly shaped data
- Business metrics with hard floor/ceiling effects that distort standard distributions

## How It Works

1. `DataProfiler` captures P1/P5/P10/P25/P50/P75/P90/P95/P99 from the real column
2. `SchemaBuilder` emits an `empirical` strategy with these quantiles
3. At generation time, uniform(0,1) samples are mapped through the quantile curve via interpolation

This preserves the source distribution's shape at all nine fingerprint points and smoothly interpolates between them.

## Schema Format

```json
{
  "strategy": "empirical",
  "quantiles": {
    "p1": 1.25,
    "p5": 4.99,
    "p10": 9.99,
    "p25": 24.99,
    "p50": 49.99,
    "p75": 99.99,
    "p90": 199.99,
    "p95": 349.99,
    "p99": 799.99
  },
  "interpolation": "linear"
}
```

All nine keys (`p1`–`p99`) are required.

## Interpolation Options

| Value | Algorithm | Requires |
| --- | --- | --- |
| `"linear"` | `numpy.interp` — piecewise linear | NumPy (always available) |
| `"cubic"` | `scipy.interpolate.interp1d` — smooth cubic spline | `[inference]` or `[fabric-inference]` extra |

Cubic interpolation produces a smoother distribution but requires scipy. If `"cubic"` is specified and scipy is absent, Spindle falls back to linear with a warning.

## Automatic Emission from SchemaBuilder

```python
from sqllocks_spindle.inference import DataProfiler, SchemaBuilder

profiler = DataProfiler(fit_threshold=0.80)
profile = profiler.from_csv("transactions.csv")

schema = SchemaBuilder().build(
    profile,
    domain_name="transactions",
    fit_threshold=0.80,   # same threshold; columns below this → empirical
)
```

Columns where the profiler's KS test score < 0.80 will automatically receive an `empirical` strategy populated with the captured quantiles.

## Extracting Quantiles from a Profiled Column

```python
from sqllocks_spindle.inference import DataProfiler

profiler = DataProfiler()
profile = profiler.profile_dataset({"orders": orders_df})

table_profile = profile.tables["orders"]
for col in table_profile.columns:
    if col.quantiles:
        print(f"{col.name}: {col.quantiles}")
```

---

## See Also

- **Guide:** [Generation Strategies](strategies.md) — all 22 strategies
- **Guide:** [Schema Learning](schema-learning.md) — profiling and schema inference
- **Guide:** [Fidelity Scoring](fidelity-scoring.md) — verify distribution fidelity after generation
