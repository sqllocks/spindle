# Spindle Phase 3B — Inference Depth Design

## Goal

Make Spindle's generated data statistically match real source data across all fidelity dimensions: distribution shape, cardinality, null rates, temporal patterns, string formats, outlier rates, and column correlations. The entry point is profiling real data (DataFrame, CSV, SQL, or Fabric Lakehouse), and the output is both a schema that drives generation and a `FidelityReport` that scores how closely generated data matches the source.

## Architecture

`ExportedProfile` is the unifying abstraction. Everything flows through it:

```
Source data
  → DataProfiler.profile(df, ...)        # enhanced to capture all statistics
  → ExportedProfile                       # serializable, versionable, shareable
  → SchemaBuilder.build(profile, ...)    # maps statistics → Spindle schema
  → Spindle schema
  → Spindle.generate(schema, ...)        # engine generates data
  → DataFrame
  → FidelityReport.score(df, profile)   # scores generated vs. source profile
```

All configurable knobs have opinionated defaults. Users can override anything.

---

## Section 1: Entry Points

Three ways to reach an `ExportedProfile`:

### 1A — DataFrame / CSV
```python
from sqllocks_spindle.inference import DataProfiler

profiler = DataProfiler()
profile = profiler.profile(df)                       # from pandas DataFrame
profile = DataProfiler.from_csv("data.csv")          # convenience wrapper
profile = DataProfiler.from_csv("data.csv", sample_rows=50_000)
```

### 1B — SQL (existing, no change)
```python
profile = DataProfiler.from_sql(conn_str, "SELECT * FROM orders TABLESAMPLE (100000 ROWS)")
```

### 1C — Fabric Lakehouse (NEW)
```python
from sqllocks_spindle.inference import LakehouseProfiler

lp = LakehouseProfiler(workspace_id="...", lakehouse_id="...", token_provider=...)
profile = lp.profile_table("sales_orders")
profile = lp.profile_table("sales_orders", sample_rows=100_000)   # default
profile = lp.profile_table("sales_orders", sample_rows=None)      # full scan
```

`LakehouseProfiler` reads via `abfss://` + Spark (if available) or REST delta-scan fallback. Returns identical `ExportedProfile` as the other entry points.

---

## Section 2: DataProfiler Enhancements

Current `profiler.py` captures: distribution (KS test + `distribution_params`), basic patterns (email/phone/uuid/date), null rates.

**New statistics captured per column:**

| Statistic | Field in ExportedProfile | Used by |
|---|---|---|
| Quantile fingerprint (P1, P5, P10, P25, P50, P75, P90, P95, P99) | `quantiles: dict[str, float]` | empirical strategy fallback |
| 24-bin hour histogram | `hour_histogram: list[float]` (normalized) | temporal strategy |
| 7-bin day-of-week histogram | `dow_histogram: list[float]` (normalized) | temporal strategy |
| Null rate | `null_rate: float` | schema nullable + null strategy |
| String length distribution (min/mean/max/p95) | `string_length: dict` | string strategies |
| Outlier rate (IQR method, 1.5×IQR) | `outlier_rate: float` | AnomalyRegistry calibration |
| Exact value frequencies | `value_counts: dict[str, float]` (ratios, top-N capped at 500) | weighted_enum strategy |
| Correlation matrix (Pearson, numeric columns only) | `correlation_matrix: dict[str, dict[str, float]]` | Gaussian copula post-pass |

**Config knobs (all have defaults):**

```python
DataProfiler(
    fit_threshold=0.80,      # KS test score below this → use empirical strategy
    top_n_values=500,        # max cardinality for value_counts capture
    outlier_iqr_factor=1.5,  # IQR multiplier for outlier detection
    sample_rows=None,        # None = full scan; int = random sample
)
```

---

## Section 3: SchemaBuilder Enhancements

`schema_builder.py` `_column_to_generator()` currently uses a simple priority tree. Extended priority tree (evaluated top-to-bottom, first match wins):

```
1. uuid pattern detected                  → uuid strategy
2. email pattern detected                 → email strategy (Faker)
3. phone pattern detected                 → phone strategy (Faker)
4. url pattern detected                   → url strategy (Faker)
5. date/datetime detected                 → timestamp/date + temporal profile if hour/dow histograms present
6. categorical (cardinality ≤ threshold)  → weighted_enum using value_counts ratios (exact frequencies)
7. numeric, KS fit ≥ fit_threshold        → parametric (distribution_params from profiler)
8. numeric, KS fit < fit_threshold        → empirical (quantile fingerprint interpolation)
9. string with length distribution        → string strategy bounded by string_length stats
10. fallback                              → existing inference logic
```

**New `build()` kwargs:**

```python
SchemaBuilder.build(
    profile,
    fit_threshold=0.80,         # overrides profiler's fit_threshold for this build
    include_anomaly_registry=True,   # emit suggested AnomalyRegistry (default True)
    correlation_threshold=0.5,       # min |r| to include a pair in copula config
)
```

When `correlation_matrix` is present in the profile, `build()` emits a `correlated_columns` key in the schema dict containing pairs where `|r| ≥ correlation_threshold`. This is what `GaussianCopula` reads in the engine post-pass.

**Suggested AnomalyRegistry output:**

When `include_anomaly_registry=True`, `SchemaBuilder.build()` returns a second value:

```python
schema, suggested_registry = SchemaBuilder.build(profile, include_anomaly_registry=True)
schema = SchemaBuilder.build(profile, include_anomaly_registry=False)  # single value
```

`suggested_registry` is a ready-to-use `AnomalyRegistry` populated from:
- `outlier_rate` → `PointAnomaly(fraction=outlier_rate, ...)`
- `correlation_matrix` → `ContextualAnomaly` entries for strongly correlated pairs
- `value_counts` skew → `CollectiveAnomaly` for columns with extreme frequency skew

---

## Section 4: Engine Additions

### 4A — `empirical` Strategy (new file: `engine/strategies/empirical.py`)

Quantile interpolation for numeric columns when parametric fit is poor:

```python
class EmpiricalStrategy(BaseStrategy):
    """
    Generates values by interpolating the stored quantile fingerprint.
    Uses scipy.interpolate.interp1d (linear by default, cubic optional).
    """
    def __init__(self, quantiles: dict[str, float], interpolation="linear"):
        ...

    def generate(self, n: int, rng) -> np.ndarray:
        # map uniform(0,1) samples → quantile space via interp1d
        ...
```

Config kwarg: `interpolation` — `"linear"` (default) or `"cubic"`.

### 4B — Gaussian Copula Post-Pass (new file: `engine/correlation.py`)

Enforces column correlations as a post-generation pass using Gaussian copula:

```python
class GaussianCopula:
    """
    Given a generated DataFrame and a correlation matrix, reorders column values
    to achieve target correlations without changing any column's marginal distribution.

    Algorithm:
    1. Cholesky decompose target correlation matrix
    2. Map each column to uniform [0,1] via rank-based CDF
    3. Apply Cholesky transform to correlated Gaussian space
    4. Map back via quantile function
    """
    def __init__(self, correlation_matrix: dict[str, dict[str, float]], threshold=0.5):
        ...

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        ...
```

Applied automatically by `Spindle.generate()` when `schema["correlated_columns"]` is present. Can be disabled:

```python
Spindle.generate(schema, enforce_correlations=True)  # default True
```

### 4C — Extended String Patterns

New patterns in `engine/strategies/string.py` (or new dedicated files):
- `ssn` — `NNN-NN-NNNN` masked
- `credit_card` — Luhn-valid, masked (last 4 visible)
- `iban` — country-prefix + check digits
- `ip_address` — IPv4 and IPv6
- `mac_address` — colon-separated hex
- `postal_code` — US ZIP, UK postcode, CA postal (configurable `country`)
- `currency_code` — ISO 4217
- `language_code` — ISO 639-1

---

## Section 5: FidelityReport

Per-column fidelity scoring across 5 dimensions.

### 5A — Score Dimensions

| Dimension | Metric | Score range |
|---|---|---|
| Distribution | KS statistic between generated and source quantile fingerprint | 0–1 |
| Null rate | `1 - abs(generated_null_rate - source_null_rate)` | 0–1 |
| Cardinality | Ratio of unique values (generated / source), clipped to [0, 1] | 0–1 |
| Pattern match | Fraction of generated values matching detected pattern regex | 0–1 |
| Temporal alignment | Jensen-Shannon divergence of hour/DOW histograms | 0–1 |

Composite score = weighted average (equal weights by default, configurable).

### 5B — API

**Standalone callable:**
```python
from sqllocks_spindle.inference import FidelityReport

report = FidelityReport.score(generated_df, profile)
report.summary()          # prints table of per-column scores + composite
report.failing_columns()  # list of columns below threshold
report.to_dict()          # serializable
report.to_dataframe()     # pandas DataFrame for downstream analysis
```

**As second return from generate():**
```python
df, report = Spindle.generate(schema, fidelity_profile=profile)
df = Spindle.generate(schema)  # original signature unchanged — single return
```

`fidelity_profile` kwarg defaults to `None` (no report). When supplied, returns tuple.

### 5C — Config

```python
FidelityReport.score(
    generated_df,
    profile,
    threshold=0.85,          # columns below this are "failing"
    weights=None,            # equal weights by default; dict of dimension→float
)
```

---

## Section 6: LakehouseProfiler

Fabric-native profiling without requiring a local Spark session.

```python
from sqllocks_spindle.inference import LakehouseProfiler

lp = LakehouseProfiler(
    workspace_id="...",
    lakehouse_id="...",
    token_provider=None,      # DefaultAzureCredential if None
)

# Single table
profile = lp.profile_table("sales_orders")
profile = lp.profile_table("sales_orders", sample_rows=100_000)   # default
profile = lp.profile_table("sales_orders", sample_rows=None)      # full scan

# All tables in lakehouse (returns dict[str, ExportedProfile])
profiles = lp.profile_all(sample_rows=100_000)

# Save/load
ProfileIO.save(profile, "orders_profile.json")
profile = ProfileIO.load("orders_profile.json")
```

**Implementation:**
- Uses `deltalake` (already in `[fabric]` extra) to read Delta table locally via ABFSS
- Falls back to OneLake REST API for small samples when `deltalake` is unavailable
- Requires `[fabric]` extra; `[inference]` extra for scoring components
- New combined extra: `[fabric-inference]` — `deltalake + pyarrow + scipy`

---

## Section 7: New Module Structure

```
sqllocks_spindle/
  inference/
    profiler.py              # enhanced DataProfiler
    schema_builder.py        # enhanced SchemaBuilder
    fidelity.py              # NEW — FidelityReport
    lakehouse_profiler.py    # NEW — LakehouseProfiler
    __init__.py              # exports all new classes
  engine/
    strategies/
      empirical.py           # NEW — EmpiricalStrategy
    correlation.py           # NEW — GaussianCopula
```

---

## Section 8: Packaging

### New extras

```toml
[project.optional-dependencies]
inference = [
    "scipy>=1.11",
]
fabric-inference = [
    "scipy>=1.11",
    "deltalake>=0.17.0",
    "pyarrow>=14.0",
]
```

### Version

`2.9.0` — this is a substantial capability addition, not a patch. Inference module is optional-extra gated; no breaking changes to existing API.

---

## Open Questions / Deferred

- **Scenario Packs (Phase 3A)**: Deferred. Will be brainstormed separately.
- **Streaming fidelity**: `SpindleStreamer` does not get a fidelity pass in this release — temporal histogram alignment covers the most important streaming dimension already.
- **GPU copula**: Gaussian copula is CPU-only in this release; GPU acceleration deferred.
- **Profile versioning / migration**: `ExportedProfile` gets a `schema_version` field (starts at `"1"`) for forward compatibility. Migration logic deferred to when a breaking change is needed.
