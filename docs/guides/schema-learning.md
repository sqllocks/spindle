# Schema Learning

Spindle's inference pipeline lets you profile existing data, generate synthetic copies, compare fidelity, and mask PII — all from the CLI or Python API.

## Pipeline Overview

```
Real Data (CSV/Parquet)
    │
    ▼
spindle learn     →  .spindle.json schema
    │
    ▼
spindle generate  →  Synthetic data
    │
    ▼
spindle compare   →  Fidelity report (real vs synthetic)
    │
    ▼
spindle mask      →  Masked copy of real data
```

---

## Learn: Infer Schema from Data

Profile existing CSV, Parquet, or JSONL files and produce a `.spindle.json` schema with fitted distributions.

### Quick Start

```bash
spindle learn ./real_data/ --format csv --output inferred.spindle.json --domain my_retail
```

```python
from sqllocks_spindle.inference import DataProfiler, SchemaBuilder
import pandas as pd

# Option A: profile a dict of DataFrames
profiler = DataProfiler()
profile = profiler.profile_dataset({
    "customer": pd.read_csv("customer.csv"),
    "order": pd.read_csv("order.csv"),
})

# Option B: profile a single DataFrame (alias for profile_dataset)
profile = profiler.profile({"orders": orders_df})

# Option C: convenience classmethod for CSV files
profile = DataProfiler.from_csv("orders.csv")
profile = DataProfiler.from_csv("orders.csv", sample_rows=50_000)

builder = SchemaBuilder()
schema = builder.build(profile, domain_name="my_retail")

# Build with correlation detection and an auto-suggested AnomalyRegistry
schema, suggested_registry = SchemaBuilder().build(
    profile,
    domain_name="my_retail",
    fit_threshold=0.80,          # columns with KS fit < 0.80 → empirical strategy
    correlation_threshold=0.5,   # pairs with |r| >= 0.5 → included in copula config
    include_anomaly_registry=True,
)
```

### DataProfiler Configuration

```python
DataProfiler(
    fit_threshold=0.80,      # KS fit score below this → use empirical strategy instead of parametric
    top_n_values=500,        # max cardinality tracked in value_counts for enum detection
    outlier_iqr_factor=1.5,  # IQR multiplier for outlier detection (standard = 1.5)
    sample_rows=None,        # None = full scan; int = random sample
)
```

### What Gets Detected

| Feature | Detection Method | Threshold |
| --- | --- | --- |
| **Column types** | pandas dtype + heuristics | Integer, float, boolean, date, datetime, string |
| **Null rates** | Count / total rows | Exact |
| **Primary keys** | Unique + non-null + naming hints (`_id`, `pk`) | 100% unique |
| **Foreign keys** | Naming convention (`*_id`) + 90% value overlap with parent PK | Cross-table |
| **Enums** | Cardinality < 50 or ratio < 5% | With weighted probabilities |
| **Distributions** | KS test (scipy): normal, uniform, exponential, lognormal | p-value > 0.05 |
| **Quantile fingerprint** | P1/P5/P10/P25/P50/P75/P90/P95/P99 | Empirical fallback when KS fit < threshold |
| **Outlier rate** | IQR method (1.5× IQR) | Calibrates AnomalyRegistry |
| **String length** | min/mean/max/p95 | Bounds string generation |
| **Temporal histograms** | 24-bin hour + 7-bin day-of-week | Drives temporal strategy profiles |
| **Column correlations** | Pearson (numeric columns only) | Gaussian copula post-pass |
| **String patterns** | Regex: email, UUID, phone, date, SSN, IP (v4+v6), MAC, IBAN, currency code, language code, postal code | 90% match rate |

### CLI Reference

```bash
spindle learn INPUT_PATH [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `INPUT_PATH` | — | Directory or single file (required) |
| `--output, -o` | — | Output `.spindle.json` path |
| `--format` | `csv` | Input format: `csv`, `parquet`, `jsonl` |
| `--domain` | `inferred` | Domain name for the generated schema |

---

## Compare: Fidelity Report

Compare real data against synthetic data and produce a scored fidelity report.

### Quick Start

```bash
spindle compare ./real_data/ ./synthetic_data/ --format csv --output report.md
```

```python
from sqllocks_spindle.inference.comparator import FidelityComparator, FidelityReport

# Option A: compare two DataFrames directly
report = FidelityReport.score(real_df, synthetic_df, table_name="orders")
report.summary()                       # prints per-column score table
failing = report.failing_columns()     # list of (table, column, score) tuples below threshold
df_scores = report.to_dataframe()      # pandas DataFrame for downstream analysis
report_dict = report.to_dict()         # serializable dict

# Option B: compare full datasets (multi-table)
comp = FidelityComparator()
report = comp.compare(
    real={"customer": real_df, "order": real_order_df},
    synthetic={"customer": synth_df, "order": synth_order_df},
)

print(f"Overall fidelity: {report.overall_score:.1f}/100")
print(report.to_markdown())
```

### Inline Fidelity During Generation

Pass `fidelity_profile` to `Spindle.generate()` to get a `FidelityReport` in the same call:

```python
from sqllocks_spindle import Spindle
from sqllocks_spindle.inference import DataProfiler, SchemaBuilder, ProfileIO

# Profile real data once and save
profiler = DataProfiler()
profile = profiler.profile_dataset({"orders": real_df})
ProfileIO.save(profile, "orders_profile.json")

# Later: generate and score in one step
profile = ProfileIO.load("orders_profile.json")
schema = SchemaBuilder().build(profile)
result, fidelity = Spindle().generate(schema, seed=42, fidelity_profile=profile)

fidelity.summary()
```

### Scoring Breakdown

Each column is scored 0–100 based on:

| Metric | Points | Applies To |
| --- | --- | --- |
| dtype match | 10 | All columns |
| Null rate delta | 10 | All columns |
| Cardinality ratio | 10 | All columns |
| Mean delta | 20 | Numeric columns |
| Std ratio | 10 | Numeric columns |
| KS statistic | 10 | Numeric columns |
| Value overlap | 20 | Categorical columns |
| Chi-squared | 20 | Categorical columns |

Table scores are column averages. Overall score is a weighted average across tables.

### CLI Reference

```bash
spindle compare REAL_PATH SYNTH_PATH [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `REAL_PATH` | — | Path to real data directory (required) |
| `SYNTH_PATH` | — | Path to synthetic data directory (required) |
| `--format` | `csv` | Input format: `csv`, `parquet` |
| `--output, -o` | — | Save markdown report to file |

---

## Mask: PII Anonymization

Mask sensitive columns in real datasets. Spindle auto-detects PII by column name and content patterns.

### Quick Start

```bash
spindle mask ./real_data/ --output ./masked/ --format csv --seed 42
```

```python
from sqllocks_spindle.inference.masker import DataMasker, MaskConfig

masker = DataMasker()
result = masker.mask(
    tables={"customer": customer_df, "order": order_df},
    config=MaskConfig(
        seed=42,
        preserve_nulls=True,
        preserve_fks=True,
        exclude_columns=["customer_id", "order_id"],
    ),
)

masked_customers = result.tables["customer"]
print(f"Masked {result.columns_masked} columns across {result.tables_masked} tables")
```

### Auto-Detected PII Types

| PII Type | Detection | Masking Method |
| --- | --- | --- |
| `email` | Column name or regex pattern | Faker replacement |
| `phone` | Column name or regex pattern | Faker replacement |
| `name` / `first_name` / `last_name` | Column name | Faker replacement |
| `address` / `city` / `state` / `zip` | Column name | Faker replacement |
| `ssn` | Column name | Faker replacement |
| `credit_card` | Column name | Faker replacement |
| `ip_address` | Column name | Faker replacement |
| `username` | Column name | Faker replacement |
| `date_of_birth` | Column name | Random date shift |

### MaskConfig Options

```python
MaskConfig(
    seed=42,                    # Reproducible masking
    locale="en_US",             # Faker locale
    preserve_nulls=True,        # Keep NULLs as NULL
    preserve_distributions=True,# Maintain statistical shape
    preserve_fks=True,          # Don't mask FK columns
    pii_columns={               # Override auto-detection
        "customer.secret_field": "email",
    },
    exclude_columns=["id"],     # Never mask these
)
```

### CLI Reference

```bash
spindle mask INPUT_PATH [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `INPUT_PATH` | — | Directory or file to mask (required) |
| `--output, -o` | — | Output directory (required) |
| `--format` | `csv` | Input format: `csv`, `parquet`, `jsonl` |
| `--seed` | `42` | Random seed |
| `--exclude` | — | Columns to skip (repeatable) |

---

## End-to-End Example

```bash
# 1. Profile real data and generate a schema
spindle learn ./production_export/ --format parquet --output inferred.spindle.json

# 2. Generate synthetic data from the inferred schema
spindle generate --schema inferred.spindle.json --scale medium --output ./synthetic/

# 3. Compare fidelity
spindle compare ./production_export/ ./synthetic/ --format parquet --output fidelity.md

# 4. Mask the original data for dev environments
spindle mask ./production_export/ --output ./masked/ --format parquet --seed 42
```
