# Spindle Phase 7 — Complete Fidelity Suite Design

**Date:** 2026-04-29
**Version target:** v2.15.0
**Status:** Approved for implementation

---

## Goal

Extend Spindle's fidelity stack with four new subsystems — privacy/membership inference, query-result fidelity, downstream model fidelity, and relational/multi-table fidelity — and expose them through a unified `FidelityEngine` orchestrator that produces a single `ComprehensiveFidelityReport`.

---

## Architecture

### New package

```
sqllocks_spindle/inference/fidelity/
    __init__.py          # exports FidelityEngine + all result classes
    privacy.py           # DCR, NNDR, membership inference → PrivacyReport
    query.py             # aggregation + filter battery → QueryFidelityReport
    model.py             # TSTR / TRTR + feature importance → ModelFidelityReport
    relational.py        # FK integrity, join stats, cardinality → RelationalFidelityReport
    engine.py            # FidelityEngine + ComprehensiveFidelityReport
```

### Existing code — untouched

`FidelityComparator`, `AdvancedProfiler`, `Tier2Profiler`, `Tier3Research` remain in `sqllocks_spindle/inference/`. The `FidelityEngine` calls them internally as an orchestrator, not a replacement.

### New top-level exports (`sqllocks_spindle/__init__.py`)

Added inside a `try/except ImportError` block alongside the existing inference imports:

```python
from sqllocks_spindle.inference.fidelity import (
    FidelityEngine,
    ComprehensiveFidelityReport,
    FKRelationship,
    PrivacyChecker,
    PrivacyReport,
    QueryFidelityChecker,
    QueryFidelityReport,
    ModelFidelityTester,
    ModelFidelityReport,
    RelationalFidelityChecker,
    RelationalFidelityReport,
)
```

### New tests

```
tests/
    test_privacy_fidelity.py        # ~20 tests
    test_query_fidelity.py          # ~18 tests
    test_model_fidelity.py          # ~18 tests
    test_relational_fidelity.py     # ~18 tests
    test_fidelity_engine.py         # ~15 tests
```

---

## Module 1: Privacy Fidelity (`privacy.py`)

### Purpose

Measure whether synthetic records can be traced back to real ones. Reports three complementary metrics: DCR, NNDR, and membership inference AUC.

### Metrics

**DCR — Distance to Closest Record**
For each synthetic row, compute the Euclidean distance to the nearest real row. All numeric columns are normalised to [0, 1] before distance computation; categorical columns are label-encoded. Report median and 5th-percentile (p5) DCR across all synthetic rows. Higher DCR = more privacy. p5 is the worst-case leakage measure.

**NNDR — Nearest Neighbour Distance Ratio**
For each synthetic row: `dist_to_nearest_real / dist_to_nearest_synthetic`. NNDR > 1.0 means the synthetic row is closer to other synthetic rows than to any real row — a good sign. NNDR < 0.5 is a red flag. Uses `scipy.spatial.KDTree` for efficient neighbour lookups.

**Membership Inference AUC**
Train a `GradientBoostingClassifier` to distinguish real rows (label 0) from synthetic rows (label 1) using 3-fold CV. AUC ≈ 0.5 = the model cannot distinguish them = strong privacy. Reuses the `_encode_for_adversarial` encoding pattern from `AdvancedProfiler`.

### API

```python
from sqllocks_spindle.inference.fidelity import PrivacyChecker

checker = PrivacyChecker(real_df, synth_df)
report = checker.check()

report.dcr_median           # float — median DCR
report.dcr_p5               # float — 5th-percentile DCR (worst-case proximity)
report.nndr_median          # float — median NNDR
report.nndr_p5              # float — 5th-percentile NNDR
report.membership_auc       # float — 0.5 = perfect privacy, 1.0 = fully distinguishable
report.privacy_score        # float 0–100 composite
report.is_private           # bool — True if dcr_p5 > 0.05 and nndr_p5 > 0.5
```

### Score formula

```
privacy_score = min(100,
    clip(dcr_p5 / 0.10, 0, 1) * 40        # DCR component (40 pts)
    + clip(nndr_p5, 0, 1) * 30             # NNDR component (30 pts)
    + (1 - membership_auc) * 2 * 30        # AUC component (30 pts)
)
```

`dcr_p5 / 0.10` normalises to full score when p5 ≥ 10% of the normalised distance range.

### Dependencies

- `scipy.spatial.KDTree` — already in `[inference]` extra via `scipy>=1.11`
- `scikit-learn` — already in `[advanced]` extra via `scikit-learn>=1.3`

### Graceful degradation

If sklearn is absent, the membership AUC check is skipped and `membership_auc` is set to `None`. The `privacy_score` is computed from DCR and NNDR only (reweighted to 100 pts).

---

## Module 2: Query-Result Fidelity (`query.py`)

### Purpose

Verify that aggregation queries on synthetic data return results within a configurable tolerance of the same queries run on real data. Designed for BI/analytics use cases where synthetic data is used to develop reports.

### Auto-generated query battery

The checker automatically derives a standard query set from the DataFrame schema — no user configuration required:

| Query type | Logic |
|------------|-------|
| Global aggregates | `MEAN`, `MEDIAN`, `STD`, `MIN`, `MAX` of each numeric column |
| Grouped aggregates | `COUNT(*)`, `MEAN`, `SUM` of numeric columns, grouped by each categorical column with ≤ 50 unique values |
| NULL rate | Fraction of NULL values per column |
| Filter result rate | Fraction of rows passing `col > p75` threshold per numeric column |

### Custom queries

```python
checker = QueryFidelityChecker(real_df, synth_df, tolerance=0.10)
checker.add_query(
    name="revenue_by_region",
    group_by=["region"],
    agg={"revenue": "sum"},
)
report = checker.check()
```

`add_query` appends to the battery; the rest of the auto-generated queries still run.

### Result structure

```python
report.query_results          # list[QueryResult]

# Per QueryResult:
result.name                   # str
result.real_value             # float
result.synth_value            # float
result.relative_error         # abs(real - synth) / max(abs(real), 1e-9)
result.passed                 # bool — relative_error < tolerance

report.pass_rate              # float — fraction of queries passing
report.query_score            # float 0–100 (= pass_rate * 100)
report.failing_queries        # list[QueryResult] where not passed
```

### Dependencies

pandas only. No new extras.

---

## Module 3: Downstream Model Fidelity (`model.py`)

### Purpose

Measure ML utility of synthetic data via the TSTR protocol (Train on Synthetic, Test on Real). The gap between TSTR accuracy and TRTR accuracy (Train on Real, Test on Real) quantifies how much utility is lost by substituting synthetic data.

### Protocol

1. User specifies `target_col` and `task` (`"classification"` or `"regression"`).
2. Module encodes features (numeric fillna→median, datetime→epoch, string→LabelEncoder).
3. Trains two models with 3-fold stratified CV:
   - **TRTR:** `GradientBoostingClassifier` / `GradientBoostingRegressor` on real data, evaluated on real held-out folds.
   - **TSTR:** Same model class trained on full synthetic data, evaluated on full real data.
4. Extracts feature importances from both models; computes Spearman rank correlation.

### API

```python
from sqllocks_spindle.inference.fidelity import ModelFidelityTester

tester = ModelFidelityTester(
    real_df, synth_df,
    target_col="churn",
    task="classification",    # or "regression"
)
report = tester.test()

report.trtr_score                       # float — metric on real-trained model
report.tstr_score                       # float — metric on synth-trained model
report.utility_gap                      # float — trtr_score - tstr_score
report.feature_importance_correlation   # float — Spearman rho (-1 to 1)
report.model_fidelity_score             # float 0–100
report.task                             # str
report.metric                           # "accuracy" | "f1_weighted" | "r2"
```

### Score formula

```
utility_gap_capped = min(1.0, max(0.0, utility_gap))
fi_score = (feature_importance_correlation + 1) / 2   # normalise -1..1 → 0..1
model_fidelity_score = (1 - utility_gap_capped) * 50 + fi_score * 50
```

### Metric selection

| Task | CV metric | Report metric |
|------|-----------|---------------|
| classification | `accuracy` | `f1_weighted` |
| regression | `r2` | `r2` |

### Error handling

- `target_col` not in DataFrame → `ValueError` with message listing available columns.
- sklearn absent → `ImportError` with `pip install sqllocks-spindle[advanced]` instructions.
- Fewer than 10 real rows → `ValueError` (insufficient data for 3-fold CV).

### Dependencies

`scikit-learn>=1.3` — already in `[advanced]` extra.

---

## Module 4: Relational Fidelity (`relational.py`)

### Purpose

When synthetic data spans multiple tables, verify that inter-table relationships are preserved: foreign key integrity, parent-child cardinality ratios, and join result statistics.

### Schema declaration

```python
from sqllocks_spindle.inference.fidelity import FKRelationship, RelationalFidelityChecker

relationships = [
    FKRelationship(
        parent_table="customers", parent_col="customer_id",
        child_table="orders",     child_col="customer_id",
    ),
    FKRelationship(
        parent_table="orders",    parent_col="order_id",
        child_table="order_items", child_col="order_id",
    ),
]

checker = RelationalFidelityChecker(
    real_tables={"customers": real_cust, "orders": real_ord, "order_items": real_items},
    synth_tables={"customers": syn_cust, "orders": syn_ord, "order_items": syn_items},
    relationships=relationships,
    tolerance=0.10,
)
report = checker.check()
```

### Per-relationship checks

**FK integrity rate** — fraction of child FK values that exist in the parent PK. Computed on real and synthetic separately. Synthetic integrity < 0.99 is flagged.

**Cardinality ratio** — mean number of child rows per parent row (e.g., mean orders per customer). Relative error between real and synthetic vs. tolerance.

**Join result fidelity** — perform an inner join of real tables and of synthetic tables on the FK columns; compare COUNT and column means of numeric columns in the joined result within tolerance.

### Result structure

```python
report.relationship_results         # list[RelationshipResult]

# Per RelationshipResult:
result.parent_table
result.child_table
result.real_fk_integrity            # float 0–1
result.synth_fk_integrity           # float 0–1
result.cardinality_ratio_error      # float — relative error
result.join_fidelity_score          # float 0–100
result.passed                       # bool

report.overall_integrity            # float — fraction of relationships passing
report.relational_score             # float 0–100 — mean join_fidelity_score
```

### Single-table fallback

If `relationships=[]` or `relationships=None`, `RelationalFidelityChecker` skips FK/join checks and only reports within-table constraint satisfaction (NOT NULL rates, value range bounds per column).

### Dependencies

pandas only. No new extras.

---

## Module 5: FidelityEngine + ComprehensiveFidelityReport (`engine.py`)

### Purpose

One-liner orchestrator that runs all available fidelity checks against a set of tables and returns a unified report with a composite score and HTML output.

### API

```python
from sqllocks_spindle.inference.fidelity import FidelityEngine

engine = FidelityEngine(
    real_tables={"orders": real_df},            # dict[str, pd.DataFrame]
    synth_tables={"orders": synth_df},
    relationships=None,                          # optional list[FKRelationship]
    target_col=None,                             # optional — enables ModelFidelityTester
    task="classification",                       # ignored if target_col is None
    tolerance=0.10,                              # query + relational tolerance
)
report = engine.run(checks="all")
# or selective: engine.run(checks=["privacy", "query"])
```

### Checks executed by `run()`

The engine targets the first/primary table (first key in `real_tables`/`synth_tables`) for single-table checks. Multi-table checks use all tables.

| Check key | Class | Condition |
|-----------|-------|-----------|
| `"marginal"` | `FidelityComparator` | always |
| `"tier2"` | `run_tier2()` | always |
| `"advanced"` | `AdvancedProfiler` | scipy + sklearn present |
| `"privacy"` | `PrivacyChecker` | scipy present |
| `"query"` | `QueryFidelityChecker` | always |
| `"model"` | `ModelFidelityTester` | `target_col` provided + sklearn present |
| `"relational"` | `RelationalFidelityChecker` | `relationships` provided |

### ComprehensiveFidelityReport

```python
report.marginal             # FidelityReport
report.tier2                # Tier2Report
report.advanced             # AdvancedTableProfile | None
report.privacy              # PrivacyReport | None
report.query                # QueryFidelityReport
report.model                # ModelFidelityReport | None
report.relational           # RelationalFidelityReport | None

report.overall_score()      # float 0–100 — weighted average of available sub-scores
report.to_html()            # unified HTML report — consistent styling with FidelityReport.to_html()
report.to_dict()            # serialisable dict of all sub-reports
report.summary()            # str: "Fidelity: 87/100 (marginal: 91, privacy: 88, query: 84)"
```

### Score weighting (all checks present)

| Check | Weight |
|-------|--------|
| Marginal fidelity | 25% |
| Privacy | 20% |
| Query | 20% |
| Model | 20% |
| Tier 2 | 10% |
| Relational | 5% |

When a check is absent (optional dep missing or condition not met), its weight is redistributed proportionally among the available checks.

### Error policy

Missing optional dependencies cause that sub-report to be `None` with a `logging.warning()` — never a crash. Missing required inputs (`target_col` absent for model check) are silently skipped at the engine level (the individual checker raises if called directly).

---

## Testing Strategy

Each module has its own test file. All tests use synthetic DataFrames — no real Fabric connection required.

### test_privacy_fidelity.py (~20 tests)
- Near-identical synth (small Gaussian jitter on real) → very low DCR, low NNDR, AUC ≈ 0.5 — privacy score near zero
- Completely independent synth (different distribution) → high DCR, NNDR > 1, AUC ≈ 0.5 — privacy score high
- `is_private` flag behaviour at boundary values
- `privacy_score` range always 0–100
- Handles single-row edge case
- Handles all-numeric, all-categorical, mixed DataFrames
- Graceful degradation when sklearn absent (mock ImportError)

### test_query_fidelity.py (~18 tests)
- Identical real/synth → all queries pass, score = 100
- Synthetic with 50% shifted means → some queries fail
- `add_query` appends to battery
- Custom query with invalid column → ValueError
- `pass_rate` and `query_score` consistent
- Empty battery (no valid columns) → report with 0 queries, score = 100 (vacuous truth)
- Handles DataFrame with no numeric columns
- Handles DataFrame with no categorical columns

### test_model_fidelity.py (~18 tests)
- Classification task: identical data → utility_gap ≈ 0
- Regression task: identical data → utility_gap ≈ 0
- Heavily degraded synth → large utility_gap
- `feature_importance_correlation` range -1 to 1
- `model_fidelity_score` always 0–100
- Missing `target_col` → ValueError
- Fewer than 10 rows → ValueError
- sklearn absent → ImportError (mock)
- Both classification and regression metrics reported correctly

### test_relational_fidelity.py (~18 tests)
- Perfect FK integrity → `synth_fk_integrity` = 1.0
- Broken FK in synth → `synth_fk_integrity` < 1.0
- Cardinality ratio preserved → error ≈ 0
- Cardinality shifted by 2× → error flagged
- Join result count preserved
- No relationships provided → single-table fallback runs
- Unknown table name in relationship → ValueError
- `relational_score` always 0–100

### test_fidelity_engine.py (~15 tests)
- `run("all")` returns `ComprehensiveFidelityReport` with all fields populated
- `run(["privacy", "query"])` returns only those sub-reports; others are None
- `overall_score()` is weighted mean of available sub-scores
- `to_dict()` is JSON-serialisable
- `to_html()` returns valid HTML string containing score
- `summary()` lists available sub-scores
- Missing sklearn → model sub-report is None, score reweighted
- No relationships → relational sub-report is None, score reweighted
- Single-table dict → no crash
- Multi-table dict → relational check runs when relationships provided

---

## Dependencies & Packaging

No new extras required. All dependencies are already covered:

| Dependency | Used by | Already in extra |
|-----------|---------|-----------------|
| `scipy>=1.11` | `PrivacyChecker` (KDTree) | `[inference]`, `[advanced]` |
| `scikit-learn>=1.3` | `PrivacyChecker` (AUC), `ModelFidelityTester` | `[advanced]` |
| `pandas>=2.0` | All modules | core |
| `numpy>=1.24` | All modules | core |

`pyproject.toml` — no changes needed.

---

## Version & Release

- **Version:** 2.15.0 (current: 2.13.0; 2.14.x reserved for patches if needed)
- **Branch:** `phase7/fidelity-suite` (new worktree)
- **PyPI:** publish on merge to main

---

## What This Adds vs. Current Stack

| Capability | Before Phase 7 | After Phase 7 |
|-----------|----------------|---------------|
| Marginal distributions | ✓ FidelityComparator | ✓ |
| Format preservation | ✓ Tier2 | ✓ |
| Adversarial indistinguishability | ✓ AdvancedProfiler | ✓ |
| Drift detection | ✓ DriftMonitor | ✓ |
| **Privacy / membership inference** | ✗ | ✓ PrivacyChecker |
| **Query-result accuracy** | ✗ | ✓ QueryFidelityChecker |
| **ML utility (TSTR)** | ✗ | ✓ ModelFidelityTester |
| **Multi-table FK integrity** | ✗ | ✓ RelationalFidelityChecker |
| **Unified composite report** | ✗ | ✓ FidelityEngine |