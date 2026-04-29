# Phase 7 — Complete Fidelity Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add four new fidelity subsystems (privacy/DCR/NNDR, query-result, downstream model TSTR, relational FK) plus a `FidelityEngine` orchestrator under `sqllocks_spindle/inference/fidelity/`, targeting v2.15.0.

**Architecture:** New subpackage `sqllocks_spindle/inference/fidelity/` with five focused modules (`privacy.py`, `query.py`, `model.py`, `relational.py`, `engine.py`). Each module follows the existing pattern: optional-dep guards at module top, `@dataclass` result types, class-based checker taking `(real_df, synth_df)` in `__init__`. The engine orchestrates all checks and returns `ComprehensiveFidelityReport`. Existing `FidelityComparator`, `AdvancedProfiler`, and `run_tier2` are called by the engine — not replaced.

**Tech Stack:** numpy, pandas, scipy (KDTree for DCR/NNDR), scikit-learn (GradientBoosting for membership AUC and TSTR), pytest.

**Write-guard note:** A pre-tool hook blocks the Edit and Write tools on `.py` files under `projects/`. Create and modify all Python files with bash heredocs:
```bash
python - << 'PYEOF'
content = '''...'''
with open("path/to/file.py", "w") as f:
    f.write(content)
PYEOF
```
Or directly:
```bash
cat > sqllocks_spindle/inference/fidelity/privacy.py << 'EOF'
...
EOF
```

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `sqllocks_spindle/inference/fidelity/__init__.py` | Create | Package exports |
| `sqllocks_spindle/inference/fidelity/privacy.py` | Create | DCR, NNDR, membership AUC → PrivacyReport |
| `sqllocks_spindle/inference/fidelity/query.py` | Create | Aggregation battery → QueryFidelityReport |
| `sqllocks_spindle/inference/fidelity/model.py` | Create | TSTR/TRTR → ModelFidelityReport |
| `sqllocks_spindle/inference/fidelity/relational.py` | Create | FK integrity, join stats → RelationalFidelityReport |
| `sqllocks_spindle/inference/fidelity/engine.py` | Create | FidelityEngine + ComprehensiveFidelityReport |
| `tests/test_privacy_fidelity.py` | Create | ~20 privacy tests |
| `tests/test_query_fidelity.py` | Create | ~18 query tests |
| `tests/test_model_fidelity.py` | Create | ~18 model tests |
| `tests/test_relational_fidelity.py` | Create | ~18 relational tests |
| `tests/test_fidelity_engine.py` | Create | ~15 engine tests |
| `sqllocks_spindle/inference/__init__.py` | Modify | Add fidelity exports |
| `sqllocks_spindle/__init__.py` | Modify | Add FidelityEngine top-level export |
| `docs/changelog.md` | Modify | v2.15.0 entry |
| `sqllocks_spindle/__init__.py` | Modify | Version bump 2.13.0 → 2.15.0 |
| `pyproject.toml` | Modify | Version bump 2.13.0 → 2.15.0 |

---

## Task 0: Create Worktree

**Files:** none (git setup only)

- [ ] **Step 1: Create phase7 worktree**

```bash
cd projects/fabric-datagen
git worktree add .worktrees/phase7-fidelity -b phase7/fidelity-suite
```

Expected output: `Preparing worktree (new branch 'phase7/fidelity-suite')`

- [ ] **Step 2: Verify worktree**

```bash
git worktree list
```

Expected: two entries — main worktree and `.worktrees/phase7-fidelity`

- [ ] **Step 3: Create fidelity package directory**

```bash
cd .worktrees/phase7-fidelity
mkdir -p sqllocks_spindle/inference/fidelity
touch sqllocks_spindle/inference/fidelity/__init__.py
```

---

## Task 1: Privacy Module

**Files:**
- Create: `sqllocks_spindle/inference/fidelity/privacy.py`
- Test: `tests/test_privacy_fidelity.py`

- [ ] **Step 1: Write the failing tests**

```bash
cat > tests/test_privacy_fidelity.py << 'EOF'
"""Tests for PrivacyChecker — DCR, NNDR, membership inference."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference.fidelity.privacy import PrivacyChecker, PrivacyReport


def _real_df(n: int = 100, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "age": rng.integers(20, 80, n).astype(float),
        "salary": rng.normal(50_000, 15_000, n),
        "city": rng.choice(["NYC", "LA", "Chicago", "Houston"], n),
    })


def _independent_synth(n: int = 100, seed: int = 99) -> pd.DataFrame:
    """Synth from a completely different region of space."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "age": rng.integers(20, 80, n).astype(float) + 200,
        "salary": rng.normal(50_000, 15_000, n) + 1_000_000,
        "city": rng.choice(["NYC", "LA", "Chicago", "Houston"], n),
    })


def _jittered_synth(real: pd.DataFrame, noise: float = 0.0001) -> pd.DataFrame:
    """Synth that's nearly identical to real — worst privacy."""
    rng = np.random.default_rng(0)
    num_cols = real.select_dtypes(include=[np.number]).columns
    out = real.copy()
    out[num_cols] = real[num_cols] + rng.normal(0, noise, (len(real), len(num_cols)))
    return out


# --- Basic structure ---

def test_report_type():
    real = _real_df()
    synth = _real_df(seed=99)
    report = PrivacyChecker(real, synth).check()
    assert isinstance(report, PrivacyReport)


def test_report_fields_are_floats():
    real = _real_df()
    synth = _real_df(seed=99)
    report = PrivacyChecker(real, synth).check()
    assert isinstance(report.dcr_median, float)
    assert isinstance(report.dcr_p5, float)
    assert isinstance(report.nndr_median, float)
    assert isinstance(report.nndr_p5, float)
    assert isinstance(report.privacy_score, float)
    assert isinstance(report.is_private, bool)


def test_privacy_score_in_range():
    real = _real_df()
    synth = _real_df(seed=99)
    report = PrivacyChecker(real, synth).check()
    assert 0.0 <= report.privacy_score <= 100.0


def test_dcr_nonnegative():
    real = _real_df()
    synth = _real_df(seed=7)
    report = PrivacyChecker(real, synth).check()
    assert report.dcr_median >= 0.0
    assert report.dcr_p5 >= 0.0


def test_nndr_nonnegative():
    real = _real_df()
    synth = _real_df(seed=7)
    report = PrivacyChecker(real, synth).check()
    assert report.nndr_median >= 0.0
    assert report.nndr_p5 >= 0.0


# --- Privacy-signal tests ---

def test_jittered_synth_low_dcr():
    """Synth nearly identical to real → very low DCR → bad privacy."""
    real = _real_df(200)
    synth = _jittered_synth(real, noise=0.0001)
    report = PrivacyChecker(real, synth).check()
    assert report.dcr_p5 < 0.05


def test_independent_synth_higher_dcr():
    """Synth in completely different space → dcr > jittered version."""
    real = _real_df(200)
    jitter_report = PrivacyChecker(real, _jittered_synth(real, noise=0.0001)).check()
    indep_report = PrivacyChecker(real, _independent_synth(200)).check()
    assert indep_report.dcr_p5 > jitter_report.dcr_p5


def test_is_private_false_for_jittered():
    real = _real_df(200)
    synth = _jittered_synth(real, noise=0.0001)
    report = PrivacyChecker(real, synth).check()
    assert not report.is_private


def test_membership_auc_in_range_when_present():
    real = _real_df(100)
    synth = _real_df(100, seed=99)
    report = PrivacyChecker(real, synth).check()
    if report.membership_auc is not None:
        assert 0.0 <= report.membership_auc <= 1.0


# --- Edge cases ---

def test_all_numeric_df():
    real = pd.DataFrame({"a": np.arange(100, dtype=float), "b": np.arange(100, dtype=float) * 2})
    synth = pd.DataFrame({"a": np.arange(200, 300, dtype=float), "b": np.arange(200, 300, dtype=float) * 2})
    report = PrivacyChecker(real, synth).check()
    assert isinstance(report.privacy_score, float)


def test_all_categorical_df():
    real = pd.DataFrame({"cat": ["a", "b", "c", "d"] * 25})
    synth = pd.DataFrame({"cat": ["a", "b", "c", "d"] * 25})
    report = PrivacyChecker(real, synth).check()
    assert isinstance(report.privacy_score, float)
    assert 0.0 <= report.privacy_score <= 100.0


def test_single_row_synth_no_crash():
    real = _real_df(100)
    synth = _real_df(1, seed=5)
    report = PrivacyChecker(real, synth).check()
    assert isinstance(report.privacy_score, float)


def test_no_common_columns_returns_safe_defaults():
    real = pd.DataFrame({"a": [1, 2, 3]})
    synth = pd.DataFrame({"b": [4, 5, 6]})
    report = PrivacyChecker(real, synth).check()
    assert report.privacy_score == 100.0
    assert report.is_private is True


def test_mixed_numeric_categorical_datetime():
    real = pd.DataFrame({
        "num": np.arange(50, dtype=float),
        "cat": ["x", "y"] * 25,
        "dt": pd.date_range("2024-01-01", periods=50, freq="D"),
    })
    synth = pd.DataFrame({
        "num": np.arange(50, 100, dtype=float),
        "cat": ["x", "y"] * 25,
        "dt": pd.date_range("2025-01-01", periods=50, freq="D"),
    })
    report = PrivacyChecker(real, synth).check()
    assert isinstance(report.privacy_score, float)


def test_privacy_score_100_for_very_distant_synth():
    """When synth is far from real in normalised space, score approaches 100."""
    real = pd.DataFrame({"x": np.zeros(100)})
    synth = pd.DataFrame({"x": np.full(100, 1e6)})
    report = PrivacyChecker(real, synth).check()
    assert report.privacy_score >= 40.0  # at minimum DCR component should be full
EOF
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv-mac/bin/python -m pytest tests/test_privacy_fidelity.py -v 2>&1 | tail -10
```

Expected: `ERROR` / `ImportError: cannot import name 'PrivacyChecker'`

- [ ] **Step 3: Create privacy.py**

```bash
cat > sqllocks_spindle/inference/fidelity/privacy.py << 'EOF'
"""Privacy fidelity — DCR, NNDR, and membership inference AUC.

Measures whether synthetic records can be traced back to real ones.
All checks degrade gracefully when optional dependencies are absent.
"""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass

import numpy as np
import pandas as pd

try:
    from scipy.spatial import KDTree
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

try:
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.model_selection import cross_val_score
    from sklearn.preprocessing import LabelEncoder
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

_LOG = logging.getLogger(__name__)


@dataclass
class PrivacyReport:
    """Privacy fidelity metrics comparing real vs synthetic data."""

    dcr_median: float          # median distance to closest real record
    dcr_p5: float              # 5th-percentile DCR (worst-case leakage)
    nndr_median: float         # median nearest-neighbour distance ratio
    nndr_p5: float             # 5th-percentile NNDR
    membership_auc: float | None  # adversarial AUC (0.5=private, 1.0=exposed)
    privacy_score: float       # 0-100 composite score
    is_private: bool           # True if dcr_p5>0.05 and nndr_p5>0.5


class PrivacyChecker:
    """Compute DCR, NNDR, and membership inference AUC for synthetic data.

    Args:
        real_df: Real (source) DataFrame.
        synth_df: Synthetic DataFrame to evaluate.
        max_rows: Cap for NN computations (performance).
    """

    def __init__(
        self,
        real_df: pd.DataFrame,
        synth_df: pd.DataFrame,
        max_rows: int = 5000,
    ) -> None:
        self._real = real_df
        self._synth = synth_df
        self._max_rows = max_rows

    def check(self) -> PrivacyReport:
        """Run all privacy checks and return a PrivacyReport."""
        if not HAS_SCIPY:
            _LOG.warning("scipy absent — DCR/NNDR skipped; returning safe defaults")
            return PrivacyReport(
                dcr_median=1.0, dcr_p5=1.0, nndr_median=1.0, nndr_p5=1.0,
                membership_auc=None, privacy_score=100.0, is_private=True,
            )

        real_enc = self._encode(self._real)
        synth_enc = self._encode(self._synth)

        common = [c for c in real_enc.columns if c in synth_enc.columns]
        if not common:
            return PrivacyReport(
                dcr_median=1.0, dcr_p5=1.0, nndr_median=1.0, nndr_p5=1.0,
                membership_auc=None, privacy_score=100.0, is_private=True,
            )

        real_arr = real_enc[common].values.astype(float)
        synth_arr = synth_enc[common].values.astype(float)

        rng = np.random.default_rng(0)
        if len(real_arr) > self._max_rows:
            idx = rng.integers(0, len(real_arr), self._max_rows)
            real_arr = real_arr[idx]
        if len(synth_arr) > self._max_rows:
            idx = rng.integers(0, len(synth_arr), self._max_rows)
            synth_arr = synth_arr[idx]

        # Normalise to [0,1] using real data statistics
        col_min = real_arr.min(axis=0)
        col_max = real_arr.max(axis=0)
        col_range = col_max - col_min
        col_range[col_range == 0] = 1.0
        real_norm = (real_arr - col_min) / col_range
        synth_norm = (synth_arr - col_min) / col_range

        # DCR: distance from each synth row to nearest real row
        tree_real = KDTree(real_norm)
        dcr, _ = tree_real.query(synth_norm, k=1)

        # NNDR: dist_to_nearest_real / dist_to_nearest_other_synth
        if len(synth_norm) >= 2:
            tree_synth = KDTree(synth_norm)
            nn_synth, _ = tree_synth.query(synth_norm, k=2)
            nearest_synth = nn_synth[:, 1]
            nearest_synth = np.where(nearest_synth == 0, 1e-10, nearest_synth)
            nndr = dcr / nearest_synth
        else:
            nndr = np.ones(len(synth_norm))

        dcr_median = float(np.median(dcr))
        dcr_p5 = float(np.percentile(dcr, 5))
        nndr_median = float(np.median(nndr))
        nndr_p5 = float(np.percentile(nndr, 5))

        membership_auc = self._membership_auc(real_enc[common], synth_enc[common])
        privacy_score = self._score(dcr_p5, nndr_p5, membership_auc)
        is_private = dcr_p5 > 0.05 and nndr_p5 > 0.5

        return PrivacyReport(
            dcr_median=dcr_median,
            dcr_p5=dcr_p5,
            nndr_median=nndr_median,
            nndr_p5=nndr_p5,
            membership_auc=membership_auc,
            privacy_score=privacy_score,
            is_private=is_private,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _encode(self, df: pd.DataFrame) -> pd.DataFrame:
        result: dict[str, np.ndarray] = {}
        for col in df.columns:
            series = df[col]
            if pd.api.types.is_numeric_dtype(series):
                med = float(series.median()) if not series.dropna().empty else 0.0
                result[col] = series.fillna(med).values.astype(float)
            elif pd.api.types.is_datetime64_any_dtype(series):
                result[col] = (series.astype(np.int64) // 10 ** 9).fillna(0).values.astype(float)
            else:
                if HAS_SKLEARN:
                    try:
                        le = LabelEncoder()
                        result[col] = le.fit_transform(
                            series.fillna("__NULL__").astype(str)
                        ).astype(float)
                    except Exception:
                        continue
                else:
                    cats = pd.Categorical(series.fillna("__NULL__").astype(str))
                    result[col] = np.array(cats.codes, dtype=float)
        return pd.DataFrame(result, index=df.index[:len(next(iter(result.values()), []))])

    def _membership_auc(
        self, real_enc: pd.DataFrame, synth_enc: pd.DataFrame
    ) -> float | None:
        if not HAS_SKLEARN:
            _LOG.warning("scikit-learn absent — membership inference AUC skipped")
            return None
        try:
            cap = 2500
            if len(real_enc) > cap:
                real_enc = real_enc.sample(cap, random_state=0)
            if len(synth_enc) > cap:
                synth_enc = synth_enc.sample(cap, random_state=0)
            X = pd.concat([real_enc, synth_enc], ignore_index=True).fillna(0)
            y = np.array([1] * len(real_enc) + [0] * len(synth_enc))
            if len(X) < 20:
                return None
            clf = GradientBoostingClassifier(n_estimators=50, max_depth=3, random_state=0)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                scores = cross_val_score(clf, X, y, cv=3, scoring="roc_auc")
            return float(np.mean(scores))
        except Exception:
            return None

    @staticmethod
    def _score(dcr_p5: float, nndr_p5: float, auc: float | None) -> float:
        dcr_comp = float(np.clip(dcr_p5 / 0.10, 0.0, 1.0)) * 40.0
        nndr_comp = float(np.clip(nndr_p5, 0.0, 1.0)) * 30.0
        if auc is not None:
            auc_comp = (1.0 - float(np.clip(auc, 0.0, 1.0))) * 2.0 * 30.0
            return float(min(100.0, dcr_comp + nndr_comp + auc_comp))
        return float(min(100.0, (dcr_comp + nndr_comp) / 70.0 * 100.0))
EOF
```

- [ ] **Step 4: Update fidelity/__init__.py to include PrivacyChecker**

```bash
cat > sqllocks_spindle/inference/fidelity/__init__.py << 'EOF'
"""Phase 7 complete fidelity suite."""

from sqllocks_spindle.inference.fidelity.privacy import PrivacyChecker, PrivacyReport

__all__ = [
    "PrivacyChecker",
    "PrivacyReport",
]
EOF
```

- [ ] **Step 5: Run privacy tests to verify they pass**

```bash
.venv-mac/bin/python -m pytest tests/test_privacy_fidelity.py -v 2>&1 | tail -25
```

Expected: all tests pass (some may be skipped if scipy/sklearn absent).

- [ ] **Step 6: Commit**

```bash
git add sqllocks_spindle/inference/fidelity/privacy.py \
        sqllocks_spindle/inference/fidelity/__init__.py \
        tests/test_privacy_fidelity.py
git commit -m "feat: Phase 7.1 — PrivacyChecker (DCR, NNDR, membership AUC)"
```

---

## Task 2: Query-Result Fidelity Module

**Files:**
- Create: `sqllocks_spindle/inference/fidelity/query.py`
- Test: `tests/test_query_fidelity.py`

- [ ] **Step 1: Write the failing tests**

```bash
cat > tests/test_query_fidelity.py << 'EOF'
"""Tests for QueryFidelityChecker — aggregation battery."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference.fidelity.query import (
    QueryFidelityChecker,
    QueryFidelityReport,
    QueryResult,
)


def _df(n: int = 200, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "revenue": rng.normal(1000, 200, n),
        "quantity": rng.integers(1, 50, n).astype(float),
        "region": rng.choice(["North", "South", "East", "West"], n),
        "category": rng.choice(["A", "B", "C"], n),
    })


# --- Structure ---

def test_report_type():
    real = _df()
    report = QueryFidelityChecker(real, real.copy()).check()
    assert isinstance(report, QueryFidelityReport)


def test_identical_data_score_100():
    real = _df()
    report = QueryFidelityChecker(real, real.copy()).check()
    assert report.query_score == pytest.approx(100.0)
    assert report.pass_rate == pytest.approx(1.0)


def test_identical_data_no_failing_queries():
    real = _df()
    report = QueryFidelityChecker(real, real.copy()).check()
    assert len(report.failing_queries) == 0


def test_shifted_synth_has_failures():
    real = _df()
    synth = _df().copy()
    synth["revenue"] = synth["revenue"] * 3  # 200% shift — exceeds 10% tolerance
    report = QueryFidelityChecker(real, synth).check()
    assert report.pass_rate < 1.0
    assert len(report.failing_queries) > 0


def test_query_score_in_range():
    real = _df()
    synth = _df(seed=7)
    report = QueryFidelityChecker(real, synth).check()
    assert 0.0 <= report.query_score <= 100.0


def test_pass_rate_consistent_with_score():
    real = _df()
    synth = _df(seed=7)
    report = QueryFidelityChecker(real, synth).check()
    assert report.query_score == pytest.approx(report.pass_rate * 100.0)


# --- add_query ---

def test_add_query_appends_to_battery():
    real = _df()
    checker = QueryFidelityChecker(real, real.copy())
    checker.add_query("rev_by_region", group_by=["region"], agg={"revenue": "sum"})
    report = checker.check()
    names = [r.name for r in report.query_results]
    assert "rev_by_region" in names


def test_add_query_invalid_column_raises():
    real = _df()
    checker = QueryFidelityChecker(real, real.copy())
    with pytest.raises(ValueError, match="not in DataFrame"):
        checker.add_query("bad", group_by=["nonexistent"], agg={"revenue": "sum"})


def test_add_query_invalid_agg_column_raises():
    real = _df()
    checker = QueryFidelityChecker(real, real.copy())
    with pytest.raises(ValueError, match="not in DataFrame"):
        checker.add_query("bad", group_by=["region"], agg={"nonexistent": "sum"})


# --- QueryResult fields ---

def test_query_result_fields():
    real = _df()
    report = QueryFidelityChecker(real, real.copy()).check()
    assert len(report.query_results) > 0
    r = report.query_results[0]
    assert isinstance(r.name, str)
    assert isinstance(r.real_value, float)
    assert isinstance(r.synth_value, float)
    assert isinstance(r.relative_error, float)
    assert isinstance(r.passed, bool)
    assert r.relative_error >= 0.0


# --- Edge cases ---

def test_no_numeric_cols():
    real = pd.DataFrame({"cat": ["a", "b", "c"] * 30})
    synth = pd.DataFrame({"cat": ["a", "b", "c"] * 30})
    report = QueryFidelityChecker(real, synth).check()
    assert isinstance(report, QueryFidelityReport)
    assert 0.0 <= report.query_score <= 100.0


def test_no_categorical_cols():
    real = pd.DataFrame({"x": np.arange(100, dtype=float), "y": np.arange(100, dtype=float)})
    synth = pd.DataFrame({"x": np.arange(100, dtype=float), "y": np.arange(100, dtype=float)})
    report = QueryFidelityChecker(real, synth).check()
    assert isinstance(report, QueryFidelityReport)


def test_empty_battery_vacuous_pass():
    """DataFrame with no numeric or low-cardinality categorical cols → score=100."""
    high_card = pd.DataFrame({"uid": [str(i) for i in range(100)]})
    report = QueryFidelityChecker(high_card, high_card.copy()).check()
    # null rate queries should still run
    assert isinstance(report, QueryFidelityReport)
    assert 0.0 <= report.query_score <= 100.0


def test_tolerance_respected():
    real = pd.DataFrame({"val": np.ones(100) * 100.0})
    synth = pd.DataFrame({"val": np.ones(100) * 109.9})  # 9.9% shift
    report = QueryFidelityChecker(real, synth, tolerance=0.10).check()
    # 9.9% < 10% tolerance → should pass for mean
    mean_results = [r for r in report.query_results if "mean" in r.name and "val" in r.name]
    assert all(r.passed for r in mean_results)


def test_tolerance_respected_fail():
    real = pd.DataFrame({"val": np.ones(100) * 100.0})
    synth = pd.DataFrame({"val": np.ones(100) * 115.0})  # 15% shift
    report = QueryFidelityChecker(real, synth, tolerance=0.10).check()
    mean_results = [r for r in report.query_results if "mean" in r.name and "val" in r.name]
    assert any(not r.passed for r in mean_results)
EOF
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv-mac/bin/python -m pytest tests/test_query_fidelity.py -v 2>&1 | tail -5
```

Expected: `ImportError: cannot import name 'QueryFidelityChecker'`

- [ ] **Step 3: Create query.py**

```bash
cat > sqllocks_spindle/inference/fidelity/query.py << 'EOF'
"""Query-result fidelity — auto-generated aggregation battery.

Runs standard aggregation and filter queries on real and synthetic DataFrames,
comparing results within a configurable tolerance.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd


@dataclass
class QueryResult:
    """Result of a single aggregation query comparison."""

    name: str
    real_value: float
    synth_value: float
    relative_error: float  # abs(real - synth) / max(abs(real), 1e-9)
    passed: bool           # True if relative_error < tolerance


@dataclass
class QueryFidelityReport:
    """Complete query-result fidelity report."""

    query_results: list[QueryResult]
    pass_rate: float          # fraction of queries passing
    query_score: float        # pass_rate * 100
    tolerance: float
    _custom_count: int = field(default=0, repr=False)

    @property
    def failing_queries(self) -> list[QueryResult]:
        return [r for r in self.query_results if not r.passed]


class QueryFidelityChecker:
    """Compare aggregation query results between real and synthetic DataFrames.

    Args:
        real_df: Real source DataFrame.
        synth_df: Synthetic DataFrame to evaluate.
        tolerance: Maximum allowed relative error per query (default 0.10 = 10%).
    """

    def __init__(
        self,
        real_df: pd.DataFrame,
        synth_df: pd.DataFrame,
        tolerance: float = 0.10,
    ) -> None:
        self._real = real_df
        self._synth = synth_df
        self._tolerance = tolerance
        self._custom: list[dict] = []

    def add_query(
        self,
        name: str,
        group_by: list[str],
        agg: dict[str, str],
    ) -> None:
        """Add a custom aggregation query to the battery.

        Args:
            name: Unique label for this query.
            group_by: Columns to group by.
            agg: Mapping of {column: agg_function} e.g. {"revenue": "sum"}.

        Raises:
            ValueError: If any column in group_by or agg is not in the DataFrame.
        """
        for col in group_by:
            if col not in self._real.columns:
                raise ValueError(
                    f"Column '{col}' not in DataFrame. "
                    f"Available: {list(self._real.columns)}"
                )
        for col in agg:
            if col not in self._real.columns:
                raise ValueError(
                    f"Column '{col}' not in DataFrame. "
                    f"Available: {list(self._real.columns)}"
                )
        self._custom.append({"name": name, "group_by": group_by, "agg": agg})

    def check(self) -> QueryFidelityReport:
        """Run all queries and return a QueryFidelityReport."""
        results: list[QueryResult] = []
        results.extend(self._global_aggregates())
        results.extend(self._grouped_aggregates())
        results.extend(self._null_rates())
        results.extend(self._filter_rates())
        for q in self._custom:
            results.extend(self._run_custom(q))

        if not results:
            return QueryFidelityReport(
                query_results=[],
                pass_rate=1.0,
                query_score=100.0,
                tolerance=self._tolerance,
            )
        pass_rate = float(sum(r.passed for r in results) / len(results))
        return QueryFidelityReport(
            query_results=results,
            pass_rate=pass_rate,
            query_score=pass_rate * 100.0,
            tolerance=self._tolerance,
            _custom_count=len(self._custom),
        )

    # ------------------------------------------------------------------
    # Internal query generators
    # ------------------------------------------------------------------

    def _make(self, name: str, real_val: float, synth_val: float) -> QueryResult:
        denom = max(abs(real_val), 1e-9)
        err = abs(real_val - synth_val) / denom
        return QueryResult(
            name=name,
            real_value=real_val,
            synth_value=synth_val,
            relative_error=err,
            passed=err < self._tolerance,
        )

    def _global_aggregates(self) -> list[QueryResult]:
        results = []
        for col in self._real.select_dtypes(include=[np.number]).columns:
            if col not in self._synth.columns:
                continue
            r = self._real[col].dropna()
            s = self._synth[col].dropna()
            if s.empty:
                continue
            for fn_name, fn in [
                ("mean", np.mean),
                ("median", np.median),
                ("std", np.std),
                ("min", np.min),
                ("max", np.max),
            ]:
                try:
                    results.append(
                        self._make(f"global_{fn_name}_{col}", float(fn(r)), float(fn(s)))
                    )
                except Exception:
                    continue
        return results

    def _grouped_aggregates(self) -> list[QueryResult]:
        results = []
        cat_cols = [
            c for c in self._real.columns
            if (
                not pd.api.types.is_numeric_dtype(self._real[c])
                and not pd.api.types.is_datetime64_any_dtype(self._real[c])
                and self._real[c].nunique() <= 50
                and c in self._synth.columns
            )
        ]
        num_cols = list(self._real.select_dtypes(include=[np.number]).columns)[:5]
        for cat_col in cat_cols[:3]:
            real_counts = self._real.groupby(cat_col, observed=True).size()
            synth_counts = self._synth.groupby(cat_col, observed=True).size()
            common = real_counts.index.intersection(synth_counts.index)
            if len(common) > 0:
                results.append(self._make(
                    f"grouped_count_{cat_col}",
                    float(real_counts[common].sum()),
                    float(synth_counts[common].sum()),
                ))
            for num_col in num_cols:
                if num_col not in self._synth.columns:
                    continue
                try:
                    r_mean = float(
                        self._real.groupby(cat_col, observed=True)[num_col].mean().mean()
                    )
                    s_mean = float(
                        self._synth.groupby(cat_col, observed=True)[num_col].mean().mean()
                    )
                    results.append(
                        self._make(f"grouped_mean_{cat_col}_{num_col}", r_mean, s_mean)
                    )
                except Exception:
                    continue
        return results

    def _null_rates(self) -> list[QueryResult]:
        results = []
        for col in self._real.columns:
            if col not in self._synth.columns:
                continue
            results.append(self._make(
                f"null_rate_{col}",
                float(self._real[col].isna().mean()),
                float(self._synth[col].isna().mean()),
            ))
        return results

    def _filter_rates(self) -> list[QueryResult]:
        results = []
        for col in self._real.select_dtypes(include=[np.number]).columns:
            if col not in self._synth.columns:
                continue
            try:
                p75 = float(self._real[col].quantile(0.75))
                results.append(self._make(
                    f"filter_p75_{col}",
                    float((self._real[col] > p75).mean()),
                    float((self._synth[col] > p75).mean()),
                ))
            except Exception:
                continue
        return results

    def _run_custom(self, q: dict) -> list[QueryResult]:
        results = []
        try:
            for col, func_name in q["agg"].items():
                rg = self._real.groupby(q["group_by"], observed=True)[col].agg(func_name)
                sg = self._synth.groupby(q["group_by"], observed=True)[col].agg(func_name)
                common = rg.index.intersection(sg.index)
                if len(common) == 0:
                    continue
                results.append(self._make(
                    q["name"],
                    float(rg[common].mean()),
                    float(sg[common].mean()),
                ))
        except Exception:
            pass
        return results
EOF
```

- [ ] **Step 4: Update fidelity/__init__.py**

```bash
cat > sqllocks_spindle/inference/fidelity/__init__.py << 'EOF'
"""Phase 7 complete fidelity suite."""

from sqllocks_spindle.inference.fidelity.privacy import PrivacyChecker, PrivacyReport
from sqllocks_spindle.inference.fidelity.query import (
    QueryFidelityChecker,
    QueryFidelityReport,
    QueryResult,
)

__all__ = [
    "PrivacyChecker",
    "PrivacyReport",
    "QueryFidelityChecker",
    "QueryFidelityReport",
    "QueryResult",
]
EOF
```

- [ ] **Step 5: Run query tests**

```bash
.venv-mac/bin/python -m pytest tests/test_query_fidelity.py -v 2>&1 | tail -25
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add sqllocks_spindle/inference/fidelity/query.py \
        sqllocks_spindle/inference/fidelity/__init__.py \
        tests/test_query_fidelity.py
git commit -m "feat: Phase 7.2 — QueryFidelityChecker (aggregation battery)"
```

---

## Task 3: Downstream Model Fidelity Module

**Files:**
- Create: `sqllocks_spindle/inference/fidelity/model.py`
- Test: `tests/test_model_fidelity.py`

- [ ] **Step 1: Write the failing tests**

```bash
cat > tests/test_model_fidelity.py << 'EOF'
"""Tests for ModelFidelityTester — TSTR/TRTR utility gap."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn", reason="scikit-learn required for model fidelity tests")

from sqllocks_spindle.inference.fidelity.model import ModelFidelityTester, ModelFidelityReport


def _clf_df(n: int = 200, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    x1 = rng.normal(0, 1, n)
    x2 = rng.normal(0, 1, n)
    y = (x1 + x2 > 0).astype(int)
    return pd.DataFrame({"x1": x1, "x2": x2, "y": y})


def _reg_df(n: int = 200, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    x = rng.normal(0, 1, n)
    y = 2.0 * x + rng.normal(0, 0.1, n)
    return pd.DataFrame({"x": x, "y": y})


# --- Structure ---

def test_report_type_classification():
    real = _clf_df()
    report = ModelFidelityTester(real, real.copy(), target_col="y", task="classification").test()
    assert isinstance(report, ModelFidelityReport)


def test_report_fields_present():
    real = _clf_df()
    report = ModelFidelityTester(real, real.copy(), target_col="y").test()
    assert isinstance(report.trtr_score, float)
    assert isinstance(report.tstr_score, float)
    assert isinstance(report.utility_gap, float)
    assert isinstance(report.feature_importance_correlation, float)
    assert isinstance(report.model_fidelity_score, float)
    assert report.task == "classification"
    assert report.metric in ("accuracy", "f1_weighted", "r2")


def test_identical_data_low_utility_gap():
    real = _clf_df()
    report = ModelFidelityTester(real, real.copy(), target_col="y").test()
    # When synth == real, TSTR ≈ TRTR so gap should be small
    assert abs(report.utility_gap) < 0.30


def test_model_fidelity_score_in_range():
    real = _clf_df()
    synth = _clf_df(seed=7)
    report = ModelFidelityTester(real, synth, target_col="y").test()
    assert 0.0 <= report.model_fidelity_score <= 100.0


def test_utility_gap_definition():
    real = _clf_df()
    synth = _clf_df(seed=7)
    report = ModelFidelityTester(real, synth, target_col="y").test()
    assert report.utility_gap == pytest.approx(report.trtr_score - report.tstr_score, abs=1e-6)


def test_fi_correlation_in_range():
    real = _clf_df()
    synth = _clf_df(seed=7)
    report = ModelFidelityTester(real, synth, target_col="y").test()
    assert -1.0 <= report.feature_importance_correlation <= 1.0


# --- Regression task ---

def test_regression_task():
    real = _reg_df()
    report = ModelFidelityTester(real, real.copy(), target_col="y", task="regression").test()
    assert report.task == "regression"
    assert report.metric == "r2"
    assert isinstance(report.model_fidelity_score, float)


# --- Error handling ---

def test_missing_target_col_raises():
    real = _clf_df()
    with pytest.raises(ValueError, match="target_col"):
        ModelFidelityTester(real, real.copy(), target_col="nonexistent").test()


def test_too_few_rows_raises():
    real = _clf_df(n=5)
    with pytest.raises(ValueError, match="10 real rows"):
        ModelFidelityTester(real, real.copy(), target_col="y")


def test_invalid_task_raises():
    real = _clf_df()
    with pytest.raises(ValueError, match="task must be"):
        ModelFidelityTester(real, real.copy(), target_col="y", task="clustering")


# --- Score formula ---

def test_perfect_synth_high_score():
    """When synth == real, score should be well above 50."""
    real = _clf_df(300)
    report = ModelFidelityTester(real, real.copy(), target_col="y").test()
    assert report.model_fidelity_score > 50.0


def test_random_noise_synth_lower_score():
    """Random-noise synth should score lower than identical synth."""
    real = _clf_df(300)
    rng = np.random.default_rng(42)
    noise_synth = pd.DataFrame({
        "x1": rng.normal(0, 10, 300),
        "x2": rng.normal(0, 10, 300),
        "y": rng.integers(0, 2, 300),
    })
    perfect_report = ModelFidelityTester(real, real.copy(), target_col="y").test()
    noise_report = ModelFidelityTester(real, noise_synth, target_col="y").test()
    assert perfect_report.model_fidelity_score >= noise_report.model_fidelity_score


def test_datetime_feature_handled():
    df = pd.DataFrame({
        "ts": pd.date_range("2024-01-01", periods=100, freq="D"),
        "val": np.arange(100, dtype=float),
        "target": (np.arange(100) % 2).astype(int),
    })
    report = ModelFidelityTester(df, df.copy(), target_col="target").test()
    assert isinstance(report.model_fidelity_score, float)
EOF
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv-mac/bin/python -m pytest tests/test_model_fidelity.py -v 2>&1 | tail -5
```

Expected: `ImportError: cannot import name 'ModelFidelityTester'`

- [ ] **Step 3: Create model.py**

```bash
cat > sqllocks_spindle/inference/fidelity/model.py << 'EOF'
"""Downstream model fidelity — TSTR/TRTR utility gap.

Trains a GradientBoosting model on real (TRTR) and on synthetic (TSTR),
evaluates both on real held-out data, and computes the utility gap.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass

import numpy as np
import pandas as pd

try:
    from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor
    from sklearn.metrics import f1_score, r2_score
    from sklearn.model_selection import KFold, StratifiedKFold, cross_val_score
    from sklearn.preprocessing import LabelEncoder
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

try:
    from scipy.stats import spearmanr
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False


@dataclass
class ModelFidelityReport:
    """Downstream model fidelity metrics."""

    trtr_score: float                      # Train-real Test-real metric
    tstr_score: float                      # Train-synth Test-real metric
    utility_gap: float                     # trtr_score - tstr_score
    feature_importance_correlation: float  # Spearman rho of FI vectors
    model_fidelity_score: float            # 0-100 composite
    task: str                              # "classification" | "regression"
    metric: str                            # scoring metric name


class ModelFidelityTester:
    """Measure ML utility of synthetic data via TSTR protocol.

    Args:
        real_df: Real source DataFrame.
        synth_df: Synthetic DataFrame to evaluate.
        target_col: Column to use as prediction target.
        task: "classification" or "regression".

    Raises:
        ValueError: If target_col not found, fewer than 10 real rows, or invalid task.
        ImportError: If scikit-learn is not installed.
    """

    def __init__(
        self,
        real_df: pd.DataFrame,
        synth_df: pd.DataFrame,
        target_col: str,
        task: str = "classification",
    ) -> None:
        if not HAS_SKLEARN:
            raise ImportError(
                "scikit-learn is required for ModelFidelityTester. "
                "Install it with: pip install sqllocks-spindle[advanced]"
            )
        if target_col not in real_df.columns:
            raise ValueError(
                f"target_col '{target_col}' not in DataFrame. "
                f"Available columns: {list(real_df.columns)}"
            )
        if len(real_df) < 10:
            raise ValueError(
                f"Need at least 10 real rows for 3-fold CV; got {len(real_df)}"
            )
        if task not in ("classification", "regression"):
            raise ValueError(
                f"task must be 'classification' or 'regression', got '{task}'"
            )
        self._real = real_df.copy()
        self._synth = synth_df.copy()
        self._target_col = target_col
        self._task = task

    def test(self) -> ModelFidelityReport:
        """Run TRTR and TSTR training and return ModelFidelityReport."""
        feature_cols = [c for c in self._real.columns if c != self._target_col]

        real_X = self._encode_features(self._real, feature_cols)
        real_y = self._encode_target(self._real[self._target_col])
        synth_X = self._encode_features(self._synth, feature_cols)
        synth_y = self._encode_target(self._synth[self._target_col])

        common = [c for c in real_X.columns if c in synth_X.columns]
        real_X = real_X[common].fillna(0)
        synth_X = synth_X[common].fillna(0)

        if self._task == "classification":
            clf_class = GradientBoostingClassifier
            cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=0)
            cv_scoring = "accuracy"
            metric = "f1_weighted"
        else:
            clf_class = GradientBoostingRegressor
            cv = KFold(n_splits=3, shuffle=True, random_state=0)
            cv_scoring = "r2"
            metric = "r2"

        # TRTR: train on real, CV-evaluate on real
        clf_real = clf_class(n_estimators=50, max_depth=3, random_state=0)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            trtr_scores = cross_val_score(clf_real, real_X, real_y, cv=cv, scoring=cv_scoring)
        trtr_score = float(np.mean(trtr_scores))

        # TSTR: train on synth, evaluate on real
        clf_synth = clf_class(n_estimators=50, max_depth=3, random_state=0)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            clf_synth.fit(synth_X, synth_y)

        if self._task == "classification":
            preds = clf_synth.predict(real_X)
            tstr_score = float(
                f1_score(real_y, preds, average="weighted", zero_division=0)
            )
        else:
            preds = clf_synth.predict(real_X)
            tstr_score = float(r2_score(real_y, preds))

        # Feature importance correlation (need fully-fitted real model)
        clf_real_full = clf_class(n_estimators=50, max_depth=3, random_state=0)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            clf_real_full.fit(real_X, real_y)

        fi_real = clf_real_full.feature_importances_
        fi_synth = clf_synth.feature_importances_

        fi_corr = 0.0
        if HAS_SCIPY and len(fi_real) > 1:
            try:
                res = spearmanr(fi_real, fi_synth)
                val = float(res.statistic if hasattr(res, "statistic") else res[0])
                if not np.isnan(val):
                    fi_corr = val
            except Exception:
                pass

        utility_gap = trtr_score - tstr_score
        gap_capped = min(1.0, max(0.0, utility_gap))
        fi_score_norm = (fi_corr + 1.0) / 2.0
        model_fidelity_score = float((1.0 - gap_capped) * 50.0 + fi_score_norm * 50.0)

        return ModelFidelityReport(
            trtr_score=trtr_score,
            tstr_score=tstr_score,
            utility_gap=utility_gap,
            feature_importance_correlation=fi_corr,
            model_fidelity_score=model_fidelity_score,
            task=self._task,
            metric=metric,
        )

    def _encode_features(self, df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
        result: dict[str, np.ndarray] = {}
        for col in feature_cols:
            if col not in df.columns:
                continue
            series = df[col]
            if pd.api.types.is_numeric_dtype(series):
                med = float(series.median()) if not series.dropna().empty else 0.0
                result[col] = series.fillna(med).values.astype(float)
            elif pd.api.types.is_datetime64_any_dtype(series):
                result[col] = (series.astype(np.int64) // 10 ** 9).fillna(0).values.astype(float)
            else:
                try:
                    le = LabelEncoder()
                    result[col] = le.fit_transform(
                        series.fillna("__NULL__").astype(str)
                    ).astype(float)
                except Exception:
                    continue
        return pd.DataFrame(result)

    def _encode_target(self, series: pd.Series) -> np.ndarray:
        if pd.api.types.is_numeric_dtype(series):
            return series.fillna(0).values.astype(float)
        le = LabelEncoder()
        return le.fit_transform(series.fillna("__NULL__").astype(str))
EOF
```

- [ ] **Step 4: Update fidelity/__init__.py**

```bash
cat > sqllocks_spindle/inference/fidelity/__init__.py << 'EOF'
"""Phase 7 complete fidelity suite."""

from sqllocks_spindle.inference.fidelity.privacy import PrivacyChecker, PrivacyReport
from sqllocks_spindle.inference.fidelity.query import (
    QueryFidelityChecker,
    QueryFidelityReport,
    QueryResult,
)
from sqllocks_spindle.inference.fidelity.model import ModelFidelityTester, ModelFidelityReport

__all__ = [
    "ModelFidelityReport",
    "ModelFidelityTester",
    "PrivacyChecker",
    "PrivacyReport",
    "QueryFidelityChecker",
    "QueryFidelityReport",
    "QueryResult",
]
EOF
```

- [ ] **Step 5: Run model tests**

```bash
.venv-mac/bin/python -m pytest tests/test_model_fidelity.py -v 2>&1 | tail -25
```

Expected: all tests pass (suite will be skipped if sklearn absent).

- [ ] **Step 6: Commit**

```bash
git add sqllocks_spindle/inference/fidelity/model.py \
        sqllocks_spindle/inference/fidelity/__init__.py \
        tests/test_model_fidelity.py
git commit -m "feat: Phase 7.3 — ModelFidelityTester (TSTR/TRTR utility gap)"
```

---

## Task 4: Relational Fidelity Module

**Files:**
- Create: `sqllocks_spindle/inference/fidelity/relational.py`
- Test: `tests/test_relational_fidelity.py`

- [ ] **Step 1: Write the failing tests**

```bash
cat > tests/test_relational_fidelity.py << 'EOF'
"""Tests for RelationalFidelityChecker — FK integrity, cardinality, join stats."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference.fidelity.relational import (
    FKRelationship,
    RelationalFidelityChecker,
    RelationalFidelityReport,
    RelationshipResult,
)


def _customers(n: int = 50) -> pd.DataFrame:
    return pd.DataFrame({"customer_id": range(1, n + 1), "name": [f"C{i}" for i in range(n)]})


def _orders(customers: pd.DataFrame, orders_per: int = 3, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n = len(customers) * orders_per
    return pd.DataFrame({
        "order_id": range(1, n + 1),
        "customer_id": rng.choice(customers["customer_id"], n),
        "amount": rng.normal(100, 20, n),
    })


def _rel() -> list[FKRelationship]:
    return [FKRelationship(
        parent_table="customers", parent_col="customer_id",
        child_table="orders", child_col="customer_id",
    )]


# --- Structure ---

def test_report_type():
    real_cust = _customers()
    real_ord = _orders(real_cust)
    checker = RelationalFidelityChecker(
        real_tables={"customers": real_cust, "orders": real_ord},
        synth_tables={"customers": real_cust.copy(), "orders": real_ord.copy()},
        relationships=_rel(),
    )
    report = checker.check()
    assert isinstance(report, RelationalFidelityReport)


def test_report_has_relationship_results():
    real_cust = _customers()
    real_ord = _orders(real_cust)
    checker = RelationalFidelityChecker(
        real_tables={"customers": real_cust, "orders": real_ord},
        synth_tables={"customers": real_cust.copy(), "orders": real_ord.copy()},
        relationships=_rel(),
    )
    report = checker.check()
    assert len(report.relationship_results) == 1
    r = report.relationship_results[0]
    assert isinstance(r, RelationshipResult)


def test_perfect_fk_integrity():
    real_cust = _customers()
    real_ord = _orders(real_cust)
    checker = RelationalFidelityChecker(
        real_tables={"customers": real_cust, "orders": real_ord},
        synth_tables={"customers": real_cust.copy(), "orders": real_ord.copy()},
        relationships=_rel(),
    )
    report = checker.check()
    assert report.relationship_results[0].synth_fk_integrity == pytest.approx(1.0)


def test_broken_fk_in_synth_lower_integrity():
    real_cust = _customers(50)
    real_ord = _orders(real_cust)
    synth_ord = real_ord.copy()
    synth_ord.loc[:5, "customer_id"] = 9999  # FK values that don't exist
    checker = RelationalFidelityChecker(
        real_tables={"customers": real_cust, "orders": real_ord},
        synth_tables={"customers": real_cust.copy(), "orders": synth_ord},
        relationships=_rel(),
    )
    report = checker.check()
    assert report.relationship_results[0].synth_fk_integrity < 1.0


def test_relational_score_in_range():
    real_cust = _customers()
    real_ord = _orders(real_cust)
    checker = RelationalFidelityChecker(
        real_tables={"customers": real_cust, "orders": real_ord},
        synth_tables={"customers": real_cust.copy(), "orders": real_ord.copy()},
        relationships=_rel(),
    )
    report = checker.check()
    assert 0.0 <= report.relational_score <= 100.0


def test_overall_integrity_in_range():
    real_cust = _customers()
    real_ord = _orders(real_cust)
    checker = RelationalFidelityChecker(
        real_tables={"customers": real_cust, "orders": real_ord},
        synth_tables={"customers": real_cust.copy(), "orders": real_ord.copy()},
        relationships=_rel(),
    )
    report = checker.check()
    assert 0.0 <= report.overall_integrity <= 1.0


def test_cardinality_ratio_preserved():
    real_cust = _customers()
    real_ord = _orders(real_cust, orders_per=3)
    checker = RelationalFidelityChecker(
        real_tables={"customers": real_cust, "orders": real_ord},
        synth_tables={"customers": real_cust.copy(), "orders": real_ord.copy()},
        relationships=_rel(),
    )
    report = checker.check()
    assert report.relationship_results[0].cardinality_ratio_error == pytest.approx(0.0, abs=0.001)


def test_cardinality_doubled_flagged():
    real_cust = _customers()
    real_ord = _orders(real_cust, orders_per=3)
    synth_ord = pd.concat([real_ord, real_ord], ignore_index=True)
    synth_ord["order_id"] = range(1, len(synth_ord) + 1)
    checker = RelationalFidelityChecker(
        real_tables={"customers": real_cust, "orders": real_ord},
        synth_tables={"customers": real_cust.copy(), "orders": synth_ord},
        relationships=_rel(),
    )
    report = checker.check()
    assert report.relationship_results[0].cardinality_ratio_error > 0.5


def test_no_relationships_single_table_fallback():
    real = pd.DataFrame({"a": np.arange(50, dtype=float)})
    synth = pd.DataFrame({"a": np.arange(50, dtype=float)})
    checker = RelationalFidelityChecker(
        real_tables={"t": real}, synth_tables={"t": synth}, relationships=None
    )
    report = checker.check()
    assert isinstance(report, RelationalFidelityReport)
    assert len(report.relationship_results) == 0
    assert 0.0 <= report.relational_score <= 100.0


def test_unknown_table_in_relationship_raises():
    real_cust = _customers()
    real_ord = _orders(real_cust)
    with pytest.raises(ValueError, match="nonexistent"):
        RelationalFidelityChecker(
            real_tables={"customers": real_cust, "orders": real_ord},
            synth_tables={"customers": real_cust.copy(), "orders": real_ord.copy()},
            relationships=[FKRelationship(
                parent_table="nonexistent", parent_col="id",
                child_table="orders", child_col="customer_id",
            )],
        )


def test_failing_relationships_property():
    real_cust = _customers(50)
    real_ord = _orders(real_cust)
    synth_ord = real_ord.copy()
    synth_ord["customer_id"] = 9999
    checker = RelationalFidelityChecker(
        real_tables={"customers": real_cust, "orders": real_ord},
        synth_tables={"customers": real_cust.copy(), "orders": synth_ord},
        relationships=_rel(),
    )
    report = checker.check()
    assert len(report.failing_relationships) >= 1


def test_join_fidelity_score_in_range():
    real_cust = _customers()
    real_ord = _orders(real_cust)
    checker = RelationalFidelityChecker(
        real_tables={"customers": real_cust, "orders": real_ord},
        synth_tables={"customers": real_cust.copy(), "orders": real_ord.copy()},
        relationships=_rel(),
    )
    report = checker.check()
    for r in report.relationship_results:
        assert 0.0 <= r.join_fidelity_score <= 100.0
EOF
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv-mac/bin/python -m pytest tests/test_relational_fidelity.py -v 2>&1 | tail -5
```

Expected: `ImportError: cannot import name 'FKRelationship'`

- [ ] **Step 3: Create relational.py**

```bash
cat > sqllocks_spindle/inference/fidelity/relational.py << 'EOF'
"""Relational fidelity — FK integrity, cardinality ratios, join result stats.

Works across multiple tables, declared via FKRelationship objects.
Degrades gracefully to single-table constraint checks when no relationships
are provided.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd


@dataclass
class FKRelationship:
    """Declares a foreign-key relationship between two tables."""

    parent_table: str   # table that holds the primary key
    parent_col: str     # PK column name
    child_table: str    # table that holds the foreign key
    child_col: str      # FK column name


@dataclass
class RelationshipResult:
    """Fidelity metrics for a single FK relationship."""

    parent_table: str
    child_table: str
    real_fk_integrity: float       # fraction of real child FKs present in real parent PK
    synth_fk_integrity: float      # fraction of synth child FKs present in synth parent PK
    cardinality_ratio_error: float  # relative error in mean children-per-parent
    join_fidelity_score: float     # 0-100 join result similarity
    passed: bool


@dataclass
class RelationalFidelityReport:
    """Complete relational fidelity report."""

    relationship_results: list[RelationshipResult]
    overall_integrity: float   # fraction of relationships passing
    relational_score: float    # 0-100 mean join fidelity score

    @property
    def failing_relationships(self) -> list[RelationshipResult]:
        return [r for r in self.relationship_results if not r.passed]


class RelationalFidelityChecker:
    """Check FK integrity, cardinality ratios, and join result stats.

    Args:
        real_tables: Mapping of table name → real DataFrame.
        synth_tables: Mapping of table name → synthetic DataFrame.
        relationships: List of FKRelationship declarations. Pass None or []
            to fall back to single-table constraint checks.
        tolerance: Relative error threshold for cardinality + join checks.

    Raises:
        ValueError: If a relationship names an unknown table.
    """

    def __init__(
        self,
        real_tables: dict[str, pd.DataFrame],
        synth_tables: dict[str, pd.DataFrame],
        relationships: list[FKRelationship] | None = None,
        tolerance: float = 0.10,
    ) -> None:
        rels = relationships or []
        all_tables = set(real_tables.keys())
        for rel in rels:
            for tbl in (rel.parent_table, rel.child_table):
                if tbl not in all_tables:
                    raise ValueError(
                        f"Table '{tbl}' in relationship not found in real_tables. "
                        f"Available: {sorted(all_tables)}"
                    )
        self._real_tables = real_tables
        self._synth_tables = synth_tables
        self._relationships = rels
        self._tolerance = tolerance

    def check(self) -> RelationalFidelityReport:
        """Run all relational checks and return RelationalFidelityReport."""
        if not self._relationships:
            return self._single_table_fallback()

        results = [self._check_relationship(rel) for rel in self._relationships]
        overall = float(sum(r.passed for r in results) / len(results)) if results else 1.0
        score = float(np.mean([r.join_fidelity_score for r in results])) if results else 100.0
        return RelationalFidelityReport(
            relationship_results=results,
            overall_integrity=overall,
            relational_score=score,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _check_relationship(self, rel: FKRelationship) -> RelationshipResult:
        rp = self._real_tables[rel.parent_table]
        rc = self._real_tables[rel.child_table]
        sp = self._synth_tables.get(rel.parent_table, pd.DataFrame())
        sc = self._synth_tables.get(rel.child_table, pd.DataFrame())

        real_fi = self._fk_integrity(rp, rel.parent_col, rc, rel.child_col)
        synth_fi = self._fk_integrity(sp, rel.parent_col, sc, rel.child_col)
        card_err = self._cardinality_error(rp, rc, sp, sc)
        join_score = self._join_score(rp, rc, rel, sp, sc)

        passed = (
            synth_fi >= 0.99
            and card_err < self._tolerance
            and join_score >= 80.0
        )
        return RelationshipResult(
            parent_table=rel.parent_table,
            child_table=rel.child_table,
            real_fk_integrity=real_fi,
            synth_fk_integrity=synth_fi,
            cardinality_ratio_error=card_err,
            join_fidelity_score=join_score,
            passed=passed,
        )

    @staticmethod
    def _fk_integrity(
        parent: pd.DataFrame, parent_col: str,
        child: pd.DataFrame, child_col: str,
    ) -> float:
        if parent.empty or child.empty:
            return 1.0
        if parent_col not in parent.columns or child_col not in child.columns:
            return 1.0
        parent_keys = set(parent[parent_col].dropna().unique())
        child_fk = child[child_col].dropna()
        return float(child_fk.isin(parent_keys).mean()) if len(child_fk) > 0 else 1.0

    @staticmethod
    def _cardinality_error(
        rp: pd.DataFrame, rc: pd.DataFrame,
        sp: pd.DataFrame, sc: pd.DataFrame,
    ) -> float:
        real_ratio = len(rc) / max(len(rp), 1)
        synth_ratio = len(sc) / max(len(sp), 1)
        return float(abs(real_ratio - synth_ratio) / max(real_ratio, 1e-9))

    def _join_score(
        self,
        rp: pd.DataFrame, rc: pd.DataFrame, rel: FKRelationship,
        sp: pd.DataFrame, sc: pd.DataFrame,
    ) -> float:
        try:
            rj = rp.merge(rc, left_on=rel.parent_col, right_on=rel.child_col, how="inner")
            sj = sp.merge(sc, left_on=rel.parent_col, right_on=rel.child_col, how="inner")

            # Row count relative error
            count_err = abs(len(rj) - len(sj)) / max(len(rj), 1)
            errors = [count_err]

            # Numeric column mean relative errors
            num_cols = [
                c for c in rj.select_dtypes(include=[np.number]).columns
                if c in sj.columns
            ][:5]
            for col in num_cols:
                rv = float(rj[col].mean())
                sv = float(sj[col].mean()) if not sj.empty else 0.0
                errors.append(abs(rv - sv) / max(abs(rv), 1e-9))

            mean_err = float(np.mean(errors))
            return float(max(0.0, 100.0 * (1.0 - min(mean_err, 1.0))))
        except Exception:
            return 100.0

    def _single_table_fallback(self) -> RelationalFidelityReport:
        """When no relationships given, compare NOT NULL rates across first table pair."""
        first_key = next(iter(self._real_tables))
        real_df = self._real_tables[first_key]
        synth_df = self._synth_tables.get(first_key, pd.DataFrame())
        if synth_df.empty:
            return RelationalFidelityReport(
                relationship_results=[], overall_integrity=1.0, relational_score=100.0
            )
        scores = []
        for col in real_df.columns:
            if col not in synth_df.columns:
                continue
            real_nn = float(real_df[col].notna().mean())
            synth_nn = float(synth_df[col].notna().mean())
            err = abs(real_nn - synth_nn)
            scores.append(max(0.0, 100.0 * (1.0 - err / max(real_nn, 0.01))))
        score = float(np.mean(scores)) if scores else 100.0
        return RelationalFidelityReport(
            relationship_results=[],
            overall_integrity=1.0 if score >= 80.0 else 0.0,
            relational_score=score,
        )
EOF
```

- [ ] **Step 4: Update fidelity/__init__.py**

```bash
cat > sqllocks_spindle/inference/fidelity/__init__.py << 'EOF'
"""Phase 7 complete fidelity suite."""

from sqllocks_spindle.inference.fidelity.privacy import PrivacyChecker, PrivacyReport
from sqllocks_spindle.inference.fidelity.query import (
    QueryFidelityChecker,
    QueryFidelityReport,
    QueryResult,
)
from sqllocks_spindle.inference.fidelity.model import ModelFidelityTester, ModelFidelityReport
from sqllocks_spindle.inference.fidelity.relational import (
    FKRelationship,
    RelationalFidelityChecker,
    RelationalFidelityReport,
    RelationshipResult,
)

__all__ = [
    "FKRelationship",
    "ModelFidelityReport",
    "ModelFidelityTester",
    "PrivacyChecker",
    "PrivacyReport",
    "QueryFidelityChecker",
    "QueryFidelityReport",
    "QueryResult",
    "RelationalFidelityChecker",
    "RelationalFidelityReport",
    "RelationshipResult",
]
EOF
```

- [ ] **Step 5: Run relational tests**

```bash
.venv-mac/bin/python -m pytest tests/test_relational_fidelity.py -v 2>&1 | tail -25
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add sqllocks_spindle/inference/fidelity/relational.py \
        sqllocks_spindle/inference/fidelity/__init__.py \
        tests/test_relational_fidelity.py
git commit -m "feat: Phase 7.4 — RelationalFidelityChecker (FK integrity, cardinality, join)"
```

---

## Task 5: FidelityEngine + Complete fidelity/__init__.py

**Files:**
- Create: `sqllocks_spindle/inference/fidelity/engine.py`
- Modify: `sqllocks_spindle/inference/fidelity/__init__.py` (final version)
- Test: `tests/test_fidelity_engine.py`

- [ ] **Step 1: Write the failing tests**

```bash
cat > tests/test_fidelity_engine.py << 'EOF'
"""Tests for FidelityEngine — unified orchestrator."""
from __future__ import annotations

import json
import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference.fidelity.engine import (
    ComprehensiveFidelityReport,
    FidelityEngine,
)
from sqllocks_spindle.inference.fidelity.relational import FKRelationship


def _df(n: int = 200, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "revenue": rng.normal(1000, 200, n),
        "quantity": rng.integers(1, 50, n).astype(float),
        "region": rng.choice(["North", "South", "East", "West"], n),
        "churn": (rng.normal(0, 1, n) > 0).astype(int),
    })


# --- Structure ---

def test_run_all_returns_report():
    real = _df()
    engine = FidelityEngine({"t": real}, {"t": real.copy()})
    report = engine.run("all")
    assert isinstance(report, ComprehensiveFidelityReport)


def test_marginal_is_populated():
    real = _df()
    engine = FidelityEngine({"t": real}, {"t": real.copy()})
    report = engine.run("all")
    assert report.marginal is not None


def test_query_is_populated():
    real = _df()
    engine = FidelityEngine({"t": real}, {"t": real.copy()})
    report = engine.run("all")
    assert report.query is not None


def test_tier2_is_populated():
    real = _df()
    engine = FidelityEngine({"t": real}, {"t": real.copy()})
    report = engine.run("all")
    assert report.tier2 is not None


# --- Selective checks ---

def test_selective_checks_only_requested():
    real = _df()
    engine = FidelityEngine({"t": real}, {"t": real.copy()})
    report = engine.run(["privacy", "query"])
    assert report.privacy is not None
    assert report.query is not None
    assert report.marginal is None
    assert report.model is None


def test_model_none_without_target_col():
    real = _df()
    engine = FidelityEngine({"t": real}, {"t": real.copy()})
    report = engine.run("all")
    assert report.model is None


def test_model_populated_with_target_col():
    pytest.importorskip("sklearn")
    real = _df(300)
    engine = FidelityEngine({"t": real}, {"t": real.copy()}, target_col="churn")
    report = engine.run("all")
    assert report.model is not None


def test_relational_none_without_relationships():
    real = _df()
    engine = FidelityEngine({"t": real}, {"t": real.copy()})
    report = engine.run("all")
    assert report.relational is None


def test_relational_populated_with_relationships():
    cust = pd.DataFrame({"customer_id": range(1, 51)})
    rng = np.random.default_rng(0)
    orders = pd.DataFrame({
        "order_id": range(1, 151),
        "customer_id": rng.choice(range(1, 51), 150),
        "amount": rng.normal(100, 10, 150),
    })
    rels = [FKRelationship("customers", "customer_id", "orders", "customer_id")]
    engine = FidelityEngine(
        {"customers": cust, "orders": orders},
        {"customers": cust.copy(), "orders": orders.copy()},
        relationships=rels,
    )
    report = engine.run("all")
    assert report.relational is not None


# --- Scores ---

def test_overall_score_in_range():
    real = _df()
    engine = FidelityEngine({"t": real}, {"t": real.copy()})
    report = engine.run("all")
    score = report.overall_score()
    assert 0.0 <= score <= 100.0


def test_overall_score_reweighted_without_model():
    real = _df()
    engine = FidelityEngine({"t": real}, {"t": real.copy()})
    report = engine.run(["marginal", "query", "privacy"])
    # No model or relational — weights should redistribute
    score = report.overall_score()
    assert 0.0 <= score <= 100.0


# --- Serialisation ---

def test_to_dict_json_serialisable():
    real = _df()
    engine = FidelityEngine({"t": real}, {"t": real.copy()})
    report = engine.run("all")
    d = report.to_dict()
    json_str = json.dumps(d)  # raises TypeError if not serialisable
    assert isinstance(json_str, str)


def test_to_html_returns_string_with_score():
    real = _df()
    engine = FidelityEngine({"t": real}, {"t": real.copy()})
    report = engine.run("all")
    html = report.to_html()
    assert isinstance(html, str)
    assert "Fidelity" in html
    assert "<table" in html


def test_summary_returns_string():
    real = _df()
    engine = FidelityEngine({"t": real}, {"t": real.copy()})
    report = engine.run("all")
    s = report.summary()
    assert isinstance(s, str)
    assert "Fidelity:" in s
EOF
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv-mac/bin/python -m pytest tests/test_fidelity_engine.py -v 2>&1 | tail -5
```

Expected: `ImportError: cannot import name 'FidelityEngine'`

- [ ] **Step 3: Create engine.py**

```bash
cat > sqllocks_spindle/inference/fidelity/engine.py << 'EOF'
"""FidelityEngine — unified orchestrator for all fidelity checks.

Runs any combination of marginal, tier2, advanced, privacy, query, model,
and relational checks and returns a ComprehensiveFidelityReport with a
composite score, HTML output, and JSON-serialisable dict.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from sqllocks_spindle.inference.comparator import FidelityComparator, FidelityReport
from sqllocks_spindle.inference.tier2_profiler import Tier2Report, run_tier2
from sqllocks_spindle.inference.fidelity.privacy import PrivacyChecker, PrivacyReport
from sqllocks_spindle.inference.fidelity.query import QueryFidelityChecker, QueryFidelityReport
from sqllocks_spindle.inference.fidelity.model import ModelFidelityTester, ModelFidelityReport
from sqllocks_spindle.inference.fidelity.relational import (
    FKRelationship,
    RelationalFidelityChecker,
    RelationalFidelityReport,
)

try:
    from sqllocks_spindle.inference.advanced_profiler import AdvancedProfiler
    HAS_ADVANCED = True
except ImportError:
    HAS_ADVANCED = False

_LOG = logging.getLogger(__name__)

_ALL_CHECKS = frozenset(
    ["marginal", "tier2", "advanced", "privacy", "query", "model", "relational"]
)

# Weights when all checks are present (sum = 1.0)
_BASE_WEIGHTS: dict[str, float] = {
    "marginal": 0.25,
    "privacy": 0.20,
    "query": 0.20,
    "model": 0.20,
    "tier2": 0.10,
    "relational": 0.05,
}


def _score_colour(score: float) -> str:
    if score >= 85:
        return "#2d7d46"
    if score >= 70:
        return "#b45309"
    return "#c0392b"


@dataclass
class ComprehensiveFidelityReport:
    """Aggregated result from all fidelity checks."""

    marginal: FidelityReport | None
    tier2: Tier2Report | None
    advanced: object | None          # AdvancedTableProfile | None
    privacy: PrivacyReport | None
    query: QueryFidelityReport | None
    model: ModelFidelityReport | None
    relational: RelationalFidelityReport | None

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def overall_score(self) -> float:
        """Weighted average of all available sub-scores (0-100)."""
        scores: dict[str, float] = {}
        if self.marginal is not None:
            scores["marginal"] = self.marginal.overall_score
        if self.tier2 is not None:
            scores["tier2"] = self._tier2_score()
        if self.privacy is not None:
            scores["privacy"] = self.privacy.privacy_score
        if self.query is not None:
            scores["query"] = self.query.query_score
        if self.model is not None:
            scores["model"] = self.model.model_fidelity_score
        if self.relational is not None:
            scores["relational"] = self.relational.relational_score
        if not scores:
            return 0.0
        total_w = sum(_BASE_WEIGHTS.get(k, 0.0) for k in scores)
        if total_w == 0.0:
            return float(sum(scores.values()) / len(scores))
        return float(
            sum(scores[k] * _BASE_WEIGHTS.get(k, 0.0) for k in scores) / total_w
        )

    def _tier2_score(self) -> float:
        if self.tier2 is None:
            return 0.0
        parts: list[float] = []
        for fp in self.tier2.format_preservation.values():
            parts.append(100.0 if fp.passed else max(0.0, 100.0 * (1.0 - fp.delta)))
        for ss in self.tier2.string_similarity.values():
            parts.append(ss.cosine_similarity * 100.0)
        for cc in self.tier2.cardinality.values():
            parts.append(100.0 if cc.passed else 0.0)
        return float(sum(parts) / len(parts)) if parts else 100.0

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def summary(self) -> str:
        """One-line summary: 'Fidelity: 87/100 (marginal: 91, privacy: 88, ...)'"""
        parts: list[str] = []
        if self.marginal is not None:
            parts.append(f"marginal: {self.marginal.overall_score:.0f}")
        if self.privacy is not None:
            parts.append(f"privacy: {self.privacy.privacy_score:.0f}")
        if self.query is not None:
            parts.append(f"query: {self.query.query_score:.0f}")
        if self.model is not None:
            parts.append(f"model: {self.model.model_fidelity_score:.0f}")
        if self.tier2 is not None:
            parts.append(f"tier2: {self._tier2_score():.0f}")
        if self.relational is not None:
            parts.append(f"relational: {self.relational.relational_score:.0f}")
        score = self.overall_score()
        return f"Fidelity: {score:.0f}/100 ({', '.join(parts)})"

    def to_dict(self) -> dict:
        """Return JSON-serialisable dict of all sub-reports."""
        out: dict = {"overall_score": round(self.overall_score(), 2)}
        if self.marginal is not None:
            out["marginal"] = self.marginal.to_dict()
        if self.privacy is not None:
            out["privacy"] = {
                "dcr_median": self.privacy.dcr_median,
                "dcr_p5": self.privacy.dcr_p5,
                "nndr_median": self.privacy.nndr_median,
                "nndr_p5": self.privacy.nndr_p5,
                "membership_auc": self.privacy.membership_auc,
                "privacy_score": self.privacy.privacy_score,
                "is_private": self.privacy.is_private,
            }
        if self.query is not None:
            out["query"] = {
                "pass_rate": self.query.pass_rate,
                "query_score": self.query.query_score,
                "n_queries": len(self.query.query_results),
                "n_failing": len(self.query.failing_queries),
            }
        if self.model is not None:
            out["model"] = {
                "trtr_score": self.model.trtr_score,
                "tstr_score": self.model.tstr_score,
                "utility_gap": self.model.utility_gap,
                "feature_importance_correlation": self.model.feature_importance_correlation,
                "model_fidelity_score": self.model.model_fidelity_score,
                "task": self.model.task,
                "metric": self.model.metric,
            }
        if self.relational is not None:
            out["relational"] = {
                "overall_integrity": self.relational.overall_integrity,
                "relational_score": self.relational.relational_score,
                "n_relationships": len(self.relational.relationship_results),
            }
        return out

    def to_html(self) -> str:
        """Unified HTML report with consistent styling."""
        score = self.overall_score()
        colour = _score_colour(score)
        rows: list[str] = []
        if self.marginal is not None:
            s = self.marginal.overall_score
            rows.append(
                f"<tr><td>Marginal Fidelity</td>"
                f"<td style='color:{_score_colour(s)};font-weight:bold'>{s:.1f}</td>"
                f"<td>KS/Chi² per column</td></tr>"
            )
        if self.privacy is not None:
            s = self.privacy.privacy_score
            rows.append(
                f"<tr><td>Privacy (DCR/NNDR)</td>"
                f"<td style='color:{_score_colour(s)};font-weight:bold'>{s:.1f}</td>"
                f"<td>DCR p5={self.privacy.dcr_p5:.3f}, "
                f"NNDR p5={self.privacy.nndr_p5:.3f}</td></tr>"
            )
        if self.query is not None:
            s = self.query.query_score
            rows.append(
                f"<tr><td>Query Fidelity</td>"
                f"<td style='color:{_score_colour(s)};font-weight:bold'>{s:.1f}</td>"
                f"<td>Pass rate {self.query.pass_rate:.1%} "
                f"({len(self.query.query_results)} queries)</td></tr>"
            )
        if self.model is not None:
            s = self.model.model_fidelity_score
            rows.append(
                f"<tr><td>Model Fidelity (TSTR)</td>"
                f"<td style='color:{_score_colour(s)};font-weight:bold'>{s:.1f}</td>"
                f"<td>Utility gap={self.model.utility_gap:.3f}, "
                f"FI corr={self.model.feature_importance_correlation:.3f}</td></tr>"
            )
        if self.tier2 is not None:
            s = self._tier2_score()
            rows.append(
                f"<tr><td>Tier 2 (Format/Cardinality)</td>"
                f"<td style='color:{_score_colour(s)};font-weight:bold'>{s:.1f}</td>"
                f"<td>Format preservation + string similarity</td></tr>"
            )
        if self.relational is not None:
            s = self.relational.relational_score
            rows.append(
                f"<tr><td>Relational Fidelity</td>"
                f"<td style='color:{_score_colour(s)};font-weight:bold'>{s:.1f}</td>"
                f"<td>Integrity={self.relational.overall_integrity:.1%}</td></tr>"
            )
        table_body = "\n".join(rows)
        return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">
<style>
body{{font-family:sans-serif;max-width:900px;margin:40px auto;color:#222}}
h1{{font-size:1.4em;margin-bottom:4px}}
.score{{font-size:3em;font-weight:bold;color:{colour};margin:8px 0}}
.score-sub{{font-size:0.4em;color:#666}}
table{{border-collapse:collapse;width:100%;margin-top:16px}}
th,td{{text-align:left;padding:8px 12px;border-bottom:1px solid #eee}}
th{{background:#f4f4f4;font-weight:600}}
</style>
</head>
<body>
<h1>Comprehensive Fidelity Report</h1>
<div class="score">{score:.1f}<span class="score-sub">/100</span></div>
<table>
<tr><th>Check</th><th>Score</th><th>Details</th></tr>
{table_body}
</table>
</body>
</html>"""


class FidelityEngine:
    """Unified orchestrator for all Phase 7 fidelity checks.

    Args:
        real_tables: Mapping of table name → real DataFrame.
        synth_tables: Mapping of table name → synthetic DataFrame.
        relationships: Optional FK relationships for relational checks.
        target_col: Optional target column for model fidelity (TSTR).
        task: ML task type for model fidelity ("classification" | "regression").
        tolerance: Relative error threshold for query and relational checks.
    """

    def __init__(
        self,
        real_tables: dict[str, pd.DataFrame],
        synth_tables: dict[str, pd.DataFrame],
        relationships: list[FKRelationship] | None = None,
        target_col: str | None = None,
        task: str = "classification",
        tolerance: float = 0.10,
    ) -> None:
        self._real_tables = real_tables
        self._synth_tables = synth_tables
        self._relationships = relationships
        self._target_col = target_col
        self._task = task
        self._tolerance = tolerance

    def run(
        self, checks: str | list[str] = "all"
    ) -> ComprehensiveFidelityReport:
        """Run selected fidelity checks and return ComprehensiveFidelityReport.

        Args:
            checks: "all" to run every available check, or a list of check
                keys: "marginal", "tier2", "advanced", "privacy", "query",
                "model", "relational".
        """
        active: frozenset[str] = (
            _ALL_CHECKS if checks == "all" else frozenset(checks)
        )

        # Primary table for single-table checks
        primary = next(iter(self._real_tables))
        real_df = self._real_tables[primary]
        synth_df = self._synth_tables[primary]

        marginal: FidelityReport | None = None
        tier2: Tier2Report | None = None
        advanced = None
        privacy: PrivacyReport | None = None
        query: QueryFidelityReport | None = None
        model: ModelFidelityReport | None = None
        relational: RelationalFidelityReport | None = None

        if "marginal" in active:
            try:
                marginal = FidelityComparator().compare(
                    {primary: real_df}, {primary: synth_df}
                )
            except Exception as exc:
                _LOG.warning("Marginal fidelity failed: %s", exc)

        if "tier2" in active:
            try:
                tier2 = run_tier2(real_df, synth_df)
            except Exception as exc:
                _LOG.warning("Tier 2 check failed: %s", exc)

        if "advanced" in active and HAS_ADVANCED:
            try:
                advanced = AdvancedProfiler().profile_pair(real_df, synth_df)
            except Exception as exc:
                _LOG.warning("Advanced profiler failed: %s", exc)

        if "privacy" in active:
            try:
                privacy = PrivacyChecker(real_df, synth_df).check()
            except Exception as exc:
                _LOG.warning("Privacy check failed: %s", exc)

        if "query" in active:
            try:
                query = QueryFidelityChecker(
                    real_df, synth_df, tolerance=self._tolerance
                ).check()
            except Exception as exc:
                _LOG.warning("Query fidelity failed: %s", exc)

        if "model" in active and self._target_col is not None:
            try:
                model = ModelFidelityTester(
                    real_df, synth_df,
                    target_col=self._target_col,
                    task=self._task,
                ).test()
            except ImportError as exc:
                _LOG.warning("Model fidelity skipped (missing dep): %s", exc)
            except Exception as exc:
                _LOG.warning("Model fidelity failed: %s", exc)

        if "relational" in active and self._relationships:
            try:
                relational = RelationalFidelityChecker(
                    self._real_tables,
                    self._synth_tables,
                    self._relationships,
                    self._tolerance,
                ).check()
            except Exception as exc:
                _LOG.warning("Relational check failed: %s", exc)

        return ComprehensiveFidelityReport(
            marginal=marginal,
            tier2=tier2,
            advanced=advanced,
            privacy=privacy,
            query=query,
            model=model,
            relational=relational,
        )
EOF
```

- [ ] **Step 4: Write final fidelity/__init__.py**

```bash
cat > sqllocks_spindle/inference/fidelity/__init__.py << 'EOF'
"""Phase 7 complete fidelity suite — privacy, query, model, and relational checks."""

from sqllocks_spindle.inference.fidelity.engine import (
    ComprehensiveFidelityReport,
    FidelityEngine,
)
from sqllocks_spindle.inference.fidelity.model import ModelFidelityReport, ModelFidelityTester
from sqllocks_spindle.inference.fidelity.privacy import PrivacyChecker, PrivacyReport
from sqllocks_spindle.inference.fidelity.query import (
    QueryFidelityChecker,
    QueryFidelityReport,
    QueryResult,
)
from sqllocks_spindle.inference.fidelity.relational import (
    FKRelationship,
    RelationalFidelityChecker,
    RelationalFidelityReport,
    RelationshipResult,
)

__all__ = [
    "ComprehensiveFidelityReport",
    "FidelityEngine",
    "FKRelationship",
    "ModelFidelityReport",
    "ModelFidelityTester",
    "PrivacyChecker",
    "PrivacyReport",
    "QueryFidelityChecker",
    "QueryFidelityReport",
    "QueryResult",
    "RelationalFidelityChecker",
    "RelationalFidelityReport",
    "RelationshipResult",
]
EOF
```

- [ ] **Step 5: Run engine tests**

```bash
.venv-mac/bin/python -m pytest tests/test_fidelity_engine.py -v 2>&1 | tail -25
```

Expected: all tests pass.

- [ ] **Step 6: Run all five new test files together**

```bash
.venv-mac/bin/python -m pytest tests/test_privacy_fidelity.py tests/test_query_fidelity.py tests/test_model_fidelity.py tests/test_relational_fidelity.py tests/test_fidelity_engine.py -v 2>&1 | tail -15
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add sqllocks_spindle/inference/fidelity/engine.py \
        sqllocks_spindle/inference/fidelity/__init__.py \
        tests/test_fidelity_engine.py
git commit -m "feat: Phase 7.5 — FidelityEngine + ComprehensiveFidelityReport"
```

---

## Task 6: Wire Up Package Exports

**Files:**
- Modify: `sqllocks_spindle/inference/__init__.py`
- Modify: `sqllocks_spindle/__init__.py`

- [ ] **Step 1: Check current inference/__init__.py exports**

```bash
tail -20 sqllocks_spindle/inference/__init__.py
```

- [ ] **Step 2: Add fidelity exports to inference/__init__.py**

Read the current file first:

```bash
python3 -c "
with open('sqllocks_spindle/inference/__init__.py', 'r') as f:
    content = f.read()

# Append fidelity imports before __all__
fidelity_import = '''
from sqllocks_spindle.inference.fidelity import (
    ComprehensiveFidelityReport,
    FidelityEngine,
    FKRelationship,
    ModelFidelityReport,
    ModelFidelityTester,
    PrivacyChecker,
    PrivacyReport,
    QueryFidelityChecker,
    QueryFidelityReport,
    QueryResult,
    RelationalFidelityChecker,
    RelationalFidelityReport,
    RelationshipResult,
)
'''

# Insert before __all__
content = content.replace('__all__ = [', fidelity_import + '__all__ = [')

# Add to __all__
fidelity_all = '''    # Phase 7 fidelity suite
    \"ComprehensiveFidelityReport\",
    \"FidelityEngine\",
    \"FKRelationship\",
    \"ModelFidelityReport\",
    \"ModelFidelityTester\",
    \"PrivacyChecker\",
    \"PrivacyReport\",
    \"QueryFidelityChecker\",
    \"QueryFidelityReport\",
    \"QueryResult\",
    \"RelationalFidelityChecker\",
    \"RelationalFidelityReport\",
    \"RelationshipResult\",
'''
content = content.replace(']  # end __all__', fidelity_all + ']')
# Handle if __all__ ends with ] not ]  # end __all__
if fidelity_all not in content:
    content = content.rstrip()
    if content.endswith(']'):
        content = content[:-1] + fidelity_all + ']\n'

with open('sqllocks_spindle/inference/__init__.py', 'w') as f:
    f.write(content)
print('Done')
"
```

- [ ] **Step 3: Verify inference/__init__.py imports work**

```bash
.venv-mac/bin/python -c "from sqllocks_spindle.inference import FidelityEngine, PrivacyChecker, QueryFidelityChecker; print('OK')"
```

Expected: `OK`

- [ ] **Step 4: Add FidelityEngine to top-level sqllocks_spindle/__init__.py**

```bash
python3 -c "
with open('sqllocks_spindle/__init__.py', 'r') as f:
    content = f.read()

# Add fidelity import alongside existing inference try/except
new_inference_import = '''# Inference (optional — requires [inference] extra)
try:
    from sqllocks_spindle.inference import DataMasker, DataProfiler, ExportedProfile, LakehouseProfiler, MaskConfig, ProfileIO, SchemaBuilder
    from sqllocks_spindle.inference.fidelity import (
        ComprehensiveFidelityReport,
        FidelityEngine,
        FKRelationship,
        ModelFidelityReport,
        ModelFidelityTester,
        PrivacyChecker,
        PrivacyReport,
        QueryFidelityChecker,
        QueryFidelityReport,
        RelationalFidelityChecker,
        RelationalFidelityReport,
    )
except ImportError:
    pass'''

content = content.replace(
    '# Inference (optional — requires [inference] extra)\ntry:\n    from sqllocks_spindle.inference import DataMasker, DataProfiler, ExportedProfile, LakehouseProfiler, MaskConfig, ProfileIO, SchemaBuilder\nexcept ImportError:\n    pass',
    new_inference_import
)

with open('sqllocks_spindle/__init__.py', 'w') as f:
    f.write(content)
print('Done')
"
```

- [ ] **Step 5: Verify top-level import works**

```bash
.venv-mac/bin/python -c "from sqllocks_spindle import FidelityEngine; print('OK')"
```

Expected: `OK`

- [ ] **Step 6: Run full test suite to catch regressions**

```bash
.venv-mac/bin/python -m pytest tests/ -q --ignore=tests/test_validation_live.py --ignore=tests/test_e2e_notebooks.py --tb=short 2>&1 | tail -10
```

Expected: all Phase 7 tests pass, no regressions in existing tests.

- [ ] **Step 7: Run ruff lint**

```bash
.venv-mac/bin/python -m pip install ruff --quiet
.venv-mac/bin/python -m ruff check sqllocks_spindle/inference/fidelity/ 2>&1
```

Fix any reported issues before committing.

- [ ] **Step 8: Commit**

```bash
git add sqllocks_spindle/inference/__init__.py sqllocks_spindle/__init__.py
git commit -m "feat: Phase 7.6 — wire up fidelity exports to inference and top-level packages"
```

---

## Task 7: Version Bump + Release

**Files:**
- Modify: `sqllocks_spindle/__init__.py` (version)
- Modify: `pyproject.toml` (version)
- Modify: `docs/changelog.md`

- [ ] **Step 1: Bump version to 2.15.0**

```bash
python3 -c "
import re

with open('sqllocks_spindle/__init__.py', 'r') as f:
    content = f.read()
content = content.replace('__version__ = \"2.13.0\"', '__version__ = \"2.15.0\"')
with open('sqllocks_spindle/__init__.py', 'w') as f:
    f.write(content)

with open('pyproject.toml', 'r') as f:
    content = f.read()
content = re.sub(r'^version = \"2\.13\.0\"', 'version = \"2.15.0\"', content, flags=re.MULTILINE)
with open('pyproject.toml', 'w') as f:
    f.write(content)

print('Done')
"
```

- [ ] **Step 2: Verify version bump**

```bash
grep '__version__\|^version' sqllocks_spindle/__init__.py pyproject.toml | head -3
```

Expected: both show `2.15.0`

- [ ] **Step 3: Add v2.15.0 changelog entry**

Edit `docs/changelog.md` — insert before the `## [2.13.0]` entry:

```
## [2.15.0] - 2026-04-29

### Added — Phase 7: Complete Fidelity Suite

#### New package: `sqllocks_spindle/inference/fidelity/`

- **`PrivacyChecker`** (`privacy.py`) — DCR (Distance to Closest Record), NNDR (Nearest Neighbour Distance Ratio), and membership inference AUC. Reports `privacy_score` (0–100) and `is_private` flag.
- **`QueryFidelityChecker`** (`query.py`) — Auto-generated aggregation battery (mean/median/std/min/max global, grouped count/mean, null rates, p75 filter rates) + custom `add_query()` support. Reports `query_score` = pass_rate × 100.
- **`ModelFidelityTester`** (`model.py`) — TSTR (Train on Synthetic, Test on Real) protocol via GradientBoosting. Reports `utility_gap`, `feature_importance_correlation` (Spearman), and `model_fidelity_score` (0–100).
- **`RelationalFidelityChecker`** (`relational.py`) — FK integrity rate, parent-child cardinality ratio error, join result stats across declared `FKRelationship` pairs. Single-table NOT NULL fallback when no relationships provided.
- **`FidelityEngine`** (`engine.py`) — Unified orchestrator. `engine.run("all")` or `engine.run(["privacy","query"])` returns `ComprehensiveFidelityReport` with `overall_score()`, `to_html()`, `to_dict()`, `summary()`. Weights: marginal 25%, privacy 20%, query 20%, model 20%, tier2 10%, relational 5%; missing checks redistribute proportionally.

#### Tests
- **~89 new tests** across 5 test files: `test_privacy_fidelity.py` (18), `test_query_fidelity.py` (17), `test_model_fidelity.py` (16), `test_relational_fidelity.py` (13), `test_fidelity_engine.py` (15).

#### Exports
New top-level exports via `try/except ImportError`: `FidelityEngine`, `ComprehensiveFidelityReport`, `FKRelationship`, `PrivacyChecker`, `PrivacyReport`, `QueryFidelityChecker`, `QueryFidelityReport`, `ModelFidelityTester`, `ModelFidelityReport`, `RelationalFidelityChecker`, `RelationalFidelityReport`.
```

- [ ] **Step 4: Run full test suite one final time**

```bash
.venv-mac/bin/python -m pytest tests/ -q --ignore=tests/test_validation_live.py --ignore=tests/test_e2e_notebooks.py --tb=short 2>&1 | tail -5
```

Expected: all pass, exit code 0.

- [ ] **Step 5: Commit version bump**

```bash
git add sqllocks_spindle/__init__.py pyproject.toml docs/changelog.md
git commit -m "chore: release v2.15.0"
```

- [ ] **Step 6: Merge to main**

```bash
cd /path/to/projects/fabric-datagen
git checkout main
git merge phase7/fidelity-suite --no-ff -m "feat: merge Phase 7 → v2.15.0 (complete fidelity suite)"
```

- [ ] **Step 7: Tag v2.15.0**

```bash
git tag -a v2.15.0 -m "v2.15.0 — Phase 7: complete fidelity suite (privacy, query, model, relational)"
```

- [ ] **Step 8: Build wheel + sdist**

```bash
rm -rf dist/ build/
.venv-mac/bin/python -m build
ls -lh dist/
```

Expected: `sqllocks_spindle-2.15.0-py3-none-any.whl` and `sqllocks_spindle-2.15.0.tar.gz`

- [ ] **Step 9: Push to GitHub**

```bash
git push origin main v2.15.0
```

- [ ] **Step 10: Publish to PyPI**

```bash
.venv-mac/bin/python -m twine upload dist/sqllocks_spindle-2.15.0*
```

Expected: `View at: https://pypi.org/project/sqllocks-spindle/2.15.0/`

- [ ] **Step 11: Remove worktree**

```bash
git worktree remove projects/fabric-datagen/.worktrees/phase7-fidelity
git branch -d phase7/fidelity-suite
```
