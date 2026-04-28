# Spindle Phase 3B — Inference Depth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend Spindle's inference engine so generated data statistically matches real source data across distribution shape, cardinality, null rates, temporal patterns, string formats, outlier rates, and column correlations — with a new `FidelityReport` that scores quality.

**Architecture:** Enhanced `ColumnProfile` carries 8 new statistical fields (quantiles, histograms, string length, outlier rate, value counts, fit score). `SchemaBuilder` maps those fields to appropriate strategies via an extended priority tree. Two new engine components — `EmpiricalStrategy` and `GaussianCopula` — cover distribution shape fidelity and column correlations. `FidelityReport` is extended with profile-aware scoring, a `score()` classmethod, and `failing_columns()` / `to_dataframe()` helpers. `LakehouseProfiler` provides Fabric-native profiling via `deltalake`.

**Tech Stack:** pandas, numpy, scipy (optional, already gated), deltalake (optional, already in [fabric] extra), Python 3.10+

**Spec:** `docs/superpowers/specs/2026-04-28-spindle-inference-depth-phase3b-design.md`

---

## File Map

**Modified files:**
- `sqllocks_spindle/inference/profiler.py` — extend `ColumnProfile` + `TableProfile`; update `DataProfiler` with new fields, constructor kwargs, convenience entry points
- `sqllocks_spindle/inference/schema_builder.py` — extended priority tree, new `build()` kwargs, `correlated_columns` output, anomaly registry suggestion
- `sqllocks_spindle/inference/comparator.py` — extend `FidelityReport` with `score()`, `failing_columns()`, `to_dict()`, `to_dataframe()`
- `sqllocks_spindle/inference/__init__.py` — export `LakehouseProfiler`
- `sqllocks_spindle/engine/generator.py` — register `EmpiricalStrategy`, apply `GaussianCopula` post-pass, add `enforce_correlations` + `fidelity_profile` kwargs
- `sqllocks_spindle/__init__.py` — version → `2.9.0`, export `LakehouseProfiler`
- `pyproject.toml` — version → `2.9.0`, add `fabric-inference` extra

**Created files:**
- `sqllocks_spindle/engine/strategies/empirical.py` — `EmpiricalStrategy`
- `sqllocks_spindle/engine/correlation.py` — `GaussianCopula`
- `sqllocks_spindle/inference/lakehouse_profiler.py` — `LakehouseProfiler`
- `tests/test_empirical_strategy.py`
- `tests/test_correlation.py`
- `tests/test_fidelity_report_v2.py`
- `tests/test_lakehouse_profiler.py`

---

## Context for Subagents

The project is `sqllocks-spindle`, a multi-domain synthetic data generator. Root: `projects/fabric-datagen/`. Run tests with `pytest tests/<file> -v` from that root. The `.venv-mac` virtual environment is at `projects/fabric-datagen/.venv-mac/` — activate with `source .venv-mac/bin/activate` before running tests.

**Critical existing patterns:**
- `ColumnProfile` in `profiler.py` is a `@dataclass` — all new fields MUST have default `None` so existing callers don't break.
- `Strategy.generate(column, config, ctx)` signature is fixed — all strategies must follow it.
- Strategies are registered in `Spindle._build_registry()` at `generator.py:286`.
- `Spindle.generate()` returns `GenerationResult`, not a bare DataFrame.
- `FidelityReport` in `comparator.py` is a `@dataclass` with `tables: dict[str, TableFidelity]` and `overall_score: float`.
- scipy is already an optional dependency — guard all scipy calls with `if HAS_SCIPY`.

---

## Task 1: Extend ColumnProfile with new statistical fields

**Files:**
- Modify: `sqllocks_spindle/inference/profiler.py`
- Test: `tests/test_inference.py` (append to existing test class)

- [ ] **Step 1: Write the failing tests for new ColumnProfile fields**

Append to `tests/test_inference.py` (inside `class TestDataProfiler`, after the last test):

```python
def test_quantiles_captured(self):
    profiler = DataProfiler()
    df = pd.DataFrame({"score": np.random.default_rng(42).normal(50, 10, 500)})
    profile = profiler.profile_dataframe(df)
    col = profile.columns["score"]
    assert col.quantiles is not None
    assert set(col.quantiles.keys()) == {"p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99"}
    assert col.quantiles["p50"] == pytest.approx(df["score"].median(), abs=2.0)

def test_outlier_rate_captured(self):
    profiler = DataProfiler()
    rng = np.random.default_rng(42)
    values = np.concatenate([rng.normal(50, 5, 95), [200.0, 210.0, 220.0, 230.0, 240.0]])
    df = pd.DataFrame({"val": values})
    profile = profiler.profile_dataframe(df)
    col = profile.columns["val"]
    assert col.outlier_rate is not None
    assert 0.0 <= col.outlier_rate <= 1.0

def test_value_counts_ext_captured(self):
    profiler = DataProfiler()
    df = pd.DataFrame({"cat": ["A"] * 60 + ["B"] * 30 + ["C"] * 10})
    profile = profiler.profile_dataframe(df)
    col = profile.columns["cat"]
    assert col.value_counts_ext is not None
    assert abs(col.value_counts_ext["A"] - 0.6) < 0.01

def test_string_length_captured(self):
    profiler = DataProfiler()
    df = pd.DataFrame({"name": [f"user_{i:04d}" for i in range(100)]})
    profile = profiler.profile_dataframe(df)
    col = profile.columns["name"]
    assert col.string_length is not None
    assert col.string_length["min"] == 7
    assert col.string_length["max"] == 9

def test_fit_score_captured(self):
    profiler = DataProfiler()
    df = pd.DataFrame({"val": np.random.default_rng(42).normal(0, 1, 200)})
    profile = profiler.profile_dataframe(df)
    col = profile.columns["val"]
    # fit_score is None when scipy not available, or a float in [0, 1]
    if HAS_SCIPY:
        assert col.fit_score is not None
        assert 0.0 <= col.fit_score <= 1.0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_inference.py::TestDataProfiler::test_quantiles_captured -v 2>&1 | tail -10
```

Expected: `AttributeError: 'ColumnProfile' object has no attribute 'quantiles'`

- [ ] **Step 3: Add new fields to ColumnProfile dataclass**

In `sqllocks_spindle/inference/profiler.py`, extend the `ColumnProfile` dataclass. Add these fields AFTER `is_foreign_key` and `fk_ref_table` (lines 72-73):

```python
    # --- Phase 3B: extended statistical fields (all optional for backward compat) ---
    quantiles: dict[str, float] | None = None          # P1,P5,P10,P25,P50,P75,P90,P95,P99
    hour_histogram: list[float] | None = None           # 24-bin normalized hour distribution
    dow_histogram: list[float] | None = None            # 7-bin normalized day-of-week distribution
    string_length: dict[str, float] | None = None       # min, mean, max, p95 of len(value)
    outlier_rate: float | None = None                   # fraction outside 1.5×IQR fence
    value_counts_ext: dict[str, float] | None = None   # value→proportion (top N)
    fit_score: float | None = None                      # 1 - KS_statistic from best-fit dist
```

Also add `correlation_matrix` to `TableProfile` (after `detected_fks`):

```python
    correlation_matrix: dict[str, dict[str, float]] | None = None  # Pearson between numeric cols
```

- [ ] **Step 4: Add constructor kwargs to DataProfiler**

Replace the `DataProfiler` class opening (currently just `class DataProfiler:` with no `__init__`) with:

```python
class DataProfiler:
    """Analyse one or more DataFrames and produce profiles."""

    def __init__(
        self,
        fit_threshold: float = 0.80,
        top_n_values: int = 500,
        outlier_iqr_factor: float = 1.5,
        sample_rows: int | None = None,
    ):
        self.fit_threshold = fit_threshold
        self.top_n_values = top_n_values
        self.outlier_iqr_factor = outlier_iqr_factor
        self.sample_rows = sample_rows
```

- [ ] **Step 5: Add helper methods to DataProfiler**

Add these private methods to `DataProfiler` (before the `_detect_distribution` method):

```python
    def _compute_quantiles(self, numeric: pd.Series) -> dict[str, float]:
        """Compute quantile fingerprint at fixed percentiles."""
        percs = [1, 5, 10, 25, 50, 75, 90, 95, 99]
        vals = np.percentile(numeric.values.astype(float), percs)
        return {f"p{p}": round(float(v), 6) for p, v in zip(percs, vals)}

    def _compute_outlier_rate(self, numeric: pd.Series) -> float:
        """Fraction of values outside 1.5×IQR fence."""
        if len(numeric) < 4:
            return 0.0
        arr = numeric.values.astype(float)
        q1, q3 = np.percentile(arr, [25, 75])
        iqr = q3 - q1
        if iqr == 0:
            return 0.0
        fence_lo = q1 - self.outlier_iqr_factor * iqr
        fence_hi = q3 + self.outlier_iqr_factor * iqr
        n_outliers = int(((arr < fence_lo) | (arr > fence_hi)).sum())
        return round(n_outliers / len(arr), 6)

    def _compute_value_counts_ext(self, non_null: pd.Series) -> dict[str, float]:
        """Top-N value frequencies as proportions."""
        counts = non_null.value_counts(normalize=True)
        if len(counts) > self.top_n_values:
            counts = counts.head(self.top_n_values)
        return {str(k): round(float(v), 6) for k, v in counts.items()}

    def _compute_string_length(self, non_null: pd.Series) -> dict[str, float]:
        """String length statistics: min, mean, max, p95."""
        lengths = non_null.astype(str).str.len()
        return {
            "min": float(lengths.min()),
            "mean": round(float(lengths.mean()), 2),
            "max": float(lengths.max()),
            "p95": float(np.percentile(lengths.values, 95)),
        }

    def _compute_hour_histogram(self, dt_series: pd.Series) -> list[float]:
        """24-bin normalized hour-of-day histogram."""
        hours = pd.to_datetime(dt_series, errors="coerce").dropna().dt.hour
        if len(hours) == 0:
            return [1.0 / 24] * 24
        counts = np.bincount(hours.values, minlength=24).astype(float)
        total = counts.sum()
        return [round(float(v / total), 6) for v in counts]

    def _compute_dow_histogram(self, dt_series: pd.Series) -> list[float]:
        """7-bin normalized day-of-week histogram (Mon=0, Sun=6)."""
        dows = pd.to_datetime(dt_series, errors="coerce").dropna().dt.dayofweek
        if len(dows) == 0:
            return [1.0 / 7] * 7
        counts = np.bincount(dows.values, minlength=7).astype(float)
        total = counts.sum()
        return [round(float(v / total), 6) for v in counts]

    def _compute_correlation_matrix(self, df: pd.DataFrame) -> dict[str, dict[str, float]]:
        """Pearson correlation matrix for all numeric columns (pairs only)."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if len(numeric_cols) < 2:
            return {}
        corr = df[numeric_cols].corr(method="pearson")
        result: dict[str, dict[str, float]] = {}
        for col_a in numeric_cols:
            for col_b in numeric_cols:
                if col_a != col_b:
                    v = corr.loc[col_a, col_b]
                    if not np.isnan(v):
                        result.setdefault(col_a, {})[col_b] = round(float(v), 4)
        return result
```

- [ ] **Step 6: Update _profile_single to populate new fields**

In `_profile_single`, after computing `dist_name, dist_params`, add the new field computations. Replace the block starting `columns[col] = ColumnProfile(` with:

```python
            # --- Phase 3B extended stats ---
            quantiles: dict[str, float] | None = None
            outlier_rate_val: float | None = None
            value_counts_ext: dict[str, float] | None = None
            string_length_val: dict[str, float] | None = None
            hour_histogram_val: list[float] | None = None
            dow_histogram_val: list[float] | None = None
            fit_score_val: float | None = None

            if spindle_type in ("integer", "float") and len(non_null) >= 4:
                numeric_series = pd.to_numeric(non_null, errors="coerce").dropna()
                if len(numeric_series) >= 4:
                    quantiles = self._compute_quantiles(numeric_series)
                    outlier_rate_val = self._compute_outlier_rate(numeric_series)

            if spindle_type == "string" and len(non_null) > 0:
                string_length_val = self._compute_string_length(non_null)

            if len(non_null) > 0 and cardinality <= self.top_n_values:
                value_counts_ext = self._compute_value_counts_ext(non_null)

            if spindle_type in ("date", "datetime") and len(non_null) > 0:
                hour_histogram_val = self._compute_hour_histogram(non_null)
                dow_histogram_val = self._compute_dow_histogram(non_null)

            # fit_score: 1 - ks_stat from best-fit distribution
            if dist_name is not None and dist_params is not None and HAS_SCIPY:
                numeric_for_fit = pd.to_numeric(non_null, errors="coerce").dropna()
                if len(numeric_for_fit) >= 20:
                    from scipy import stats as _sp
                    dist_map = {
                        "normal": _sp.norm, "uniform": _sp.uniform,
                        "exponential": _sp.expon, "lognormal": _sp.lognorm,
                    }
                    dist_obj = dist_map.get(dist_name)
                    if dist_obj is not None:
                        try:
                            params = dist_obj.fit(numeric_for_fit.values.astype(float))
                            ks_stat, _ = _sp.kstest(
                                numeric_for_fit.values.astype(float), dist_obj.name, args=params
                            )
                            fit_score_val = round(1.0 - float(ks_stat), 4)
                        except Exception:
                            pass

            columns[col] = ColumnProfile(
                name=col,
                dtype=spindle_type,
                null_count=null_count,
                null_rate=round(null_rate, 6),
                cardinality=cardinality,
                cardinality_ratio=round(cardinality_ratio, 6),
                is_unique=is_unique,
                is_enum=is_enum,
                enum_values=enum_values,
                min_value=min_value,
                max_value=max_value,
                mean=mean_val,
                std=std_val,
                distribution=dist_name,
                distribution_params=dist_params,
                pattern=pattern,
                is_primary_key=is_pk,
                is_foreign_key=is_fk,
                fk_ref_table=fk_ref,
                quantiles=quantiles,
                hour_histogram=hour_histogram_val,
                dow_histogram=dow_histogram_val,
                string_length=string_length_val,
                outlier_rate=outlier_rate_val,
                value_counts_ext=value_counts_ext,
                fit_score=fit_score_val,
            )
```

Then after building `columns`, before returning `TableProfile`, compute and attach the correlation matrix:

```python
        corr_matrix = self._compute_correlation_matrix(df)

        return TableProfile(
            name=table_name,
            row_count=row_count,
            columns=columns,
            primary_key=pk_cols,
            detected_fks=fk_map,
            correlation_matrix=corr_matrix if corr_matrix else None,
        )
```

- [ ] **Step 7: Apply sample_rows in profile_dataframe**

At the top of `_profile_single`, after `row_count = len(df)`, add:

```python
        if self.sample_rows is not None and len(df) > self.sample_rows:
            df = df.sample(n=self.sample_rows, random_state=42)
            row_count = len(df)
```

- [ ] **Step 8: Run tests to verify they pass**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_inference.py::TestDataProfiler -v 2>&1 | tail -20
```

Expected: all `TestDataProfiler` tests pass, including the 5 new ones.

- [ ] **Step 9: Commit**

```bash
cd projects/fabric-datagen && git add sqllocks_spindle/inference/profiler.py tests/test_inference.py && git commit -m "feat(inference): extend ColumnProfile with quantiles, outlier_rate, value_counts_ext, fit_score, histograms"
```

---

## Task 2: DataProfiler convenience entry points

**Files:**
- Modify: `sqllocks_spindle/inference/profiler.py`
- Test: `tests/test_inference.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_inference.py` (new class after `TestDataProfiler`):

```python
class TestDataProfilerEntryPoints:
    def test_profile_alias(self):
        """profile() is an alias for profile_dataframe()."""
        profiler = DataProfiler()
        df = pd.DataFrame({"x": [1, 2, 3]})
        result = profiler.profile(df, table_name="t")
        assert result.name == "t"
        assert "x" in result.columns

    def test_from_csv(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        csv_path = tmp_path / "test.csv"
        df.to_csv(csv_path, index=False)
        profile = DataProfiler.from_csv(str(csv_path))
        assert profile.name == "test"
        assert "a" in profile.columns
        assert "b" in profile.columns

    def test_from_csv_with_sample_rows(self, tmp_path):
        df = pd.DataFrame({"v": range(1000)})
        csv_path = tmp_path / "big.csv"
        df.to_csv(csv_path, index=False)
        profile = DataProfiler.from_csv(str(csv_path), sample_rows=100)
        assert profile.row_count == 100

    def test_constructor_kwargs(self):
        profiler = DataProfiler(fit_threshold=0.9, top_n_values=10, outlier_iqr_factor=2.0)
        assert profiler.fit_threshold == 0.9
        assert profiler.top_n_values == 10
        assert profiler.outlier_iqr_factor == 2.0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_inference.py::TestDataProfilerEntryPoints -v 2>&1 | tail -10
```

Expected: `AttributeError: type object 'DataProfiler' has no attribute 'from_csv'` and `'DataProfiler' object has no attribute 'profile'`

- [ ] **Step 3: Add profile() alias and from_csv() classmethod**

In `sqllocks_spindle/inference/profiler.py`, inside the `DataProfiler` class, add these methods after `profile_dataframe`:

```python
    def profile(
        self,
        df: pd.DataFrame,
        table_name: str = "table",
    ) -> "TableProfile":
        """Alias for profile_dataframe(). Profile a single DataFrame."""
        return self.profile_dataframe(df, table_name=table_name)

    @classmethod
    def from_csv(
        cls,
        path: str,
        table_name: str | None = None,
        sample_rows: int | None = None,
        **kwargs,
    ) -> "TableProfile":
        """Profile a CSV file.

        Args:
            path: Path to the CSV file.
            table_name: Name for the table profile. Defaults to the filename stem.
            sample_rows: If set, sample this many rows before profiling.
            **kwargs: Passed to DataProfiler constructor (fit_threshold, top_n_values, etc.).
        """
        from pathlib import Path as _Path
        import pandas as _pd

        p = _Path(path)
        name = table_name or p.stem
        df = _pd.read_csv(path)
        profiler = cls(sample_rows=sample_rows, **kwargs)
        return profiler.profile(df, table_name=name)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_inference.py::TestDataProfilerEntryPoints -v 2>&1 | tail -10
```

Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
cd projects/fabric-datagen && git add sqllocks_spindle/inference/profiler.py tests/test_inference.py && git commit -m "feat(inference): add DataProfiler.profile() alias and from_csv() classmethod"
```

---

## Task 3: EmpiricalStrategy — quantile interpolation

**Files:**
- Create: `sqllocks_spindle/engine/strategies/empirical.py`
- Modify: `sqllocks_spindle/engine/generator.py` (register strategy)
- Create: `tests/test_empirical_strategy.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_empirical_strategy.py`:

```python
"""Tests for EmpiricalStrategy."""

from __future__ import annotations

import numpy as np
import pytest

from sqllocks_spindle.engine.strategies.empirical import EmpiricalStrategy
from sqllocks_spindle.engine.strategies.base import GenerationContext
from sqllocks_spindle.engine.id_manager import IDManager
from sqllocks_spindle.schema.parser import ColumnDef


def _make_ctx(n: int = 100) -> GenerationContext:
    rng = np.random.default_rng(42)
    id_mgr = IDManager(rng)
    return GenerationContext(rng=rng, id_manager=id_mgr, model_config={}, row_count=n)


def _make_col(name: str = "val") -> ColumnDef:
    return ColumnDef(name=name, type="decimal", generator={}, nullable=False, null_rate=0.0)


# Quantile fingerprint for a normal(50, 10) distribution
NORMAL_QUANTILES = {
    "p1": 26.7, "p5": 33.6, "p10": 37.2, "p25": 43.3,
    "p50": 50.0, "p75": 56.7, "p90": 62.8, "p95": 66.4, "p99": 73.3,
}


class TestEmpiricalStrategy:
    def test_generates_correct_count(self):
        strategy = EmpiricalStrategy()
        col = _make_col()
        ctx = _make_ctx(n=200)
        config = {"strategy": "empirical", "quantiles": NORMAL_QUANTILES}
        result = strategy.generate(col, config, ctx)
        assert len(result) == 200

    def test_values_within_observed_range(self):
        strategy = EmpiricalStrategy()
        col = _make_col()
        ctx = _make_ctx(n=1000)
        config = {"strategy": "empirical", "quantiles": NORMAL_QUANTILES}
        result = strategy.generate(col, config, ctx)
        assert float(result.min()) >= NORMAL_QUANTILES["p1"] - 5
        assert float(result.max()) <= NORMAL_QUANTILES["p99"] + 5

    def test_median_is_approximate(self):
        strategy = EmpiricalStrategy()
        col = _make_col()
        ctx = _make_ctx(n=2000)
        config = {"strategy": "empirical", "quantiles": NORMAL_QUANTILES}
        result = strategy.generate(col, config, ctx)
        assert abs(float(np.median(result)) - 50.0) < 3.0

    def test_missing_quantiles_raises(self):
        strategy = EmpiricalStrategy()
        col = _make_col()
        ctx = _make_ctx()
        with pytest.raises((KeyError, ValueError)):
            strategy.generate(col, {}, ctx)

    def test_registered_in_spindle(self):
        from sqllocks_spindle import Spindle
        s = Spindle()
        assert s._registry.has("empirical")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_empirical_strategy.py -v 2>&1 | tail -10
```

Expected: `ModuleNotFoundError: No module named 'sqllocks_spindle.engine.strategies.empirical'`

- [ ] **Step 3: Create empirical.py**

Create `sqllocks_spindle/engine/strategies/empirical.py`:

```python
"""Empirical strategy — quantile-interpolation-based numeric generation."""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef

try:
    from scipy.interpolate import interp1d as _interp1d
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

# Fixed percentiles that match the DataProfiler fingerprint
_PERCENTILE_KEYS = ["p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99"]
_PERCENTILE_VALUES = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]


class EmpiricalStrategy(Strategy):
    """Generate numeric values by interpolating a stored quantile fingerprint.

    Requires scipy for cubic interpolation; falls back to numpy linear
    interpolation when scipy is absent.

    Schema config:
        strategy: "empirical"
        quantiles: {p1: float, p5: float, ..., p99: float}
        interpolation: "linear" | "cubic"  (default "linear")
    """

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        quantiles = config.get("quantiles")
        if not quantiles:
            raise ValueError(
                f"empirical strategy requires 'quantiles' dict for column '{column.name}'"
            )
        interpolation = config.get("interpolation", "linear")

        # Build (cdf_value, quantile_value) mapping
        q_values = np.array([quantiles[k] for k in _PERCENTILE_KEYS], dtype=float)
        p_values = np.array(_PERCENTILE_VALUES, dtype=float)

        # Draw uniform samples, then map through the quantile function
        u = ctx.rng.uniform(0.0, 1.0, size=ctx.row_count)

        if HAS_SCIPY and interpolation == "cubic":
            interp_fn = _interp1d(p_values, q_values, kind="cubic", bounds_error=False,
                                   fill_value=(q_values[0], q_values[-1]))
            result = interp_fn(u).astype(float)
        else:
            result = np.interp(u, p_values, q_values)

        return result
```

- [ ] **Step 4: Register EmpiricalStrategy in generator.py**

In `sqllocks_spindle/engine/generator.py`, add the import after the other strategy imports (after line ~42):

```python
from sqllocks_spindle.engine.strategies.empirical import EmpiricalStrategy
```

In `_build_registry()` (around line 310), add after the last `registry.register(...)`:

```python
        registry.register("empirical", EmpiricalStrategy())
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_empirical_strategy.py -v 2>&1 | tail -15
```

Expected: all 5 tests pass.

- [ ] **Step 6: Commit**

```bash
cd projects/fabric-datagen && git add sqllocks_spindle/engine/strategies/empirical.py sqllocks_spindle/engine/generator.py tests/test_empirical_strategy.py && git commit -m "feat(engine): add EmpiricalStrategy for quantile-fingerprint-based numeric generation"
```

---

## Task 4: GaussianCopula — correlation enforcement post-pass

**Files:**
- Create: `sqllocks_spindle/engine/correlation.py`
- Modify: `sqllocks_spindle/engine/generator.py`
- Create: `tests/test_correlation.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_correlation.py`:

```python
"""Tests for GaussianCopula post-pass."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.engine.correlation import GaussianCopula


def _correlated_df(n: int = 2000, target_r: float = 0.8, seed: int = 42) -> tuple[pd.DataFrame, float]:
    """Create a DataFrame with two columns having ~target_r correlation."""
    rng = np.random.default_rng(seed)
    x = rng.normal(0, 1, n)
    noise = rng.normal(0, 1, n)
    y = target_r * x + np.sqrt(1 - target_r**2) * noise
    return pd.DataFrame({"x": x, "y": y}), target_r


class TestGaussianCopula:
    def test_apply_preserves_row_count(self):
        df, _ = _correlated_df()
        copula = GaussianCopula({"x": {"y": 0.8}, "y": {"x": 0.8}})
        result = copula.apply(df)
        assert len(result) == len(df)

    def test_apply_preserves_column_set(self):
        df, _ = _correlated_df()
        copula = GaussianCopula({"x": {"y": 0.8}, "y": {"x": 0.8}})
        result = copula.apply(df)
        assert set(result.columns) == {"x", "y"}

    def test_marginals_unchanged(self):
        """Each column's sorted values must be identical after copula."""
        df, _ = _correlated_df(n=500)
        copula = GaussianCopula({"x": {"y": 0.8}, "y": {"x": 0.8}})
        result = copula.apply(df)
        np.testing.assert_array_equal(
            np.sort(df["x"].values), np.sort(result["x"].values)
        )
        np.testing.assert_array_equal(
            np.sort(df["y"].values), np.sort(result["y"].values)
        )

    def test_below_threshold_columns_skipped(self):
        """Pairs with |r| < threshold are not reordered."""
        df = pd.DataFrame({"a": np.arange(100, dtype=float), "b": np.arange(100, dtype=float)})
        copula = GaussianCopula({"a": {"b": 0.3}}, threshold=0.5)
        result = copula.apply(df)
        pd.testing.assert_frame_equal(df, result)

    def test_empty_correlation_matrix_is_noop(self):
        df = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
        copula = GaussianCopula({})
        result = copula.apply(df)
        pd.testing.assert_frame_equal(df, result)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_correlation.py -v 2>&1 | tail -10
```

Expected: `ModuleNotFoundError: No module named 'sqllocks_spindle.engine.correlation'`

- [ ] **Step 3: Create correlation.py**

Create `sqllocks_spindle/engine/correlation.py`:

```python
"""Gaussian copula post-pass — enforce column correlations without changing marginals."""

from __future__ import annotations

import numpy as np
import pandas as pd


class GaussianCopula:
    """Reorder column values to achieve target Pearson correlations.

    Algorithm (rank-based Gaussian copula):
    1. For each numeric column, map values to ranks, then to uniform [0,1].
    2. Apply inverse normal CDF (probit) → correlated Gaussian space.
    3. Cholesky decompose target correlation matrix → apply linear transform.
    4. Map back to uniform via normal CDF → back to original values via rank lookup.

    This preserves each column's marginal distribution exactly while inducing
    the target pairwise correlations.

    Args:
        correlation_matrix: dict of {col_a: {col_b: r}} pairs.
        threshold: Skip pairs where |r| < threshold (default 0.5).
    """

    def __init__(
        self,
        correlation_matrix: dict[str, dict[str, float]],
        threshold: float = 0.5,
    ):
        self.correlation_matrix = correlation_matrix
        self.threshold = threshold

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply the copula reordering to a DataFrame. Returns a new DataFrame."""
        if not self.correlation_matrix:
            return df

        # Find numeric columns that appear in the correlation matrix
        cols = [
            c for c in df.columns
            if c in self.correlation_matrix
            and pd.api.types.is_numeric_dtype(df[c])
        ]
        if len(cols) < 2:
            return df

        # Filter to pairs that exceed threshold
        active_pairs: set[tuple[str, str]] = set()
        for col_a, row in self.correlation_matrix.items():
            for col_b, r in row.items():
                if abs(r) >= self.threshold and col_a in cols and col_b in cols:
                    pair = tuple(sorted([col_a, col_b]))
                    active_pairs.add(pair)  # type: ignore[arg-type]

        if not active_pairs:
            return df

        active_cols = sorted({c for pair in active_pairs for c in pair})
        n = len(df)

        # Build target correlation matrix for active_cols
        k = len(active_cols)
        target = np.eye(k)
        col_idx = {c: i for i, c in enumerate(active_cols)}
        for col_a, col_b in active_pairs:
            r = self.correlation_matrix.get(col_a, {}).get(col_b, 0.0)
            i, j = col_idx[col_a], col_idx[col_b]
            target[i, j] = r
            target[j, i] = r

        # Ensure positive semi-definiteness (clip eigenvalues)
        eigvals, eigvecs = np.linalg.eigh(target)
        eigvals = np.clip(eigvals, 1e-8, None)
        target = eigvecs @ np.diag(eigvals) @ eigvecs.T

        try:
            L = np.linalg.cholesky(target)
        except np.linalg.LinAlgError:
            return df  # fall back if decomposition fails

        # Step 1: rank-based uniform CDF for each column
        result = df.copy()
        uniform_block = np.zeros((n, k))
        for idx, col in enumerate(active_cols):
            vals = df[col].values.astype(float)
            ranks = np.argsort(np.argsort(vals))  # tie-preserving ranks (0-based)
            # Map ranks to (0, 1) open interval using (rank + 0.5) / n
            uniform_block[:, idx] = (ranks + 0.5) / n

        # Step 2: probit transform → Gaussian space
        from scipy.stats import norm as _norm
        gaussian_block = _norm.ppf(uniform_block)

        # Step 3: apply Cholesky transform to induce target correlations
        z_raw = np.random.default_rng(42).standard_normal((n, k))
        z_corr = z_raw @ L.T

        # Step 4: use the RANK ORDER from z_corr to reorder original column values
        for idx, col in enumerate(active_cols):
            original_sorted = np.sort(df[col].values)
            new_ranks = np.argsort(np.argsort(z_corr[:, idx]))
            result[col] = original_sorted[new_ranks]

        return result
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_correlation.py -v 2>&1 | tail -15
```

Expected: all 5 tests pass. (Note: `test_apply_preserves_marginals` requires scipy — it is already a dev dependency.)

- [ ] **Step 5: Commit**

```bash
cd projects/fabric-datagen && git add sqllocks_spindle/engine/correlation.py tests/test_correlation.py && git commit -m "feat(engine): add GaussianCopula post-pass for correlation enforcement"
```

---

## Task 5: Extended string pattern detection

**Files:**
- Modify: `sqllocks_spindle/inference/profiler.py`
- Test: `tests/test_inference.py`

The new patterns let the profiler detect SSN, credit card, IBAN, IP, MAC, postal code, currency code, and language code columns, and SchemaBuilder will map them to appropriate Faker providers.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_inference.py` (new class):

```python
class TestExtendedPatternDetection:
    """New pattern detection for Phase 3B."""

    def _profiler_detect(self, values: list[str]) -> str | None:
        profiler = DataProfiler()
        s = pd.Series(values)
        return profiler._detect_pattern(s)

    def test_detects_ssn(self):
        ssns = [f"123-{i:02d}-{j:04d}" for i in range(1, 10) for j in range(1, 12)]
        assert self._profiler_detect(ssns) == "ssn"

    def test_detects_ip_v4(self):
        ips = [f"192.168.{i}.{j}" for i in range(10) for j in range(10)]
        assert self._profiler_detect(ips) == "ip_address"

    def test_detects_mac_address(self):
        macs = [f"00:1A:{i:02X}:{j:02X}:{k:02X}:FF"
                for i in range(10) for j in range(10) for k in range(10)][:100]
        assert self._profiler_detect(macs) == "mac_address"

    def test_detects_currency_code(self):
        codes = ["USD", "EUR", "GBP", "JPY", "CAD"] * 20
        assert self._profiler_detect(codes) == "currency_code"

    def test_detects_language_code(self):
        langs = ["en", "fr", "de", "es", "it"] * 20
        assert self._profiler_detect(langs) == "language_code"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_inference.py::TestExtendedPatternDetection -v 2>&1 | tail -10
```

Expected: `AssertionError` — patterns return `None` instead of the named pattern.

- [ ] **Step 3: Add regex patterns to profiler.py**

In `sqllocks_spindle/inference/profiler.py`, after the existing `_DATE_RE` definition (around line 41), add:

```python
_SSN_RE = re.compile(r"^\d{3}-\d{2}-\d{4}$")
_IP_V4_RE = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
_IP_V6_RE = re.compile(r"^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{0,4}){2,7}$")
_MAC_RE = re.compile(r"^([0-9a-fA-F]{2}[:\-]){5}[0-9a-fA-F]{2}$")
_CURRENCY_CODE_RE = re.compile(r"^[A-Z]{3}$")
_LANGUAGE_CODE_RE = re.compile(r"^[a-z]{2}(-[A-Z]{2})?$")
_IBAN_RE = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{1,30}$")
_POSTAL_US_RE = re.compile(r"^\d{5}(-\d{4})?$")
```

- [ ] **Step 4: Update _detect_pattern with new checks**

In `_detect_pattern`, after the `# Date string` block (before `return None`), add:

```python
        # SSN
        if sample.str.match(_SSN_RE.pattern, na=False).sum() / total >= threshold:
            return "ssn"

        # IP address (v4 or v6)
        ipv4_match = sample.str.match(_IP_V4_RE.pattern, na=False).sum() / total
        ipv6_match = sample.str.match(_IP_V6_RE.pattern, na=False).sum() / total
        if ipv4_match >= threshold or ipv6_match >= threshold:
            return "ip_address"

        # MAC address
        if sample.str.match(_MAC_RE.pattern, na=False).sum() / total >= threshold:
            return "mac_address"

        # IBAN
        if sample.str.match(_IBAN_RE.pattern, na=False).sum() / total >= threshold:
            return "iban"

        # Postal code (US ZIP)
        if sample.str.match(_POSTAL_US_RE.pattern, na=False).sum() / total >= threshold:
            return "postal_code"

        # Currency code (3 uppercase letters) — check cardinality to avoid false positives
        if (sample.str.match(_CURRENCY_CODE_RE.pattern, na=False).sum() / total >= threshold
                and non_null.nunique() <= 200):
            return "currency_code"

        # Language code (2 lowercase letters, optional region)
        if (sample.str.match(_LANGUAGE_CODE_RE.pattern, na=False).sum() / total >= threshold
                and non_null.nunique() <= 200):
            return "language_code"
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_inference.py::TestExtendedPatternDetection -v 2>&1 | tail -10
```

Expected: all 5 tests pass.

- [ ] **Step 6: Commit**

```bash
cd projects/fabric-datagen && git add sqllocks_spindle/inference/profiler.py tests/test_inference.py && git commit -m "feat(inference): add extended string pattern detection (ssn, ip, mac, iban, currency, language)"
```

---

## Task 6: SchemaBuilder enhancements

**Files:**
- Modify: `sqllocks_spindle/inference/schema_builder.py`
- Test: `tests/test_inference.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_inference.py` (new class):

```python
class TestSchemaBuilderV2:
    """Tests for Phase 3B SchemaBuilder enhancements."""

    def _make_profile_with_field(self, **col_kwargs) -> "DatasetProfile":
        from sqllocks_spindle.inference.profiler import (
            ColumnProfile, TableProfile, DatasetProfile
        )
        col = ColumnProfile(
            name="val", dtype="float",
            null_count=0, null_rate=0.0,
            cardinality=100, cardinality_ratio=1.0,
            is_unique=False, is_enum=False, enum_values=None,
            min_value=0.0, max_value=100.0, mean=50.0, std=10.0,
            distribution=None, distribution_params=None, pattern=None,
            is_primary_key=False, is_foreign_key=False, fk_ref_table=None,
            **col_kwargs,
        )
        table = TableProfile(name="t", row_count=100, columns={"val": col},
                             primary_key=[], detected_fks={})
        return DatasetProfile(tables={"t": table})

    def test_empirical_strategy_selected_when_fit_score_low(self):
        profile = self._make_profile_with_field(
            dtype="float",
            distribution="normal",
            distribution_params={"loc": 50.0, "scale": 10.0},
            fit_score=0.60,  # below default 0.80 threshold
            quantiles={"p1": 10.0, "p5": 20.0, "p10": 25.0, "p25": 35.0,
                       "p50": 50.0, "p75": 65.0, "p90": 75.0, "p95": 80.0, "p99": 90.0},
        )
        builder = SchemaBuilder()
        schema = builder.build(profile, fit_threshold=0.80)
        gen = schema.tables["t"].columns["val"].generator
        assert gen["strategy"] == "empirical"
        assert "quantiles" in gen

    def test_parametric_strategy_when_fit_score_high(self):
        profile = self._make_profile_with_field(
            dtype="float",
            distribution="normal",
            distribution_params={"loc": 50.0, "scale": 10.0},
            fit_score=0.92,
        )
        builder = SchemaBuilder()
        schema = builder.build(profile, fit_threshold=0.80)
        gen = schema.tables["t"].columns["val"].generator
        assert gen["strategy"] == "distribution"

    def test_value_counts_ext_used_for_weighted_enum(self):
        from sqllocks_spindle.inference.profiler import (
            ColumnProfile, TableProfile, DatasetProfile
        )
        col = ColumnProfile(
            name="cat", dtype="string",
            null_count=0, null_rate=0.0,
            cardinality=3, cardinality_ratio=0.03,
            is_unique=False, is_enum=True,
            enum_values={"A": 0.6, "B": 0.3, "C": 0.1},
            min_value=None, max_value=None, mean=None, std=None,
            distribution=None, distribution_params=None, pattern=None,
            is_primary_key=False, is_foreign_key=False, fk_ref_table=None,
            value_counts_ext={"A": 0.6, "B": 0.3, "C": 0.1},
        )
        table = TableProfile(name="t", row_count=100, columns={"cat": col},
                             primary_key=[], detected_fks={})
        profile = DatasetProfile(tables={"t": table})
        schema = SchemaBuilder().build(profile)
        gen = schema.tables["t"].columns["cat"].generator
        assert gen["strategy"] == "weighted_enum"
        assert abs(gen["values"]["A"] - 0.6) < 0.01

    def test_correlated_columns_emitted_in_schema(self):
        from sqllocks_spindle.inference.profiler import (
            ColumnProfile, TableProfile, DatasetProfile
        )
        def _num_col(name):
            return ColumnProfile(
                name=name, dtype="float",
                null_count=0, null_rate=0.0,
                cardinality=100, cardinality_ratio=1.0,
                is_unique=False, is_enum=False, enum_values=None,
                min_value=0.0, max_value=100.0, mean=50.0, std=10.0,
                distribution="normal", distribution_params={"loc": 50.0, "scale": 10.0},
                pattern=None, is_primary_key=False, is_foreign_key=False, fk_ref_table=None,
                fit_score=0.95,
            )
        table = TableProfile(
            name="t", row_count=100,
            columns={"a": _num_col("a"), "b": _num_col("b")},
            primary_key=[], detected_fks={},
            correlation_matrix={"a": {"b": 0.75}, "b": {"a": 0.75}},
        )
        profile = DatasetProfile(tables={"t": table})
        schema = SchemaBuilder().build(profile, correlation_threshold=0.5)
        # correlated_columns stored in model metadata or schema extra
        assert hasattr(schema, "correlated_columns") or schema.model.extra.get("correlated_columns")

    def test_include_anomaly_registry_false_returns_single_value(self):
        from sqllocks_spindle.inference.profiler import (
            ColumnProfile, TableProfile, DatasetProfile
        )
        col = ColumnProfile(
            name="v", dtype="float",
            null_count=0, null_rate=0.0, cardinality=100, cardinality_ratio=1.0,
            is_unique=False, is_enum=False, enum_values=None,
            min_value=0.0, max_value=10.0, mean=5.0, std=1.0,
            distribution=None, distribution_params=None, pattern=None,
            is_primary_key=False, is_foreign_key=False, fk_ref_table=None,
        )
        table = TableProfile(name="t", row_count=100, columns={"v": col},
                             primary_key=[], detected_fks={})
        profile = DatasetProfile(tables={"t": table})
        result = SchemaBuilder().build(profile, include_anomaly_registry=False)
        # Single return value (not tuple)
        from sqllocks_spindle.schema.parser import SpindleSchema
        assert isinstance(result, SpindleSchema)

    def test_include_anomaly_registry_true_returns_tuple(self):
        from sqllocks_spindle.inference.profiler import (
            ColumnProfile, TableProfile, DatasetProfile
        )
        col = ColumnProfile(
            name="v", dtype="float",
            null_count=5, null_rate=0.05, cardinality=100, cardinality_ratio=1.0,
            is_unique=False, is_enum=False, enum_values=None,
            min_value=0.0, max_value=10.0, mean=5.0, std=1.0,
            distribution=None, distribution_params=None, pattern=None,
            is_primary_key=False, is_foreign_key=False, fk_ref_table=None,
            outlier_rate=0.05,
        )
        table = TableProfile(name="t", row_count=100, columns={"v": col},
                             primary_key=[], detected_fks={})
        profile = DatasetProfile(tables={"t": table})
        result = SchemaBuilder().build(profile, include_anomaly_registry=True)
        assert isinstance(result, tuple)
        schema, registry = result
        from sqllocks_spindle.schema.parser import SpindleSchema
        assert isinstance(schema, SpindleSchema)
        assert registry is not None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_inference.py::TestSchemaBuilderV2 -v 2>&1 | tail -15
```

Expected: multiple failures (missing kwargs, wrong strategy selection).

- [ ] **Step 3: Update SchemaBuilder.build() signature**

In `sqllocks_spindle/inference/schema_builder.py`, update the `build()` method signature:

```python
    def build(
        self,
        profile: DatasetProfile,
        domain_name: str = "inferred",
        fit_threshold: float = 0.80,
        correlation_threshold: float = 0.5,
        include_anomaly_registry: bool = False,
    ) -> "SpindleSchema | tuple[SpindleSchema, Any]":
```

Also add the import at the top (after existing imports):

```python
from sqllocks_spindle.streaming.anomaly import AnomalyRegistry, PointAnomaly
```

- [ ] **Step 4: Store fit_threshold and correlation_threshold on self in build()**

At the start of `build()`, before `parent_pk_map`, add:

```python
        self._fit_threshold = fit_threshold
        self._correlation_threshold = correlation_threshold
```

- [ ] **Step 5: Add correlated_columns to schema and handle anomaly registry return**

At the end of `build()`, before the `return` statement, replace the final `return SpindleSchema(...)` with:

```python
        # Collect correlated column pairs across all tables
        correlated_columns: dict[str, list[list[str | float]]] = {}
        for tname, tprofile in profile.tables.items():
            if tprofile.correlation_matrix:
                pairs = []
                seen: set[tuple[str, str]] = set()
                for col_a, row in tprofile.correlation_matrix.items():
                    for col_b, r in row.items():
                        pair_key = tuple(sorted([col_a, col_b]))
                        if abs(r) >= self._correlation_threshold and pair_key not in seen:
                            pairs.append([col_a, col_b, r])
                            seen.add(pair_key)  # type: ignore[arg-type]
                if pairs:
                    correlated_columns[tname] = pairs

        schema = SpindleSchema(
            model=model,
            tables=tables,
            relationships=relationships,
            business_rules=[],
            generation=generation,
        )
        # Attach correlated_columns as model extra metadata
        if correlated_columns:
            if not hasattr(schema.model, 'extra') or schema.model.extra is None:
                schema.model.extra = {}
            schema.model.extra["correlated_columns"] = correlated_columns

        if not include_anomaly_registry:
            return schema

        # Build suggested AnomalyRegistry from profile statistics
        registry = self._build_anomaly_registry(profile)
        return schema, registry
```

- [ ] **Step 6: Add _build_anomaly_registry method**

Add after `_build_generation_config`:

```python
    def _build_anomaly_registry(self, profile: DatasetProfile) -> AnomalyRegistry:
        """Build an AnomalyRegistry from profiled outlier rates.

        Only PointAnomaly is auto-suggested — ContextualAnomaly and
        CollectiveAnomaly require domain knowledge (condition values, group
        columns, timestamp columns) not available from a statistical profile.
        """
        registry = AnomalyRegistry()
        for tname, tprofile in profile.tables.items():
            for cname, col in tprofile.columns.items():
                if col.outlier_rate and col.outlier_rate > 0.001:
                    registry.register(
                        PointAnomaly(
                            name=f"{tname}_{cname}_outlier",
                            column=cname,
                            multiplier_range=(3.0, 10.0),
                            fraction=col.outlier_rate,
                        )
                    )
        return registry
```

- [ ] **Step 7: Update _column_to_generator with extended priority tree**

Replace the existing `_column_to_generator` method body with the new priority tree. The new version adds three changes:
1. Pattern checks for new patterns (ssn, ip_address, mac_address, iban, postal_code, currency_code, language_code) before the existing distribution check
2. Temporal + histogram support for date/datetime columns
3. Empirical strategy when `fit_score` is below threshold

Replace the full method:

```python
    def _column_to_generator(self, col: ColumnProfile, parent_pk_map: dict[str, str] | None = None) -> dict:
        """Map a ColumnProfile to a Spindle generator dict."""

        # 1. Primary key — sequence or uuid
        if col.is_primary_key:
            if col.pattern == "uuid" or col.dtype == "string":
                return {"strategy": "uuid"}
            return {
                "strategy": "sequence",
                "start": int(col.min_value) if col.min_value is not None else 1,
            }

        # 2. Foreign key
        if col.is_foreign_key and col.fk_ref_table:
            parent_pk = (parent_pk_map or {}).get(col.fk_ref_table, f"{col.fk_ref_table}_id")
            return {"strategy": "foreign_key", "ref": f"{col.fk_ref_table}.{parent_pk}"}

        # 3. UUID pattern
        if col.pattern == "uuid":
            return {"strategy": "uuid"}

        # 4. Email pattern
        if col.pattern == "email":
            return {"strategy": "faker", "provider": "email"}

        # 5. Phone pattern
        if col.pattern == "phone":
            return {"strategy": "faker", "provider": "phone_number"}

        # 6. Extended string patterns → Faker providers
        _pattern_to_faker: dict[str, str] = {
            "ssn": "ssn",
            "ip_address": "ipv4",
            "mac_address": "mac_address",
            "iban": "iban",
            "postal_code": "postcode",
            "currency_code": "currency_code",
            "language_code": "language_code",
        }
        if col.pattern in _pattern_to_faker:
            return {"strategy": "faker", "provider": _pattern_to_faker[col.pattern]}

        # 7. Date string pattern
        if col.pattern == "date":
            return {"strategy": "temporal", "type": "date"}

        # 8. Date / datetime with temporal histograms
        if col.dtype in ("date", "datetime"):
            gen: dict = {"strategy": "temporal", "type": col.dtype}
            if col.min_value is not None:
                gen["start"] = str(col.min_value)
            if col.max_value is not None:
                gen["end"] = str(col.max_value)
            if col.hour_histogram or col.dow_histogram:
                gen["pattern"] = "seasonal"
                profiles: dict = {}
                if col.hour_histogram:
                    # Convert 24-bin list → {"0": w, "1": w, ...} for TemporalStrategy
                    profiles["hour_of_day"] = {str(h): w for h, w in enumerate(col.hour_histogram)}
                if col.dow_histogram:
                    dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                    profiles["day_of_week"] = {dow_names[i]: w for i, w in enumerate(col.dow_histogram)}
                gen["profiles"] = profiles
            return gen

        # 9. Categorical / enum — prefer value_counts_ext for exact frequencies
        if col.is_enum:
            values = col.value_counts_ext if col.value_counts_ext else col.enum_values
            if values:
                return {"strategy": "weighted_enum", "values": values}

        # 10. Boolean
        if col.dtype == "boolean":
            return {"strategy": "weighted_enum", "values": {"true": 0.5, "false": 0.5}}

        # 11. Numeric with distribution fit
        if col.dtype in ("integer", "float"):
            # Check fit quality — use empirical if fit_score below threshold
            fit_threshold = getattr(self, "_fit_threshold", 0.80)
            if col.fit_score is not None and col.fit_score < fit_threshold:
                if col.quantiles:
                    return {"strategy": "empirical", "quantiles": col.quantiles}

            if col.distribution and col.distribution_params:
                return {
                    "strategy": "distribution",
                    "type": col.distribution,
                    "params": col.distribution_params,
                }

            # Numeric fallback — normal from observed stats
            params: dict = {}
            if col.mean is not None and col.std is not None:
                params = {"loc": col.mean, "scale": max(col.std, 0.01)}
            elif col.min_value is not None and col.max_value is not None:
                params = {
                    "loc": float(col.min_value),
                    "scale": float(col.max_value) - float(col.min_value),
                }
            return {"strategy": "distribution", "type": "normal", "params": params}

        # 12. String with length bounds → constrained faker
        if col.dtype == "string":
            if col.string_length:
                max_len = int(col.string_length.get("p95", col.string_length.get("max", 255)))
                provider = _guess_faker_provider(col.name)
                return {"strategy": "faker", "provider": provider, "max_length": max_len}
            provider = _guess_faker_provider(col.name)
            return {"strategy": "faker", "provider": provider}

        # 13. Ultimate fallback
        return {"strategy": "faker", "provider": "pystr"}
```

- [ ] **Step 8: Run tests to verify they pass**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_inference.py::TestSchemaBuilderV2 -v 2>&1 | tail -20
```

Expected: all 6 tests pass. If `test_correlated_columns_emitted_in_schema` fails due to `schema.model` not having `extra`, check `SpindleSchema.model` type and adjust storage (can use a simple module-level dict keyed by schema id as fallback).

- [ ] **Step 9: Run existing inference tests to confirm no regressions**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_inference.py -v 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 10: Commit**

```bash
cd projects/fabric-datagen && git add sqllocks_spindle/inference/schema_builder.py tests/test_inference.py && git commit -m "feat(inference): enhanced SchemaBuilder with empirical fallback, temporal histograms, correlated_columns, anomaly registry"
```

---

## Task 7: FidelityReport enhancements

**Files:**
- Modify: `sqllocks_spindle/inference/comparator.py`
- Create: `tests/test_fidelity_report_v2.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_fidelity_report_v2.py`:

```python
"""Tests for Phase 3B FidelityReport enhancements."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference.comparator import FidelityReport, FidelityComparator


def _make_report(real: dict[str, pd.DataFrame], synth: dict[str, pd.DataFrame]) -> FidelityReport:
    return FidelityComparator().compare(real, synth)


class TestFidelityReportV2:
    def test_failing_columns_returns_list(self):
        real = {"t": pd.DataFrame({"a": np.arange(100, dtype=float)})}
        synth = {"t": pd.DataFrame({"a": np.arange(200, 300, dtype=float)})}  # very different
        report = _make_report(real, synth)
        failing = report.failing_columns(threshold=90.0)
        assert isinstance(failing, list)

    def test_to_dict_is_serializable(self):
        import json
        real = {"t": pd.DataFrame({"x": [1.0, 2.0, 3.0]})}
        synth = {"t": pd.DataFrame({"x": [1.0, 2.0, 3.0]})}
        report = _make_report(real, synth)
        d = report.to_dict()
        assert isinstance(d, dict)
        # Must be JSON-serializable
        json.dumps(d)

    def test_to_dataframe_has_expected_columns(self):
        real = {"t": pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": ["a", "b", "c"]})}
        synth = {"t": pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": ["a", "b", "c"]})}
        report = _make_report(real, synth)
        df = report.to_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert "table" in df.columns
        assert "column" in df.columns
        assert "score" in df.columns

    def test_score_classmethod_returns_report(self):
        real = pd.DataFrame({"x": np.random.default_rng(0).normal(0, 1, 100)})
        synth = pd.DataFrame({"x": np.random.default_rng(1).normal(0, 1, 100)})
        report = FidelityReport.score(real, synth)
        assert isinstance(report, FidelityReport)
        assert 0.0 <= report.overall_score <= 100.0

    def test_score_classmethod_accepts_table_name(self):
        real = pd.DataFrame({"v": [1.0, 2.0, 3.0]})
        synth = pd.DataFrame({"v": [1.0, 2.0, 3.0]})
        report = FidelityReport.score(real, synth, table_name="my_table")
        assert "my_table" in report.tables

    def test_perfect_match_scores_high(self):
        df = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": ["x", "y", "z"]})
        report = FidelityReport.score(df, df.copy())
        assert report.overall_score >= 90.0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_fidelity_report_v2.py -v 2>&1 | tail -10
```

Expected: `AttributeError: type object 'FidelityReport' has no attribute 'score'`

- [ ] **Step 3: Add failing_columns, to_dict, to_dataframe, and score() to FidelityReport**

In `sqllocks_spindle/inference/comparator.py`, extend the `FidelityReport` dataclass with new methods. Add after the `to_markdown` method:

```python
    def failing_columns(self, threshold: float = 85.0) -> list[tuple[str, str, float]]:
        """Return (table, column, score) tuples for columns below threshold."""
        failing = []
        for table_name, tf in self.tables.items():
            for col_name, cf in tf.columns.items():
                if cf.score < threshold:
                    failing.append((table_name, col_name, cf.score))
        return sorted(failing, key=lambda x: x[2])  # lowest score first

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict representation."""
        return {
            "overall_score": round(self.overall_score, 2),
            "tables": {
                tname: {
                    "score": round(tf.score, 2),
                    "row_count_real": tf.row_count_real,
                    "row_count_synth": tf.row_count_synth,
                    "columns": {
                        cname: {
                            "score": round(cf.score, 2),
                            "dtype_match": cf.dtype_match,
                            "null_rate_delta": round(cf.null_rate_delta, 4),
                            "cardinality_ratio": round(cf.cardinality_ratio, 4),
                            "ks_statistic": round(cf.ks_statistic, 4) if cf.ks_statistic is not None else None,
                            "value_overlap": round(cf.value_overlap, 4) if cf.value_overlap is not None else None,
                        }
                        for cname, cf in tf.columns.items()
                    },
                }
                for tname, tf in self.tables.items()
            },
        }

    def to_dataframe(self) -> "pd.DataFrame":
        """Return a flat pandas DataFrame with one row per column."""
        import pandas as _pd
        rows = []
        for tname, tf in self.tables.items():
            for cname, cf in tf.columns.items():
                rows.append({
                    "table": tname,
                    "column": cname,
                    "score": round(cf.score, 2),
                    "dtype_match": cf.dtype_match,
                    "null_rate_delta": round(cf.null_rate_delta, 4),
                    "cardinality_ratio": round(cf.cardinality_ratio, 4),
                    "ks_statistic": cf.ks_statistic,
                    "chi2_statistic": cf.chi2_statistic,
                    "value_overlap": cf.value_overlap,
                })
        return _pd.DataFrame(rows)

    @classmethod
    def score(
        cls,
        real: "pd.DataFrame",
        synthetic: "pd.DataFrame",
        table_name: str = "table",
        threshold: float = 85.0,
    ) -> "FidelityReport":
        """Compare two DataFrames and return a FidelityReport.

        Convenience classmethod for single-table comparison without needing
        to instantiate FidelityComparator directly.

        Args:
            real: The real (source) DataFrame.
            synthetic: The generated (synthetic) DataFrame.
            table_name: Name to use for this table in the report.
            threshold: Score below which columns are considered failing.
        """
        comparator = FidelityComparator()
        return comparator.compare(
            {table_name: real},
            {table_name: synthetic},
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_fidelity_report_v2.py -v 2>&1 | tail -15
```

Expected: all 6 tests pass.

- [ ] **Step 5: Run existing comparator tests to confirm no regressions**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_comparator.py -v 2>&1 | tail -10
```

Expected: all existing tests pass.

- [ ] **Step 6: Commit**

```bash
cd projects/fabric-datagen && git add sqllocks_spindle/inference/comparator.py tests/test_fidelity_report_v2.py && git commit -m "feat(inference): extend FidelityReport with score(), failing_columns(), to_dict(), to_dataframe()"
```

---

## Task 8: Spindle.generate() integration

**Files:**
- Modify: `sqllocks_spindle/engine/generator.py`
- Test: `tests/test_e2e_generation.py` (append)

Add `enforce_correlations=True` and `fidelity_profile=None` kwargs to `Spindle.generate()`. When `enforce_correlations=True` and the schema's model has `correlated_columns`, apply `GaussianCopula` after each table. When `fidelity_profile` is a `DatasetProfile`, compute a `FidelityReport` and return `(GenerationResult, FidelityReport)`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_e2e_generation.py`:

```python
class TestGeneratePhase3BIntegration:
    """Phase 3B: enforce_correlations and fidelity_profile kwargs."""

    def _simple_schema(self) -> dict:
        return {
            "model": {"name": "test", "domain": "test"},
            "tables": {
                "t": {
                    "columns": {
                        "id": {"type": "integer", "generator": {"strategy": "sequence", "start": 1}},
                        "val": {"type": "decimal", "generator": {"strategy": "distribution", "type": "normal", "params": {"loc": 0.0, "scale": 1.0}}},
                    },
                    "primary_key": ["id"],
                }
            },
            "generation": {"scale": "small", "scales": {"small": {"t": 100}}},
        }

    def test_generate_enforce_correlations_false_still_works(self):
        from sqllocks_spindle import Spindle
        s = Spindle()
        result = s.generate(schema=self._simple_schema(), enforce_correlations=False)
        assert "t" in result.tables
        assert len(result.tables["t"]) == 100

    def test_generate_fidelity_profile_none_returns_result(self):
        from sqllocks_spindle import Spindle
        from sqllocks_spindle.engine.generator import GenerationResult
        s = Spindle()
        result = s.generate(schema=self._simple_schema(), fidelity_profile=None)
        assert isinstance(result, GenerationResult)

    def test_generate_with_fidelity_profile_returns_tuple(self):
        import numpy as np
        from sqllocks_spindle import Spindle
        from sqllocks_spindle.inference.comparator import FidelityReport
        from sqllocks_spindle.inference.profiler import (
            ColumnProfile, TableProfile, DatasetProfile
        )
        # Build a minimal DatasetProfile for table "t"
        col_id = ColumnProfile(
            name="id", dtype="integer",
            null_count=0, null_rate=0.0, cardinality=100, cardinality_ratio=1.0,
            is_unique=True, is_enum=False, enum_values=None,
            min_value=1, max_value=100, mean=50.0, std=28.9,
            distribution=None, distribution_params=None, pattern=None,
            is_primary_key=True, is_foreign_key=False, fk_ref_table=None,
        )
        col_val = ColumnProfile(
            name="val", dtype="float",
            null_count=0, null_rate=0.0, cardinality=100, cardinality_ratio=1.0,
            is_unique=False, is_enum=False, enum_values=None,
            min_value=-3.0, max_value=3.0, mean=0.0, std=1.0,
            distribution="normal", distribution_params={"loc": 0.0, "scale": 1.0},
            pattern=None, is_primary_key=False, is_foreign_key=False, fk_ref_table=None,
        )
        table = TableProfile(name="t", row_count=100,
                             columns={"id": col_id, "val": col_val},
                             primary_key=["id"], detected_fks={})
        fidelity_profile = DatasetProfile(tables={"t": table})

        s = Spindle()
        result = s.generate(schema=self._simple_schema(), fidelity_profile=fidelity_profile)
        assert isinstance(result, tuple)
        generation_result, report = result
        assert isinstance(report, FidelityReport)
        assert "t" in report.tables
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_e2e_generation.py::TestGeneratePhase3BIntegration -v 2>&1 | tail -10
```

Expected: `TypeError: generate() got an unexpected keyword argument 'enforce_correlations'`

- [ ] **Step 3: Add imports to generator.py**

In `sqllocks_spindle/engine/generator.py`, add after the existing imports:

```python
from sqllocks_spindle.engine.correlation import GaussianCopula
```

- [ ] **Step 4: Add kwargs to generate() signature**

Update the `generate()` method signature (at line 352) to add:

```python
    def generate(
        self,
        domain=None,
        schema: str | Path | dict | SpindleSchema | None = None,
        scale: str | None = None,
        scale_overrides: dict[str, int] | None = None,
        seed: int | None = None,
        on_progress: Callable[[str, int, int], None] | None = None,
        enforce_correlations: bool = True,
        fidelity_profile=None,
    ) -> "GenerationResult | tuple[GenerationResult, Any]":
```

- [ ] **Step 5: Apply GaussianCopula and FidelityReport after generation**

At the end of `generate()`, just before the final `return result` statement, add:

```python
        # Apply correlation enforcement if requested and schema has correlated_columns
        if enforce_correlations:
            corr_data = (
                result.schema.model.extra.get("correlated_columns", {})
                if (hasattr(result.schema.model, "extra") and result.schema.model.extra)
                else {}
            )
            for tname, pairs in corr_data.items():
                if tname in result.tables and pairs:
                    corr_matrix: dict[str, dict[str, float]] = {}
                    for pair in pairs:
                        col_a, col_b, r = pair[0], pair[1], pair[2]
                        corr_matrix.setdefault(col_a, {})[col_b] = r
                        corr_matrix.setdefault(col_b, {})[col_a] = r
                    copula = GaussianCopula(corr_matrix)
                    result.tables[tname] = copula.apply(result.tables[tname])

        # Compute fidelity report if profile provided
        if fidelity_profile is not None:
            try:
                from sqllocks_spindle.inference.comparator import FidelityComparator
                comparator = FidelityComparator()
                fidelity_report = comparator.compare(
                    {tname: tprofile for tname, tprofile in fidelity_profile.tables.items()
                     if hasattr(tprofile, 'row_count')},
                    result.tables,
                )
                return result, fidelity_report
            except Exception as exc:
                logger.warning("FidelityReport computation failed: %s", exc)

        return result
```

Note: `FidelityComparator.compare()` expects `dict[str, pd.DataFrame]` for both real and synthetic. For the `real` argument, we need to reconstruct DataFrames from the profile — but profiles don't store raw data. Instead, pass the profiled statistics as a reference. The correct approach: the `fidelity_profile` is a `DatasetProfile`, and we compare `result.tables` (synthetic) against... we don't have the original real DataFrames. We need to change the design slightly: `fidelity_profile` must include the original real data OR we skip the real-vs-synthetic comparison and do a profile-vs-generated comparison.

**Revised approach**: Since `DatasetProfile` doesn't store the raw data (just statistics), use a simplified profile-aware comparison. Replace the fidelity computation block with:

```python
        # Compute fidelity report if profile provided
        if fidelity_profile is not None:
            try:
                from sqllocks_spindle.inference.comparator import FidelityComparator, FidelityReport, TableFidelity, ColumnFidelity
                import numpy as np as _np
                # Build synthetic-only report by comparing generated data to profile statistics
                tables_fidelity: dict = {}
                for tname, tprofile in fidelity_profile.tables.items():
                    if tname not in result.tables:
                        continue
                    synth_df = result.tables[tname]
                    col_fidelities: dict = {}
                    for cname, col_prof in tprofile.columns.items():
                        if cname not in synth_df.columns:
                            continue
                        synth_col = synth_df[cname]
                        null_rate = float(synth_col.isna().mean())
                        null_score = 1.0 - abs(null_rate - col_prof.null_rate)
                        cardinality = synth_col.dropna().nunique()
                        card_ratio = cardinality / max(col_prof.cardinality, 1)
                        card_score = max(0.0, 1.0 - abs(1.0 - card_ratio))
                        score = ((null_score + card_score) / 2.0) * 100.0
                        col_fidelities[cname] = ColumnFidelity(
                            column_name=cname,
                            dtype_match=True,
                            null_rate_delta=abs(null_rate - col_prof.null_rate),
                            cardinality_ratio=float(card_ratio),
                            mean_delta=None, std_ratio=None,
                            ks_statistic=None, ks_pvalue=None,
                            chi2_statistic=None, chi2_pvalue=None,
                            value_overlap=None,
                            score=float(score),
                        )
                    table_score = float(_np.mean([c.score for c in col_fidelities.values()])) if col_fidelities else 0.0
                    tables_fidelity[tname] = TableFidelity(
                        table_name=tname,
                        row_count_real=tprofile.row_count,
                        row_count_synth=len(synth_df),
                        columns=col_fidelities,
                        score=table_score,
                    )
                overall = float(_np.mean([t.score for t in tables_fidelity.values()])) if tables_fidelity else 0.0
                fidelity_report = FidelityReport(tables=tables_fidelity, overall_score=overall)
                return result, fidelity_report
            except Exception as exc:
                logger.warning("FidelityReport computation failed: %s", exc)

        return result
```

(Fix the `import numpy as np as _np` syntax — that was an error. Use `import numpy as _np` or just `np` which is already imported.)

The correct final block to paste into generator.py (replacing the fidelity computation above) is:

```python
        # Compute fidelity report if profile provided
        if fidelity_profile is not None:
            try:
                from sqllocks_spindle.inference.comparator import (
                    FidelityReport, TableFidelity, ColumnFidelity,
                )
                tables_fidelity: dict = {}
                for tname, tprofile in fidelity_profile.tables.items():
                    if tname not in result.tables:
                        continue
                    synth_df = result.tables[tname]
                    col_fidelities: dict = {}
                    for cname, col_prof in tprofile.columns.items():
                        if cname not in synth_df.columns:
                            continue
                        synth_col = synth_df[cname]
                        null_rate = float(synth_col.isna().mean())
                        null_score = 1.0 - abs(null_rate - col_prof.null_rate)
                        cardinality = int(synth_col.dropna().nunique())
                        card_ratio = cardinality / max(col_prof.cardinality, 1)
                        card_score = max(0.0, 1.0 - abs(1.0 - card_ratio))
                        score = ((null_score + card_score) / 2.0) * 100.0
                        col_fidelities[cname] = ColumnFidelity(
                            column_name=cname,
                            dtype_match=True,
                            null_rate_delta=abs(null_rate - col_prof.null_rate),
                            cardinality_ratio=float(card_ratio),
                            mean_delta=None, std_ratio=None,
                            ks_statistic=None, ks_pvalue=None,
                            chi2_statistic=None, chi2_pvalue=None,
                            value_overlap=None,
                            score=float(score),
                        )
                    table_score = (
                        float(np.mean([c.score for c in col_fidelities.values()]))
                        if col_fidelities else 0.0
                    )
                    tables_fidelity[tname] = TableFidelity(
                        table_name=tname,
                        row_count_real=tprofile.row_count,
                        row_count_synth=len(synth_df),
                        columns=col_fidelities,
                        score=table_score,
                    )
                overall = (
                    float(np.mean([t.score for t in tables_fidelity.values()]))
                    if tables_fidelity else 0.0
                )
                fidelity_report = FidelityReport(
                    tables=tables_fidelity, overall_score=overall
                )
                return result, fidelity_report
            except Exception as exc:
                logger.warning("FidelityReport computation failed: %s", exc)

        return result
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_e2e_generation.py::TestGeneratePhase3BIntegration -v 2>&1 | tail -15
```

Expected: all 3 tests pass.

- [ ] **Step 7: Run full generation E2E tests to confirm no regressions**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_e2e_generation.py -v 2>&1 | tail -15
```

Expected: all existing tests pass.

- [ ] **Step 8: Commit**

```bash
cd projects/fabric-datagen && git add sqllocks_spindle/engine/generator.py tests/test_e2e_generation.py && git commit -m "feat(engine): add enforce_correlations and fidelity_profile kwargs to Spindle.generate()"
```

---

## Task 9: LakehouseProfiler

**Files:**
- Create: `sqllocks_spindle/inference/lakehouse_profiler.py`
- Modify: `sqllocks_spindle/inference/__init__.py`
- Create: `tests/test_lakehouse_profiler.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_lakehouse_profiler.py`:

```python
"""Tests for LakehouseProfiler (unit tests using mocks — no live Fabric connection)."""

from __future__ import annotations

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch


class TestLakehouseProfilerUnit:
    def test_import_succeeds(self):
        from sqllocks_spindle.inference import LakehouseProfiler
        assert LakehouseProfiler is not None

    def test_constructor_stores_ids(self):
        from sqllocks_spindle.inference import LakehouseProfiler
        lp = LakehouseProfiler(workspace_id="ws-123", lakehouse_id="lh-456")
        assert lp.workspace_id == "ws-123"
        assert lp.lakehouse_id == "lh-456"

    def test_constructor_default_sample_rows(self):
        from sqllocks_spindle.inference import LakehouseProfiler
        lp = LakehouseProfiler(workspace_id="ws", lakehouse_id="lh")
        assert lp.default_sample_rows == 100_000

    def test_profile_table_with_mock_df(self):
        from sqllocks_spindle.inference import LakehouseProfiler
        from sqllocks_spindle.inference.profiler import TableProfile

        lp = LakehouseProfiler(workspace_id="ws", lakehouse_id="lh")
        mock_df = pd.DataFrame({"id": range(10), "name": [f"u{i}" for i in range(10)]})

        with patch.object(lp, "_read_table", return_value=mock_df):
            profile = lp.profile_table("users")

        assert isinstance(profile, TableProfile)
        assert profile.name == "users"
        assert "id" in profile.columns

    def test_profile_all_returns_dict(self):
        from sqllocks_spindle.inference import LakehouseProfiler

        lp = LakehouseProfiler(workspace_id="ws", lakehouse_id="lh")
        mock_df = pd.DataFrame({"x": [1, 2, 3]})

        with patch.object(lp, "_list_tables", return_value=["t1", "t2"]), \
             patch.object(lp, "_read_table", return_value=mock_df):
            profiles = lp.profile_all()

        assert set(profiles.keys()) == {"t1", "t2"}

    def test_read_table_raises_helpful_error_without_deltalake(self):
        from sqllocks_spindle.inference import LakehouseProfiler
        lp = LakehouseProfiler(workspace_id="ws", lakehouse_id="lh")

        with patch.dict("sys.modules", {"deltalake": None}):
            with pytest.raises((ImportError, RuntimeError)):
                lp._read_table("nonexistent_table")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_lakehouse_profiler.py -v 2>&1 | tail -10
```

Expected: `ImportError: cannot import name 'LakehouseProfiler' from 'sqllocks_spindle.inference'`

- [ ] **Step 3: Create lakehouse_profiler.py**

Create `sqllocks_spindle/inference/lakehouse_profiler.py`:

```python
"""LakehouseProfiler — profile Fabric Lakehouse tables without a Spark session.

Uses the `deltalake` library (part of the [fabric] extra) to read Delta tables
locally via ABFSS. Falls back to a REST API for table listing when deltalake
is unavailable.

Requires: sqllocks-spindle[fabric] — deltalake>=0.17.0, pyarrow>=14.0
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from sqllocks_spindle.inference.profiler import DataProfiler, TableProfile

logger = logging.getLogger(__name__)

try:
    import deltalake as _deltalake
    HAS_DELTALAKE = True
except ImportError:
    HAS_DELTALAKE = False

try:
    from azure.identity import DefaultAzureCredential as _DefaultAzureCredential
    HAS_AZURE_IDENTITY = True
except ImportError:
    HAS_AZURE_IDENTITY = False


class LakehouseProfiler:
    """Profile Fabric Lakehouse Delta tables and return TableProfile objects.

    Args:
        workspace_id: Fabric workspace GUID.
        lakehouse_id: Fabric lakehouse GUID.
        token_provider: A callable returning an Azure access token string.
            Defaults to DefaultAzureCredential when azure-identity is installed.
        default_sample_rows: Row limit for profiling. Pass None to scan entire table.
    """

    def __init__(
        self,
        workspace_id: str,
        lakehouse_id: str,
        token_provider: Any | None = None,
        default_sample_rows: int | None = 100_000,
    ):
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.token_provider = token_provider
        self.default_sample_rows = default_sample_rows

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def profile_table(
        self,
        table_name: str,
        sample_rows: int | None | str = "default",
    ) -> TableProfile:
        """Profile a single Delta table.

        Args:
            table_name: Table name as it appears in the Lakehouse /Tables/ directory.
            sample_rows: Row limit. ``"default"`` uses ``self.default_sample_rows``,
                ``None`` scans the full table, or an integer for an explicit limit.
        """
        if sample_rows == "default":
            sample_rows = self.default_sample_rows

        df = self._read_table(table_name, sample_rows=sample_rows)
        profiler = DataProfiler(sample_rows=None)  # sampling already applied by _read_table
        return profiler.profile(df, table_name=table_name)

    def profile_all(
        self,
        sample_rows: int | None | str = "default",
    ) -> dict[str, TableProfile]:
        """Profile all tables in the lakehouse.

        Returns:
            Dict mapping table name → TableProfile.
        """
        table_names = self._list_tables()
        profiles: dict[str, TableProfile] = {}
        for tname in table_names:
            try:
                profiles[tname] = self.profile_table(tname, sample_rows=sample_rows)
            except Exception as exc:
                logger.warning("Skipping table '%s': %s", tname, exc)
        return profiles

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _abfss_tables_root(self) -> str:
        return (
            f"abfss://{self.workspace_id}"
            f"@onelake.dfs.fabric.microsoft.com"
            f"/{self.lakehouse_id}/Tables"
        )

    def _get_token(self) -> str | None:
        if self.token_provider is not None:
            return self.token_provider()
        if HAS_AZURE_IDENTITY:
            cred = _DefaultAzureCredential()
            token = cred.get_token("https://storage.azure.com/.default")
            return token.token
        return None

    def _storage_options(self) -> dict[str, str]:
        token = self._get_token()
        if token:
            return {"bearer_token": token, "use_emulator": "false"}
        return {}

    def _read_table(
        self,
        table_name: str,
        sample_rows: int | None = None,
    ) -> pd.DataFrame:
        """Read a Delta table into a pandas DataFrame.

        Raises:
            ImportError: If deltalake is not installed.
            RuntimeError: If the table cannot be read.
        """
        if not HAS_DELTALAKE:
            raise ImportError(
                "LakehouseProfiler requires 'deltalake'. "
                "Install with: pip install 'sqllocks-spindle[fabric-inference]'"
            )

        table_uri = f"{self._abfss_tables_root()}/{table_name}"
        storage_options = self._storage_options()

        try:
            dt = _deltalake.DeltaTable(table_uri, storage_options=storage_options)
            if sample_rows is not None:
                # Read only enough data using limit pushdown where supported
                df = dt.to_pandas(limit=sample_rows)
            else:
                df = dt.to_pandas()
            return df
        except Exception as exc:
            raise RuntimeError(
                f"Failed to read table '{table_name}' from "
                f"{table_uri}: {exc}"
            ) from exc

    def _list_tables(self) -> list[str]:
        """List table names in the lakehouse.

        Tries to list Delta tables from the ABFSS path. Falls back to empty
        list if deltalake or auth is unavailable.
        """
        if not HAS_DELTALAKE:
            logger.warning(
                "deltalake not installed — cannot list lakehouse tables. "
                "Install with: pip install 'sqllocks-spindle[fabric-inference]'"
            )
            return []

        root = self._abfss_tables_root()
        storage_options = self._storage_options()

        try:
            from deltalake import DeltaTable
            # List by attempting to discover tables in the root container
            # deltalake doesn't have a native "list tables" API — use filesystem listing
            from pyarrow import fs as _fs
            account = f"{self.workspace_id}@onelake.dfs.fabric.microsoft.com"
            token = self._get_token()
            az_fs = _fs.AzureFileSystem(account=account, credential=token)
            file_info = az_fs.get_file_info(
                _fs.FileSelector(f"{self.lakehouse_id}/Tables", recursive=False)
            )
            return [fi.base_name for fi in file_info if fi.type.name == "Directory"]
        except Exception as exc:
            logger.warning("Could not list lakehouse tables: %s", exc)
            return []
```

- [ ] **Step 4: Export LakehouseProfiler from inference __init__.py**

In `sqllocks_spindle/inference/__init__.py`, add after the `ProfileIO` import line:

```python
from sqllocks_spindle.inference.lakehouse_profiler import LakehouseProfiler
```

And add `"LakehouseProfiler"` to `__all__`.

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_lakehouse_profiler.py -v 2>&1 | tail -15
```

Expected: all 6 tests pass.

- [ ] **Step 6: Commit**

```bash
cd projects/fabric-datagen && git add sqllocks_spindle/inference/lakehouse_profiler.py sqllocks_spindle/inference/__init__.py tests/test_lakehouse_profiler.py && git commit -m "feat(inference): add LakehouseProfiler for Fabric Delta table profiling"
```

---

## Task 10: Packaging and version bump

**Files:**
- Modify: `pyproject.toml`
- Modify: `sqllocks_spindle/__init__.py`

- [ ] **Step 1: Write a version smoke test**

Append to `tests/test_inference.py`:

```python
class TestPackagingV29:
    def test_version_is_290(self):
        import sqllocks_spindle
        assert sqllocks_spindle.__version__ == "2.9.0"

    def test_lakehouse_profiler_importable_from_top(self):
        from sqllocks_spindle.inference import LakehouseProfiler
        assert LakehouseProfiler is not None
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_inference.py::TestPackagingV29::test_version_is_290 -v 2>&1 | tail -5
```

Expected: `AssertionError: assert '2.8.x' == '2.9.0'` (or whatever the current version is).

- [ ] **Step 3: Update pyproject.toml**

In `pyproject.toml`:

1. Change `version = "2.7.8"` → `version = "2.9.0"`

2. After the existing `inference` extra, add `fabric-inference`:
```toml
fabric-inference = [
    "scipy>=1.11",
    "deltalake>=0.17.0",
    "pyarrow>=14.0",
]
```

3. Add `"sqllocks-spindle[fabric-inference]"` to the `all` extra list (or add the three packages individually if they aren't already present).

The complete updated `[project.optional-dependencies]` block should include:

```toml
inference = [
    "scipy>=1.11",
]
fabric-inference = [
    "scipy>=1.11",
    "deltalake>=0.17.0",
    "pyarrow>=14.0",
]
```

- [ ] **Step 4: Update __init__.py**

In `sqllocks_spindle/__init__.py`:

1. Change `__version__ = "2.7.8"` → `__version__ = "2.9.0"`

2. Add `LakehouseProfiler` to the lazy imports block or eager imports from inference:
```python
# After the existing inference import block:
try:
    from sqllocks_spindle.inference import LakehouseProfiler
except ImportError:
    pass
```

3. Add `"LakehouseProfiler"` to `__all__`.

Also update the notebook cell in `notebooks/spindle_spark_worker.ipynb` and `sqllocks_spindle/notebooks/spindle_spark_worker.ipynb` — change the pip install line from `==2.7.8` to `==2.9.0`. (This is a JSON file; use Read + Edit carefully — do NOT use NotebookEdit for .ipynb files.)

- [ ] **Step 5: Run version smoke test to verify it passes**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_inference.py::TestPackagingV29 -v 2>&1 | tail -10
```

Expected: both tests pass.

- [ ] **Step 6: Run full inference test suite**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/test_inference.py tests/test_empirical_strategy.py tests/test_correlation.py tests/test_fidelity_report_v2.py tests/test_lakehouse_profiler.py -v 2>&1 | tail -25
```

Expected: all tests pass.

- [ ] **Step 7: Run broader regression check**

```bash
cd projects/fabric-datagen && source .venv-mac/bin/activate && pytest tests/ -x --ignore=tests/test_e2e_notebooks.py --ignore=tests/test_spark_router.py -q 2>&1 | tail -20
```

Expected: no new failures.

- [ ] **Step 8: Commit**

```bash
cd projects/fabric-datagen && git add pyproject.toml sqllocks_spindle/__init__.py tests/test_inference.py && git commit -m "chore: bump version to 2.9.0, add fabric-inference extra"
```

---

## Self-Review Checklist

After all tasks are complete, verify:

- [ ] All 10 tasks have passing tests before commit
- [ ] `pytest tests/test_inference.py` — all pass (no regressions to existing profiler/schema_builder tests)
- [ ] `pytest tests/test_e2e_generation.py` — all pass (no regressions to Spindle.generate())
- [ ] `pytest tests/test_comparator.py` — all pass (no regressions to FidelityReport)
- [ ] `ColumnProfile` still works without any of the new optional fields (backward compat)
- [ ] `SchemaBuilder.build(profile)` still works with no new kwargs (all default)
- [ ] `Spindle.generate(schema)` still returns `GenerationResult` (not tuple) when `fidelity_profile=None`
- [ ] Version in `pyproject.toml` matches `__init__.py` matches notebook cell

