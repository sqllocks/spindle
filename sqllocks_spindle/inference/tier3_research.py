"""Tier 3 research-grade fidelity features.

Experimental features that raise the synthetic data quality ceiling:

- ChowLiuNetwork      — Bayesian network structure learning via Chow-Liu algorithm
- DifferentialPrivacy  — Laplace/Gaussian noise injection for (ε,δ)-DP
- DriftMonitor         — Statistical drift detection between two DataFrames
- BootstrapMode        — Bootstrap-based synthetic generation from real data
- CTGANWrapper         — Optional wrapper around sdv/ctgan when installed

All features fail gracefully when optional dependencies (sdv, sklearn) are absent.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

try:
    from scipy import stats as sp_stats
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

try:
    from sklearn.preprocessing import LabelEncoder
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False


# ---------------------------------------------------------------------------
# Chow-Liu Bayesian Network
# ---------------------------------------------------------------------------


@dataclass
class BayesianEdge:
    """A directed edge in the Chow-Liu tree."""
    parent: str
    child: str
    mutual_information: float


@dataclass
class ChowLiuResult:
    """Result of Chow-Liu tree structure learning."""
    edges: list[BayesianEdge]
    column_order: list[str]
    mutual_info_matrix: dict[str, dict[str, float]]


class ChowLiuNetwork:
    """Learn a Bayesian network tree structure using the Chow-Liu algorithm.

    Computes pairwise mutual information between columns and finds the
    maximum spanning tree — the tree that best represents the joint distribution.

    This is the theoretical backbone of synthetic data that preserves
    inter-column dependencies.
    """

    def __init__(self, n_bins: int = 10, sample_size: int = 2000) -> None:
        self.n_bins = n_bins
        self.sample_size = sample_size

    def fit(self, df: pd.DataFrame) -> ChowLiuResult:
        """Learn the Chow-Liu tree from a DataFrame."""
        df_sample = df.head(self.sample_size) if len(df) > self.sample_size else df
        encoded = self._encode(df_sample)
        columns = list(encoded.columns)
        n = len(columns)

        # Compute pairwise mutual information
        mi_matrix: dict[str, dict[str, float]] = {c: {} for c in columns}
        for i in range(n):
            for j in range(i + 1, n):
                ci, cj = columns[i], columns[j]
                mi = self._mutual_information(encoded[ci], encoded[cj])
                mi_matrix[ci][cj] = mi
                mi_matrix[cj][ci] = mi

        # Kruskal's algorithm for maximum spanning tree
        edges = self._max_spanning_tree(columns, mi_matrix)
        return ChowLiuResult(
            edges=edges,
            column_order=columns,
            mutual_info_matrix=mi_matrix,
        )

    def _encode(self, df: pd.DataFrame) -> pd.DataFrame:
        """Encode all columns to discrete integer bins."""
        result: dict[str, pd.Series] = {}
        for col in df.columns:
            series = df[col].copy()
            if pd.api.types.is_numeric_dtype(series):
                series = series.fillna(series.median() if not series.empty else 0)
                result[col] = pd.cut(series, bins=self.n_bins, labels=False).fillna(0).astype(int)
            elif pd.api.types.is_datetime64_any_dtype(series):
                result[col] = pd.cut(
                    series.astype(np.int64), bins=self.n_bins, labels=False
                ).fillna(0).astype(int)
            else:
                if HAS_SKLEARN:
                    le = LabelEncoder()
                    result[col] = pd.Series(
                        le.fit_transform(series.fillna("__NULL__").astype(str)),
                        index=df.index
                    )
                else:
                    result[col] = series.fillna("__NULL__").astype("category").cat.codes
        return pd.DataFrame(result)

    def _mutual_information(self, x: pd.Series, y: pd.Series) -> float:
        """Compute mutual information between two discrete series."""
        try:
            xy = pd.crosstab(x, y, normalize=True)
            px = xy.sum(axis=1).values
            py = xy.sum(axis=0).values
            mi = 0.0
            for i, xi in enumerate(xy.index):
                for j, yj in enumerate(xy.columns):
                    pxy = xy.loc[xi, yj]
                    if pxy > 0:
                        mi += pxy * np.log(pxy / (px[i] * py[j] + 1e-12) + 1e-12)
            return float(max(0.0, mi))
        except Exception:
            return 0.0

    def _max_spanning_tree(
        self, columns: list[str], mi: dict[str, dict[str, float]]
    ) -> list[BayesianEdge]:
        """Kruskal's algorithm for maximum spanning tree."""
        if len(columns) < 2:
            return []

        # Gather all edges sorted by MI descending
        all_edges: list[tuple[float, str, str]] = []
        for i in range(len(columns)):
            for j in range(i + 1, len(columns)):
                ci, cj = columns[i], columns[j]
                all_edges.append((mi[ci].get(cj, 0.0), ci, cj))
        all_edges.sort(reverse=True)

        # Union-Find
        parent = {c: c for c in columns}

        def find(x: str) -> str:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x: str, y: str) -> bool:
            rx, ry = find(x), find(y)
            if rx == ry:
                return False
            parent[rx] = ry
            return True

        edges: list[BayesianEdge] = []
        for mi_val, ci, cj in all_edges:
            if union(ci, cj):
                edges.append(BayesianEdge(parent=ci, child=cj, mutual_information=mi_val))
            if len(edges) == len(columns) - 1:
                break
        return edges


# ---------------------------------------------------------------------------
# Differential Privacy
# ---------------------------------------------------------------------------


@dataclass
class DPResult:
    """Result of applying differential privacy noise."""

    epsilon: float
    mechanism: str  # "laplace" or "gaussian"
    columns_noised: list[str]
    actual_sensitivity: dict[str, float]  # L1 sensitivity per column


class DifferentialPrivacy:
    """Apply Laplace or Gaussian noise to achieve (ε,δ)-differential privacy.

    For synthetic data, this adds calibrated noise to numeric columns
    proportional to their sensitivity / ε, ensuring individual records
    cannot be re-identified.
    """

    def __init__(
        self,
        epsilon: float = 1.0,
        delta: float = 1e-5,
        mechanism: str = "laplace",
        clip_to_range: bool = True,
    ) -> None:
        if mechanism not in ("laplace", "gaussian"):
            raise ValueError("mechanism must be 'laplace' or 'gaussian'")
        self.epsilon = epsilon
        self.delta = delta
        self.mechanism = mechanism
        self.clip_to_range = clip_to_range

    def apply(self, df: pd.DataFrame, rng: np.random.Generator | None = None) -> tuple[pd.DataFrame, DPResult]:
        """Apply differential privacy noise to all numeric columns.

        Returns (noised_df, DPResult).
        """
        if rng is None:
            rng = np.random.default_rng(0)
        df_out = df.copy()
        columns_noised: list[str] = []
        sensitivity: dict[str, float] = {}

        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            series = df[col].dropna()
            if series.empty:
                continue
            col_min = float(series.min())
            col_max = float(series.max())
            col_range = col_max - col_min
            if col_range == 0:
                continue

            # L1 sensitivity = range of the column
            l1_sensitivity = col_range
            sensitivity[col] = l1_sensitivity

            if self.mechanism == "laplace":
                scale = l1_sensitivity / self.epsilon
                noise = rng.laplace(0, scale, size=len(df))
            else:
                # Gaussian: σ = sensitivity * sqrt(2 * ln(1.25/δ)) / ε
                sigma = l1_sensitivity * np.sqrt(2 * np.log(1.25 / self.delta)) / self.epsilon
                noise = rng.normal(0, sigma, size=len(df))

            noised = df_out[col] + noise
            if self.clip_to_range:
                noised = noised.clip(col_min, col_max)
            df_out[col] = noised
            columns_noised.append(col)

        return df_out, DPResult(
            epsilon=self.epsilon,
            mechanism=self.mechanism,
            columns_noised=columns_noised,
            actual_sensitivity=sensitivity,
        )


# ---------------------------------------------------------------------------
# Drift Monitor
# ---------------------------------------------------------------------------


@dataclass
class ColumnDriftResult:
    """Drift result for a single column."""
    column: str
    drift_score: float  # 0 = no drift, 1 = complete drift
    test_statistic: float | None
    p_value: float | None
    is_drifted: bool
    method: str  # "ks", "chi2", "psi"


@dataclass
class DriftReport:
    """Drift report comparing a reference and current DataFrame."""
    columns: dict[str, ColumnDriftResult]
    drifted_columns: list[str]
    drift_fraction: float  # fraction of columns drifted
    overall_drift_score: float  # weighted average drift score


def _population_stability_index(expected: pd.Series, actual: pd.Series, n_bins: int = 10) -> float:
    """Compute PSI between expected and actual numeric distributions."""
    mn = min(expected.min(), actual.min())
    mx = max(expected.max(), actual.max())
    if mn == mx:
        return 0.0
    bins = np.linspace(mn, mx, n_bins + 1)
    exp_hist, _ = np.histogram(expected, bins=bins, density=False)
    act_hist, _ = np.histogram(actual, bins=bins, density=False)
    exp_pct = exp_hist / len(expected) + 1e-10
    act_pct = act_hist / len(actual) + 1e-10
    return float(np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct)))


class DriftMonitor:
    """Detect statistical drift between reference and current DataFrames.

    Uses KS test for numeric columns, Chi-squared for categoricals,
    and PSI as a supplementary signal.
    """

    def __init__(
        self,
        pvalue_threshold: float = 0.05,
        psi_threshold: float = 0.2,
        sample_size: int = 5000,
    ) -> None:
        self.pvalue_threshold = pvalue_threshold
        self.psi_threshold = psi_threshold
        self.sample_size = sample_size

    def compare(
        self,
        reference: pd.DataFrame,
        current: pd.DataFrame,
    ) -> DriftReport:
        """Compare reference and current DataFrames for drift."""
        col_results: dict[str, ColumnDriftResult] = {}
        shared = [c for c in reference.columns if c in current.columns
                  and not c.startswith("_spindle_")]

        for col in shared:
            ref_col = reference[col].dropna()
            cur_col = current[col].dropna()
            if len(ref_col) < 10 or len(cur_col) < 10:
                continue
            if len(ref_col) > self.sample_size:
                ref_col = ref_col.sample(self.sample_size, random_state=0)
            if len(cur_col) > self.sample_size:
                cur_col = cur_col.sample(self.sample_size, random_state=0)

            if pd.api.types.is_numeric_dtype(ref_col):
                col_results[col] = self._ks_test(col, ref_col, cur_col)
            else:
                col_results[col] = self._chi2_test(col, ref_col, cur_col)

        drifted = [col for col, r in col_results.items() if r.is_drifted]
        drift_fraction = len(drifted) / len(col_results) if col_results else 0.0
        overall_score = (
            sum(r.drift_score for r in col_results.values()) / len(col_results)
            if col_results else 0.0
        )

        return DriftReport(
            columns=col_results,
            drifted_columns=drifted,
            drift_fraction=drift_fraction,
            overall_drift_score=round(overall_score, 4),
        )

    def _ks_test(self, col: str, ref: pd.Series, cur: pd.Series) -> ColumnDriftResult:
        if not HAS_SCIPY:
            return ColumnDriftResult(col, 0.0, None, None, False, "none")
        stat, pvalue = sp_stats.ks_2samp(ref.values, cur.values)
        psi = _population_stability_index(ref, cur)
        is_drifted = (float(pvalue) < self.pvalue_threshold) or (psi > self.psi_threshold)
        return ColumnDriftResult(
            column=col,
            drift_score=float(min(1.0, stat + psi / 2)),
            test_statistic=float(stat),
            p_value=float(pvalue),
            is_drifted=is_drifted,
            method="ks",
        )

    def _chi2_test(self, col: str, ref: pd.Series, cur: pd.Series) -> ColumnDriftResult:
        if not HAS_SCIPY:
            return ColumnDriftResult(col, 0.0, None, None, False, "none")
        try:
            all_cats = sorted(set(ref.astype(str)) | set(cur.astype(str)))
            ref_counts = ref.astype(str).value_counts()
            cur_counts = cur.astype(str).value_counts()
            ref_arr = np.array([ref_counts.get(c, 0) for c in all_cats], dtype=float)
            cur_arr = np.array([cur_counts.get(c, 0) for c in all_cats], dtype=float)
            # Scale expected to match observed total
            expected = ref_arr * (cur_arr.sum() / (ref_arr.sum() + 1e-10))
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                stat, pvalue = sp_stats.chisquare(cur_arr + 1, expected + 1)
            is_drifted = float(pvalue) < self.pvalue_threshold
            drift_score = min(1.0, float(stat) / (len(all_cats) * 10 + 1))
            return ColumnDriftResult(
                column=col,
                drift_score=drift_score,
                test_statistic=float(stat),
                p_value=float(pvalue),
                is_drifted=is_drifted,
                method="chi2",
            )
        except Exception:
            return ColumnDriftResult(col, 0.0, None, None, False, "chi2")


# ---------------------------------------------------------------------------
# Bootstrap Mode
# ---------------------------------------------------------------------------


@dataclass
class BootstrapResult:
    """Result of bootstrap synthetic generation."""
    table_name: str
    n_rows: int
    n_bootstrap_samples: int
    seed: int
    source_rows_used: int


class BootstrapMode:
    """Generate synthetic data by bootstrapping (sampling with replacement) from real data.

    The simplest form of synthetic generation — preserves all real distributions
    exactly, but does not generalize beyond the source data. Useful as a baseline.
    """

    def __init__(self, add_jitter: bool = True, jitter_std_fraction: float = 0.01) -> None:
        self.add_jitter = add_jitter
        self.jitter_std_fraction = jitter_std_fraction

    def generate(
        self,
        source: pd.DataFrame,
        n_rows: int | None = None,
        table_name: str = "table",
        seed: int = 42,
    ) -> tuple[pd.DataFrame, BootstrapResult]:
        """Generate synthetic DataFrame by bootstrapping source.

        Args:
            source: Real data to bootstrap from.
            n_rows: Number of rows to generate (default: same as source).
            table_name: Name for result metadata.
            seed: Random seed.

        Returns:
            (synthetic_df, BootstrapResult)
        """
        n = n_rows or len(source)
        rng = np.random.default_rng(seed)
        idx = rng.integers(0, len(source), size=n)
        synth = source.iloc[idx].reset_index(drop=True).copy()

        if self.add_jitter:
            for col in synth.select_dtypes(include=[np.number]).columns:
                std = float(source[col].std())
                if std > 0:
                    jitter = rng.normal(0, std * self.jitter_std_fraction, size=n)
                    synth[col] = synth[col] + jitter

        return synth, BootstrapResult(
            table_name=table_name,
            n_rows=n,
            n_bootstrap_samples=n,
            seed=seed,
            source_rows_used=len(source),
        )


# ---------------------------------------------------------------------------
# CTGAN Wrapper (optional)
# ---------------------------------------------------------------------------


class CTGANWrapper:
    """Optional wrapper around CTGAN/TVAE from the sdv library.

    Falls back gracefully if sdv is not installed. When available,
    CTGAN provides deep generative model quality for tabular data.

    Install with: pip install sqllocks-spindle[deep]
    """

    def __init__(self, epochs: int = 300, batch_size: int = 500) -> None:
        self.epochs = epochs
        self.batch_size = batch_size
        self._model: Any = None
        self._fitted = False

    @staticmethod
    def is_available() -> bool:
        try:
            import ctgan  # noqa: F401
            return True
        except ImportError:
            return False

    def fit(self, df: pd.DataFrame, discrete_columns: list[str] | None = None) -> "CTGANWrapper":
        """Fit the CTGAN model on real data."""
        if not self.is_available():
            raise ImportError(
                "ctgan is not installed. Install with: pip install sqllocks-spindle[deep]"
            )
        from ctgan import CTGAN
        discrete = discrete_columns or [
            c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])
        ]
        self._model = CTGAN(epochs=self.epochs, batch_size=self.batch_size)
        self._model.fit(df, discrete_columns=discrete)
        self._fitted = True
        return self

    def sample(self, n_rows: int) -> pd.DataFrame:
        """Sample from the fitted CTGAN model."""
        if not self._fitted or self._model is None:
            raise RuntimeError("CTGANWrapper must be fitted before sampling.")
        return self._model.sample(n_rows)
