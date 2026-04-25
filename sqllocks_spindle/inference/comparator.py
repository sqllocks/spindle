"""Fidelity comparator — compare real vs synthetic data quality.

Produces a FidelityReport with per-column and per-table scores (0-100)
based on statistical tests, distribution matching, null rates, and
cardinality analysis.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

# Scipy is optional — KS / chi-squared tests are skipped when absent.
try:
    from scipy import stats as sp_stats

    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ColumnFidelity:
    """Fidelity metrics for a single column."""

    column_name: str
    dtype_match: bool
    null_rate_delta: float  # abs(real_null_rate - synth_null_rate)
    cardinality_ratio: float  # synth_cardinality / real_cardinality (1.0 = perfect)
    mean_delta: float | None  # abs(real_mean - synth_mean) / real_std if numeric
    std_ratio: float | None  # synth_std / real_std if numeric (1.0 = perfect)
    ks_statistic: float | None  # KS test statistic for numeric columns (0 = identical)
    ks_pvalue: float | None  # KS test p-value
    chi2_statistic: float | None  # Chi-squared stat for categorical columns
    chi2_pvalue: float | None
    value_overlap: float | None  # Jaccard similarity of unique values (for categoricals)
    score: float  # 0-100 composite fidelity score for this column


@dataclass
class TableFidelity:
    """Fidelity metrics for a table."""

    table_name: str
    row_count_real: int
    row_count_synth: int
    columns: dict[str, ColumnFidelity]
    score: float  # average of column scores


@dataclass
class FidelityReport:
    """Complete fidelity report comparing real vs synthetic data."""

    tables: dict[str, TableFidelity]
    overall_score: float  # weighted average across tables

    def summary(self) -> str:
        """Generate a plain-text summary."""
        lines = [
            "Fidelity Report",
            "=" * 60,
            f"Overall Score: {self.overall_score:.1f}/100",
            "",
        ]
        for table_name, tf in self.tables.items():
            lines.append(
                f"  {table_name} — Score: {tf.score:.1f}/100 "
                f"({tf.row_count_real} real, {tf.row_count_synth} synth rows)"
            )
            for col_name, cf in tf.columns.items():
                lines.append(
                    f"    {col_name}: {cf.score:.1f}/100 "
                    f"(type_match={cf.dtype_match})"
                )
        return "\n".join(lines)

    def to_markdown(self) -> str:
        """Generate markdown report."""
        lines = [
            "# Fidelity Report",
            "",
            f"**Overall Score: {self.overall_score:.1f}/100**",
            "",
        ]
        for table_name, tf in self.tables.items():
            lines.append(f"## {table_name} — {tf.score:.1f}/100")
            lines.append("")
            lines.append(
                "| Column | Score | Type Match | Null Delta | KS Stat | Chi2 Stat |"
            )
            lines.append(
                "|--------|-------|------------|------------|---------|-----------|"
            )
            for col_name, cf in tf.columns.items():
                ks = f"{cf.ks_statistic:.3f}" if cf.ks_statistic is not None else "—"
                chi2 = (
                    f"{cf.chi2_statistic:.1f}"
                    if cf.chi2_statistic is not None
                    else "—"
                )
                check = "\u2713" if cf.dtype_match else "\u2717"
                lines.append(
                    f"| {col_name} | {cf.score:.1f} | {check} "
                    f"| {cf.null_rate_delta:.3f} | {ks} | {chi2} |"
                )
            lines.append("")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Type classification helpers
# ---------------------------------------------------------------------------

_NUMERIC_DTYPES = {"int8", "int16", "int32", "int64", "uint8", "uint16",
                   "uint32", "uint64", "float16", "float32", "float64",
                   "Float32", "Float64", "Int8", "Int16", "Int32", "Int64",
                   "UInt8", "UInt16", "UInt32", "UInt64"}


def _general_type(series: pd.Series) -> str:
    """Classify a Series as 'numeric', 'string', 'datetime', or 'other'."""
    dtype = series.dtype
    if pd.api.types.is_bool_dtype(dtype):
        return "string"  # treat booleans as categorical for comparison
    if pd.api.types.is_numeric_dtype(dtype):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"
    # Object dtype — try to determine if numeric
    if dtype == object or pd.api.types.is_string_dtype(dtype):
        non_null = series.dropna()
        if len(non_null) > 0:
            numeric = pd.to_numeric(non_null, errors="coerce")
            if numeric.notna().all():
                return "numeric"
        return "string"
    return "other"


def _to_numeric_safe(series: pd.Series) -> pd.Series:
    """Convert a series to numeric, dropping NaN."""
    return pd.to_numeric(series, errors="coerce").dropna()


# ---------------------------------------------------------------------------
# FidelityComparator
# ---------------------------------------------------------------------------


class FidelityComparator:
    """Compare real and synthetic datasets to produce a fidelity report."""

    def compare(
        self,
        real: dict[str, pd.DataFrame],
        synthetic: dict[str, pd.DataFrame],
    ) -> FidelityReport:
        """Compare real vs synthetic data across all shared tables."""
        tables: dict[str, TableFidelity] = {}
        common_tables = set(real.keys()) & set(synthetic.keys())

        for table_name in sorted(common_tables):
            tables[table_name] = self._compare_table(
                table_name, real[table_name], synthetic[table_name]
            )

        if tables:
            overall = float(np.mean([t.score for t in tables.values()]))
        else:
            overall = 0.0

        return FidelityReport(tables=tables, overall_score=overall)

    def _compare_table(
        self, name: str, real_df: pd.DataFrame, synth_df: pd.DataFrame
    ) -> TableFidelity:
        common_cols = set(real_df.columns) & set(synth_df.columns)
        columns: dict[str, ColumnFidelity] = {}
        for col in sorted(common_cols):
            columns[col] = self._compare_column(col, real_df[col], synth_df[col])

        score = (
            float(np.mean([c.score for c in columns.values()])) if columns else 0.0
        )
        return TableFidelity(
            table_name=name,
            row_count_real=len(real_df),
            row_count_synth=len(synth_df),
            columns=columns,
            score=score,
        )

    def _compare_column(
        self, name: str, real: pd.Series, synth: pd.Series
    ) -> ColumnFidelity:
        real_type = _general_type(real)
        synth_type = _general_type(synth)
        dtype_match = real_type == synth_type

        # Null rates
        real_null_rate = real.isna().mean() if len(real) > 0 else 0.0
        synth_null_rate = synth.isna().mean() if len(synth) > 0 else 0.0
        null_rate_delta = abs(float(real_null_rate) - float(synth_null_rate))

        # Cardinality
        real_card = real.dropna().nunique()
        synth_card = synth.dropna().nunique()
        cardinality_ratio = synth_card / max(real_card, 1)

        # Initialise optional metrics
        mean_delta: float | None = None
        std_ratio: float | None = None
        ks_statistic: float | None = None
        ks_pvalue: float | None = None
        chi2_statistic: float | None = None
        chi2_pvalue: float | None = None
        value_overlap: float | None = None

        # Scoring accumulators
        points = 0.0
        max_points = 0.0

        # --- dtype_match (10 points) ---
        max_points += 10
        if dtype_match:
            points += 10

        # --- null_rate_delta (10 points) ---
        max_points += 10
        points += 10 * (1.0 - null_rate_delta)

        # --- cardinality (10 points) ---
        max_points += 10
        points += 10 * max(0.0, 1.0 - abs(1.0 - cardinality_ratio))

        is_numeric = real_type == "numeric" and synth_type == "numeric"

        if is_numeric:
            real_num = _to_numeric_safe(real)
            synth_num = _to_numeric_safe(synth)

            real_mean = float(real_num.mean()) if len(real_num) > 0 else 0.0
            synth_mean = float(synth_num.mean()) if len(synth_num) > 0 else 0.0
            real_std = float(real_num.std()) if len(real_num) > 0 else 0.0
            synth_std = float(synth_num.std()) if len(synth_num) > 0 else 0.0

            # mean_delta (normalised by std)
            mean_delta = abs(real_mean - synth_mean) / max(real_std, 1e-9)
            std_ratio = synth_std / max(real_std, 1e-9)

            # --- mean_delta score (20 points) ---
            max_points += 20
            points += 20 * max(0.0, 1.0 - mean_delta)

            # --- std_ratio score (10 points) ---
            max_points += 10
            points += 10 * max(0.0, 1.0 - abs(1.0 - std_ratio))

            # --- KS test (10 points) ---
            max_points += 10
            if HAS_SCIPY and len(real_num) >= 5 and len(synth_num) >= 5:
                ks_result = sp_stats.ks_2samp(real_num.values, synth_num.values)
                ks_statistic = float(ks_result.statistic)
                ks_pvalue = float(ks_result.pvalue)
                points += 10 * (1.0 - ks_statistic)
            else:
                # No scipy — give partial credit based on mean/std match
                points += 5

        else:
            # Categorical / string / datetime path
            real_vals = set(real.dropna().unique())
            synth_vals = set(synth.dropna().unique())

            # --- value_overlap Jaccard (20 points) ---
            max_points += 20
            if real_vals or synth_vals:
                intersection = len(real_vals & synth_vals)
                union = len(real_vals | synth_vals)
                value_overlap = intersection / union if union > 0 else 0.0
                points += 20 * value_overlap
            else:
                value_overlap = 1.0  # both empty
                points += 20

            # --- chi2 test (20 points) ---
            max_points += 20
            if HAS_SCIPY and real_vals and synth_vals:
                chi2_stat, chi2_p = self._chi2_test(real, synth)
                if chi2_stat is not None:
                    chi2_statistic = chi2_stat
                    chi2_pvalue = chi2_p
                    if chi2_p is not None and chi2_p > 0.05:
                        points += 20
                    else:
                        points += 20 * (
                            1.0 - min(chi2_stat / 100.0, 1.0)
                        )
                else:
                    points += 10  # fallback
            else:
                points += 10  # no scipy — partial credit

        # Normalise to 0-100
        score = (points / max_points) * 100.0 if max_points > 0 else 0.0

        return ColumnFidelity(
            column_name=name,
            dtype_match=dtype_match,
            null_rate_delta=null_rate_delta,
            cardinality_ratio=float(cardinality_ratio),
            mean_delta=mean_delta,
            std_ratio=std_ratio,
            ks_statistic=ks_statistic,
            ks_pvalue=ks_pvalue,
            chi2_statistic=chi2_statistic,
            chi2_pvalue=chi2_pvalue,
            value_overlap=value_overlap,
            score=float(score),
        )

    @staticmethod
    def _chi2_test(
        real: pd.Series, synth: pd.Series
    ) -> tuple[float | None, float | None]:
        """Run a chi-squared test on two categorical Series."""
        real_counts = real.dropna().value_counts()
        synth_counts = synth.dropna().value_counts()

        # Align categories (cast to str to handle mixed types like str/bool)
        all_cats = sorted(set(real_counts.index) | set(synth_counts.index), key=str)
        if len(all_cats) < 2:
            return None, None

        observed = np.array([synth_counts.get(c, 0) for c in all_cats], dtype=float)
        expected_raw = np.array([real_counts.get(c, 0) for c in all_cats], dtype=float)

        # Scale expected to match synth total
        real_total = expected_raw.sum()
        synth_total = observed.sum()
        if real_total == 0 or synth_total == 0:
            return None, None

        expected = expected_raw * (synth_total / real_total)

        # Avoid zero expected values
        expected = np.where(expected == 0, 1e-10, expected)

        try:
            chi2, p = sp_stats.chisquare(observed, f_exp=expected)
            return float(chi2), float(p)
        except Exception:
            return None, None
