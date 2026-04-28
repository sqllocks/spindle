"""Data profiler — analyse pandas DataFrames and produce statistical profiles.

The profiler inspects column types, value distributions, cardinality,
null rates, and inter-table foreign-key relationships to build a
comprehensive DatasetProfile that the SchemaBuilder can convert into a
SpindleSchema.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# Scipy is optional — distribution fitting is skipped when absent.
try:
    from scipy import stats as sp_stats

    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False


# ---------------------------------------------------------------------------
# Regex patterns for string-type detection
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)
_PHONE_RE = re.compile(
    r"^[\+]?[\d\s\-\(\)\.]{7,20}$"
)
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_DATE_RE = re.compile(
    r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$"
)
_SSN_RE = re.compile(r"^\d{3}-\d{2}-\d{4}$")
_IP_V4_RE = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
_IP_V6_RE = re.compile(r"^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{0,4}){2,7}$")
_MAC_RE = re.compile(r"^([0-9a-fA-F]{2}[:\-]){5}[0-9a-fA-F]{2}$")
_CURRENCY_CODE_RE = re.compile(r"^[A-Z]{3}$")
_LANGUAGE_CODE_RE = re.compile(r"^[a-z]{2}(-[A-Z]{2})?$")
_IBAN_RE = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{1,30}$")
_POSTAL_US_RE = re.compile(r"^\d{5}(-\d{4})?$")


# ---------------------------------------------------------------------------
# Profile dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ColumnProfile:
    """Statistical profile of a single column."""

    name: str
    dtype: str  # spindle type: integer, float, string, date, datetime, boolean
    null_count: int
    null_rate: float
    cardinality: int
    cardinality_ratio: float  # cardinality / row_count
    is_unique: bool
    is_enum: bool  # True if cardinality < 200 or cardinality_ratio < 0.20
    enum_values: dict[str, float] | None  # value -> probability (if is_enum)
    min_value: Any
    max_value: Any
    mean: float | None  # numeric only
    std: float | None  # numeric only
    distribution: str | None  # best-fit name or None
    distribution_params: dict[str, float] | None
    pattern: str | None  # detected regex pattern for strings
    is_primary_key: bool
    is_foreign_key: bool
    fk_ref_table: str | None

    # --- Phase 3B: extended statistical fields (all optional for backward compat) ---
    quantiles: dict[str, float] | None = None          # P1,P5,P10,P25,P50,P75,P90,P95,P99
    hour_histogram: list[float] | None = None           # 24-bin normalized hour distribution
    dow_histogram: list[float] | None = None            # 7-bin normalized day-of-week distribution
    string_length: dict[str, float] | None = None       # min, mean, max, p95 of len(value)
    outlier_rate: float | None = None                   # fraction outside 1.5×IQR fence
    value_counts_ext: dict[str, float] | None = None   # value→proportion (top N)
    fit_score: float | None = None                      # 1 - KS_statistic from best-fit dist


@dataclass
class TableProfile:
    """Profile of a single table (DataFrame)."""

    name: str
    row_count: int
    columns: dict[str, ColumnProfile]
    primary_key: list[str]
    detected_fks: dict[str, str]  # col_name -> parent_table
    correlation_matrix: dict[str, dict[str, float]] | None = None  # Pearson between numeric cols


@dataclass
class DatasetProfile:
    """Profile of a multi-table dataset."""

    tables: dict[str, TableProfile]
    relationships: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# DataProfiler
# ---------------------------------------------------------------------------


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

    # ----- public API -----

    def profile_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str = "table",
    ) -> TableProfile:
        """Profile a single DataFrame."""
        return self._profile_single(df, table_name, all_tables={})

    def profile_dataset(
        self,
        tables: dict[str, pd.DataFrame],
    ) -> DatasetProfile:
        """Profile a dict of DataFrames and detect cross-table relationships."""
        # First pass: per-table profiles (no FK detection yet).
        profiles: dict[str, TableProfile] = {}
        for tname, df in tables.items():
            profiles[tname] = self._profile_single(df, tname, all_tables=tables)

        # Collect relationships from detected FKs.
        relationships: list[dict] = []
        for tname, tprofile in profiles.items():
            for col_name, parent_table in tprofile.detected_fks.items():
                relationships.append(
                    {
                        "name": f"fk_{tname}_{col_name}",
                        "parent": parent_table,
                        "child": tname,
                        "parent_columns": profiles[parent_table].primary_key,
                        "child_columns": [col_name],
                        "type": "one_to_many",
                    }
                )

        return DatasetProfile(tables=profiles, relationships=relationships)

    def profile(
        self,
        df: pd.DataFrame,
        table_name: str = "table",
    ) -> TableProfile:
        """Alias for profile_dataframe(). Profile a single DataFrame."""
        return self.profile_dataframe(df, table_name=table_name)

    @classmethod
    def from_csv(
        cls,
        path: str | Path,
        table_name: str | None = None,
        sample_rows: int | None = None,
        **kwargs,
    ) -> TableProfile:
        """Profile a CSV file.

        Args:
            path: Path to the CSV file.
            table_name: Name for the table profile. Defaults to the filename stem.
            sample_rows: If set, sample this many rows before profiling.
            **kwargs: Passed to DataProfiler constructor (fit_threshold, top_n_values, etc.).
        """
        p = Path(path)
        name = table_name or p.stem
        df = pd.read_csv(path)
        profiler = cls(sample_rows=sample_rows, **kwargs)
        return profiler.profile(df, table_name=name)

    # ----- internal helpers -----

    def _profile_single(
        self,
        df: pd.DataFrame,
        table_name: str,
        all_tables: dict[str, pd.DataFrame],
    ) -> TableProfile:
        row_count = len(df)
        if self.sample_rows is not None and len(df) > self.sample_rows:
            df = df.sample(n=self.sample_rows, random_state=42)
            row_count = len(df)
        pk_cols = self._detect_primary_key(df)
        fk_map = self._detect_foreign_keys(table_name, df, all_tables)

        columns: dict[str, ColumnProfile] = {}
        for col in df.columns:
            series = df[col]
            spindle_type = self._infer_spindle_type(series)
            null_count = int(series.isna().sum())
            null_rate = null_count / row_count if row_count > 0 else 0.0
            non_null = series.dropna()
            cardinality = int(non_null.nunique())
            cardinality_ratio = cardinality / row_count if row_count > 0 else 0.0
            is_unique = cardinality == row_count and null_count == 0
            is_enum = (cardinality < 200 or (cardinality_ratio < 0.30 and cardinality < 50_000)) and cardinality > 0

            # Enum value probabilities
            enum_values: dict[str, float] | None = None
            if is_enum:
                counts = non_null.value_counts(normalize=True)
                enum_values = {str(k): round(float(v), 6) for k, v in counts.items()}

            # Min / max
            min_value: Any = None
            max_value: Any = None
            if len(non_null) > 0:
                try:
                    min_value = non_null.min()
                    max_value = non_null.max()
                    # Convert numpy types to Python builtins for JSON safety.
                    if hasattr(min_value, "item"):
                        min_value = min_value.item()
                    if hasattr(max_value, "item"):
                        max_value = max_value.item()
                except TypeError:
                    pass

            # Numeric stats
            mean_val: float | None = None
            std_val: float | None = None
            if spindle_type in ("integer", "float"):
                numeric = pd.to_numeric(non_null, errors="coerce").dropna()
                if len(numeric) > 0:
                    mean_val = float(numeric.mean())
                    std_val = float(numeric.std())

            # Distribution fitting (numeric only, scipy required)
            dist_name, dist_params = (None, None)
            if spindle_type in ("integer", "float"):
                dist_name, dist_params = self._detect_distribution(
                    pd.to_numeric(non_null, errors="coerce").dropna()
                )

            # String pattern detection
            pattern: str | None = None
            if spindle_type == "string":
                pattern = self._detect_pattern(non_null)

            is_pk = col in pk_cols
            is_fk = col in fk_map
            fk_ref = fk_map.get(col)

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

            if len(non_null) > 0:
                value_counts_ext = self._compute_value_counts_ext(non_null)

            if spindle_type in ("date", "datetime") and len(non_null) > 0:
                hour_histogram_val = self._compute_hour_histogram(non_null)
                dow_histogram_val = self._compute_dow_histogram(non_null)

            # fit_score: 1 - ks_stat from best-fit distribution
            if dist_name is not None and dist_params is not None and HAS_SCIPY:
                numeric_for_fit = pd.to_numeric(non_null, errors="coerce").dropna()
                if len(numeric_for_fit) >= 20:
                    _sp = sp_stats
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

        corr_matrix = self._compute_correlation_matrix(df)

        return TableProfile(
            name=table_name,
            row_count=row_count,
            columns=columns,
            primary_key=pk_cols,
            detected_fks=fk_map,
            correlation_matrix=corr_matrix if corr_matrix else None,
        )

    # ----- type inference -----

    def _infer_spindle_type(self, series: pd.Series) -> str:
        """Map a pandas Series dtype to a Spindle type string."""
        dtype = series.dtype

        # Boolean
        if pd.api.types.is_bool_dtype(dtype):
            return "boolean"

        # Integer
        if pd.api.types.is_integer_dtype(dtype):
            return "integer"

        # Float
        if pd.api.types.is_float_dtype(dtype):
            # Check if all non-null values are actually whole numbers
            non_null = series.dropna()
            if len(non_null) > 0 and (non_null == non_null.astype(int)).all():
                return "integer"
            return "float"

        # Datetime
        if pd.api.types.is_datetime64_any_dtype(dtype):
            # If all times are midnight, treat as date
            non_null = series.dropna()
            if len(non_null) > 0:
                times = pd.to_datetime(non_null)
                if (times.dt.time == pd.Timestamp("00:00:00").time()).all():
                    return "date"
            return "datetime"

        # Object / string — try to infer further
        if dtype == object or pd.api.types.is_string_dtype(dtype):
            non_null = series.dropna()
            if len(non_null) == 0:
                return "string"

            # Try boolean-like
            unique_lower = set(str(v).lower() for v in non_null.unique())
            if unique_lower <= {"true", "false", "0", "1", "yes", "no"}:
                return "boolean"

            # Try numeric
            numeric = pd.to_numeric(non_null, errors="coerce")
            if numeric.notna().all():
                if (numeric == numeric.astype(int)).all():
                    return "integer"
                return "float"

            # Try datetime
            try:
                pd.to_datetime(non_null, format="mixed", dayfirst=False)
                return "datetime"
            except (ValueError, TypeError):
                pass

            return "string"

        return "string"

    # ----- Phase 3B helper methods -----

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
        """Pearson correlation matrix for all numeric columns."""
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

    # ----- distribution fitting -----

    def _detect_distribution(
        self,
        series: pd.Series,
    ) -> tuple[str | None, dict[str, float] | None]:
        """Fit candidate distributions and return the best match via KS test."""
        if not HAS_SCIPY:
            return None, None

        values = series.dropna().values.astype(float)
        if len(values) > 2000:
            values = np.random.default_rng(42).choice(values, size=2000, replace=False)
        if len(values) < 20:
            return None, None

        candidates = {
            "normal": sp_stats.norm,
            "uniform": sp_stats.uniform,
            "exponential": sp_stats.expon,
            "lognormal": sp_stats.lognorm,
        }

        best_name: str | None = None
        best_stat: float = float("inf")
        best_params: dict[str, float] | None = None
        best_pvalue: float = 0.0

        for name, dist in candidates.items():
            try:
                params = dist.fit(values)
                ks_stat, p_value = sp_stats.kstest(values, dist.name, args=params)
                if p_value > 0.05 and ks_stat < best_stat:
                    best_name = name
                    best_stat = ks_stat
                    best_pvalue = p_value
                    # Store params as a dict with meaningful keys
                    param_names = _distribution_param_names(name, params)
                    best_params = param_names
            except Exception:
                continue

        return best_name, best_params

    # ----- string pattern detection -----

    def _detect_pattern(self, series: pd.Series) -> str | None:
        """Detect common string patterns (email, phone, uuid, date, ssn, ip, mac, etc)."""
        non_null = series.dropna()
        if len(non_null) == 0:
            return None

        sample = non_null.astype(str)
        # Use a sample for large datasets
        if len(sample) > 1000:
            sample = sample.sample(n=1000, random_state=42)

        total = len(sample)
        threshold = 0.9  # at least 90% must match

        # Email
        if sample.str.fullmatch(_EMAIL_RE.pattern, na=False).sum() / total >= threshold:
            return "email"

        # UUID
        if sample.str.fullmatch(_UUID_RE.pattern, na=False).sum() / total >= threshold:
            return "uuid"

        # SSN (more specific than phone)
        if sample.str.fullmatch(_SSN_RE.pattern, na=False).sum() / total >= threshold:
            return "ssn"

        # MAC address (before IP to avoid confusion)
        if sample.str.fullmatch(_MAC_RE.pattern, na=False).sum() / total >= threshold:
            return "mac_address"

        # IP address (v4 or v6)
        ipv4_match = sample.str.fullmatch(_IP_V4_RE.pattern, na=False).sum() / total
        ipv6_match = sample.str.fullmatch(_IP_V6_RE.pattern, na=False).sum() / total
        if ipv4_match >= threshold or ipv6_match >= threshold:
            return "ip_address"

        # IBAN
        if sample.str.fullmatch(_IBAN_RE.pattern, na=False).sum() / total >= threshold:
            return "iban"

        # Postal code (US ZIP)
        if sample.str.fullmatch(_POSTAL_US_RE.pattern, na=False).sum() / total >= threshold:
            return "postal_code"

        # Date string
        if sample.str.fullmatch(_DATE_RE.pattern, na=False).sum() / total >= threshold:
            return "date"

        # Phone (more general, check after specific patterns)
        if sample.str.fullmatch(_PHONE_RE.pattern, na=False).sum() / total >= threshold:
            return "phone"

        # Currency code (3 uppercase letters) — cardinality guard to avoid false positives
        if (sample.str.fullmatch(_CURRENCY_CODE_RE.pattern, na=False).sum() / total >= threshold
                and non_null.nunique() <= 200):
            return "currency_code"

        # Language code (2 lowercase letters, optional region)
        if (sample.str.fullmatch(_LANGUAGE_CODE_RE.pattern, na=False).sum() / total >= threshold
                and non_null.nunique() <= 200):
            return "language_code"

        return None

    # ----- primary key detection -----

    def _detect_primary_key(self, df: pd.DataFrame) -> list[str]:
        """Heuristic: unique, non-null column that is integer or uuid-like."""
        candidates: list[str] = []
        for col in df.columns:
            series = df[col]
            if series.isna().any():
                continue
            if series.nunique() != len(df):
                continue
            # Good candidate
            spindle_type = self._infer_spindle_type(series)
            if spindle_type == "integer":
                candidates.append(col)
            elif spindle_type == "string":
                pattern = self._detect_pattern(series)
                if pattern == "uuid":
                    candidates.append(col)

        # Prefer columns with common PK name patterns
        pk_name_patterns = {"id", "_id", "pk", "key"}
        for c in candidates:
            lower = c.lower()
            if any(lower == p or lower.endswith(p) for p in pk_name_patterns):
                return [c]

        return candidates[:1] if candidates else []

    # ----- foreign key detection -----

    def _detect_foreign_keys(
        self,
        table_name: str,
        df: pd.DataFrame,
        all_tables: dict[str, pd.DataFrame],
    ) -> dict[str, str]:
        """Detect FK columns by name convention (*_id) and value overlap."""
        if not all_tables:
            return {}

        # Pre-build PK index once per parent table (avoid O(n_fk_cols) re-detection)
        pk_cache: dict[str, list[str]] = {}
        for tname, tdf in all_tables.items():
            pk_cache[tname] = self._detect_primary_key(tdf)

        fk_map: dict[str, str] = {}

        for col in df.columns:
            lower = col.lower()
            if not lower.endswith("_id"):
                continue

            # Derive candidate parent table name from column name
            # e.g. customer_id -> customer
            candidate_name = lower.rsplit("_id", 1)[0]

            # Find matching table (case-insensitive)
            parent_table: str | None = None
            for tname in all_tables:
                if tname.lower() == candidate_name:
                    parent_table = tname
                    break

            if parent_table is None or parent_table == table_name:
                continue

            # Check value overlap with parent's PK
            parent_df = all_tables[parent_table]
            parent_pk = pk_cache[parent_table]
            if not parent_pk:
                continue

            parent_pk_col = parent_pk[0]
            child_values = set(df[col].dropna().unique())
            parent_values = set(parent_df[parent_pk_col].dropna().unique())

            if not child_values:
                continue

            overlap = len(child_values & parent_values) / len(child_values)
            if overlap >= 0.9:
                fk_map[col] = parent_table

        return fk_map


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _distribution_param_names(
    dist_name: str,
    params: tuple,
) -> dict[str, float]:
    """Map scipy fit params tuple to a named dict."""
    if dist_name == "normal":
        return {"loc": float(params[0]), "scale": float(params[1])}
    elif dist_name == "uniform":
        return {"loc": float(params[0]), "scale": float(params[1])}
    elif dist_name == "exponential":
        return {"loc": float(params[0]), "scale": float(params[1])}
    elif dist_name == "lognormal":
        return {"s": float(params[0]), "loc": float(params[1]), "scale": float(params[2])}
    else:
        return {f"p{i}": float(v) for i, v in enumerate(params)}
