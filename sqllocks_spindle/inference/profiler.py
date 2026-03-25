"""Data profiler — analyse pandas DataFrames and produce statistical profiles.

The profiler inspects column types, value distributions, cardinality,
null rates, and inter-table foreign-key relationships to build a
comprehensive DatasetProfile that the SchemaBuilder can convert into a
SpindleSchema.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
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
    is_enum: bool  # True if cardinality < 50 or cardinality_ratio < 0.05
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


@dataclass
class TableProfile:
    """Profile of a single table (DataFrame)."""

    name: str
    row_count: int
    columns: dict[str, ColumnProfile]
    primary_key: list[str]
    detected_fks: dict[str, str]  # col_name -> parent_table


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

    # ----- internal helpers -----

    def _profile_single(
        self,
        df: pd.DataFrame,
        table_name: str,
        all_tables: dict[str, pd.DataFrame],
    ) -> TableProfile:
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
            is_enum = (cardinality < 50 or cardinality_ratio < 0.05) and cardinality > 0

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
            )

        return TableProfile(
            name=table_name,
            row_count=row_count,
            columns=columns,
            primary_key=pk_cols,
            detected_fks=fk_map,
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
        """Detect common string patterns (email, phone, uuid, date)."""
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
        if sample.str.match(_EMAIL_RE.pattern, na=False).sum() / total >= threshold:
            return "email"

        # UUID
        if sample.str.match(_UUID_RE.pattern, na=False).sum() / total >= threshold:
            return "uuid"

        # Phone
        if sample.str.match(_PHONE_RE.pattern, na=False).sum() / total >= threshold:
            return "phone"

        # Date string
        if sample.str.match(_DATE_RE.pattern, na=False).sum() / total >= threshold:
            return "date"

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
