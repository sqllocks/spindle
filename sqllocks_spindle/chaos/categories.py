"""Individual chaos-category mutators (strategy pattern).

Each mutator implements a single chaos category and exposes a uniform
``mutate(data, day, rng, intensity_multiplier)`` interface so that the
:class:`~sqllocks_spindle.chaos.engine.ChaosEngine` can dispatch to them
without knowing the implementation details.
"""

from __future__ import annotations

import math
import string
from abc import ABC, abstractmethod
from typing import Any

import numpy as np
import pandas as pd


class ChaosMutator(ABC):
    """Base class for all chaos-category mutators.

    Subclasses implement :meth:`mutate` which receives the data to corrupt,
    the current simulation day, a seeded numpy ``RandomState``, and the
    intensity multiplier from the active preset.
    """

    @property
    @abstractmethod
    def category(self) -> str:
        """Short category label matching :class:`ChaosCategory` values."""
        ...

    @abstractmethod
    def mutate(
        self,
        data: Any,
        day: int,
        rng: np.random.RandomState,
        intensity_multiplier: float,
    ) -> Any:
        """Apply chaos to *data* and return the mutated result.

        The concrete type of *data* depends on the category (DataFrame,
        bytes, dict of DataFrames, etc.).
        """
        ...

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _pick_fraction(
        base: float,
        intensity_multiplier: float,
        cap: float = 0.8,
    ) -> float:
        """Scale a base fraction by intensity, capping at *cap*."""
        return min(base * intensity_multiplier, cap)

    @staticmethod
    def _sample_indices(
        rng: np.random.RandomState,
        n_rows: int,
        fraction: float,
    ) -> np.ndarray:
        """Return row indices to corrupt, given a fraction of *n_rows*."""
        k = max(1, int(n_rows * fraction))
        k = min(k, n_rows)
        return rng.choice(n_rows, size=k, replace=False)


# ======================================================================
# Schema chaos
# ======================================================================


class SchemaChaosMutator(ChaosMutator):
    """Add, remove, rename, reorder, or retype columns.

    Before ``breaking_change_day`` only additive changes (add column,
    reorder) are applied.  After that day, destructive mutations (drop,
    rename, retype) are also possible.

    Args:
        breaking_change_day: Simulation day after which destructive schema
            mutations are enabled.
    """

    def __init__(self, breaking_change_day: int = 20) -> None:
        self.breaking_change_day = breaking_change_day

    @property
    def category(self) -> str:
        return "schema"

    def mutate(
        self,
        data: pd.DataFrame,
        day: int,
        rng: np.random.RandomState,
        intensity_multiplier: float,
    ) -> pd.DataFrame:
        if data.empty or len(data.columns) == 0:
            return data

        df = data.copy()

        # Choose which mutations to apply this round
        allow_breaking = day >= self.breaking_change_day

        # Always-safe mutations
        actions: list[str] = ["add_column", "reorder"]
        if allow_breaking:
            actions.extend(["drop_column", "rename_column", "retype_column"])

        # Pick 1-2 actions based on intensity
        n_actions = 1 if intensity_multiplier < 2.0 else 2
        chosen = rng.choice(actions, size=min(n_actions, len(actions)), replace=False)

        for action in chosen:
            if action == "add_column":
                df = self._add_column(df, rng)
            elif action == "reorder":
                df = self._reorder_columns(df, rng)
            elif action == "drop_column":
                df = self._drop_column(df, rng)
            elif action == "rename_column":
                df = self._rename_column(df, rng)
            elif action == "retype_column":
                df = self._retype_column(df, rng)

        return df

    # ------------------------------------------------------------------
    # Individual mutations
    # ------------------------------------------------------------------

    @staticmethod
    def _add_column(df: pd.DataFrame, rng: np.random.RandomState) -> pd.DataFrame:
        name = f"_chaos_extra_{rng.randint(1000, 9999)}"
        df[name] = rng.choice(["A", "B", None], size=len(df))
        return df

    @staticmethod
    def _reorder_columns(
        df: pd.DataFrame, rng: np.random.RandomState
    ) -> pd.DataFrame:
        cols = list(df.columns)
        rng.shuffle(cols)
        return df[cols]

    @staticmethod
    def _drop_column(df: pd.DataFrame, rng: np.random.RandomState) -> pd.DataFrame:
        if len(df.columns) <= 1:
            return df
        col = rng.choice(df.columns)
        return df.drop(columns=[col])

    @staticmethod
    def _rename_column(
        df: pd.DataFrame, rng: np.random.RandomState
    ) -> pd.DataFrame:
        if len(df.columns) == 0:
            return df
        col = rng.choice(df.columns)
        suffix = rng.choice(["_v2", "_old", "_bak", "_RENAMED", ""])
        new_name = f"{col}{suffix}" if suffix else f"x_{col}"
        return df.rename(columns={col: new_name})

    @staticmethod
    def _retype_column(
        df: pd.DataFrame, rng: np.random.RandomState
    ) -> pd.DataFrame:
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        if not numeric_cols:
            return df
        col = rng.choice(numeric_cols)
        df[col] = df[col].astype(str)
        return df


# ======================================================================
# Value chaos
# ======================================================================


class ValueChaosMutator(ChaosMutator):
    """Corrupt individual cell values: nulls, out-of-range, wrong types,
    encoding issues (BOM, Latin-1), future dates, negative amounts.
    """

    @property
    def category(self) -> str:
        return "value"

    def mutate(
        self,
        data: pd.DataFrame,
        day: int,
        rng: np.random.RandomState,
        intensity_multiplier: float,
    ) -> pd.DataFrame:
        if data.empty:
            return data

        df = data.copy()

        # Pick 1-3 sub-mutations
        mutations = [
            self._inject_nulls,
            self._out_of_range,
            self._wrong_types,
            self._encoding_issues,
            self._future_dates,
            self._negative_amounts,
        ]
        n = min(rng.randint(1, 4), len(mutations))
        chosen = rng.choice(len(mutations), size=n, replace=False)

        for i in chosen:
            df = mutations[i](df, rng, intensity_multiplier)

        return df

    # ------------------------------------------------------------------
    # Sub-mutations
    # ------------------------------------------------------------------

    def _inject_nulls(
        self,
        df: pd.DataFrame,
        rng: np.random.RandomState,
        intensity: float,
    ) -> pd.DataFrame:
        frac = self._pick_fraction(0.05, intensity)
        cols = df.columns.tolist()
        if not cols:
            return df
        col = rng.choice(cols)
        idx = self._sample_indices(rng, len(df), frac)
        df.iloc[idx, df.columns.get_loc(col)] = None
        return df

    def _out_of_range(
        self,
        df: pd.DataFrame,
        rng: np.random.RandomState,
        intensity: float,
    ) -> pd.DataFrame:
        numeric = df.select_dtypes(include="number").columns.tolist()
        if not numeric:
            return df
        col = rng.choice(numeric)
        frac = self._pick_fraction(0.03, intensity)
        idx = self._sample_indices(rng, len(df), frac)
        col_max = df[col].abs().max()
        baseline = col_max if col_max and not math.isnan(col_max) else 1000.0
        extreme = rng.uniform(baseline * 100, baseline * 1000, size=len(idx))
        # Cast column to float first to avoid pandas int64 upcast errors
        df[col] = df[col].astype(float)
        df.iloc[idx, df.columns.get_loc(col)] = extreme
        return df

    def _wrong_types(
        self,
        df: pd.DataFrame,
        rng: np.random.RandomState,
        intensity: float,
    ) -> pd.DataFrame:
        numeric = df.select_dtypes(include="number").columns.tolist()
        if not numeric:
            return df
        col = rng.choice(numeric)
        frac = self._pick_fraction(0.02, intensity)
        idx = self._sample_indices(rng, len(df), frac)
        junk = rng.choice(["N/A", "null", "#REF!", "---", "TBD"], size=len(idx))
        # Must cast column to object to hold mixed types
        df[col] = df[col].astype(object)
        df.iloc[idx, df.columns.get_loc(col)] = junk
        return df

    def _encoding_issues(
        self,
        df: pd.DataFrame,
        rng: np.random.RandomState,
        intensity: float,
    ) -> pd.DataFrame:
        str_cols = df.select_dtypes(include="object").columns.tolist()
        if not str_cols:
            return df
        col = rng.choice(str_cols)
        frac = self._pick_fraction(0.03, intensity)
        idx = self._sample_indices(rng, len(df), frac)
        bom = "\ufeff"
        latin_chars = ["\xe9", "\xf1", "\xfc", "\xe4", "\xf6"]
        for i in idx:
            val = df.iat[i, df.columns.get_loc(col)]
            if val is None or (isinstance(val, float) and math.isnan(val)):
                continue
            choice = rng.randint(0, 2)
            if choice == 0:
                df.iat[i, df.columns.get_loc(col)] = bom + str(val)
            else:
                char = rng.choice(latin_chars)
                df.iat[i, df.columns.get_loc(col)] = str(val) + char
        return df

    def _future_dates(
        self,
        df: pd.DataFrame,
        rng: np.random.RandomState,
        intensity: float,
    ) -> pd.DataFrame:
        dt_cols = df.select_dtypes(include="datetime").columns.tolist()
        if not dt_cols:
            return df
        col = rng.choice(dt_cols)
        frac = self._pick_fraction(0.03, intensity)
        idx = self._sample_indices(rng, len(df), frac)
        future_offsets = rng.randint(365, 3650, size=len(idx))
        future_dates = [
            pd.Timestamp("2030-01-01") + pd.Timedelta(days=int(d))
            for d in future_offsets
        ]
        df.iloc[idx, df.columns.get_loc(col)] = future_dates
        return df

    def _negative_amounts(
        self,
        df: pd.DataFrame,
        rng: np.random.RandomState,
        intensity: float,
    ) -> pd.DataFrame:
        numeric = df.select_dtypes(include="number").columns.tolist()
        # Prefer columns that look like amounts
        amount_cols = [c for c in numeric if any(
            kw in c.lower() for kw in ("amount", "total", "price", "cost", "qty", "quantity")
        )]
        target = amount_cols if amount_cols else numeric
        if not target:
            return df
        col = rng.choice(target)
        frac = self._pick_fraction(0.03, intensity)
        idx = self._sample_indices(rng, len(df), frac)
        df[col] = df[col].astype(float)
        df.iloc[idx, df.columns.get_loc(col)] = (
            -1 * df.iloc[idx, df.columns.get_loc(col)]
        )
        return df


# ======================================================================
# File chaos
# ======================================================================


class FileChaosMutator(ChaosMutator):
    """Corrupt raw file bytes: truncation, encoding corruption, partial
    writes, zero-byte files, wrong extension content, wrong delimiters,
    invalid JSON/Parquet poison payloads.
    """

    @property
    def category(self) -> str:
        return "file"

    def mutate(
        self,
        data: bytes,
        day: int,
        rng: np.random.RandomState,
        intensity_multiplier: float,
    ) -> bytes:
        if not data:
            return data

        mutations = [
            self._truncate,
            self._corrupt_encoding,
            self._partial_write,
            self._zero_byte,
            self._garbage_header,
            self._wrong_delimiter,
            self._invalid_json_poison,
            self._bom_injection,
        ]
        choice = rng.randint(0, len(mutations))
        return mutations[choice](data, rng, intensity_multiplier)

    # ------------------------------------------------------------------
    # Sub-mutations
    # ------------------------------------------------------------------

    @staticmethod
    def _truncate(
        data: bytes,
        rng: np.random.RandomState,
        intensity: float,
    ) -> bytes:
        cut_point = max(1, int(len(data) * rng.uniform(0.1, 0.6)))
        return data[:cut_point]

    @staticmethod
    def _corrupt_encoding(
        data: bytes,
        rng: np.random.RandomState,
        intensity: float,
    ) -> bytes:
        arr = bytearray(data)
        n_corruptions = max(1, int(len(arr) * 0.01 * intensity))
        for _ in range(n_corruptions):
            pos = rng.randint(0, len(arr))
            arr[pos] = rng.randint(0, 256)
        return bytes(arr)

    @staticmethod
    def _partial_write(
        data: bytes,
        rng: np.random.RandomState,
        intensity: float,
    ) -> bytes:
        # Simulate a partial write followed by null bytes
        cut = max(1, int(len(data) * rng.uniform(0.3, 0.7)))
        padding = b"\x00" * (len(data) - cut)
        return data[:cut] + padding

    @staticmethod
    def _zero_byte(
        data: bytes,
        rng: np.random.RandomState,
        intensity: float,
    ) -> bytes:
        return b""

    @staticmethod
    def _garbage_header(
        data: bytes,
        rng: np.random.RandomState,
        intensity: float,
    ) -> bytes:
        garbage_len = rng.randint(8, 64)
        garbage = bytes(rng.randint(0, 256, size=garbage_len).tolist())
        return garbage + data

    @staticmethod
    def _wrong_delimiter(
        data: bytes,
        rng: np.random.RandomState,
        intensity: float,
    ) -> bytes:
        """Replace commas with pipes or tabs to break CSV parsing."""
        replacements = {b",": b"|", b"\t": b",", b"|": b"\t"}
        text = data
        for old, new in replacements.items():
            if old in text:
                text = text.replace(old, new)
                break
        return text

    @staticmethod
    def _invalid_json_poison(
        data: bytes,
        rng: np.random.RandomState,
        intensity: float,
    ) -> bytes:
        """Inject invalid JSON fragments to break JSONL parsers."""
        poison_payloads = [
            b'\n{"_poison": true, "value": NaN}\n',
            b"\n{incomplete json\n",
            b"\n\x00\x00\x00\n",
            b'\n{"nested": {"too": {"deep": {"for": {"parsers": "maybe"}}}}}\n',
            b"\n[]\n",  # array instead of object
        ]
        payload = poison_payloads[rng.randint(0, len(poison_payloads))]
        # Insert at a random position
        if len(data) > 1:
            pos = rng.randint(0, len(data))
            return data[:pos] + payload + data[pos:]
        return data + payload

    @staticmethod
    def _bom_injection(
        data: bytes,
        rng: np.random.RandomState,
        intensity: float,
    ) -> bytes:
        """Prepend a UTF-8 BOM or inject mid-stream BOMs."""
        bom = b"\xef\xbb\xbf"
        choice = rng.randint(0, 2)
        if choice == 0:
            # Prepend BOM
            return bom + data
        else:
            # Inject BOM at random position
            if len(data) > 1:
                pos = rng.randint(1, len(data))
                return data[:pos] + bom + data[pos:]
            return bom + data


# ======================================================================
# Referential chaos
# ======================================================================


class ReferentialChaosMutator(ChaosMutator):
    """Corrupt referential integrity: orphan foreign keys, duplicate
    primary keys.

    Expects *data* to be a ``dict[str, pd.DataFrame]`` (table name to DF).
    Returns the same structure with mutations applied.
    """

    @property
    def category(self) -> str:
        return "referential"

    def mutate(
        self,
        data: dict[str, pd.DataFrame],
        day: int,
        rng: np.random.RandomState,
        intensity_multiplier: float,
    ) -> dict[str, pd.DataFrame]:
        if not data:
            return data

        result = {k: v.copy() for k, v in data.items()}

        actions = [self._orphan_fks, self._duplicate_pks]
        choice = rng.randint(0, len(actions))
        return actions[choice](result, rng, intensity_multiplier)

    # ------------------------------------------------------------------

    def _orphan_fks(
        self,
        tables: dict[str, pd.DataFrame],
        rng: np.random.RandomState,
        intensity: float,
    ) -> dict[str, pd.DataFrame]:
        """Replace some FK values with IDs that don't exist in the parent."""
        table_names = list(tables.keys())
        if len(table_names) < 2:
            return tables

        # Pick a random table and column that looks like an FK
        target_name = rng.choice(table_names)
        df = tables[target_name]
        fk_cols = [c for c in df.columns if c.endswith("_id") and c != df.columns[0]]
        if not fk_cols:
            return tables

        col = rng.choice(fk_cols)
        frac = self._pick_fraction(0.05, intensity)
        idx = self._sample_indices(rng, len(df), frac)

        # Generate orphan IDs that are extremely unlikely to exist
        orphan_base = 9_000_000
        orphan_ids = [orphan_base + int(rng.randint(0, 999_999)) for _ in idx]
        df[col] = df[col].astype(object)
        df.iloc[idx, df.columns.get_loc(col)] = orphan_ids
        tables[target_name] = df
        return tables

    def _duplicate_pks(
        self,
        tables: dict[str, pd.DataFrame],
        rng: np.random.RandomState,
        intensity: float,
    ) -> dict[str, pd.DataFrame]:
        """Introduce duplicate primary key values."""
        table_names = list(tables.keys())
        target_name = rng.choice(table_names)
        df = tables[target_name]

        if len(df) < 2:
            return tables

        pk_col = df.columns[0]  # Assume first column is PK
        frac = self._pick_fraction(0.03, intensity)
        n_dupes = max(1, int(len(df) * frac))
        n_dupes = min(n_dupes, len(df) - 1)

        # Pick source values and overwrite random targets
        source_idx = rng.choice(len(df), size=n_dupes, replace=True)
        target_idx = rng.choice(len(df), size=n_dupes, replace=False)
        df.iloc[target_idx, df.columns.get_loc(pk_col)] = (
            df.iloc[source_idx, df.columns.get_loc(pk_col)].values
        )
        tables[target_name] = df
        return tables


# ======================================================================
# Temporal chaos
# ======================================================================


class TemporalChaosMutator(ChaosMutator):
    """Corrupt temporal columns: late arrivals, out-of-order timestamps,
    timezone mismatches, DST boundary issues.
    """

    @property
    def category(self) -> str:
        return "temporal"

    def mutate(
        self,
        data: pd.DataFrame,
        day: int,
        rng: np.random.RandomState,
        intensity_multiplier: float,
        date_columns: list[str] | None = None,
    ) -> pd.DataFrame:
        if data.empty:
            return data

        df = data.copy()

        # Auto-detect date columns if not provided
        if date_columns is None:
            date_columns = df.select_dtypes(include="datetime").columns.tolist()
        if not date_columns:
            return df

        mutations = [
            self._late_arrivals,
            self._out_of_order,
            self._timezone_mismatch,
            self._dst_boundary,
        ]
        n = min(rng.randint(1, 3), len(mutations))
        chosen = rng.choice(len(mutations), size=n, replace=False)

        for i in chosen:
            df = mutations[i](df, date_columns, rng, intensity_multiplier)

        return df

    # ------------------------------------------------------------------

    def _late_arrivals(
        self,
        df: pd.DataFrame,
        date_columns: list[str],
        rng: np.random.RandomState,
        intensity: float,
    ) -> pd.DataFrame:
        """Push some timestamps 1-30 days into the past (late-arriving data)."""
        col = rng.choice(date_columns)
        if col not in df.columns:
            return df
        frac = self._pick_fraction(0.05, intensity)
        idx = self._sample_indices(rng, len(df), frac)
        delays = rng.randint(1, 31, size=len(idx))
        for i, d in zip(idx, delays):
            val = df.iat[i, df.columns.get_loc(col)]
            if pd.notna(val):
                df.iat[i, df.columns.get_loc(col)] = val - pd.Timedelta(days=int(d))
        return df

    def _out_of_order(
        self,
        df: pd.DataFrame,
        date_columns: list[str],
        rng: np.random.RandomState,
        intensity: float,
    ) -> pd.DataFrame:
        """Swap timestamp values between random row pairs."""
        col = rng.choice(date_columns)
        if col not in df.columns or len(df) < 2:
            return df
        frac = self._pick_fraction(0.03, intensity)
        n_swaps = max(1, int(len(df) * frac) // 2)
        for _ in range(n_swaps):
            a, b = rng.choice(len(df), size=2, replace=False)
            col_loc = df.columns.get_loc(col)
            df.iat[a, col_loc], df.iat[b, col_loc] = (
                df.iat[b, col_loc],
                df.iat[a, col_loc],
            )
        return df

    @staticmethod
    def _timezone_mismatch(
        df: pd.DataFrame,
        date_columns: list[str],
        rng: np.random.RandomState,
        intensity: float,
    ) -> pd.DataFrame:
        """Shift some timestamps by common timezone offsets (as if
        timezone was wrong).
        """
        col = rng.choice(date_columns)
        if col not in df.columns:
            return df
        offsets_hours = [-5, -6, -8, 0, 1, 5, 8, 9]
        offset = int(rng.choice(offsets_hours))
        frac = min(0.05 * intensity, 0.5)
        n = max(1, int(len(df) * frac))
        idx = rng.choice(len(df), size=min(n, len(df)), replace=False)
        for i in idx:
            val = df.iat[i, df.columns.get_loc(col)]
            if pd.notna(val):
                df.iat[i, df.columns.get_loc(col)] = val + pd.Timedelta(
                    hours=offset
                )
        return df

    @staticmethod
    def _dst_boundary(
        df: pd.DataFrame,
        date_columns: list[str],
        rng: np.random.RandomState,
        intensity: float,
    ) -> pd.DataFrame:
        """Set some timestamps exactly on a DST boundary (2 AM spring-forward)."""
        col = rng.choice(date_columns)
        if col not in df.columns:
            return df
        dst_dates = [
            pd.Timestamp("2024-03-10 02:30:00"),
            pd.Timestamp("2024-11-03 01:30:00"),
            pd.Timestamp("2025-03-09 02:30:00"),
            pd.Timestamp("2025-11-02 01:30:00"),
        ]
        frac = min(0.02 * intensity, 0.3)
        n = max(1, int(len(df) * frac))
        idx = rng.choice(len(df), size=min(n, len(df)), replace=False)
        for i in idx:
            df.iat[i, df.columns.get_loc(col)] = rng.choice(dst_dates)
        return df


# ======================================================================
# Volume chaos
# ======================================================================


class VolumeChaosMutator(ChaosMutator):
    """Corrupt data volume: 10x spike, empty batch, single-row batch."""

    @property
    def category(self) -> str:
        return "volume"

    def mutate(
        self,
        data: pd.DataFrame,
        day: int,
        rng: np.random.RandomState,
        intensity_multiplier: float,
    ) -> pd.DataFrame:
        if data.empty:
            return data

        actions = ["spike", "empty", "single_row"]
        weights = np.array([0.3, 0.3, 0.4])
        action = rng.choice(actions, p=weights)

        if action == "spike":
            return self._spike(data, rng, intensity_multiplier)
        elif action == "empty":
            return self._empty(data)
        else:
            return self._single_row(data, rng)

    @staticmethod
    def _spike(
        df: pd.DataFrame,
        rng: np.random.RandomState,
        intensity: float,
    ) -> pd.DataFrame:
        """Duplicate rows to simulate a 10x volume spike."""
        multiplier = int(max(2, 10 * intensity))
        # Sample with replacement to create the spike
        extra_idx = rng.choice(len(df), size=len(df) * multiplier, replace=True)
        spike_df = df.iloc[extra_idx].reset_index(drop=True)
        return pd.concat([df, spike_df], ignore_index=True)

    @staticmethod
    def _empty(df: pd.DataFrame) -> pd.DataFrame:
        """Return an empty DataFrame with the same schema."""
        return df.iloc[:0].copy()

    @staticmethod
    def _single_row(
        df: pd.DataFrame,
        rng: np.random.RandomState,
    ) -> pd.DataFrame:
        """Return a single random row."""
        idx = rng.randint(0, len(df))
        return df.iloc[[idx]].copy().reset_index(drop=True)
