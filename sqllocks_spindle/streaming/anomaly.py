"""Anomaly injection for the Spindle streaming engine.

Three anomaly types (matching the taxonomy from the design spec):

- :class:`PointAnomaly`      — single row with an extreme column value
- :class:`ContextualAnomaly` — normal value placed in the wrong context
- :class:`CollectiveAnomaly` — a cluster of rows that is abnormal together

Combine them in an :class:`AnomalyRegistry` and call :meth:`AnomalyRegistry.inject`
on any DataFrame before streaming.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd


class Anomaly(ABC):
    """Base class for anomaly definitions."""

    @property
    @abstractmethod
    def anomaly_type(self) -> str:
        """Short label used in the ``_spindle_anomaly_type`` column."""
        ...

    @property
    @abstractmethod
    def fraction(self) -> float:
        """Fraction of eligible rows (or groups) to mark as anomalous."""
        ...

    @abstractmethod
    def inject(self, df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        """Inject anomalies into *df* and return a modified copy."""
        ...


@dataclass
class PointAnomaly(Anomaly):
    """Single-row anomaly: a column value that is far outside the normal range.

    Example: an order total of $99,999 when the average is $50.

    Args:
        name: Short label, used in ``_spindle_anomaly_type``.
        column: Column to modify.
        multiplier_range: ``(min, max)`` multiplier applied to the column mean
            to produce the anomalous value.
        fraction: Fraction of rows to mark (default 0.01 = 1%).
    """

    name: str
    column: str
    multiplier_range: tuple[float, float] = (10.0, 100.0)
    _fraction: float = field(default=0.01, repr=False)

    def __init__(
        self,
        name: str,
        column: str,
        multiplier_range: tuple[float, float] = (10.0, 100.0),
        fraction: float = 0.01,
    ) -> None:
        self.name = name
        self.column = column
        self.multiplier_range = multiplier_range
        self._fraction = fraction

    @property
    def anomaly_type(self) -> str:
        return f"point:{self.name}"

    @property
    def fraction(self) -> float:
        return self._fraction

    def inject(self, df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        if self.column not in df.columns or df.empty:
            return df

        n = max(1, int(len(df) * self._fraction))
        idx = rng.choice(len(df), size=n, replace=False)

        df = df.copy()
        col = pd.to_numeric(df[self.column], errors="coerce")
        baseline = float(col.abs().mean()) if col.notna().any() else 1.0
        if baseline == 0.0:
            baseline = 1.0

        multipliers = rng.uniform(self.multiplier_range[0], self.multiplier_range[1], size=n)
        noise = rng.uniform(0.8, 1.2, size=n)
        anomalous = (baseline * multipliers * noise).round(2)

        # Cast column to float to avoid pandas int64 upcast errors
        if pd.api.types.is_integer_dtype(df[self.column]):
            df[self.column] = df[self.column].astype(float)
        df.iloc[idx, df.columns.get_loc(self.column)] = anomalous
        df.loc[df.index[idx], "_spindle_is_anomaly"] = True
        df.loc[df.index[idx], "_spindle_anomaly_type"] = self.anomaly_type
        return df


@dataclass
class ContextualAnomaly(Anomaly):
    """Normal value placed in the wrong context.

    Example: a winter coat sold in July, or a NULL shipping address on a
    delivered order.

    Args:
        name: Short label, used in ``_spindle_anomaly_type``.
        column: Column whose value will be replaced with an anomalous value.
        condition_column: Column used to identify "normal" rows to corrupt.
        normal_values: Values of ``condition_column`` that make a row eligible.
        anomalous_values: Replacement values to write into ``column``.
        fraction: Fraction of eligible rows to corrupt (default 0.01).
    """

    name: str
    column: str
    condition_column: str
    normal_values: list[Any]
    anomalous_values: list[Any]

    def __init__(
        self,
        name: str,
        column: str,
        condition_column: str,
        normal_values: list[Any],
        anomalous_values: list[Any],
        fraction: float = 0.01,
    ) -> None:
        self.name = name
        self.column = column
        self.condition_column = condition_column
        self.normal_values = list(normal_values)
        self.anomalous_values = list(anomalous_values)
        self._fraction = fraction

    @property
    def anomaly_type(self) -> str:
        return f"contextual:{self.name}"

    @property
    def fraction(self) -> float:
        return self._fraction

    def inject(self, df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        if (
            self.column not in df.columns
            or self.condition_column not in df.columns
            or df.empty
        ):
            return df

        mask = df[self.condition_column].isin(self.normal_values)
        eligible = df.index[mask].tolist()
        if not eligible:
            return df

        n = max(1, int(len(eligible) * self._fraction))
        n = min(n, len(eligible))
        chosen_pos = rng.choice(len(eligible), size=n, replace=False)
        chosen_idx = [eligible[i] for i in chosen_pos]

        df = df.copy()
        replacements = rng.choice(self.anomalous_values, size=n)
        df.loc[chosen_idx, self.column] = replacements
        df.loc[chosen_idx, "_spindle_is_anomaly"] = True
        df.loc[chosen_idx, "_spindle_anomaly_type"] = self.anomaly_type
        return df


@dataclass
class CollectiveAnomaly(Anomaly):
    """A sequence of rows that is abnormal together.

    Example: 47 orders from the same customer within 10 minutes (velocity fraud).
    All rows in the affected group have their timestamp compressed into a short
    window and are labelled as anomalous.

    Args:
        name: Short label, used in ``_spindle_anomaly_type``.
        group_column: Column whose distinct values define groups (e.g. customer_id).
        timestamp_column: Datetime column to compress into a short window.
        window_seconds: Duration of the burst window (default 600 s = 10 min).
        fraction: Fraction of groups to corrupt (default 0.005).
    """

    name: str
    group_column: str
    timestamp_column: str

    def __init__(
        self,
        name: str,
        group_column: str,
        timestamp_column: str,
        window_seconds: float = 600.0,
        fraction: float = 0.005,
    ) -> None:
        self.name = name
        self.group_column = group_column
        self.timestamp_column = timestamp_column
        self.window_seconds = window_seconds
        self._fraction = fraction

    @property
    def anomaly_type(self) -> str:
        return f"collective:{self.name}"

    @property
    def fraction(self) -> float:
        return self._fraction

    def inject(self, df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        if (
            self.group_column not in df.columns
            or self.timestamp_column not in df.columns
            or df.empty
        ):
            return df

        df = df.copy()
        groups = df[self.group_column].dropna().unique()
        n_groups = max(1, int(len(groups) * self._fraction))
        n_groups = min(n_groups, len(groups))
        chosen_groups = rng.choice(groups, size=n_groups, replace=False)

        base_ts = pd.Timestamp("2024-01-15 14:00:00")

        for grp in chosen_groups:
            group_mask = df[self.group_column] == grp
            group_idx = df.index[group_mask].tolist()
            if len(group_idx) < 2:
                continue
            offsets = np.sort(rng.uniform(0, self.window_seconds, size=len(group_idx)))
            # Round to microseconds to stay compatible with datetime64[us] columns
            new_ts = [
                base_ts + pd.Timedelta(microseconds=int(o * 1_000_000))
                for o in offsets
            ]
            df.loc[group_idx, self.timestamp_column] = new_ts
            df.loc[group_idx, "_spindle_is_anomaly"] = True
            df.loc[group_idx, "_spindle_anomaly_type"] = self.anomaly_type

        return df


class AnomalyRegistry:
    """Registry of anomaly definitions applied during streaming.

    Example::

        registry = AnomalyRegistry([
            PointAnomaly("extreme_total", column="total_amount"),
            ContextualAnomaly(
                "winter_in_summer",
                column="product_category",
                condition_column="order_month",
                normal_values=[6, 7, 8],
                anomalous_values=["Winter Coats", "Heavy Jackets"],
            ),
        ])

        # Apply to any DataFrame before streaming
        df_labelled = registry.inject(df, rng)
    """

    def __init__(self, anomalies: list[Anomaly] | None = None) -> None:
        self._anomalies: list[Anomaly] = list(anomalies or [])

    def add(self, anomaly: Anomaly) -> "AnomalyRegistry":
        """Register an anomaly and return ``self`` for chaining."""
        self._anomalies.append(anomaly)
        return self

    def inject(self, df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        """Apply all registered anomalies to *df* and return a labelled copy.

        Label columns ``_spindle_is_anomaly`` and ``_spindle_anomaly_type`` are
        always present in the returned DataFrame, even when the registry is empty.
        """
        if df.empty:
            return df

        df = df.copy()
        # Initialise label columns so they always exist after inject()
        df["_spindle_is_anomaly"] = False
        df["_spindle_anomaly_type"] = pd.NA

        for anomaly in self._anomalies:
            df = anomaly.inject(df, rng)

        return df

    def __len__(self) -> int:
        return len(self._anomalies)

    def __repr__(self) -> str:
        names = [a.anomaly_type for a in self._anomalies]
        return f"AnomalyRegistry({names})"
