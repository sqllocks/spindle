"""Fabric environment detection and partition spec parsing."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class FabricEnvironment:
    """Result of Fabric environment detection."""

    is_fabric: bool
    lakehouse_path: Path | None = None
    default_tables_path: Path | None = None


def detect_fabric_environment() -> FabricEnvironment:
    """Detect whether we are running inside a Microsoft Fabric Notebook.

    Checks for:
    1. ``FABRIC_RUNTIME`` or ``TRIDENT_RUNTIME_VERSION`` environment variables.
    2. Existence of ``/lakehouse/default/`` directory.

    Returns:
        A :class:`FabricEnvironment` with detection results.
    """
    fabric_vars = ("FABRIC_RUNTIME", "TRIDENT_RUNTIME_VERSION")
    has_env = any(os.environ.get(v) for v in fabric_vars)

    lakehouse = Path("/lakehouse/default")
    has_path = lakehouse.exists() and lakehouse.is_dir()

    if has_env or has_path:
        tables_path = lakehouse / "Tables"
        return FabricEnvironment(
            is_fabric=True,
            lakehouse_path=lakehouse,
            default_tables_path=tables_path,
        )

    return FabricEnvironment(is_fabric=False)


_EXTRACTION_MAP = {
    "year": lambda s: s.dt.year,
    "month": lambda s: s.dt.month,
    "day": lambda s: s.dt.day,
    "quarter": lambda s: s.dt.quarter,
    "week": lambda s: s.dt.isocalendar().week.astype("int32"),
}


def parse_partition_spec(
    specs: list[str],
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    """Parse partition specs and add computed partition columns.

    Each spec is ``"column_name:extraction"`` where extraction is one of
    ``year``, ``month``, ``day``, ``quarter``, ``week``.

    A spec without a colon (e.g. ``"region"``) is treated as a plain column
    name — no extraction is applied, the column is used as-is.

    Args:
        specs: List of partition specs, e.g. ``["order_date:year", "order_date:month"]``.
        df: Source DataFrame (not mutated).

    Returns:
        A tuple of ``(df_with_partition_cols, partition_column_names)``.

    Raises:
        ValueError: If a source column is missing or extraction is unknown.
    """
    if not specs:
        return df, []

    df = df.copy()
    partition_cols: list[str] = []

    for spec in specs:
        if ":" not in spec:
            # Plain column — use as-is for partitioning
            if spec not in df.columns:
                raise ValueError(
                    f"Partition column '{spec}' not found in DataFrame. "
                    f"Available: {list(df.columns)}"
                )
            partition_cols.append(spec)
            continue

        source_col, extraction = spec.split(":", 1)

        if source_col not in df.columns:
            raise ValueError(
                f"Partition source column '{source_col}' not found in DataFrame. "
                f"Available: {list(df.columns)}"
            )

        if extraction not in _EXTRACTION_MAP:
            raise ValueError(
                f"Unknown partition extraction '{extraction}'. "
                f"Supported: {sorted(_EXTRACTION_MAP.keys())}"
            )

        col_name = f"{source_col}_{extraction}"
        series = pd.to_datetime(df[source_col])
        df[col_name] = _EXTRACTION_MAP[extraction](series)
        partition_cols.append(col_name)

    return df, partition_cols
