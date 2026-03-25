"""SCD Type 2 strategy — generate slowly changing dimension versioning columns.

Generates coordinated values for effective_date, end_date, is_current, and
version columns that together form a valid SCD Type 2 pattern grouped by
a business key column.

Example config:
    "effective_date": {
        "generator": {
            "strategy": "scd2",
            "role": "effective_date",
            "business_key": "customer_id",
            "avg_versions": 3,
            "min_gap_days": 1
        }
    }
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class SCD2Strategy(Strategy):
    """Generate SCD Type 2 versioning metadata columns."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        role = config.get("role", "effective_date")
        bk_col = config.get("business_key", "")
        min_gap = config.get("min_gap_days", 1)

        if not bk_col:
            raise ValueError(
                f"SCD2 strategy requires 'business_key' config for column '{column.name}'"
            )

        if bk_col not in ctx.current_table:
            raise ValueError(
                f"SCD2: business_key '{bk_col}' must be generated before SCD2 columns"
            )

        bk_values = ctx.current_table[bk_col]

        if role == "effective_date":
            return self._gen_effective_dates(bk_values, config, ctx)
        elif role == "end_date":
            return self._gen_end_dates(bk_values, config, ctx)
        elif role == "is_current":
            return self._gen_is_current(bk_values, ctx)
        elif role == "version":
            return self._gen_version(bk_values, ctx)
        else:
            raise ValueError(f"SCD2: unknown role '{role}'")

    def _group_indices(self, bk_values: np.ndarray) -> dict[Any, list[int]]:
        """Group row indices by business key value."""
        s = pd.Series(bk_values)
        return {k: list(v) for k, v in s.groupby(s).groups.items()}

    def _gen_effective_dates(
        self,
        bk_values: np.ndarray,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        """Generate effective dates — sorted ascending within each BK group."""
        date_range = ctx.model_config.get("date_range", {})
        start_str = date_range.get("start", "2022-01-01")
        end_str = date_range.get("end", "2024-12-31")
        start = pd.Timestamp(start_str)
        end = pd.Timestamp(end_str)
        total_days = (end - start).days

        min_gap = config.get("min_gap_days", 1)
        groups = self._group_indices(bk_values)

        result = np.empty(len(bk_values), dtype=object)

        for _bk, indices in groups.items():
            n_versions = len(indices)
            if n_versions == 1:
                # Single version: random date in range
                offset = int(ctx.rng.integers(0, max(total_days, 1)))
                result[indices[0]] = start + timedelta(days=offset)
            else:
                # Divide the date range into n_versions segments with gaps
                # Total gap space needed between versions
                total_gap_space = min_gap * (n_versions - 1)
                usable_days = max(total_days - total_gap_space, n_versions)

                # Generate sorted random offsets within usable range
                offsets = np.sort(
                    ctx.rng.integers(0, max(usable_days, n_versions), size=n_versions)
                )

                # Space them out to ensure minimum gaps
                for v_idx in range(n_versions):
                    day_offset = int(offsets[v_idx]) + min_gap * v_idx
                    day_offset = min(day_offset, total_days)
                    result[indices[v_idx]] = start + timedelta(days=day_offset)

        return result

    def _gen_end_dates(
        self,
        bk_values: np.ndarray,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        """Generate end dates — next version's effective_date minus gap, last is None."""
        min_gap = config.get("min_gap_days", 1)

        if "effective_date" not in ctx.current_table:
            # Try to find an effective_date column by scanning for scd2 effective dates
            raise ValueError(
                "SCD2 end_date role requires 'effective_date' to be generated first "
                "(it must appear in ctx.current_table)"
            )

        eff_dates = ctx.current_table.get("effective_date")
        # Allow custom effective_date column name via config
        eff_col = config.get("effective_date_column", "effective_date")
        if eff_col in ctx.current_table:
            eff_dates = ctx.current_table[eff_col]

        if eff_dates is None:
            raise ValueError(
                "SCD2 end_date role requires effective_date column in current_table"
            )

        groups = self._group_indices(bk_values)
        result = np.empty(len(bk_values), dtype=object)

        for _bk, indices in groups.items():
            # Sort indices by their effective date
            sorted_indices = sorted(indices, key=lambda i: eff_dates[i])

            for pos, idx in enumerate(sorted_indices):
                if pos < len(sorted_indices) - 1:
                    next_eff = eff_dates[sorted_indices[pos + 1]]
                    result[idx] = next_eff - timedelta(days=min_gap)
                else:
                    # Last (current) version has no end date
                    result[idx] = None

        return result

    def _gen_is_current(
        self,
        bk_values: np.ndarray,
        ctx: GenerationContext,
    ) -> np.ndarray:
        """Generate is_current flag — True only for the latest version per BK."""
        # Determine ordering: prefer effective_date if available, else use row position
        eff_dates = None
        for col_name in ("effective_date",):
            if col_name in ctx.current_table:
                eff_dates = ctx.current_table[col_name]
                break

        groups = self._group_indices(bk_values)
        result = np.zeros(len(bk_values), dtype=object)

        for _bk, indices in groups.items():
            if eff_dates is not None:
                sorted_indices = sorted(indices, key=lambda i: eff_dates[i])
            else:
                sorted_indices = indices

            for idx in sorted_indices:
                result[idx] = False
            # Last one is current
            result[sorted_indices[-1]] = True

        return result

    def _gen_version(
        self,
        bk_values: np.ndarray,
        ctx: GenerationContext,
    ) -> np.ndarray:
        """Generate sequential version numbers (1, 2, 3, ...) per BK group."""
        eff_dates = None
        for col_name in ("effective_date",):
            if col_name in ctx.current_table:
                eff_dates = ctx.current_table[col_name]
                break

        groups = self._group_indices(bk_values)
        result = np.empty(len(bk_values), dtype=object)

        for _bk, indices in groups.items():
            if eff_dates is not None:
                sorted_indices = sorted(indices, key=lambda i: eff_dates[i])
            else:
                sorted_indices = indices

            for ver, idx in enumerate(sorted_indices, start=1):
                result[idx] = ver

        return result
