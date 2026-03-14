"""Time-travel snapshot generation for Spindle — C5 monthly point-in-time snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class TimeTravelConfig:
    """Configuration for time-travel snapshot generation."""

    months: int = 12  # number of monthly snapshots
    start_date: str = "2023-01-01"
    growth_rate: float = 0.05  # monthly customer/entity growth rate
    seasonality: dict[int, float] = field(default_factory=dict)
    # seasonality: month -> multiplier (e.g., {11: 1.5, 12: 2.0} for holiday spike)
    churn_rate: float = 0.02  # monthly soft-delete rate
    update_fraction: float = 0.1  # monthly update fraction
    seed: int = 42


@dataclass
class Snapshot:
    """A point-in-time snapshot of the dataset."""

    snapshot_date: str  # YYYY-MM-DD
    month_index: int  # 0-based
    tables: dict[str, pd.DataFrame]
    row_counts: dict[str, int]


@dataclass
class TimeTravelResult:
    """Result of time-travel generation."""

    snapshots: list[Snapshot]
    domain_name: str
    config: TimeTravelConfig

    def summary(self) -> str:
        lines = [
            "Time-Travel Result",
            "=" * 50,
            f"Domain: {self.domain_name}",
            f"Snapshots: {len(self.snapshots)}",
            "",
            f"  {'Month':<15} {'Date':<12} {'Tables':>7} {'Rows':>10}",
            f"  {'-' * 46}",
        ]
        for snap in self.snapshots:
            total_rows = sum(snap.row_counts.values())
            lines.append(
                f"  Month {snap.month_index:<8} {snap.snapshot_date:<12} "
                f"{len(snap.tables):>7} {total_rows:>10,}"
            )
        return "\n".join(lines)

    def get_snapshot(self, month: int) -> Snapshot:
        """Get snapshot by month index."""
        return self.snapshots[month]

    def to_partitioned_dfs(self) -> dict[str, pd.DataFrame]:
        """Return all snapshots combined with a _snapshot_date column."""
        combined: dict[str, list[pd.DataFrame]] = {}
        for snap in self.snapshots:
            for table_name, df in snap.tables.items():
                df_copy = df.copy()
                df_copy["_snapshot_date"] = snap.snapshot_date
                if table_name not in combined:
                    combined[table_name] = [df_copy]
                else:
                    combined[table_name].append(df_copy)
        return {
            name: pd.concat(dfs, ignore_index=True)
            for name, dfs in combined.items()
        }


class TimeTravelEngine:
    """Generate monthly point-in-time snapshots showing data evolution."""

    def generate(
        self,
        domain: Any,  # Domain instance
        config: TimeTravelConfig | None = None,
        scale: str = "small",
    ) -> TimeTravelResult:
        """Generate monthly snapshots of a domain's data.

        Month 0 is the initial dataset. Each subsequent month applies:
        - New entity growth (inserts) at growth_rate * seasonality multiplier
        - Status transitions (updates) at update_fraction rate
        - Churn (soft deletes) at churn_rate

        Returns a TimeTravelResult with N+1 snapshots (month 0 through month N).
        """
        from sqllocks_spindle.engine.generator import Spindle

        config = config or TimeTravelConfig()
        rng = np.random.default_rng(config.seed)

        # Generate initial dataset (month 0)
        spindle = Spindle()
        initial = spindle.generate(domain=domain, scale=scale, seed=config.seed)

        snapshots: list[Snapshot] = []
        current_tables = {name: df.copy() for name, df in initial.tables.items()}

        # Snapshot 0
        start = pd.Timestamp(config.start_date)
        snapshots.append(
            Snapshot(
                snapshot_date=start.strftime("%Y-%m-%d"),
                month_index=0,
                tables={name: df.copy() for name, df in current_tables.items()},
                row_counts={name: len(df) for name, df in current_tables.items()},
            )
        )

        # Generate each subsequent month
        for month_idx in range(1, config.months + 1):
            snap_date = start + pd.DateOffset(months=month_idx)
            month_num = snap_date.month  # 1-12

            # Seasonality multiplier
            season_mult = config.seasonality.get(month_num, 1.0)

            for table_name, df in current_tables.items():
                # Find PK column (first column or integer column named *_id)
                pk_col = self._find_pk(df)
                if pk_col is None:
                    continue

                # --- GROWTH: add new rows ---
                n_new = max(1, int(len(df) * config.growth_rate * season_mult))
                new_rows = self._generate_new_rows(df, n_new, pk_col, rng)

                # --- CHURN: remove rows ---
                n_churn = int(len(df) * config.churn_rate)
                if n_churn > 0:
                    churn_idx = rng.choice(
                        len(df), size=min(n_churn, len(df)), replace=False
                    )
                    df = df.drop(df.index[churn_idx]).reset_index(drop=True)

                # --- UPDATES: modify some rows ---
                n_update = int(len(df) * config.update_fraction)
                if n_update > 0:
                    update_idx = rng.choice(
                        len(df), size=min(n_update, len(df)), replace=False
                    )
                    df = self._apply_updates(df, update_idx, pk_col, rng)

                # Combine: existing (after churn) + new
                current_tables[table_name] = pd.concat(
                    [df, new_rows], ignore_index=True
                )

            snapshots.append(
                Snapshot(
                    snapshot_date=snap_date.strftime("%Y-%m-%d"),
                    month_index=month_idx,
                    tables={
                        name: df.copy() for name, df in current_tables.items()
                    },
                    row_counts={
                        name: len(df) for name, df in current_tables.items()
                    },
                )
            )

        return TimeTravelResult(
            snapshots=snapshots,
            domain_name=domain.name if hasattr(domain, "name") else "unknown",
            config=config,
        )

    def _find_pk(self, df: pd.DataFrame) -> str | None:
        """Look for column ending in _id that's integer and unique."""
        for col in df.columns:
            if col.endswith("_id") and pd.api.types.is_integer_dtype(df[col]):
                if df[col].is_unique:
                    return col
        # Fallback: first column
        return df.columns[0] if len(df.columns) > 0 else None

    def _generate_new_rows(
        self,
        df: pd.DataFrame,
        n: int,
        pk_col: str,
        rng: np.random.Generator,
    ) -> pd.DataFrame:
        """Sample existing rows and assign new PKs."""
        sample_idx = rng.choice(len(df), size=n, replace=True)
        new_rows = df.iloc[sample_idx].copy().reset_index(drop=True)
        if pd.api.types.is_integer_dtype(df[pk_col]):
            max_pk = df[pk_col].max()
            new_rows[pk_col] = range(int(max_pk) + 1, int(max_pk) + 1 + n)
        return new_rows

    def _apply_updates(
        self,
        df: pd.DataFrame,
        indices: np.ndarray,
        pk_col: str,
        rng: np.random.Generator,
    ) -> pd.DataFrame:
        """For numeric non-PK, non-FK columns: perturb by +/-10%."""
        df = df.copy()
        for col in df.columns:
            if col == pk_col or col.endswith("_id"):
                continue
            if pd.api.types.is_numeric_dtype(df[col]):
                noise = rng.uniform(0.9, 1.1, size=len(indices))
                vals = df[col].values.copy()
                orig = vals[indices]
                # Handle potential None/NaN
                mask = pd.notna(orig)
                valid_indices = indices[mask]
                if len(valid_indices) > 0:
                    vals[valid_indices] = (
                        vals[valid_indices].astype(float)
                        * noise[: len(valid_indices)]
                    ).astype(vals.dtype)
                    df[col] = vals
                break  # Only perturb one column per update pass
        return df
