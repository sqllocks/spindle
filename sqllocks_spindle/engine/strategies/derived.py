"""Derived strategy — compute a column's values from another column.

Supports two modes:

1. **Same-table**: derive from another column already generated in this table.
   Example: end_date = start_date + uniform(3, 30) days

2. **Cross-table via FK**: derive from a column in a parent table, joined via
   a FK column already generated in this table.
   Example: return_date = order.order_date + log_normal(mean=2, sigma=0.8) days

Schema examples:

    # Same-table: end_date from start_date
    "end_date": {
        "generator": {
            "strategy": "derived",
            "source": "start_date",
            "rule": "add_days",
            "params": {"distribution": "uniform", "min": 3, "max": 30}
        }
    }

    # Cross-table: return_date from order.order_date via order_id
    "return_date": {
        "generator": {
            "strategy": "derived",
            "source": "order.order_date",
            "via": "order_id",
            "rule": "add_days",
            "params": {"distribution": "log_normal", "mean": 2.0, "sigma": 0.8, "min": 1, "max": 90}
        }
    }

Supported rules:
    - add_days: add a random number of days drawn from the given distribution
    - copy: copy the source value unchanged
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


def _sample_days(rng: np.random.Generator, params: dict, count: int) -> np.ndarray:
    """Sample day offsets using the specified distribution."""
    dist = params.get("distribution", "uniform")
    lo = float(params.get("min", 1))
    hi = float(params.get("max", 30))

    if dist == "uniform":
        days = rng.uniform(lo, hi, size=count)
    elif dist == "log_normal":
        mean = float(params.get("mean", 2.0))
        sigma = float(params.get("sigma", 0.8))
        raw = rng.lognormal(mean, sigma, size=count)
        days = np.clip(raw, lo, hi)
    elif dist == "normal":
        mean = float(params.get("mean", 10.0))
        std = float(params.get("std_dev", 3.0))
        raw = rng.normal(mean, std, size=count)
        days = np.clip(raw, lo, hi)
    else:
        days = rng.uniform(lo, hi, size=count)

    return np.round(days).astype(int)


class DerivedStrategy(Strategy):
    """Derive a column's values from another column with an optional transformation."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        source = config.get("source", "")
        via = config.get("via")  # FK column name for cross-table lookups
        rule = config.get("rule", "copy")
        params = config.get("params", {})

        # Resolve source values
        if "." in source and via:
            # Cross-table: "order.order_date" via "order_id"
            # Convention: FK column name == PK column name in parent table
            ref_table, ref_col = source.split(".", 1)
            if via not in ctx.current_table:
                raise KeyError(
                    f"derived strategy for '{column.name}': via column '{via}' "
                    f"not yet generated in current table."
                )
            fk_values = ctx.current_table[via]
            source_values = ctx.id_manager.lookup_values(
                table_name=ref_table,
                lookup_column=ref_col,
                fk_values=fk_values,
                pk_column=via,
            )
        elif source in ctx.current_table:
            # Same-table: source column already generated
            source_values = ctx.current_table[source]
        else:
            raise KeyError(
                f"derived strategy for '{column.name}': source '{source}' not found. "
                f"Ensure the source column appears before '{column.name}' in the schema."
            )

        if rule == "copy":
            return np.array(source_values)

        if rule == "add_days":
            days = _sample_days(ctx.rng, params, len(source_values))
            result = np.empty(len(source_values), dtype="datetime64[ns]")
            for i, (base, d) in enumerate(zip(source_values, days)):
                if base is None or (isinstance(base, float) and np.isnan(base)):
                    result[i] = np.datetime64("NaT")
                else:
                    ts = pd.Timestamp(base) + pd.Timedelta(days=int(d))
                    result[i] = np.datetime64(ts)
            return result

        raise ValueError(
            f"derived strategy: unknown rule '{rule}' for column '{column.name}'. "
            f"Supported rules: copy, add_days"
        )
