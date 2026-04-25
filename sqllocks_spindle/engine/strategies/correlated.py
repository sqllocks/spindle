"""Correlated strategy — derive a value from another column in the same row.

Produces a value that is mathematically related to a source column, useful for
cost/price relationships, margin calculations, derived metrics, etc.

Supported rules:
    multiply        — result = source * random_factor in [factor_min, factor_max]
    add             — result = source + random_offset in [offset_min, offset_max]
    subtract        — result = source - random_offset (clipped to >= 0)

Example: cost correlated with unit_price
    "cost": {
        "generator": {
            "strategy": "correlated",
            "source_column": "unit_price",
            "rule": "multiply",
            "params": {"factor_min": 0.30, "factor_max": 0.70}
        }
    }
"""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class CorrelatedStrategy(Strategy):
    """Generate a column whose values are derived from another column in the same row."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        source_col = config.get("source_column", "")
        rule = config.get("rule", "multiply")
        params = config.get("params", {})

        if not source_col:
            raise ValueError(
                f"correlated strategy requires 'source_column' for column '{column.name}'"
            )

        if source_col not in ctx.current_table:
            raise KeyError(
                f"correlated strategy for '{column.name}': source column "
                f"'{source_col}' not yet generated. Ensure it appears before "
                f"'{column.name}' in the schema definition."
            )

        source = np.array(ctx.current_table[source_col], dtype=float)
        n = len(source)

        if rule == "multiply":
            lo = float(params.get("factor_min", params.get("min", 0.30)))
            hi = float(params.get("factor_max", params.get("max", 0.70)))
            factors = ctx.rng.uniform(lo, hi, size=n)
            result = source * factors

        elif rule == "add":
            lo = float(params.get("offset_min", params.get("min", 0.0)))
            hi = float(params.get("offset_max", params.get("max", 10.0)))
            offsets = ctx.rng.uniform(lo, hi, size=n)
            result = source + offsets

        elif rule == "subtract":
            lo = float(params.get("offset_min", params.get("min", 0.0)))
            hi = float(params.get("offset_max", params.get("max", 10.0)))
            offsets = ctx.rng.uniform(lo, hi, size=n)
            result = np.maximum(0.0, source - offsets)

        else:
            raise ValueError(
                f"correlated strategy: unknown rule '{rule}' for column '{column.name}'. "
                f"Supported: multiply, add, subtract"
            )

        # Apply precision rounding if specified
        scale = column.scale if column.scale is not None else 2
        return np.round(result, scale)
