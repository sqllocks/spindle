"""Weighted enum strategy — pick from weighted value lists."""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class WeightedEnumStrategy(Strategy):
    """Pick from a weighted list of values."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        values_dict = config.get("values", {})
        if not values_dict:
            raise ValueError(f"weighted_enum strategy requires 'values' dict for column '{column.name}'")

        labels = list(values_dict.keys())
        weights = np.array(list(values_dict.values()), dtype=float)

        # Normalize weights to sum to 1
        weight_sum = weights.sum()
        if weight_sum > 0:
            weights = weights / weight_sum

        indices = ctx.rng.choice(len(labels), size=ctx.row_count, p=weights)
        labels_arr = np.array(labels, dtype=object)
        chosen = labels_arr[indices]

        # If all values are numeric strings, return a float array for formula compatibility
        try:
            numeric = np.array([float(v) for v in labels], dtype=float)
            return numeric[indices]
        except (TypeError, ValueError):
            return chosen
