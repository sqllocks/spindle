"""Lifecycle strategy — assign phase-based status values.

Identical to weighted_enum but uses "phases" as the config key, which is more
semantically meaningful for product/entity lifecycle columns.

Example:
    "product_status": {
        "generator": {
            "strategy": "lifecycle",
            "phases": {
                "introduced": 0.10,
                "active": 0.75,
                "discontinued": 0.15
            }
        }
    }
"""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class LifecycleStrategy(Strategy):
    """Generate phase labels based on weighted probabilities."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        phases = config.get("phases", config.get("values", {}))
        if not phases:
            raise ValueError(
                f"lifecycle strategy requires a 'phases' dict for column '{column.name}'"
            )

        labels = list(phases.keys())
        weights = np.array(list(phases.values()), dtype=float)
        weights /= weights.sum()

        indices = ctx.rng.choice(len(labels), size=ctx.row_count, p=weights)
        return np.array(labels, dtype=object)[indices]
