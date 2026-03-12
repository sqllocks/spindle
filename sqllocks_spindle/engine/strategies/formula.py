"""Formula strategy — compute column from other columns in the same row."""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class FormulaStrategy(Strategy):
    """Compute a column value from other columns using simple expressions.

    Supports expressions like:
        "quantity * unit_price"
        "quantity * unit_price * (1 - discount_percent / 100)"
    """

    # Allowed names in formula evaluation (safe math operations only)
    SAFE_BUILTINS = {
        "abs": abs,
        "min": min,
        "max": max,
        "round": round,
    }

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        expression = config.get("expression", "")
        if not expression:
            raise ValueError(f"formula strategy requires 'expression' for column '{column.name}'")

        # Build namespace from current table columns
        namespace = {}
        for col_name, col_values in ctx.current_table.items():
            namespace[col_name] = col_values

        # Add safe numpy operations
        namespace["np"] = np

        # Evaluate expression element-wise using numpy
        try:
            result = eval(expression, {"__builtins__": self.SAFE_BUILTINS, "np": np}, namespace)
        except Exception as e:
            raise ValueError(
                f"Failed to evaluate formula '{expression}' for column '{column.name}': {e}"
            ) from e

        if isinstance(result, (int, float)):
            result = np.full(ctx.row_count, result)
        elif not isinstance(result, np.ndarray):
            result = np.array(result)

        # Apply decimal precision
        if column.scale is not None:
            result = np.round(result, column.scale)

        return result
