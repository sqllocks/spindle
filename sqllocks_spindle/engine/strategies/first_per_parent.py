"""First-per-parent strategy — mark the first child row per parent group.

Generates a boolean column where the first row for each parent FK value is True
and all subsequent rows are False. Used for flags like address.is_primary.

Example:
    "is_primary": {
        "generator": {
            "strategy": "first_per_parent",
            "parent_column": "customer_id",
            "default": true
        }
    }
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class FirstPerParentStrategy(Strategy):
    """Mark the first row per parent FK group as True, rest as False."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        parent_column = config.get("parent_column")
        if not parent_column:
            raise ValueError(
                f"first_per_parent strategy requires 'parent_column' "
                f"for column '{column.name}'"
            )

        if parent_column not in ctx.current_table:
            raise ValueError(
                f"first_per_parent: parent_column '{parent_column}' has not been "
                f"generated yet. Ensure FK columns are generated before "
                f"first_per_parent columns."
            )

        default = config.get("default", True)
        parent_values = ctx.current_table[parent_column]
        is_first = ~pd.Series(parent_values).duplicated(keep='first').values
        result = np.full(ctx.row_count, not default, dtype=bool)
        result[is_first] = default

        return result
