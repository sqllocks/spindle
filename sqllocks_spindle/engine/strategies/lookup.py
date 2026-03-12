"""Lookup strategy — copy values from parent table via FK."""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class LookupStrategy(Strategy):
    """Look up a value from a parent table using a FK in the current row.

    Example: order_line.unit_price = product.unit_price (looked up via product_id)
    """

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        source_table = config.get("source_table", "")
        source_column = config.get("source_column", "")
        via = config.get("via", "")

        if not all([source_table, source_column, via]):
            raise ValueError(
                f"lookup strategy requires 'source_table', 'source_column', and 'via' "
                f"for column '{column.name}'"
            )

        # Get the FK values from the current table
        if via not in ctx.current_table:
            raise ValueError(
                f"lookup via column '{via}' not yet generated in current table. "
                f"Ensure '{via}' is defined before '{column.name}'"
            )

        fk_values = ctx.current_table[via]

        # Determine the PK column of the source table
        # Convention: via column name matches the PK column name in most cases
        pk_column = via

        return ctx.id_manager.lookup_values(
            table_name=source_table,
            lookup_column=source_column,
            fk_values=fk_values,
            pk_column=pk_column,
        )
