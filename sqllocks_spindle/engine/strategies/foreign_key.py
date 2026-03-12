"""Foreign key strategy — reference parent table PKs."""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class ForeignKeyStrategy(Strategy):
    """Generate foreign key values by referencing parent table PKs."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        ref = config.get("ref", "")
        if "." not in ref:
            raise ValueError(
                f"foreign_key strategy requires 'ref' in format 'table.column', "
                f"got '{ref}' for column '{column.name}'"
            )

        ref_table, ref_column = ref.split(".", 1)
        distribution = config.get("distribution", "uniform")
        params = config.get("params", {})

        # Check for constrained FK (e.g., address must belong to same customer)
        constrained_by = config.get("constrained_by")
        if constrained_by and constrained_by in ctx.current_table:
            return ctx.id_manager.get_constrained_fks(
                table_name=ref_table,
                constraint_column=constrained_by,
                constraint_values=ctx.current_table[constrained_by],
                nullable=column.nullable,
            )

        # Check for sample_rate (e.g., only 8% of orders get returned)
        sample_rate = config.get("sample_rate")
        if sample_rate is not None:
            filter_col = config.get("filter", "").split("=")[0].strip() if "=" in config.get("filter", "") else None
            filter_val = config.get("filter", "").split("=")[1].strip().strip("'\"") if "=" in config.get("filter", "") else None
            return ctx.id_manager.get_sampled_fks(
                table_name=ref_table,
                sample_rate=sample_rate,
                filter_column=filter_col,
                filter_value=filter_val,
            )

        return ctx.id_manager.get_random_fks(
            table_name=ref_table,
            count=ctx.row_count,
            distribution=distribution,
            params=params,
        )
