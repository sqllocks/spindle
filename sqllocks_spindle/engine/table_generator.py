"""Per-table row generation."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.engine.id_manager import IDManager
from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy, StrategyRegistry
from sqllocks_spindle.schema.parser import ColumnDef, SpindleSchema, TableDef


class TableGenerator:
    """Generate all rows for a single table."""

    def __init__(self, registry: StrategyRegistry, id_manager: IDManager):
        self._registry = registry
        self._id_manager = id_manager

    def generate(
        self,
        table: TableDef,
        row_count: int,
        rng: np.random.Generator,
        model_config: dict[str, Any],
        schema: SpindleSchema,
    ) -> pd.DataFrame:
        """Generate a DataFrame for a single table."""
        ctx = GenerationContext(
            rng=rng,
            id_manager=self._id_manager,
            model_config=model_config,
            row_count=row_count,
        )
        ctx.current_table_name = table.name

        # Determine column generation order:
        # 1. PKs first (sequences, UUIDs)
        # 2. FK columns next (need parent data)
        # 3. Independent columns
        # 4. Dependent columns (formulas, lookups, correlated)
        # 5. Computed columns last (placeholders)
        ordered_columns = self._order_columns(table)

        for col_name in ordered_columns:
            col = table.columns[col_name]
            strategy_name = col.generator.get("strategy", "")

            if not strategy_name:
                # No generator — fill with None
                ctx.current_table[col_name] = np.full(row_count, None, dtype=object)
                continue

            if not self._registry.has(strategy_name):
                raise ValueError(
                    f"Unknown strategy '{strategy_name}' for "
                    f"column '{table.name}.{col_name}'"
                )

            strategy = self._registry.get(strategy_name)
            values = strategy.generate(col, col.generator, ctx)

            # Apply null masking
            if col.nullable and col.null_rate > 0:
                values = strategy.apply_nulls(values, col, ctx)

            ctx.current_table[col_name] = values

        # Build DataFrame — exclude internal cache keys (_rs_*, _sr_*)
        public_columns = {
            k: v for k, v in ctx.current_table.items()
            if not k.startswith("_rs_") and not k.startswith("_sr_")
        }
        df = pd.DataFrame(public_columns)

        # Register PKs in ID manager
        self._id_manager.register_table(table.name, df, table.primary_key)

        return df

    def _order_columns(self, table: TableDef) -> list[str]:
        """Order columns by generation dependency."""
        pk_cols = []
        fk_cols = []
        independent_cols = []
        dependent_cols = []
        computed_cols = []

        for col_name, col in table.columns.items():
            strategy = col.generator.get("strategy", "")

            if col_name in table.primary_key and strategy in ("sequence", "uuid"):
                pk_cols.append(col_name)
            elif strategy == "foreign_key":
                fk_cols.append(col_name)
            elif strategy in (
                "formula", "lookup", "derived", "computed",
                "first_per_parent", "record_field", "self_ref_field",
                "correlated", "conditional",
            ):
                if strategy == "computed":
                    computed_cols.append(col_name)
                else:
                    dependent_cols.append(col_name)
            else:
                independent_cols.append(col_name)

        # Handle PK columns that aren't sequence/uuid (e.g., FK-based PKs)
        remaining_pks = [
            c for c in table.primary_key
            if c not in pk_cols and c not in fk_cols
        ]
        pk_cols.extend(remaining_pks)

        return pk_cols + fk_cols + independent_cols + dependent_cols + computed_cols
