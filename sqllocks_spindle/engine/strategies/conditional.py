"""Conditional strategy — generate values based on a per-row condition.

Evaluates a condition on already-generated columns and applies one of two
inline generator configs, selecting the result per row.

Supported condition forms:
    "column IS NOT NULL"
    "column IS NULL"
    "column == value"
    "column != value"

Inline generator types for true_generator / false_generator:
    {"fixed": value}                       — constant for matching rows
    {"strategy": "lookup", ...}            — same params as LookupStrategy

Example:
    "discount_percent": {
        "generator": {
            "strategy": "conditional",
            "condition": "promotion_id IS NOT NULL",
            "true_generator": {
                "strategy": "lookup",
                "source_table": "promotion",
                "source_column": "discount_pct",
                "via": "promotion_id"
            },
            "false_generator": {"fixed": 0.0}
        }
    }
"""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


def _is_null(value) -> bool:
    if value is None:
        return True
    try:
        return bool(np.isnan(value))
    except (TypeError, ValueError):
        return False


class ConditionalStrategy(Strategy):
    """Generate column values conditionally based on another column's state."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        condition = config.get("condition", "")
        true_gen = config.get("true_generator", {})
        false_gen = config.get("false_generator", {})

        # Evaluate condition: one bool per row
        mask = self._evaluate_condition(condition, ctx)

        # Generate all-rows for both branches, select per-row
        true_vals = self._run_generator(true_gen, ctx)
        false_vals = self._run_generator(false_gen, ctx)

        # np.where requires compatible dtypes; cast to object if mixed
        try:
            return np.where(mask, true_vals, false_vals).astype(float)
        except (TypeError, ValueError):
            result = np.empty(ctx.row_count, dtype=object)
            result[mask] = np.asarray(true_vals)[mask]
            result[~mask] = np.asarray(false_vals)[~mask]
            return result

    def _evaluate_condition(self, condition: str, ctx: GenerationContext) -> np.ndarray:
        condition = condition.strip()

        if "IS NOT NULL" in condition.upper():
            col_name = self._find_column(condition.upper().replace("IS NOT NULL", "").strip(), ctx)
            if col_name not in ctx.current_table:
                return np.ones(ctx.row_count, dtype=bool)
            vals = ctx.current_table[col_name]
            return np.array([not _is_null(v) for v in vals])

        if "IS NULL" in condition.upper():
            col_name = self._find_column(condition.upper().replace("IS NULL", "").strip(), ctx)
            if col_name not in ctx.current_table:
                return np.zeros(ctx.row_count, dtype=bool)
            vals = ctx.current_table[col_name]
            return np.array([_is_null(v) for v in vals])

        for op in ("!=", "=="):
            if op in condition:
                parts = condition.split(op, 1)
                col_name = parts[0].strip()
                val_str = parts[1].strip().strip("'\"")
                if col_name not in ctx.current_table:
                    return np.ones(ctx.row_count, dtype=bool)
                vals = ctx.current_table[col_name]
                try:
                    val = float(val_str)
                    arr = np.array(vals, dtype=float)
                    return arr == val if op == "==" else arr != val
                except (TypeError, ValueError):
                    cmp = [str(v) == val_str for v in vals]
                    return np.array(cmp if op == "==" else [not x for x in cmp])

        return np.ones(ctx.row_count, dtype=bool)

    def _find_column(self, upper_name: str, ctx: GenerationContext) -> str:
        """Case-insensitive column lookup."""
        return next(
            (k for k in ctx.current_table if k.upper() == upper_name),
            upper_name.lower(),
        )

    def _run_generator(self, gen_config: dict, ctx: GenerationContext) -> np.ndarray:
        """Run a simple inline generator for all ctx.row_count rows."""
        if "fixed" in gen_config:
            val = gen_config["fixed"]
            try:
                return np.full(ctx.row_count, float(val))
            except (TypeError, ValueError):
                return np.full(ctx.row_count, val, dtype=object)

        strategy = gen_config.get("strategy", "")

        if strategy == "lookup":
            source_table = gen_config.get("source_table", "")
            source_column = gen_config.get("source_column", "")
            via = gen_config.get("via", "")
            if not all([source_table, source_column, via]):
                return np.zeros(ctx.row_count)
            if via not in ctx.current_table:
                return np.zeros(ctx.row_count)
            fk_values = ctx.current_table[via]
            # Null FKs can't be looked up — replace with 0 after lookup
            looked_up = ctx.id_manager.lookup_values(
                table_name=source_table,
                lookup_column=source_column,
                fk_values=fk_values,
                pk_column=via,
            )
            return np.where(
                np.array([_is_null(v) for v in fk_values]),
                0.0,
                looked_up.astype(float),
            )

        return np.zeros(ctx.row_count)
