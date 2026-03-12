"""Computed strategy — aggregate from child table (filled in post-processing)."""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class ComputedStrategy(Strategy):
    """Placeholder for computed columns that depend on child table data.

    These columns are initially filled with NaN/placeholder values during
    the main generation pass, then back-filled during the compute phase
    after child tables are generated.

    Examples:
        order.order_total = sum(order_line.line_total) per order_id
    """

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        # Return placeholder — will be filled in compute phase
        return np.full(ctx.row_count, np.nan, dtype=float)

    @staticmethod
    def backfill(
        parent_df,
        child_df,
        parent_pk: str,
        child_fk: str,
        child_column: str,
        target_column: str,
        rule: str = "sum_children",
    ):
        """Back-fill computed column from child table aggregation.

        Args:
            parent_df: Parent DataFrame to update (the table owning target_column).
            child_df: DataFrame to aggregate/lookup from.
            parent_pk: PK column name in parent_df.
            child_fk: FK column in child_df referencing parent_pk.
            child_column: Column in child_df to aggregate or copy.
            target_column: Column in parent_df to fill.
            rule: Aggregation rule. See below for supported values.

        Child-aggregation rules (parent_df is the parent, child_df is the child):
            sum_children, count_children, avg_children, min_children, max_children

        Parent-lookup rule (parent_df is the child, child_df is the parent):
            lookup_parent — copy a value from parent_df[child_fk] → child_df[parent_pk]
                            into target_column. Used when the "computed" column lives on
                            the child table and copies a value from the parent.
        """
        import pandas as pd

        if rule == "lookup_parent":
            # parent_df here is actually the child table (e.g. return)
            # child_df here is actually the parent table (e.g. order)
            # child_fk is the FK in the "child" (e.g. return.order_id)
            # parent_pk is the PK in the "parent" (e.g. order.order_id)
            # child_column is the column to copy (e.g. order.order_total)
            lookup = child_df.set_index(parent_pk)[child_column]
            parent_df[target_column] = parent_df[child_fk].map(lookup)
            if parent_df[target_column].dtype in (np.float64, np.float32):
                parent_df[target_column] = parent_df[target_column].round(2)
            return parent_df

        if rule == "sum_children":
            agg = child_df.groupby(child_fk)[child_column].sum()
        elif rule == "count_children":
            agg = child_df.groupby(child_fk)[child_column].count()
        elif rule == "avg_children":
            agg = child_df.groupby(child_fk)[child_column].mean()
        elif rule == "min_children":
            agg = child_df.groupby(child_fk)[child_column].min()
        elif rule == "max_children":
            agg = child_df.groupby(child_fk)[child_column].max()
        else:
            raise ValueError(f"Unknown computed rule: '{rule}'")

        # Map aggregated values back to parent
        parent_df[target_column] = parent_df[parent_pk].map(agg).fillna(0)

        # Round if target looks like a decimal
        if parent_df[target_column].dtype in (np.float64, np.float32):
            parent_df[target_column] = parent_df[target_column].round(2)

        return parent_df
