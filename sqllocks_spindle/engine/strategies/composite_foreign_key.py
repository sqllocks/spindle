"""Composite foreign key strategy — reference tables with multi-column PKs.

Use this when the parent table has a composite primary key and the child table
needs FK columns that together point to a valid parent row.

Schema example — SalesReturn referencing FactInternetSales.(SalesOrderNumber, Line):

    "SalesOrderNumber": {
        "generator": {
            "strategy": "composite_foreign_key",
            "ref_table": "FactInternetSales",
            "ref_columns": ["SalesOrderNumber", "SalesOrderLineNumber"],
            "distribution": "uniform"
        }
    },
    "SalesOrderLineNumber": {
        "generator": {
            "strategy": "composite_fk_field",
            "source_column": "SalesOrderNumber",
            "ref_column": "SalesOrderLineNumber"
        }
    }

``composite_foreign_key`` samples rows from the parent table and returns a
**dict** mapping each ref_column to its value array.  ``TableGenerator``
detects this dict return and writes each key to ``ctx.current_table`` so
subsequent ``composite_fk_field`` columns can read them.

``composite_fk_field`` reads one key from the dict stashed by the primary
``composite_foreign_key`` column.  The two strategies are always used together.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef

_CFO_PREFIX = "_cfo_"


def _cfo_key(source_column: str, ref_column: str) -> str:
    return f"{_CFO_PREFIX}{source_column}__{ref_column}"


class CompositeForeignKeyStrategy(Strategy):
    """Sample rows from a parent table with a composite PK.

    Returns a dict ``{ref_column: ndarray}`` which TableGenerator expands into
    multiple ctx.current_table entries.  The first ref_column is stored under
    the triggering column name; all columns are also stored under internal cache
    keys so CompositeFKFieldStrategy can retrieve them.
    """

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> dict[str, np.ndarray]:
        ref_table = config.get("ref_table", "")
        ref_columns: list[str] = config.get("ref_columns", [])
        distribution = config.get("distribution", "uniform")
        params = config.get("params", {})

        if not ref_table:
            raise ValueError(
                f"composite_foreign_key on '{column.name}': 'ref_table' is required"
            )
        if not ref_columns:
            raise ValueError(
                f"composite_foreign_key on '{column.name}': 'ref_columns' must be a non-empty list"
            )

        df = ctx.id_manager._table_data.get(ref_table)
        if df is None:
            raise KeyError(
                f"composite_foreign_key on '{column.name}': "
                f"no data registered for '{ref_table}'. "
                f"Ensure '{ref_table}' is generated before this table."
            )

        pool_size = len(df)
        if pool_size == 0:
            raise ValueError(
                f"composite_foreign_key on '{column.name}': '{ref_table}' has 0 rows"
            )

        params = params or {}
        if distribution == "uniform":
            indices = ctx.rng.integers(0, pool_size, size=ctx.row_count)
        elif distribution == "zipf":
            alpha = params.get("alpha", 1.5)
            raw = ctx.rng.zipf(alpha, size=ctx.row_count * 2)
            valid = raw[raw <= pool_size] - 1
            while len(valid) < ctx.row_count:
                more = ctx.rng.zipf(alpha, size=ctx.row_count)
                valid = np.concatenate([valid, more[more <= pool_size] - 1])
            indices = valid[:ctx.row_count]
        else:
            indices = ctx.rng.integers(0, pool_size, size=ctx.row_count)

        result: dict[str, np.ndarray] = {}
        for col_name in ref_columns:
            if col_name not in df.columns:
                raise KeyError(
                    f"composite_foreign_key on '{column.name}': "
                    f"ref_column '{col_name}' not found in '{ref_table}'"
                )
            values = df[col_name].values[indices]
            result[col_name] = values
            # Stash under internal cache key for CompositeFKFieldStrategy
            cache_key = _cfo_key(column.name, col_name)
            ctx.current_table[cache_key] = values

        return result


class CompositeFKFieldStrategy(Strategy):
    """Read one column from the dict stashed by CompositeForeignKeyStrategy.

    Must come after the triggering composite_foreign_key column in the schema.
    """

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        source_column = config.get("source_column", "")
        ref_column = config.get("ref_column", "")

        if not source_column or not ref_column:
            raise ValueError(
                f"composite_fk_field on '{column.name}': "
                f"'source_column' and 'ref_column' are required"
            )

        cache_key = _cfo_key(source_column, ref_column)
        if cache_key not in ctx.current_table:
            raise KeyError(
                f"composite_fk_field on '{column.name}': "
                f"cache key '{cache_key}' not found. "
                f"Ensure the composite_foreign_key column '{source_column}' is "
                f"defined before '{column.name}' in the schema."
            )

        return ctx.current_table[cache_key]
