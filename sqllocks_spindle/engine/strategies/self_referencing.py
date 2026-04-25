"""Self-referencing strategy — generate a parent FK within the same table.

Builds a proper multi-level hierarchy where rows are assigned to levels and
each non-root row gets a parent from the level above.

The PK column must already be generated (sequence/uuid) before this strategy
runs — which is guaranteed by the column ordering in TableGenerator (PKs first).

Level assignments and parent IDs are stashed in ctx.current_table under
  `_sr_{table_name}_level`
so that a subsequent `level` column can use `record_field`-style retrieval
via the `self_ref_field` strategy (or just reference it directly).

Example schema:
    "product_category": {
        "primary_key": ["category_id"],
        "columns": {
            "category_id": {"generator": {"strategy": "sequence", "start": 1}},
            "parent_category_id": {
                "generator": {
                    "strategy": "self_referencing",
                    "pk_column": "category_id",
                    "levels": 3,
                    "root_count": 8
                }
            },
            "level": {
                "generator": {
                    "strategy": "self_ref_field",
                    "field": "level"
                }
            }
        }
    }
"""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef

_SR_PREFIX = "_sr_"


def _sr_key(table_name: str, field: str) -> str:
    return f"{_SR_PREFIX}{table_name}_{field}"


class SelfReferencingStrategy(Strategy):
    """Assign parent IDs within the same table to form a level hierarchy.

    Row allocation:
      - The first `root_count` rows become level-1 roots (parent = NULL)
      - Remaining rows are split evenly across levels 2..N
      - Each non-root row is assigned a random parent from the level above

    Stashes level assignments into ctx.current_table[_sr_{table}_level]
    for use by a downstream 'level' column.
    """

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        pk_column = config.get("pk_column", "")
        levels = int(config.get("levels", 3))
        root_count = int(config.get("root_count", max(1, ctx.row_count // 10)))

        if not pk_column or pk_column not in ctx.current_table:
            raise KeyError(
                f"self_referencing strategy for '{column.name}': "
                f"pk_column '{pk_column}' not found in current_table. "
                f"Ensure the PK column is defined before this column."
            )

        pks = ctx.current_table[pk_column]
        n = len(pks)

        # Clamp root_count to a sensible range
        root_count = max(1, min(root_count, n // levels))

        # Distribute remaining rows across levels 2..N
        remaining = n - root_count
        rows_per_level = remaining // (levels - 1) if levels > 1 else 0
        extra = remaining - rows_per_level * (levels - 1) if levels > 1 else remaining

        # Build level boundaries (inclusive start indices)
        level_starts = [0]  # level 1 starts at 0
        cursor = root_count
        for lv in range(2, levels + 1):
            level_starts.append(cursor)
            size = rows_per_level + (1 if lv - 2 < extra else 0)
            cursor += size

        level_ends = level_starts[1:] + [n]

        # Assign level numbers
        level_assignments = np.zeros(n, dtype=int)
        for lv, (start, end) in enumerate(zip(level_starts, level_ends), start=1):
            level_assignments[start:end] = lv

        # Build parent_id array: None for roots, FK to level above for others
        parent_ids = np.empty(n, dtype=object)
        parent_ids[:root_count] = None  # roots have no parent

        for lv in range(2, levels + 1):
            start = level_starts[lv - 1]
            end = level_ends[lv - 1]
            count = end - start

            if count <= 0:
                continue

            # Parents come from level lv-1
            parent_lv = lv - 1
            parent_start = level_starts[parent_lv - 1]
            parent_end = level_ends[parent_lv - 1]
            parent_pks = pks[parent_start:parent_end]

            if len(parent_pks) == 0:
                # Fallback: use roots
                parent_pks = pks[:root_count]

            chosen_indices = ctx.rng.integers(0, len(parent_pks), size=count)
            parent_ids[start:end] = parent_pks[chosen_indices]

        # Stash level assignments for downstream 'level' column
        cache_key = _sr_key(ctx.current_table_name, "level")
        ctx.current_table[cache_key] = level_assignments

        return parent_ids


class SelfRefFieldStrategy(Strategy):
    """Read a field stashed by SelfReferencingStrategy.

    Example: reads level assignments stored by the self_referencing column.

        "level": {
            "generator": {"strategy": "self_ref_field", "field": "level"}
        }
    """

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        field = config.get("field", "level")
        key = _sr_key(ctx.current_table_name, field)

        if key not in ctx.current_table:
            raise KeyError(
                f"self_ref_field could not find '{key}'. "
                f"Ensure a self_referencing column for this table appears before "
                f"'{column.name}' in the schema definition."
            )

        return ctx.current_table[key]
