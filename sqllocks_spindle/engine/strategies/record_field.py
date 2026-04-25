"""Record field strategy — read a field from a previously sampled record set.

Must always be paired with a preceding record_sample call for the same dataset
in the same table. See record_sample.py for full usage documentation.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.engine.strategies.record_sample import _cache_key
from sqllocks_spindle.schema.parser import ColumnDef


class RecordFieldStrategy(Strategy):
    """Read a field from records already sampled by RecordSampleStrategy."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        dataset_name = config.get("dataset", "")
        field = config.get("field", "")
        if not dataset_name or not field:
            raise ValueError(
                f"record_field strategy requires 'dataset' and 'field' "
                f"for column '{column.name}'"
            )

        key = _cache_key(dataset_name, field)
        if key not in ctx.current_table:
            raise KeyError(
                f"record_field could not find '{key}' in current_table. "
                f"Ensure a record_sample column for dataset '{dataset_name}' "
                f"appears BEFORE '{column.name}' in the schema definition."
            )

        return ctx.current_table[key]
