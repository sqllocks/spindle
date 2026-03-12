"""Record sample strategy — sample complete records from a reference dataset.

Picks N rows from a dataset of dicts, extracts one field as the column value,
and stashes all other fields into ctx.current_table under `_rs_{dataset}_{field}`
keys so that subsequent record_field strategy calls can read correlated values.

Usage in schema:
    # Anchor column — samples the record and returns one field
    "city": {
        "generator": {
            "strategy": "record_sample",
            "dataset": "us_zip_locations",
            "field": "city"
        }
    }

    # Derived columns — read fields from the already-sampled record
    "state":    {"generator": {"strategy": "record_field", "dataset": "us_zip_locations", "field": "state"}}
    "zip_code": {"generator": {"strategy": "record_field", "dataset": "us_zip_locations", "field": "zip"}}
    "lat":      {"generator": {"strategy": "record_field", "dataset": "us_zip_locations", "field": "lat"}}
    "lng":      {"generator": {"strategy": "record_field", "dataset": "us_zip_locations", "field": "lng"}}

The anchor column MUST appear before any record_field columns for the same dataset
in the schema definition (Python dicts preserve insertion order since 3.7).
"""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.engine.strategies.reference_data import _load_dataset
from sqllocks_spindle.schema.parser import ColumnDef

# Cache key prefix used in ctx.current_table for stashed record fields
_RS_PREFIX = "_rs_"


def _cache_key(dataset: str, field: str) -> str:
    return f"{_RS_PREFIX}{dataset}_{field}"


class RecordSampleStrategy(Strategy):
    """Sample complete records from a reference dataset.

    Acts as the anchor for a group of correlated columns. Samples N records
    (one per generated row) and writes ALL fields into ctx.current_table so
    that RecordFieldStrategy can read them without re-sampling.
    """

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
                f"record_sample strategy requires 'dataset' and 'field' "
                f"for column '{column.name}'"
            )

        domain_path = ctx.model_config.get("_domain_path")
        data = _load_dataset(dataset_name, domain_path)

        if not isinstance(data, list) or not data or not isinstance(data[0], dict):
            raise ValueError(
                f"record_sample requires a dataset of dicts, "
                f"but '{dataset_name}' contains {type(data[0]).__name__}"
            )

        if field not in data[0]:
            available = list(data[0].keys())
            raise ValueError(
                f"Field '{field}' not found in dataset '{dataset_name}'. "
                f"Available fields: {available}"
            )

        # Sample row indices (with replacement)
        indices = ctx.rng.integers(0, len(data), size=ctx.row_count)
        sampled = [data[i] for i in indices]

        # Stash ALL fields into ctx.current_table for record_field to read.
        # Use np.array without forcing dtype=object so numeric fields (lat, lng)
        # get float64 dtype automatically; string fields stay as object.
        all_fields = list(data[0].keys())
        for f in all_fields:
            values = [row[f] for row in sampled]
            ctx.current_table[_cache_key(dataset_name, f)] = np.array(values)

        return ctx.current_table[_cache_key(dataset_name, field)]
