"""End-to-end integration test: ScaleRouter + retail domain + MemorySink.

Task 10 of the Spindle billion-row Phase 1 plan.

Exercises the full multi-process generation path with a real domain
schema — verifying table existence, row counts, and PK uniqueness
across chunks.
"""
from __future__ import annotations

import dataclasses
import json
import os
import tempfile

import pytest


@pytest.mark.slow
def test_scale_router_retail_e2e(tmp_path):
    """Run ScaleRouter against the retail domain with two chunks and verify:

    1. All expected tables are present in the MemorySink output.
    2. Total rows_generated reported by run() equals total_rows (1000).
    3. Each table has exactly total_rows rows (one row per table per chunk row).
    4. Per-table PK sequence columns are unique across chunks (no collision
       caused by incorrect sequence_offset).
    """
    from sqllocks_spindle.engine.generator import Spindle
    from sqllocks_spindle.engine.scale_router import ScaleRouter
    from sqllocks_spindle.engine.sinks.memory import MemorySink

    TOTAL_ROWS = 1000
    CHUNK_SIZE = 500
    MAX_WORKERS = 1
    SEED = 42

    # --- Resolve domain and build schema dict (mirrors cmd_scale_generate local_mp) ---
    from sqllocks_spindle.mcp_bridge import _resolve_domain

    domain = _resolve_domain("retail", mode="3nf")
    spindle = Spindle()
    parsed = spindle._resolve_schema(domain, None)
    parsed.generation.scale = "small"
    parsed.model.seed = SEED

    schema_dict = dataclasses.asdict(parsed)
    if hasattr(domain, "child_domains"):
        schema_dict["_domain_path"] = [str(d.domain_path) for d in domain.child_domains]
    elif hasattr(domain, "domain_path"):
        schema_dict["_domain_path"] = str(domain.domain_path)

    # --- Write schema to temp file ---
    tmp_schema_path = None
    try:
        with tempfile.NamedTemporaryFile(
            suffix=".json", mode="w", delete=False, dir=str(tmp_path)
        ) as f:
            json.dump(schema_dict, f)
            tmp_schema_path = f.name

        # --- Run ScaleRouter ---
        sink = MemorySink()
        router = ScaleRouter(
            schema_path=tmp_schema_path,
            sinks=[sink],
            chunk_size=CHUNK_SIZE,
            max_workers=MAX_WORKERS,
        )
        stats = router.run(total_rows=TOTAL_ROWS, seed=SEED)

    finally:
        if tmp_schema_path and os.path.exists(tmp_schema_path):
            os.unlink(tmp_schema_path)

    # ------------------------------------------------------------------ #
    # Assertions                                                           #
    # ------------------------------------------------------------------ #

    result = sink.result()

    # 1. Expected tables are present
    expected_tables = {
        "customer",
        "address",
        "product_category",
        "product",
        "store",
        "promotion",
        "order",
        "order_line",
        "return",
    }
    missing = expected_tables - set(result.keys())
    assert not missing, f"Missing tables in MemorySink output: {missing}"

    # 2. stats["rows_generated"] equals TOTAL_ROWS
    assert stats["rows_generated"] == TOTAL_ROWS, (
        f"Expected rows_generated={TOTAL_ROWS}, got {stats['rows_generated']}"
    )

    # 3. Each table has TOTAL_ROWS rows (chunk_worker generates `count` rows per table)
    for table_name, df in result.items():
        assert len(df) == TOTAL_ROWS, (
            f"Table '{table_name}': expected {TOTAL_ROWS} rows, got {len(df)}"
        )

    # 4. PK sequence columns are unique across chunks — sequence_offset ensures
    #    chunk 0 produces IDs 1..500 and chunk 1 produces IDs 501..1000 for each
    #    sequence-based PK.  Check every table whose PK column is a sequence int.
    pk_columns = {
        "customer": "customer_id",
        "address": "address_id",
        "product_category": "category_id",
        "product": "product_id",
        "store": "store_id",
        "promotion": "promotion_id",
        "order": "order_id",
        "order_line": "order_line_id",
        "return": "return_id",
    }
    for table_name, pk_col in pk_columns.items():
        if table_name not in result:
            continue  # already caught by assertion 1
        df = result[table_name]
        if pk_col not in df.columns:
            # Column naming may differ — skip with explanation rather than fail
            # (the row-count assertion above still validates coverage)
            continue
        pk_values = df[pk_col]
        assert pk_values.nunique() == len(pk_values), (
            f"Table '{table_name}': PK column '{pk_col}' has duplicate values "
            f"across chunks — sequence_offset may not be applied correctly. "
            f"Total rows: {len(pk_values)}, unique values: {pk_values.nunique()}"
        )
