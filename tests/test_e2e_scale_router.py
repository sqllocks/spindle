"""End-to-end integration test: ScaleRouter + retail domain + MemorySink.

Task 10 of the Spindle billion-row Phase 1 plan.

Exercises the full multi-process generation path with a real domain schema,
verifying the static/dynamic table split, row counts, and PK uniqueness across
chunks.

At chunk_size=500 with the retail domain at small scale:
  Static tables  (count < chunk_size=500): product_category(50), promotion(200), store(150)
  Dynamic tables (count >= chunk_size=500): customer, address, product, order, order_line, return

Static tables are generated once (correct cardinality); dynamic tables are
generated chunk_size rows per chunk and concatenated by the sink.
"""
from __future__ import annotations

import dataclasses
import json
import os
import tempfile

import pytest


# Per-table expected counts at small scale with chunk_size=500, total_rows=1000,
# 2 chunks.  Static tables use their natural schema cardinality; dynamic tables
# get TOTAL_ROWS rows total.
_STATIC_COUNTS = {
    "product_category": 50,
    "promotion": 200,
    "store": 150,
}
_DYNAMIC_TABLES = {
    "customer",
    "address",
    "product",
    "order",
    "order_line",
    "return",
}

# PK columns for each table (sequence-based PKs only — UUID tables not checked)
_PK_COLUMNS = {
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


@pytest.mark.slow
def test_scale_router_retail_e2e(tmp_path):
    """Run ScaleRouter against the retail domain and verify:

    1. Static tables (count < chunk_size) have their natural schema cardinality.
    2. Dynamic tables have TOTAL_ROWS rows across all chunks.
    3. stats['rows_generated'] == TOTAL_ROWS (counts dynamic rows only).
    4. Per-table PK sequence columns are unique (no cross-chunk collisions).
    """
    from sqllocks_spindle.engine.generator import Spindle, calculate_row_counts
    from sqllocks_spindle.engine.scale_router import ScaleRouter
    from sqllocks_spindle.engine.sinks.memory import MemorySink

    TOTAL_ROWS = 1000
    CHUNK_SIZE = 500
    MAX_WORKERS = 1
    SEED = 42

    from sqllocks_spindle.mcp_bridge import _resolve_domain

    domain = _resolve_domain("retail", mode="3nf")
    spindle = Spindle()
    parsed = spindle._resolve_schema(domain, None)
    parsed.generation.scale = "small"
    parsed.model.seed = SEED

    # Get the schema row counts so we know what to expect
    schema_counts = calculate_row_counts(parsed)

    schema_dict = dataclasses.asdict(parsed)
    if hasattr(domain, "child_domains"):
        schema_dict["_domain_path"] = [str(d.domain_path) for d in domain.child_domains]
    elif hasattr(domain, "domain_path"):
        schema_dict["_domain_path"] = str(domain.domain_path)

    tmp_schema_path = None
    try:
        with tempfile.NamedTemporaryFile(
            suffix=".json", mode="w", delete=False, dir=str(tmp_path)
        ) as f:
            json.dump(schema_dict, f)
            tmp_schema_path = f.name

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

    result = sink.result()

    # 1. All expected tables are present
    expected_tables = set(schema_counts.keys())
    missing = expected_tables - set(result.keys())
    assert not missing, f"Missing tables in MemorySink output: {missing}"

    # 2. Static tables have their natural schema cardinality (generated once)
    for table_name, expected_count in _STATIC_COUNTS.items():
        if table_name not in result:
            continue
        actual = len(result[table_name])
        assert actual == expected_count, (
            f"Static table '{table_name}': expected {expected_count} rows "
            f"(natural cardinality), got {actual}. "
            f"ScaleRouter may be replicating reference tables across chunks."
        )

    # 3. Dynamic tables have TOTAL_ROWS rows (2 chunks × chunk_size)
    for table_name in _DYNAMIC_TABLES:
        if table_name not in result:
            continue
        actual = len(result[table_name])
        assert actual == TOTAL_ROWS, (
            f"Dynamic table '{table_name}': expected {TOTAL_ROWS} rows, got {actual}"
        )

    # 4. stats["rows_generated"] counts dynamic chunk rows (not static rows)
    assert stats["rows_generated"] == TOTAL_ROWS, (
        f"Expected rows_generated={TOTAL_ROWS}, got {stats['rows_generated']}"
    )

    # 5. PK uniqueness across chunks — sequence_offset must produce non-overlapping IDs
    for table_name, pk_col in _PK_COLUMNS.items():
        if table_name not in result:
            continue
        df = result[table_name]
        if pk_col not in df.columns:
            continue
        pk_values = df[pk_col]
        assert pk_values.nunique() == len(pk_values), (
            f"Table '{table_name}': PK column '{pk_col}' has duplicate values "
            f"across chunks — sequence_offset may not be applied correctly. "
            f"Total rows: {len(pk_values)}, unique values: {pk_values.nunique()}"
        )
