from __future__ import annotations

import json
import os
import tempfile

import pytest


def _minimal_schema_dict() -> dict:
    return {
        "model": {"name": "test", "domain": "test", "seed": 42},
        "tables": {
            "widgets": {
                "name": "widgets",
                "columns": {
                    "widget_id": {
                        "name": "widget_id",
                        "type": "integer",
                        "generator": {"strategy": "sequence", "start": 1},
                        "nullable": False,
                        "null_rate": 0.0,
                    },
                    "name": {
                        "name": "name",
                        "type": "string",
                        "generator": {"strategy": "faker", "provider": "word"},
                        "nullable": False,
                        "null_rate": 0.0,
                    },
                },
                "primary_key": ["widget_id"],
                "description": "test table",
            }
        },
        "relationships": [],
        "business_rules": [],
        "generation": {
            "scale": "small",
            "scales": {"small": {"widgets": 100}},
        },
    }


def test_generate_chunk_returns_dict_of_lists(tmp_path):
    from sqllocks_spindle.engine.chunk_worker import generate_chunk

    schema = _minimal_schema_dict()
    path = str(tmp_path / "schema.json")
    with open(path, "w") as f:
        json.dump(schema, f)

    result = generate_chunk(schema_path=path, seed=42, offset=0, count=50)

    assert isinstance(result, dict)
    assert "widgets" in result
    assert isinstance(result["widgets"], dict)
    assert "widget_id" in result["widgets"]
    assert isinstance(result["widgets"]["widget_id"], list)
    assert len(result["widgets"]["widget_id"]) == 50


def test_generate_chunk_sequence_offset(tmp_path):
    from sqllocks_spindle.engine.chunk_worker import generate_chunk

    schema = _minimal_schema_dict()
    path = str(tmp_path / "schema.json")
    with open(path, "w") as f:
        json.dump(schema, f)

    result = generate_chunk(schema_path=path, seed=42, offset=100, count=10)
    ids = result["widgets"]["widget_id"]
    assert min(ids) >= 101


def test_generate_chunk_deterministic(tmp_path):
    from sqllocks_spindle.engine.chunk_worker import generate_chunk

    schema = _minimal_schema_dict()
    path = str(tmp_path / "schema.json")
    with open(path, "w") as f:
        json.dump(schema, f)

    r1 = generate_chunk(path, seed=99, offset=0, count=20)
    r2 = generate_chunk(path, seed=99, offset=0, count=20)
    assert r1["widgets"]["widget_id"] == r2["widgets"]["widget_id"]


def test_scale_router_generates_via_memory_sink(tmp_path):
    import json
    from sqllocks_spindle.engine.scale_router import ScaleRouter
    from sqllocks_spindle.engine.sinks.memory import MemorySink

    schema = _minimal_schema_dict()
    path = str(tmp_path / "schema.json")
    with open(path, "w") as f:
        json.dump(schema, f)

    sink = MemorySink()
    router = ScaleRouter(schema_path=path, sinks=[sink], chunk_size=20, max_workers=2)
    stats = router.run(total_rows=50, seed=42)

    result = sink.result()
    assert "widgets" in result
    assert len(result["widgets"]) == 50
    assert stats["rows_generated"] == 50
    assert stats["throughput_rows_per_sec"] > 0


def test_scale_router_generates_two_chunks(tmp_path):
    import json
    from sqllocks_spindle.engine.scale_router import ScaleRouter
    from sqllocks_spindle.engine.sinks.memory import MemorySink

    schema = _minimal_schema_dict()
    path = str(tmp_path / "schema.json")
    with open(path, "w") as f:
        json.dump(schema, f)

    sink = MemorySink()
    router = ScaleRouter(schema_path=path, sinks=[sink], chunk_size=10, max_workers=2)
    router.run(total_rows=25, seed=7)

    result = sink.result()
    assert len(result["widgets"]) == 25


def test_scale_router_parquet_output(tmp_path):
    import json
    from sqllocks_spindle.engine.scale_router import ScaleRouter
    from sqllocks_spindle.engine.sinks.parquet import ParquetSink

    schema = _minimal_schema_dict()
    path = str(tmp_path / "schema.json")
    with open(path, "w") as f:
        json.dump(schema, f)

    sink = ParquetSink(output_dir=str(tmp_path))
    router = ScaleRouter(schema_path=path, sinks=[sink], chunk_size=10, max_workers=2)
    router.run(total_rows=30, seed=1)

    parts = list((tmp_path / "widgets").glob("part-*.parquet"))
    assert len(parts) == 3  # 30 rows / 10 per chunk


def test_scale_router_fan_out_both_sinks(tmp_path):
    import json
    from sqllocks_spindle.engine.scale_router import ScaleRouter
    from sqllocks_spindle.engine.sinks.memory import MemorySink
    from sqllocks_spindle.engine.sinks.parquet import ParquetSink

    schema = _minimal_schema_dict()
    path = str(tmp_path / "schema.json")
    with open(path, "w") as f:
        json.dump(schema, f)

    mem = MemorySink()
    parq = ParquetSink(output_dir=str(tmp_path))
    router = ScaleRouter(schema_path=path, sinks=[mem, parq], chunk_size=25, max_workers=2)
    router.run(total_rows=50, seed=5)

    assert len(mem.result()["widgets"]) == 50
    assert len(list((tmp_path / "widgets").glob("*.parquet"))) == 2
