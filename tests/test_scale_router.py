from __future__ import annotations

import json
import tempfile

import numpy as np
import pytest


def _write_schema_json(schema_dict: dict, path: str) -> None:
    with open(path, "w") as f:
        json.dump(schema_dict, f)


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


def test_generate_chunk_returns_dict_of_lists():
    from sqllocks_spindle.engine.chunk_worker import generate_chunk

    schema = _minimal_schema_dict()
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
        json.dump(schema, f)
        path = f.name

    result = generate_chunk(schema_path=path, seed=42, offset=0, count=50)

    assert isinstance(result, dict)
    assert "widgets" in result
    assert isinstance(result["widgets"], dict)
    assert "widget_id" in result["widgets"]
    assert isinstance(result["widgets"]["widget_id"], list)
    assert len(result["widgets"]["widget_id"]) == 50


def test_generate_chunk_sequence_offset():
    from sqllocks_spindle.engine.chunk_worker import generate_chunk

    schema = _minimal_schema_dict()
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
        json.dump(schema, f)
        path = f.name

    result = generate_chunk(schema_path=path, seed=42, offset=100, count=10)
    ids = result["widgets"]["widget_id"]
    # With offset=100 and sequence start=1, IDs should start at 101
    assert min(ids) >= 101


def test_generate_chunk_deterministic():
    from sqllocks_spindle.engine.chunk_worker import generate_chunk

    schema = _minimal_schema_dict()
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
        json.dump(schema, f)
        path = f.name

    r1 = generate_chunk(path, seed=99, offset=0, count=20)
    r2 = generate_chunk(path, seed=99, offset=0, count=20)
    assert r1["widgets"]["widget_id"] == r2["widgets"]["widget_id"]
