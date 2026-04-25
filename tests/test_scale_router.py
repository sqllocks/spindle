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
