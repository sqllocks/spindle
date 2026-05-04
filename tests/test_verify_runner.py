# tests/test_verify_runner.py
"""Tests for VerifyRunner."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.schema.parser import ColumnDef, SpindleSchema, TableDef
from sqllocks_spindle.verify.runner import VerifyRunner


def _simple_schema() -> SpindleSchema:
    """Schema: one table 'items' with PK 'id' (non-nullable)."""
    col_id = ColumnDef(
        name="id", type="integer",
        generator={"strategy": "sequence"},
        nullable=False,
    )
    col_name = ColumnDef(
        name="name", type="string",
        generator={"strategy": "faker", "method": "name"},
        nullable=True,
    )
    table = TableDef(
        name="items",
        columns={"id": col_id, "name": col_name},
        primary_key=["id"],
    )
    return SpindleSchema(
        tables={"items": table},
        relationships=[],
        business_rules=[],
        generation=None,
        model=None,
    )


class TestVerifyRunner:
    def test_no_schema_passes_with_row_counts(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        runner = VerifyRunner()
        result = runner.run({"my_table": df})
        assert result.passed
        assert result.row_counts == {"my_table": 3}
        assert result.schema_path is None
        assert result.statistical is False

    def test_with_schema_runs_conformance_gates(self):
        schema = _simple_schema()
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]})
        runner = VerifyRunner(schema=schema)
        result = runner.run({"items": df})
        assert result.passed
        gate_names = [gr.gate_name for gr in result.gate_results]
        assert "schema_conformance" in gate_names
        assert "null_constraint" in gate_names
        assert "unique_constraint" in gate_names

    def test_detects_null_in_non_nullable_column(self):
        schema = _simple_schema()
        df = pd.DataFrame({"id": [1, None, 3], "name": ["Alice", "Bob", "Carol"]})
        runner = VerifyRunner(schema=schema)
        result = runner.run({"items": df})
        assert not result.passed
        null_gate = next(gr for gr in result.gate_results if gr.gate_name == "null_constraint")
        assert not null_gate.passed
        assert len(null_gate.errors) == 1

    def test_detects_duplicate_pk(self):
        schema = _simple_schema()
        df = pd.DataFrame({"id": [1, 1, 3], "name": ["Alice", "Bob", "Carol"]})
        runner = VerifyRunner(schema=schema)
        result = runner.run({"items": df})
        assert not result.passed
        pk_gate = next(gr for gr in result.gate_results if gr.gate_name == "unique_constraint")
        assert not pk_gate.passed

    def test_statistical_flag_adds_distribution_gate(self):
        schema = _simple_schema()
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]})
        runner = VerifyRunner(schema=schema, statistical=True)
        result = runner.run({"items": df})
        gate_names = [gr.gate_name for gr in result.gate_results]
        assert "distribution" in gate_names

    def test_result_metadata_correct(self):
        df = pd.DataFrame({"x": range(50)})
        runner = VerifyRunner(data_path="/tmp/data.csv", schema_path="/tmp/schema.json")
        result = runner.run({"x_table": df})
        assert result.data_path == "/tmp/data.csv"
        assert result.schema_path == "/tmp/schema.json"
        assert result.row_counts == {"x_table": 50}
        assert result.run_at  # non-empty ISO timestamp

    def test_missing_table_in_schema_is_error(self):
        schema = _simple_schema()
        # Provide 'wrong_table' instead of 'items'
        df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        runner = VerifyRunner(schema=schema)
        result = runner.run({"wrong_table": df})
        assert not result.passed
        conf_gate = next(gr for gr in result.gate_results if gr.gate_name == "schema_conformance")
        assert not conf_gate.passed
