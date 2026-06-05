# tests/test_distribution_gate.py
"""Tests for DistributionGate."""

from __future__ import annotations

import random

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.schema.parser import ColumnDef, SpindleSchema, TableDef
from sqllocks_spindle.validation.gates import DistributionGate, ValidationContext


def _make_schema(col_name: str, col_type: str, generator: dict) -> SpindleSchema:
    col = ColumnDef(name=col_name, type=col_type, generator=generator, nullable=False)
    table = TableDef(name="test_table", columns={col_name: col}, primary_key=[])
    return SpindleSchema(
        tables={"test_table": table},
        relationships=[],
        business_rules=[],
        generation=None,
        model=None,
    )


class TestDistributionGate:
    def test_passes_without_schema(self):
        ctx = ValidationContext(tables={"t": pd.DataFrame({"x": [1, 2, 3]})}, schema=None)
        result = DistributionGate().check(ctx)
        assert result.passed
        assert "No schema" in result.warnings[0]

    def test_skips_non_distribution_columns(self):
        schema = _make_schema("id", "integer", {"strategy": "sequence"})
        df = pd.DataFrame({"id": range(100)})
        ctx = ValidationContext(tables={"test_table": df}, schema=schema)
        result = DistributionGate().check(ctx)
        assert result.passed
        assert result.errors == []

    @pytest.mark.skipif(
        not __import__("importlib").util.find_spec("scipy"),
        reason="scipy not installed",
    )
    def test_passes_on_matching_normal_distribution(self):
        rng = np.random.default_rng(42)
        data = rng.normal(loc=0.0, scale=1.0, size=500)
        schema = _make_schema(
            "value", "float",
            {"strategy": "distribution", "name": "norm", "loc": 0.0, "scale": 1.0},
        )
        df = pd.DataFrame({"value": data})
        ctx = ValidationContext(tables={"test_table": df}, schema=schema)
        result = DistributionGate().check(ctx)
        assert result.passed
        # Also verify no drift warning was emitted (data was generated from the same distribution)
        assert not any("drifted" in w for w in result.warnings), f"Unexpected drift warnings: {result.warnings}"

    @pytest.mark.skipif(
        not __import__("importlib").util.find_spec("scipy"),
        reason="scipy not installed",
    )
    def test_warns_on_drifted_distribution(self):
        rng = np.random.default_rng(99)
        # Generate from N(100, 1) but schema says N(0, 1)
        data = rng.normal(loc=100.0, scale=1.0, size=500)
        schema = _make_schema(
            "value", "float",
            {"strategy": "distribution", "name": "norm", "loc": 0.0, "scale": 1.0},
        )
        df = pd.DataFrame({"value": data})
        ctx = ValidationContext(tables={"test_table": df}, schema=schema)
        result = DistributionGate().check(ctx)
        assert len(result.warnings) > 0

    @pytest.mark.skipif(
        not __import__("importlib").util.find_spec("scipy"),
        reason="scipy not installed",
    )
    def test_passes_on_matching_enum_distribution(self):
        random.seed(42)
        data = random.choices(["A", "B", "C"], weights=[0.5, 0.3, 0.2], k=1000)
        schema = _make_schema(
            "category", "string",
            {"strategy": "enum", "values": {"A": 0.5, "B": 0.3, "C": 0.2}},
        )
        df = pd.DataFrame({"category": data})
        ctx = ValidationContext(tables={"test_table": df}, schema=schema)
        result = DistributionGate().check(ctx)
        assert result.passed

    @pytest.mark.skipif(
        not __import__("importlib").util.find_spec("scipy"),
        reason="scipy not installed",
    )
    def test_warns_on_missing_enum_values(self):
        # Only A and B present, but schema expects A, B, C
        df = pd.DataFrame({"category": ["A"] * 500 + ["B"] * 500})
        schema = _make_schema(
            "category", "string",
            {"strategy": "enum", "values": {"A": 0.5, "B": 0.3, "C": 0.2}},
        )
        ctx = ValidationContext(tables={"test_table": df}, schema=schema)
        result = DistributionGate().check(ctx)
        assert result.passed  # Missing values are warnings, not errors
        assert any("C" in w or "missing" in w.lower() for w in result.warnings)

    def test_skips_gracefully_without_scipy(self, monkeypatch):
        import sqllocks_spindle.validation.gates as gates_module
        monkeypatch.setattr(gates_module, "HAS_SCIPY", False)
        schema = _make_schema(
            "value", "float",
            {"strategy": "distribution", "name": "norm", "loc": 0.0, "scale": 1.0},
        )
        df = pd.DataFrame({"value": [1.0, 2.0, 3.0]})
        ctx = ValidationContext(tables={"test_table": df}, schema=schema)
        result = DistributionGate().check(ctx)
        assert result.passed
        assert any("scipy" in w.lower() for w in result.warnings)

    def test_registered_in_gate_registry(self):
        from sqllocks_spindle.validation.gates import GateRunner
        assert "distribution" in GateRunner.available_gates()
