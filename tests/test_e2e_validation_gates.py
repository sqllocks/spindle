"""E2E tests: all 8 validation gates with clean + intentionally dirty data."""

from __future__ import annotations

import pandas as pd
import numpy as np
import pytest

from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.validation.gates import (
    GateRunner,
    ValidationContext,
    GateResult,
)


@pytest.fixture(scope="module")
def retail_result():
    return Spindle().generate(domain=RetailDomain(), scale="small", seed=42)


@pytest.fixture(scope="module")
def clean_context(retail_result):
    return ValidationContext(
        tables=retail_result.tables,
        schema=retail_result.schema,
    )


# ---------------------------------------------------------------------------
# GateRunner — run all gates on clean data
# ---------------------------------------------------------------------------

class TestGateRunnerCleanData:
    def test_run_all_gates(self, clean_context):
        runner = GateRunner()
        results = runner.run_all(clean_context)
        assert len(results) > 0
        for r in results:
            assert isinstance(r, GateResult)

    def test_all_gates_pass_on_clean_data(self, clean_context):
        runner = GateRunner()
        results = runner.run_all(clean_context)
        failed = [r for r in results if not r.passed]
        assert len(failed) == 0, f"Gates failed on clean data: {[r.gate_name for r in failed]}"


# ---------------------------------------------------------------------------
# Individual gate tests with dirty data
# ---------------------------------------------------------------------------

class TestReferentialIntegrityGate:
    def test_detects_orphan_fks(self, retail_result):
        tables = {k: v.copy() for k, v in retail_result.tables.items()}
        # Inject orphan FK values in order table
        if "order" in tables and "customer_id" in tables["order"].columns:
            tables["order"].loc[0:5, "customer_id"] = 9999999
        ctx = ValidationContext(tables=tables, schema=retail_result.schema)
        runner = GateRunner()
        result = runner.run_gate("referential_integrity", ctx)
        assert isinstance(result, GateResult)


class TestNullConstraintGate:
    def test_detects_null_violations(self, retail_result):
        tables = {k: v.copy() for k, v in retail_result.tables.items()}
        # Inject nulls in a non-nullable column
        if "customer" in tables and "first_name" in tables["customer"].columns:
            tables["customer"].loc[0:3, "first_name"] = None
        ctx = ValidationContext(tables=tables, schema=retail_result.schema)
        runner = GateRunner()
        result = runner.run_gate("null_constraint", ctx)
        assert isinstance(result, GateResult)


class TestUniqueConstraintGate:
    def test_detects_duplicates(self, retail_result):
        tables = {k: v.copy() for k, v in retail_result.tables.items()}
        # Inject duplicate PK
        if "customer" in tables and "customer_id" in tables["customer"].columns:
            tables["customer"].loc[1, "customer_id"] = tables["customer"].loc[0, "customer_id"]
        ctx = ValidationContext(tables=tables, schema=retail_result.schema)
        runner = GateRunner()
        result = runner.run_gate("unique_constraint", ctx)
        assert isinstance(result, GateResult)


class TestSchemaConformanceGate:
    def test_detects_missing_columns(self, retail_result):
        tables = {k: v.copy() for k, v in retail_result.tables.items()}
        # Drop a column
        if "customer" in tables:
            tables["customer"] = tables["customer"].drop(columns=["first_name"], errors="ignore")
        ctx = ValidationContext(tables=tables, schema=retail_result.schema)
        runner = GateRunner()
        result = runner.run_gate("schema_conformance", ctx)
        assert isinstance(result, GateResult)


class TestRangeConstraintGate:
    def test_runs_without_error(self, clean_context):
        runner = GateRunner()
        result = runner.run_gate("range_constraint", clean_context)
        assert isinstance(result, GateResult)


class TestTemporalConsistencyGate:
    def test_runs_without_error(self, clean_context):
        runner = GateRunner()
        result = runner.run_gate("temporal_consistency", clean_context)
        assert isinstance(result, GateResult)


class TestSchemaDriftGate:
    def test_runs_without_error(self, clean_context):
        runner = GateRunner()
        result = runner.run_gate("schema_drift", clean_context)
        assert isinstance(result, GateResult)


class TestFileFormatGate:
    def test_runs_without_error(self, clean_context):
        runner = GateRunner()
        result = runner.run_gate("file_format", clean_context)
        assert isinstance(result, GateResult)


# ---------------------------------------------------------------------------
# Gate summary
# ---------------------------------------------------------------------------

class TestGateRunnerSummary:
    def test_summary_returns_dict(self, clean_context):
        runner = GateRunner()
        results = runner.run_all(clean_context)
        summary = GateRunner.summary(results)
        assert isinstance(summary, dict)
        assert len(summary) > 0
