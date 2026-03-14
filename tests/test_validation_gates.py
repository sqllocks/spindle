"""Tests for ValidationGate subclasses and GateRunner."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from sqllocks_spindle.schema.parser import (
    ColumnDef,
    GenerationConfig,
    ModelDef,
    RelationshipDef,
    SpindleSchema,
    TableDef,
)
from sqllocks_spindle.validation.gates import (
    FileFormatGate,
    GateResult,
    GateRunner,
    NullConstraintGate,
    RangeConstraintGate,
    ReferentialIntegrityGate,
    SchemaDriftGate,
    SchemaConformanceGate,
    TemporalConsistencyGate,
    UniqueConstraintGate,
    ValidationContext,
    ValidationGate,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _make_schema(
    table_name: str = "order",
    pk: list[str] | None = None,
    extra_columns: dict | None = None,
    relationships: list | None = None,
    nullable_cols: list[str] | None = None,
) -> SpindleSchema:
    base_cols: dict[str, ColumnDef] = {
        "order_id": ColumnDef(
            name="order_id", type="integer",
            generator={"strategy": "sequence"},
            nullable=False,
        ),
        "amount": ColumnDef(
            name="amount", type="float",
            generator={"strategy": "random_float", "min": 0.0, "max": 1000.0},
        ),
    }
    if extra_columns:
        base_cols.update(extra_columns)
    if nullable_cols:
        for col in nullable_cols:
            if col in base_cols:
                base_cols[col] = ColumnDef(
                    name=base_cols[col].name,
                    type=base_cols[col].type,
                    generator=base_cols[col].generator,
                    nullable=True,
                )
    table = TableDef(
        name=table_name,
        columns=base_cols,
        primary_key=pk or ["order_id"],
    )
    return SpindleSchema(
        model=ModelDef(name="test"),
        tables={table_name: table},
        relationships=relationships or [],
        business_rules=[],
        generation=GenerationConfig(),
    )


@pytest.fixture
def simple_df():
    return pd.DataFrame({
        "order_id": range(1, 6),
        "amount": [10.0, 20.0, 30.0, 40.0, 50.0],
    })


@pytest.fixture
def ctx(simple_df):
    schema = _make_schema()
    return ValidationContext(
        tables={"order": simple_df},
        schema=schema,
    )


# ---------------------------------------------------------------------------
# GateResult
# ---------------------------------------------------------------------------

class TestGateResult:
    def test_repr_pass(self):
        r = GateResult(gate_name="my_gate", passed=True)
        assert "PASS" in repr(r)
        assert "my_gate" in repr(r)

    def test_repr_fail(self):
        r = GateResult(gate_name="my_gate", passed=False, errors=["bad"])
        assert "FAIL" in repr(r)
        assert "1 errors" in repr(r)


# ---------------------------------------------------------------------------
# ReferentialIntegrityGate
# ---------------------------------------------------------------------------

class TestReferentialIntegrityGate:
    def test_no_schema_fails(self):
        gate = ReferentialIntegrityGate()
        result = gate.check(ValidationContext(tables={"t": pd.DataFrame()}))
        assert not result.passed
        assert any("schema" in e.lower() for e in result.errors)

    def test_no_relationships_passes(self, ctx):
        gate = ReferentialIntegrityGate()
        result = gate.check(ctx)
        assert result.passed

    def test_valid_fk_passes(self):
        parents = pd.DataFrame({"customer_id": [1, 2, 3]})
        children = pd.DataFrame({"order_id": [1, 2], "customer_id": [1, 2]})
        rel = RelationshipDef(
            name="order_customer",
            parent="customer",
            child="order",
            parent_columns=["customer_id"],
            child_columns=["customer_id"],
            type="many_to_one",
        )
        schema = SpindleSchema(
            model=ModelDef(name="t"),
            tables={},
            relationships=[rel],
            business_rules=[],
            generation=GenerationConfig(),
        )
        ctx = ValidationContext(
            tables={"customer": parents, "order": children},
            schema=schema,
        )
        result = ReferentialIntegrityGate().check(ctx)
        assert result.passed
        assert result.details["orphan_counts"]["order.customer_id->customer.customer_id"] == 0

    def test_orphan_fk_fails(self):
        parents = pd.DataFrame({"customer_id": [1, 2]})
        children = pd.DataFrame({"order_id": [1, 2], "customer_id": [1, 99]})
        rel = RelationshipDef(
            name="order_customer",
            parent="customer",
            child="order",
            parent_columns=["customer_id"],
            child_columns=["customer_id"],
            type="many_to_one",
        )
        schema = SpindleSchema(
            model=ModelDef(name="t"),
            tables={},
            relationships=[rel],
            business_rules=[],
            generation=GenerationConfig(),
        )
        ctx = ValidationContext(
            tables={"customer": parents, "order": children},
            schema=schema,
        )
        result = ReferentialIntegrityGate().check(ctx)
        assert not result.passed
        assert any("99" in e or "orphan" in e.lower() for e in result.errors)

    def test_self_referencing_skipped(self):
        rel = RelationshipDef(
            name="emp_manager",
            parent="employee",
            child="employee",
            parent_columns=["employee_id"],
            child_columns=["manager_id"],
            type="self_referencing",
        )
        schema = SpindleSchema(
            model=ModelDef(name="t"),
            tables={},
            relationships=[rel],
            business_rules=[],
            generation=GenerationConfig(),
        )
        ctx = ValidationContext(
            tables={"employee": pd.DataFrame({"employee_id": [1, 2], "manager_id": [None, 1]})},
            schema=schema,
        )
        result = ReferentialIntegrityGate().check(ctx)
        assert result.passed

    def test_missing_table_produces_warning(self):
        rel = RelationshipDef(
            name="order_customer",
            parent="customer",
            child="order",
            parent_columns=["customer_id"],
            child_columns=["customer_id"],
            type="many_to_one",
        )
        schema = SpindleSchema(
            model=ModelDef(name="t"),
            tables={},
            relationships=[rel],
            business_rules=[],
            generation=GenerationConfig(),
        )
        ctx = ValidationContext(tables={}, schema=schema)
        result = ReferentialIntegrityGate().check(ctx)
        assert result.passed
        assert len(result.warnings) > 0


# ---------------------------------------------------------------------------
# SchemaConformanceGate
# ---------------------------------------------------------------------------

class TestSchemaConformanceGate:
    def test_no_schema_fails(self):
        gate = SchemaConformanceGate()
        result = gate.check(ValidationContext(tables={"t": pd.DataFrame()}))
        assert not result.passed

    def test_exact_match_passes(self, ctx):
        result = SchemaConformanceGate().check(ctx)
        assert result.passed

    def test_missing_column_fails(self):
        schema = _make_schema()
        df = pd.DataFrame({"order_id": [1, 2]})  # missing 'amount'
        ctx = ValidationContext(tables={"order": df}, schema=schema)
        result = SchemaConformanceGate().check(ctx)
        assert not result.passed
        assert any("amount" in e for e in result.errors)

    def test_extra_column_is_warning(self):
        schema = _make_schema()
        df = pd.DataFrame({"order_id": [1], "amount": [5.0], "extra_col": ["x"]})
        ctx = ValidationContext(tables={"order": df}, schema=schema)
        result = SchemaConformanceGate().check(ctx)
        assert result.passed
        assert any("extra_col" in w for w in result.warnings)

    def test_missing_table_fails(self):
        schema = _make_schema()
        ctx = ValidationContext(tables={}, schema=schema)
        result = SchemaConformanceGate().check(ctx)
        assert not result.passed
        assert any("order" in e for e in result.errors)

    def test_details_populated(self, ctx):
        result = SchemaConformanceGate().check(ctx)
        assert "order" in result.details
        assert "expected_columns" in result.details["order"]


# ---------------------------------------------------------------------------
# NullConstraintGate
# ---------------------------------------------------------------------------

class TestNullConstraintGate:
    def test_no_schema_fails(self):
        result = NullConstraintGate().check(ValidationContext())
        assert not result.passed

    def test_no_nulls_passes(self, ctx):
        result = NullConstraintGate().check(ctx)
        assert result.passed

    def test_null_in_non_nullable_fails(self):
        schema = _make_schema()  # order_id is non-nullable
        df = pd.DataFrame({"order_id": [1, None, 3], "amount": [1.0, 2.0, 3.0]})
        ctx = ValidationContext(tables={"order": df}, schema=schema)
        result = NullConstraintGate().check(ctx)
        assert not result.passed
        assert any("order_id" in e for e in result.errors)

    def test_null_in_nullable_col_passes(self):
        schema = _make_schema(nullable_cols=["amount"])
        df = pd.DataFrame({"order_id": [1, 2], "amount": [1.0, None]})
        ctx = ValidationContext(tables={"order": df}, schema=schema)
        result = NullConstraintGate().check(ctx)
        assert result.passed

    def test_missing_table_skipped_gracefully(self):
        schema = _make_schema()
        ctx = ValidationContext(tables={}, schema=schema)
        result = NullConstraintGate().check(ctx)
        assert result.passed


# ---------------------------------------------------------------------------
# UniqueConstraintGate
# ---------------------------------------------------------------------------

class TestUniqueConstraintGate:
    def test_no_schema_fails(self):
        result = UniqueConstraintGate().check(ValidationContext())
        assert not result.passed

    def test_unique_pk_passes(self, ctx):
        result = UniqueConstraintGate().check(ctx)
        assert result.passed

    def test_duplicate_pk_fails(self):
        schema = _make_schema()
        df = pd.DataFrame({"order_id": [1, 1, 3], "amount": [10.0, 20.0, 30.0]})
        ctx = ValidationContext(tables={"order": df}, schema=schema)
        result = UniqueConstraintGate().check(ctx)
        assert not result.passed
        assert any("duplicate" in e.lower() for e in result.errors)

    def test_composite_pk_duplicate_fails(self):
        table = TableDef(
            name="order_line",
            columns={
                "order_id": ColumnDef(name="order_id", type="integer", generator={}),
                "line_num": ColumnDef(name="line_num", type="integer", generator={}),
            },
            primary_key=["order_id", "line_num"],
        )
        schema = SpindleSchema(
            model=ModelDef(name="t"), tables={"order_line": table},
            relationships=[], business_rules=[], generation=GenerationConfig(),
        )
        df = pd.DataFrame({"order_id": [1, 1], "line_num": [1, 1]})
        ctx = ValidationContext(tables={"order_line": df}, schema=schema)
        result = UniqueConstraintGate().check(ctx)
        assert not result.passed

    def test_no_pk_skipped_gracefully(self):
        table = TableDef(
            name="log",
            columns={"message": ColumnDef(name="message", type="string", generator={})},
            primary_key=[],
        )
        schema = SpindleSchema(
            model=ModelDef(name="t"), tables={"log": table},
            relationships=[], business_rules=[], generation=GenerationConfig(),
        )
        ctx = ValidationContext(
            tables={"log": pd.DataFrame({"message": ["a", "a"]})},
            schema=schema,
        )
        result = UniqueConstraintGate().check(ctx)
        assert result.passed


# ---------------------------------------------------------------------------
# RangeConstraintGate
# ---------------------------------------------------------------------------

class TestRangeConstraintGate:
    def test_no_ranges_passes_with_warning(self):
        result = RangeConstraintGate().check(ValidationContext())
        assert result.passed
        assert len(result.warnings) > 0

    def test_values_in_range_passes(self, simple_df):
        ctx = ValidationContext(
            tables={"order": simple_df},
            config={"ranges": {"order.amount": {"min": 0, "max": 100}}},
        )
        result = RangeConstraintGate().check(ctx)
        assert result.passed

    def test_values_below_min_fails(self, simple_df):
        ctx = ValidationContext(
            tables={"order": simple_df},
            config={"ranges": {"order.amount": {"min": 20.0}}},
        )
        result = RangeConstraintGate().check(ctx)
        assert not result.passed
        assert any("below" in e.lower() for e in result.errors)

    def test_values_above_max_fails(self, simple_df):
        ctx = ValidationContext(
            tables={"order": simple_df},
            config={"ranges": {"order.amount": {"max": 25.0}}},
        )
        result = RangeConstraintGate().check(ctx)
        assert not result.passed
        assert any("above" in e.lower() for e in result.errors)

    def test_invalid_key_format_warns(self, simple_df):
        ctx = ValidationContext(
            tables={"order": simple_df},
            config={"ranges": {"bad_key": {"min": 0}}},
        )
        result = RangeConstraintGate().check(ctx)
        assert result.passed
        assert len(result.warnings) > 0

    def test_missing_table_warns(self):
        ctx = ValidationContext(
            tables={},
            config={"ranges": {"nonexistent.amount": {"min": 0}}},
        )
        result = RangeConstraintGate().check(ctx)
        assert result.passed
        assert len(result.warnings) > 0


# ---------------------------------------------------------------------------
# TemporalConsistencyGate
# ---------------------------------------------------------------------------

class TestTemporalConsistencyGate:
    def test_empty_config_passes(self, ctx):
        result = TemporalConsistencyGate().check(ctx)
        assert result.passed

    def test_dates_in_range_passes(self):
        df = pd.DataFrame({
            "order_id": [1, 2],
            "created_at": pd.to_datetime(["2023-01-01", "2023-06-15"]),
        })
        ctx = ValidationContext(
            tables={"order": df},
            config={"date_range": {"start": "2022-01-01", "end": "2024-12-31"}},
        )
        result = TemporalConsistencyGate().check(ctx)
        assert result.passed

    def test_dates_before_range_fails(self):
        df = pd.DataFrame({
            "order_id": [1],
            "created_at": pd.to_datetime(["2019-01-01"]),
        })
        ctx = ValidationContext(
            tables={"order": df},
            config={"date_range": {"start": "2020-01-01", "end": "2025-12-31"}},
        )
        result = TemporalConsistencyGate().check(ctx)
        assert not result.passed
        assert any("before" in e.lower() for e in result.errors)

    def test_no_future_constraint_violation_fails(self):
        df = pd.DataFrame({
            "order_id": [1],
            "created_at": pd.to_datetime(["2099-01-01"]),
        })
        ctx = ValidationContext(
            tables={"order": df},
            config={"no_future": ["order.created_at"]},
        )
        result = TemporalConsistencyGate().check(ctx)
        assert not result.passed
        assert any("future" in e.lower() for e in result.errors)

    def test_temporal_ordering_violation_fails(self):
        df = pd.DataFrame({
            "order_id": [1],
            "order_date": pd.to_datetime(["2023-06-01"]),
            "ship_date": pd.to_datetime(["2023-05-01"]),  # before order!
        })
        ctx = ValidationContext(
            tables={"order": df},
            config={
                "ordering": [
                    {"table": "order", "start": "order_date", "end": "ship_date"}
                ]
            },
        )
        result = TemporalConsistencyGate().check(ctx)
        assert not result.passed

    def test_temporal_ordering_valid_passes(self):
        df = pd.DataFrame({
            "order_id": [1, 2],
            "order_date": pd.to_datetime(["2023-01-01", "2023-03-01"]),
            "ship_date": pd.to_datetime(["2023-01-15", "2023-03-20"]),
        })
        ctx = ValidationContext(
            tables={"order": df},
            config={
                "ordering": [
                    {"table": "order", "start": "order_date", "end": "ship_date"}
                ]
            },
        )
        result = TemporalConsistencyGate().check(ctx)
        assert result.passed


# ---------------------------------------------------------------------------
# FileFormatGate
# ---------------------------------------------------------------------------

class TestFileFormatGate:
    def test_no_files_passes_with_warning(self):
        result = FileFormatGate().check(ValidationContext())
        assert result.passed
        assert len(result.warnings) > 0

    def test_missing_file_fails(self, tmp_path):
        ctx = ValidationContext(file_paths=[tmp_path / "nonexistent.parquet"])
        result = FileFormatGate().check(ctx)
        assert not result.passed
        assert any("not found" in e.lower() for e in result.errors)

    def test_valid_parquet_passes(self, tmp_path, simple_df):
        path = tmp_path / "data.parquet"
        simple_df.to_parquet(path, index=False)
        ctx = ValidationContext(file_paths=[path])
        result = FileFormatGate().check(ctx)
        assert result.passed
        assert result.details[str(path)]["rows"] == len(simple_df)

    def test_valid_csv_passes(self, tmp_path, simple_df):
        path = tmp_path / "data.csv"
        simple_df.to_csv(path, index=False)
        ctx = ValidationContext(file_paths=[path])
        result = FileFormatGate().check(ctx)
        assert result.passed

    def test_valid_jsonl_passes(self, tmp_path, simple_df):
        path = tmp_path / "data.jsonl"
        simple_df.to_json(path, orient="records", lines=True)
        ctx = ValidationContext(file_paths=[path])
        result = FileFormatGate().check(ctx)
        assert result.passed

    def test_empty_file_fails(self, tmp_path):
        path = tmp_path / "empty.parquet"
        path.write_bytes(b"")
        ctx = ValidationContext(file_paths=[path])
        result = FileFormatGate().check(ctx)
        assert not result.passed
        assert any("empty" in e.lower() for e in result.errors)

    def test_unknown_extension_warns(self, tmp_path):
        path = tmp_path / "data.xlsx"
        path.write_bytes(b"some content here")
        ctx = ValidationContext(file_paths=[path])
        result = FileFormatGate().check(ctx)
        assert result.passed
        assert len(result.warnings) > 0


# ---------------------------------------------------------------------------
# SchemaDriftGate
# ---------------------------------------------------------------------------

class TestSchemaDriftGate:
    def test_no_baseline_passes_with_warning(self):
        result = SchemaDriftGate().check(ValidationContext())
        assert result.passed
        assert len(result.warnings) > 0

    def test_matching_schema_passes(self, simple_df):
        baseline = {
            "order": {
                "columns": {
                    "order_id": str(simple_df["order_id"].dtype),
                    "amount": str(simple_df["amount"].dtype),
                }
            }
        }
        ctx = ValidationContext(
            tables={"order": simple_df},
            config={"baseline": baseline},
        )
        result = SchemaDriftGate().check(ctx)
        assert result.passed

    def test_removed_column_fails(self, simple_df):
        baseline = {
            "order": {
                "columns": {
                    "order_id": str(simple_df["order_id"].dtype),
                    "amount": str(simple_df["amount"].dtype),
                    "removed_col": "object",
                }
            }
        }
        ctx = ValidationContext(
            tables={"order": simple_df},
            config={"baseline": baseline},
        )
        result = SchemaDriftGate().check(ctx)
        assert not result.passed
        assert any("removed_col" in e for e in result.errors)
        assert len(result.details["breaking"]) > 0

    def test_new_column_warns(self, simple_df):
        baseline = {
            "order": {
                "columns": {
                    "order_id": str(simple_df["order_id"].dtype),
                }
            }
        }
        ctx = ValidationContext(
            tables={"order": simple_df},
            config={"baseline": baseline},
        )
        result = SchemaDriftGate().check(ctx)
        assert result.passed  # additive only — no error
        assert len(result.details["additive"]) > 0

    def test_removed_table_fails(self, simple_df):
        baseline = {
            "order": {"columns": {"order_id": "int64"}},
            "missing_table": {"columns": {"id": "int64"}},
        }
        ctx = ValidationContext(
            tables={"order": simple_df},
            config={"baseline": baseline},
        )
        result = SchemaDriftGate().check(ctx)
        assert not result.passed

    def test_new_table_warns(self, simple_df):
        # Baseline knows about "other_table" only; "order" is new (additive)
        baseline = {"other_table": {"columns": {"id": "int64"}}}
        ctx = ValidationContext(
            tables={"order": simple_df},
            config={"baseline": baseline},
        )
        result = SchemaDriftGate().check(ctx)
        # "other_table" removed → breaking error; "order" is new → additive warning
        # We just check the additive list is populated
        assert len(result.details["additive"]) > 0


# ---------------------------------------------------------------------------
# GateRunner
# ---------------------------------------------------------------------------

class TestGateRunner:
    def test_available_gates_returns_all_builtins(self):
        gates = GateRunner.available_gates()
        assert "referential_integrity" in gates
        assert "schema_conformance" in gates
        assert "null_constraint" in gates
        assert "unique_constraint" in gates
        assert "range_constraint" in gates
        assert "temporal_consistency" in gates
        assert "file_format" in gates
        assert "schema_drift" in gates

    def test_default_runner_creates_all_gates(self):
        runner = GateRunner()
        assert len(runner._gates) == len(GateRunner.available_gates())

    def test_named_gate_subset(self):
        runner = GateRunner(gates=["null_constraint", "unique_constraint"])
        assert len(runner._gates) == 2

    def test_unknown_gate_raises(self):
        with pytest.raises(ValueError, match="Unknown gate"):
            GateRunner(gates=["nonexistent_gate"])

    def test_run_all_returns_list_of_results(self, ctx):
        runner = GateRunner(gates=["null_constraint", "schema_conformance"])
        results = runner.run_all(ctx)
        assert len(results) == 2
        assert all(isinstance(r, GateResult) for r in results)

    def test_run_gate_by_name(self, ctx):
        runner = GateRunner()
        result = runner.run_gate("null_constraint", ctx)
        assert isinstance(result, GateResult)
        assert result.gate_name == "null_constraint"

    def test_run_gate_unknown_raises(self, ctx):
        runner = GateRunner()
        with pytest.raises(ValueError):
            runner.run_gate("nonexistent", ctx)

    def test_register_custom_gate(self, ctx, monkeypatch):
        class MyCustomGate(ValidationGate):
            name = "my_custom_gate"

            def check(self, context: ValidationContext) -> GateResult:
                return GateResult(gate_name=self.name, passed=True)

        import sqllocks_spindle.validation.gates as _gates_mod
        monkeypatch.setitem(_gates_mod._GATE_REGISTRY, "my_custom_gate", MyCustomGate)
        assert "my_custom_gate" in GateRunner.available_gates()
        runner = GateRunner(gates=["my_custom_gate"])
        results = runner.run_all(ctx)
        assert results[0].passed

    def test_summary_all_passed(self, ctx):
        runner = GateRunner(gates=["null_constraint"])
        results = runner.run_all(ctx)
        summary = GateRunner.summary(results)
        assert summary["all_passed"] is True
        assert summary["passed"] == 1
        assert summary["failed"] == 0

    def test_summary_with_failure(self):
        results = [
            GateResult(gate_name="gate_a", passed=True),
            GateResult(gate_name="gate_b", passed=False, errors=["bad thing"]),
        ]
        summary = GateRunner.summary(results)
        assert not summary["all_passed"]
        assert summary["failed"] == 1
        assert "gate_b" in summary["failed_gates"]
        assert summary["total_errors"] == 1

    def test_gate_instance_accepted_directly(self, ctx):
        gate = NullConstraintGate()
        runner = GateRunner(gates=[gate])
        results = runner.run_all(ctx)
        assert len(results) == 1
        assert results[0].gate_name == "null_constraint"
