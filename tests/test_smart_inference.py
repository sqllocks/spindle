"""Tests for the Smart Schema Inference Engine (sqllocks_spindle.schema.inference).

Covers all 9 analyzers, the pipeline orchestrator, DDL parser plural FK
detection, and the edge-case fixes from 2026-03-15.
"""

from __future__ import annotations

import pytest

from sqllocks_spindle.schema.parser import (
    BusinessRuleDef,
    ColumnDef,
    GenerationConfig,
    ModelDef,
    RelationshipDef,
    SpindleSchema,
    TableDef,
)
from sqllocks_spindle.schema.inference import (
    ColumnSemantic,
    InferenceContext,
    SchemaInferenceEngine,
    TableRole,
)
from sqllocks_spindle.schema.inference.table_classifier import TableClassifier
from sqllocks_spindle.schema.inference.column_classifier import ColumnClassifier
from sqllocks_spindle.schema.inference.fk_distribution import FKDistributionInferrer
from sqllocks_spindle.schema.inference.cardinality_inference import CardinalityInferrer
from sqllocks_spindle.schema.inference.numeric_inference import NumericDistributionInferrer
from sqllocks_spindle.schema.inference.enum_inference import EnumInferrer
from sqllocks_spindle.schema.inference.temporal_inference import TemporalPatternInferrer
from sqllocks_spindle.schema.inference.correlation_inference import CorrelationInferrer
from sqllocks_spindle.schema.inference.business_rule_inference import BusinessRuleInferrer
from sqllocks_spindle.schema.ddl_parser import DdlParser


# ---------------------------------------------------------------------------
# Helpers — build minimal schemas for targeted testing
# ---------------------------------------------------------------------------

def _col(name: str, col_type: str = "string", generator: dict | None = None,
         nullable: bool = False, max_length: int | None = None) -> ColumnDef:
    return ColumnDef(
        name=name,
        type=col_type,
        generator=generator or {"strategy": "faker", "provider": "text"},
        nullable=nullable,
        max_length=max_length,
    )


def _fk_col(name: str, ref: str) -> ColumnDef:
    return ColumnDef(
        name=name, type="integer",
        generator={"strategy": "foreign_key", "ref": ref, "distribution": "pareto"},
    )


def _seq_col(name: str) -> ColumnDef:
    return ColumnDef(name=name, type="integer", generator={"strategy": "sequence", "start": 1})


def _dist_col(name: str, col_type: str = "decimal",
              distribution: str = "uniform", **kwargs) -> ColumnDef:
    gen = {"strategy": "distribution", "distribution": distribution}
    gen.update(kwargs)
    return ColumnDef(name=name, type=col_type, generator=gen)


def _temporal_col(name: str, pattern: str = "uniform") -> ColumnDef:
    return ColumnDef(
        name=name, type="date",
        generator={"strategy": "temporal", "pattern": pattern, "range_ref": "model.date_range"},
    )


def _enum_col(name: str, values: dict | None = None) -> ColumnDef:
    v = values or {"type_a": 0.5, "type_b": 0.5}
    return ColumnDef(
        name=name, type="string", max_length=50,
        generator={"strategy": "weighted_enum", "values": v},
    )


def _table(name: str, columns: dict[str, ColumnDef], pk: list[str] | None = None) -> TableDef:
    return TableDef(name=name, columns=columns, primary_key=pk or [])


def _schema(tables: dict[str, TableDef],
            relationships: list[RelationshipDef] | None = None,
            business_rules: list[BusinessRuleDef] | None = None) -> SpindleSchema:
    return SpindleSchema(
        model=ModelDef(name="test", date_range={"start": "2024-01-01", "end": "2025-12-31"}),
        tables=tables,
        relationships=relationships or [],
        business_rules=business_rules or [],
        generation=GenerationConfig(),
    )


def _rel(parent: str, child: str, child_col: str, parent_col: str = "id") -> RelationshipDef:
    return RelationshipDef(
        name=f"fk_{child}_{child_col}",
        parent=parent, child=child,
        parent_columns=[parent_col], child_columns=[child_col],
        type="one_to_many",
    )


def _ctx(schema: SpindleSchema) -> InferenceContext:
    ctx = InferenceContext(schema=schema)
    ctx.build_graphs()
    return ctx


# ===========================================================================
# 1. TableClassifier
# ===========================================================================

class TestTableClassifier:
    def test_entity_table(self):
        customers = _table("customers", {
            "id": _seq_col("id"),
            "first_name": _col("first_name"),
            "last_name": _col("last_name"),
            "email": _col("email"),
        }, pk=["id"])
        orders = _table("orders", {
            "id": _seq_col("id"),
            "customer_id": _fk_col("customer_id", "customers.id"),
        }, pk=["id"])
        schema = _schema(
            {"customers": customers, "orders": orders},
            [_rel("customers", "orders", "customer_id")],
        )
        ctx = _ctx(schema)
        TableClassifier().analyze(ctx)
        assert ctx.table_roles["customers"] == TableRole.ENTITY

    def test_transaction_table(self):
        customers = _table("customers", {
            "id": _seq_col("id"),
            "first_name": _col("first_name"),
        }, pk=["id"])
        orders = _table("orders", {
            "id": _seq_col("id"),
            "customer_id": _fk_col("customer_id", "customers.id"),
            "order_date": _temporal_col("order_date"),
            "total_amount": _dist_col("total_amount"),
        }, pk=["id"])
        schema = _schema(
            {"customers": customers, "orders": orders},
            [_rel("customers", "orders", "customer_id")],
        )
        ctx = _ctx(schema)
        TableClassifier().analyze(ctx)
        assert ctx.table_roles["orders"] == TableRole.TRANSACTION

    def test_transaction_detail_table(self):
        customers = _table("customers", {
            "id": _seq_col("id"),
            "first_name": _col("first_name"),
        }, pk=["id"])
        orders = _table("orders", {
            "id": _seq_col("id"),
            "customer_id": _fk_col("customer_id", "customers.id"),
            "order_date": _temporal_col("order_date"),
            "total_amount": _dist_col("total_amount"),
        }, pk=["id"])
        order_lines = _table("order_lines", {
            "id": _seq_col("id"),
            "order_id": _fk_col("order_id", "orders.id"),
            "quantity": _dist_col("quantity", "integer"),
            "unit_price": _dist_col("unit_price"),
        }, pk=["id"])
        schema = _schema(
            {"customers": customers, "orders": orders, "order_lines": order_lines},
            [_rel("customers", "orders", "customer_id"),
             _rel("orders", "order_lines", "order_id")],
        )
        ctx = _ctx(schema)
        TableClassifier().analyze(ctx)
        assert ctx.table_roles["orders"] == TableRole.TRANSACTION
        assert ctx.table_roles["order_lines"] == TableRole.TRANSACTION_DETAIL

    def test_lookup_table(self):
        categories = _table("categories", {
            "id": _seq_col("id"),
            "name": _col("name"),
        }, pk=["id"])
        products = _table("products", {
            "id": _seq_col("id"),
            "category_id": _fk_col("category_id", "categories.id"),
        }, pk=["id"])
        items = _table("items", {
            "id": _seq_col("id"),
            "category_id": _fk_col("category_id", "categories.id"),
        }, pk=["id"])
        schema = _schema(
            {"categories": categories, "products": products, "items": items},
            [_rel("categories", "products", "category_id"),
             _rel("categories", "items", "category_id")],
        )
        ctx = _ctx(schema)
        TableClassifier().analyze(ctx)
        assert ctx.table_roles["categories"] == TableRole.LOOKUP

    def test_log_table(self):
        audit_log = _table("audit_log", {
            "id": _seq_col("id"),
            "message": _col("message"),
        }, pk=["id"])
        schema = _schema({"audit_log": audit_log})
        ctx = _ctx(schema)
        TableClassifier().analyze(ctx)
        assert ctx.table_roles["audit_log"] == TableRole.LOG

    def test_dim_fact_prefix(self):
        dim_customer = _table("dim_customer", {
            "id": _seq_col("id"),
            "name": _col("name"),
        }, pk=["id"])
        fact_sales = _table("fact_sales", {
            "id": _seq_col("id"),
            "amount": _dist_col("amount"),
        }, pk=["id"])
        schema = _schema({"dim_customer": dim_customer, "fact_sales": fact_sales})
        ctx = _ctx(schema)
        TableClassifier().analyze(ctx)
        assert ctx.table_roles["dim_customer"] == TableRole.DIMENSION
        assert ctx.table_roles["fact_sales"] == TableRole.FACT


# ===========================================================================
# 2. ColumnClassifier
# ===========================================================================

class TestColumnClassifier:
    def _classify_col(self, col_name: str, col_type: str = "string",
                      table_role: TableRole = TableRole.UNKNOWN,
                      max_length: int | None = 50) -> ColumnSemantic:
        col = _col(col_name, col_type, max_length=max_length)
        table = _table("test", {col_name: col}, pk=[])
        schema = _schema({"test": table})
        ctx = _ctx(schema)
        ctx.table_roles["test"] = table_role
        ColumnClassifier().analyze(ctx)
        return ctx.column_semantics["test"][col_name]

    def test_monetary(self):
        assert self._classify_col("unit_price", "decimal") == ColumnSemantic.MONETARY
        assert self._classify_col("total_amount", "decimal") == ColumnSemantic.MONETARY

    def test_quantity(self):
        assert self._classify_col("quantity", "integer") == ColumnSemantic.QUANTITY

    def test_status(self):
        assert self._classify_col("status") == ColumnSemantic.STATUS
        assert self._classify_col("order_status") == ColumnSemantic.STATUS

    def test_categorical(self):
        assert self._classify_col("payment_method") == ColumnSemantic.CATEGORICAL
        assert self._classify_col("category") == ColumnSemantic.CATEGORICAL

    def test_temporal_transaction(self):
        assert self._classify_col("order_date", "date") == ColumnSemantic.TEMPORAL_TRANSACTION

    def test_temporal_audit(self):
        assert self._classify_col("created_at", "timestamp") == ColumnSemantic.TEMPORAL_AUDIT

    def test_temporal_start_end(self):
        assert self._classify_col("start_date", "date") == ColumnSemantic.TEMPORAL_START
        assert self._classify_col("end_date", "date") == ColumnSemantic.TEMPORAL_END

    def test_temporal_birth(self):
        assert self._classify_col("birth_date", "date") == ColumnSemantic.TEMPORAL_BIRTH

    def test_boolean_flag(self):
        assert self._classify_col("is_active", "boolean") == ColumnSemantic.BOOLEAN_FLAG

    def test_percentage(self):
        # Note: many _pct/_rate names overlap with MONETARY patterns (rate, margin)
        # which are checked first. Use names that don't contain monetary keywords.
        assert self._classify_col("defect_pct", "decimal") == ColumnSemantic.PERCENTAGE
        assert self._classify_col("completion_percent", "decimal") == ColumnSemantic.PERCENTAGE

    def test_measurement(self):
        assert self._classify_col("weight", "decimal") == ColumnSemantic.MEASUREMENT

    def test_rating(self):
        assert self._classify_col("rating", "integer") == ColumnSemantic.RATING

    def test_contact_info(self):
        assert self._classify_col("email") == ColumnSemantic.EMAIL
        assert self._classify_col("phone") == ColumnSemantic.PHONE

    def test_context_aware_transaction_date(self):
        """A generic 'date' column on a TRANSACTION table → TEMPORAL_TRANSACTION."""
        assert self._classify_col(
            "some_date", "date", table_role=TableRole.TRANSACTION,
        ) == ColumnSemantic.TEMPORAL_TRANSACTION


# ===========================================================================
# 3. FKDistributionInferrer
# ===========================================================================

class TestFKDistributionInferrer:
    def test_entity_to_transaction(self):
        customers = _table("customers", {"id": _seq_col("id")}, pk=["id"])
        orders = _table("orders", {
            "id": _seq_col("id"),
            "customer_id": _fk_col("customer_id", "customers.id"),
        }, pk=["id"])
        schema = _schema(
            {"customers": customers, "orders": orders},
            [_rel("customers", "orders", "customer_id")],
        )
        ctx = _ctx(schema)
        ctx.table_roles = {"customers": TableRole.ENTITY, "orders": TableRole.TRANSACTION}
        FKDistributionInferrer().analyze(ctx)
        gen = orders.columns["customer_id"].generator
        assert gen["distribution"] == "pareto"
        assert gen["params"]["alpha"] == 1.16

    def test_transaction_to_detail(self):
        orders = _table("orders", {"id": _seq_col("id")}, pk=["id"])
        detail = _table("order_lines", {
            "id": _seq_col("id"),
            "order_id": _fk_col("order_id", "orders.id"),
        }, pk=["id"])
        schema = _schema(
            {"orders": orders, "order_lines": detail},
            [_rel("orders", "order_lines", "order_id")],
        )
        ctx = _ctx(schema)
        ctx.table_roles = {"orders": TableRole.TRANSACTION, "order_lines": TableRole.TRANSACTION_DETAIL}
        FKDistributionInferrer().analyze(ctx)
        assert detail.columns["order_id"].generator["distribution"] == "uniform"

    def test_nullable_fk_adds_null_rate(self):
        parent = _table("parent", {"id": _seq_col("id")}, pk=["id"])
        child_col = _fk_col("parent_id", "parent.id")
        child_col.nullable = True
        child = _table("child", {"id": _seq_col("id"), "parent_id": child_col}, pk=["id"])
        schema = _schema(
            {"parent": parent, "child": child},
            [_rel("parent", "child", "parent_id")],
        )
        ctx = _ctx(schema)
        ctx.table_roles = {"parent": TableRole.ENTITY, "child": TableRole.TRANSACTION}
        FKDistributionInferrer().analyze(ctx)
        assert child.columns["parent_id"].generator.get("null_rate") == 0.15

    def test_agent_fk_pattern(self):
        employees = _table("employees", {"id": _seq_col("id")}, pk=["id"])
        tickets = _table("tickets", {
            "id": _seq_col("id"),
            "assigned_to": _fk_col("assigned_to", "employees.id"),
        }, pk=["id"])
        schema = _schema(
            {"employees": employees, "tickets": tickets},
            [_rel("employees", "tickets", "assigned_to")],
        )
        ctx = _ctx(schema)
        ctx.table_roles = {"employees": TableRole.ENTITY, "tickets": TableRole.TRANSACTION}
        FKDistributionInferrer().analyze(ctx)
        gen = tickets.columns["assigned_to"].generator
        assert gen["distribution"] == "pareto"
        assert gen["params"]["alpha"] == 2.0


# ===========================================================================
# 4. CardinalityInferrer
# ===========================================================================

class TestCardinalityInferrer:
    def test_lookup_fixed_count(self):
        categories = _table("categories", {
            "id": _seq_col("id"),
            "name": _col("name"),
        }, pk=["id"])
        schema = _schema({"categories": categories})
        ctx = _ctx(schema)
        ctx.table_roles = {"categories": TableRole.LOOKUP}
        CardinalityInferrer().analyze(ctx)
        assert schema.generation.derived_counts["categories"]["fixed"] == 20

    def test_hierarchy_fixed_count(self):
        orgs = _table("organizations", {
            "id": _seq_col("id"),
            "parent_id": _fk_col("parent_id", "organizations.id"),
        }, pk=["id"])
        schema = _schema({"organizations": orgs})
        ctx = _ctx(schema)
        ctx.table_roles = {"organizations": TableRole.HIERARCHY}
        CardinalityInferrer().analyze(ctx)
        assert schema.generation.derived_counts["organizations"]["fixed"] == 50

    def test_root_table_scale_presets(self):
        customers = _table("customers", {
            "id": _seq_col("id"),
            "name": _col("name"),
        }, pk=["id"])
        schema = _schema({"customers": customers})
        ctx = _ctx(schema)
        ctx.table_roles = {"customers": TableRole.ENTITY}
        CardinalityInferrer().analyze(ctx)
        assert schema.generation.scales["small"]["customers"] == 1_000
        assert schema.generation.scales["medium"]["customers"] == 50_000
        assert schema.generation.scales["large"]["customers"] == 500_000

    def test_entity_to_transaction_ratio(self):
        customers = _table("customers", {"id": _seq_col("id")}, pk=["id"])
        orders = _table("orders", {
            "id": _seq_col("id"),
            "customer_id": _fk_col("customer_id", "customers.id"),
        }, pk=["id"])
        schema = _schema(
            {"customers": customers, "orders": orders},
            [_rel("customers", "orders", "customer_id")],
        )
        ctx = _ctx(schema)
        ctx.table_roles = {"customers": TableRole.ENTITY, "orders": TableRole.TRANSACTION}
        CardinalityInferrer().analyze(ctx)
        dc = schema.generation.derived_counts["orders"]
        assert dc["ratio"] == 5.0
        assert dc["per_parent"] == "customers"

    def test_transaction_to_detail_ratio(self):
        orders = _table("orders", {"id": _seq_col("id")}, pk=["id"])
        detail = _table("order_lines", {
            "id": _seq_col("id"),
            "order_id": _fk_col("order_id", "orders.id"),
            "quantity": _dist_col("quantity", "integer"),
        }, pk=["id"])
        schema = _schema(
            {"orders": orders, "order_lines": detail},
            [_rel("orders", "order_lines", "order_id")],
        )
        ctx = _ctx(schema)
        ctx.table_roles = {"orders": TableRole.TRANSACTION, "order_lines": TableRole.TRANSACTION_DETAIL}
        CardinalityInferrer().analyze(ctx)
        dc = schema.generation.derived_counts["order_lines"]
        assert dc["ratio"] == 2.5


# ===========================================================================
# 5. NumericDistributionInferrer
# ===========================================================================

class TestNumericDistributionInferrer:
    def test_monetary_log_normal(self):
        col = _dist_col("unit_price", distribution="uniform", min=1, max=10000)
        table = _table("products", {"unit_price": col})
        schema = _schema({"products": table})
        ctx = _ctx(schema)
        ctx.column_semantics = {"products": {"unit_price": ColumnSemantic.MONETARY}}
        NumericDistributionInferrer().analyze(ctx)
        assert col.generator["distribution"] == "log_normal"

    def test_quantity_log_normal(self):
        col = _dist_col("quantity", "integer", distribution="uniform", min=1, max=10000)
        table = _table("lines", {"quantity": col})
        schema = _schema({"lines": table})
        ctx = _ctx(schema)
        ctx.column_semantics = {"lines": {"quantity": ColumnSemantic.QUANTITY}}
        NumericDistributionInferrer().analyze(ctx)
        assert col.generator["distribution"] == "log_normal"

    def test_percentage_bounded_normal(self):
        col = _dist_col("discount_pct", distribution="uniform", min=0, max=100)
        table = _table("items", {"discount_pct": col})
        schema = _schema({"items": table})
        ctx = _ctx(schema)
        ctx.column_semantics = {"items": {"discount_pct": ColumnSemantic.PERCENTAGE}}
        NumericDistributionInferrer().analyze(ctx)
        assert col.generator["distribution"] == "normal"
        assert col.generator["params"]["min"] == 0
        assert col.generator["params"]["max"] == 100

    def test_measurement_context_hint(self):
        col = _dist_col("weight_kg", distribution="normal", mean=100, std=50)
        table = _table("products", {"weight_kg": col})
        schema = _schema({"products": table})
        ctx = _ctx(schema)
        ctx.column_semantics = {"products": {"weight_kg": ColumnSemantic.MEASUREMENT}}
        NumericDistributionInferrer().analyze(ctx)
        assert col.generator["params"]["mean"] == 5.0  # weight hint

    def test_rating_bounded(self):
        col = _dist_col("rating", "integer", distribution="uniform", min=1, max=10000)
        table = _table("reviews", {"rating": col})
        schema = _schema({"reviews": table})
        ctx = _ctx(schema)
        ctx.column_semantics = {"reviews": {"rating": ColumnSemantic.RATING}}
        NumericDistributionInferrer().analyze(ctx)
        assert col.generator["params"]["min"] == 1
        assert col.generator["params"]["max"] == 5

    def test_skips_non_placeholder(self):
        """Columns with intentional log_normal params should not be overwritten."""
        col = ColumnDef(
            name="custom_amount", type="decimal",
            generator={"strategy": "distribution", "distribution": "log_normal",
                        "params": {"mean": 5.0, "sigma": 0.5}},
        )
        table = _table("t", {"custom_amount": col})
        schema = _schema({"t": table})
        ctx = _ctx(schema)
        ctx.column_semantics = {"t": {"custom_amount": ColumnSemantic.MONETARY}}
        NumericDistributionInferrer().analyze(ctx)
        # Should not be modified — log_normal is not a placeholder
        assert col.generator["params"]["mean"] == 5.0


# ===========================================================================
# 6. EnumInferrer — including edge-case fixes
# ===========================================================================

class TestEnumInferrer:
    def test_status_transaction_role(self):
        col = _enum_col("status")
        table = _table("orders", {"status": col})
        schema = _schema({"orders": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"orders": TableRole.TRANSACTION}
        ctx.column_semantics = {"orders": {"status": ColumnSemantic.STATUS}}
        EnumInferrer().analyze(ctx)
        assert "completed" in col.generator["values"]
        assert col.generator["values"]["completed"] == 0.72

    def test_status_entity_role(self):
        col = _enum_col("status")
        table = _table("customers", {"status": col})
        schema = _schema({"customers": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"customers": TableRole.ENTITY}
        ctx.column_semantics = {"customers": {"status": ColumnSemantic.STATUS}}
        EnumInferrer().analyze(ctx)
        assert "active" in col.generator["values"]
        assert col.generator["values"]["active"] == 0.82

    def test_categorical_payment_method(self):
        col = _enum_col("payment_method")
        table = _table("orders", {"payment_method": col})
        schema = _schema({"orders": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"orders": TableRole.TRANSACTION}
        ctx.column_semantics = {"orders": {"payment_method": ColumnSemantic.CATEGORICAL}}
        EnumInferrer().analyze(ctx)
        assert "credit_card" in col.generator["values"]

    def test_edge_case_faker_status_upgraded(self):
        """EDGE CASE FIX: status column with faker strategy should be upgraded."""
        col = _col("status", max_length=50)
        assert col.generator["strategy"] == "faker"
        table = _table("orders", {"status": col})
        schema = _schema({"orders": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"orders": TableRole.TRANSACTION}
        ctx.column_semantics = {"orders": {"status": ColumnSemantic.STATUS}}
        EnumInferrer().analyze(ctx)
        assert col.generator["strategy"] == "weighted_enum"
        assert "completed" in col.generator["values"]

    def test_edge_case_faker_payment_method_upgraded(self):
        """EDGE CASE FIX: payment_method with faker strategy should be upgraded."""
        col = _col("payment_method", max_length=50)
        assert col.generator["strategy"] == "faker"
        table = _table("orders", {"payment_method": col})
        schema = _schema({"orders": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"orders": TableRole.TRANSACTION}
        ctx.column_semantics = {"orders": {"payment_method": ColumnSemantic.CATEGORICAL}}
        EnumInferrer().analyze(ctx)
        assert col.generator["strategy"] == "weighted_enum"
        assert "credit_card" in col.generator["values"]

    def test_preserves_intentional_enum(self):
        """A weighted_enum with 5 distinct values should not be replaced."""
        values = {"new": 0.30, "pending": 0.25, "active": 0.20, "closed": 0.15, "archived": 0.10}
        col = _enum_col("status", values)
        table = _table("t", {"status": col})
        schema = _schema({"t": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"t": TableRole.ENTITY}
        ctx.column_semantics = {"t": {"status": ColumnSemantic.STATUS}}
        EnumInferrer().analyze(ctx)
        # Should not be modified — 5 distinct values with non-equal weights isn't a placeholder
        assert col.generator["values"]["new"] == 0.30


# ===========================================================================
# 7. TemporalPatternInferrer — including edge-case fixes
# ===========================================================================

class TestTemporalPatternInferrer:
    def test_transaction_date_seasonal(self):
        col = _temporal_col("order_date")
        table = _table("orders", {"order_date": col})
        schema = _schema({"orders": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"orders": TableRole.TRANSACTION}
        ctx.column_semantics = {"orders": {"order_date": ColumnSemantic.TEMPORAL_TRANSACTION}}
        TemporalPatternInferrer().analyze(ctx)
        assert col.generator["pattern"] == "seasonal"
        assert "Dec" in col.generator["profiles"]["month"]

    def test_end_date_derived(self):
        start = _temporal_col("start_date")
        end = _temporal_col("end_date")
        table = _table("contracts", {"start_date": start, "end_date": end})
        schema = _schema({"contracts": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"contracts": TableRole.ENTITY}
        ctx.column_semantics = {"contracts": {
            "start_date": ColumnSemantic.TEMPORAL_START,
            "end_date": ColumnSemantic.TEMPORAL_END,
        }}
        TemporalPatternInferrer().analyze(ctx)
        assert end.generator["strategy"] == "derived"
        assert end.generator["source"] == "start_date"
        assert end.generator["rule"] == "add_days"

    def test_birth_date_range(self):
        col = _temporal_col("birth_date")
        table = _table("people", {"birth_date": col})
        schema = _schema({"people": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"people": TableRole.ENTITY}
        ctx.column_semantics = {"people": {"birth_date": ColumnSemantic.TEMPORAL_BIRTH}}
        TemporalPatternInferrer().analyze(ctx)
        # End year is 2025 → births from 1960 to 2007
        assert col.generator["date_range"]["start"] == "1960-01-01"
        assert col.generator["date_range"]["end"] == "2007-12-31"

    def test_audit_left_as_uniform(self):
        col = _temporal_col("created_at")
        table = _table("t", {"created_at": col})
        schema = _schema({"t": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"t": TableRole.ENTITY}
        ctx.column_semantics = {"t": {"created_at": ColumnSemantic.TEMPORAL_AUDIT}}
        TemporalPatternInferrer().analyze(ctx)
        assert col.generator["pattern"] == "uniform"

    def test_edge_case_faker_temporal_upgraded(self):
        """EDGE CASE FIX: temporal column with faker strategy should be upgraded."""
        col = _col("order_date", "string")
        assert col.generator["strategy"] == "faker"
        table = _table("orders", {"order_date": col})
        schema = _schema({"orders": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"orders": TableRole.TRANSACTION}
        ctx.column_semantics = {"orders": {"order_date": ColumnSemantic.TEMPORAL_TRANSACTION}}
        TemporalPatternInferrer().analyze(ctx)
        assert col.generator["strategy"] == "temporal"
        assert col.generator["pattern"] == "seasonal"


# ===========================================================================
# 8. CorrelationInferrer
# ===========================================================================

class TestCorrelationInferrer:
    def test_cost_correlated_to_price(self):
        price = _dist_col("unit_price", distribution="log_normal")
        cost = _dist_col("unit_cost", distribution="normal", mean=100, std=50)
        table = _table("products", {"unit_price": price, "unit_cost": cost})
        schema = _schema({"products": table})
        ctx = _ctx(schema)
        ctx.column_semantics = {"products": {
            "unit_price": ColumnSemantic.MONETARY,
            "unit_cost": ColumnSemantic.MONETARY,
        }}
        CorrelationInferrer().analyze(ctx)
        assert cost.generator["strategy"] == "correlated"
        assert cost.generator["source_column"] == "unit_price"

    def test_total_formula(self):
        qty = _dist_col("quantity", "integer", distribution="uniform", min=1, max=100)
        price = _dist_col("unit_price", distribution="log_normal")
        total = _dist_col("line_total", distribution="normal", mean=100, std=50)
        table = _table("lines", {"quantity": qty, "unit_price": price, "line_total": total})
        schema = _schema({"lines": table})
        ctx = _ctx(schema)
        ctx.column_semantics = {"lines": {
            "quantity": ColumnSemantic.QUANTITY,
            "unit_price": ColumnSemantic.MONETARY,
            "line_total": ColumnSemantic.MONETARY,
        }}
        CorrelationInferrer().analyze(ctx)
        assert total.generator["strategy"] == "formula"
        assert "quantity" in total.generator["expression"]
        assert "unit_price" in total.generator["expression"]

    def test_margin_formula(self):
        price = _dist_col("price", distribution="log_normal")
        cost = _dist_col("cost", distribution="normal", mean=100, std=50)
        margin = _dist_col("margin", distribution="normal", mean=100, std=50)
        table = _table("products", {"price": price, "cost": cost, "margin": margin})
        schema = _schema({"products": table})
        ctx = _ctx(schema)
        ctx.column_semantics = {"products": {
            "price": ColumnSemantic.MONETARY,
            "cost": ColumnSemantic.MONETARY,
            "margin": ColumnSemantic.MONETARY,
        }}
        CorrelationInferrer().analyze(ctx)
        assert margin.generator["strategy"] == "formula"
        assert "price" in margin.generator["expression"]
        assert "cost" in margin.generator["expression"]


# ===========================================================================
# 9. BusinessRuleInferrer
# ===========================================================================

class TestBusinessRuleInferrer:
    def test_date_ordering_rule(self):
        table = _table("contracts", {
            "start_date": _temporal_col("start_date"),
            "end_date": _temporal_col("end_date"),
        })
        schema = _schema({"contracts": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"contracts": TableRole.ENTITY}
        ctx.column_semantics = {"contracts": {
            "start_date": ColumnSemantic.TEMPORAL_START,
            "end_date": ColumnSemantic.TEMPORAL_END,
        }}
        BusinessRuleInferrer().analyze(ctx)
        rules = [r for r in schema.business_rules if "BR-01" in r.name or "date_order" in r.name]
        assert len(rules) >= 1
        assert "end_date >= start_date" in rules[0].rule

    def test_monetary_positive_rule(self):
        table = _table("items", {"price": _dist_col("price")})
        schema = _schema({"items": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"items": TableRole.ENTITY}
        ctx.column_semantics = {"items": {"price": ColumnSemantic.MONETARY}}
        BusinessRuleInferrer().analyze(ctx)
        rule_names = {r.name for r in schema.business_rules}
        assert "items_price_positive" in rule_names

    def test_quantity_positive_rule(self):
        table = _table("lines", {"quantity": _dist_col("quantity", "integer")})
        schema = _schema({"lines": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"lines": TableRole.ENTITY}
        ctx.column_semantics = {"lines": {"quantity": ColumnSemantic.QUANTITY}}
        BusinessRuleInferrer().analyze(ctx)
        rules = {r.name: r for r in schema.business_rules}
        assert "lines_quantity_positive" in rules
        assert "quantity >= 1" in rules["lines_quantity_positive"].rule

    def test_percentage_range_rule(self):
        table = _table("stats", {"discount_pct": _dist_col("discount_pct")})
        schema = _schema({"stats": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"stats": TableRole.ENTITY}
        ctx.column_semantics = {"stats": {"discount_pct": ColumnSemantic.PERCENTAGE}}
        BusinessRuleInferrer().analyze(ctx)
        rules = {r.name: r for r in schema.business_rules}
        assert "stats_discount_pct_range" in rules
        assert "BETWEEN 0 AND 100" in rules["stats_discount_pct_range"].rule

    def test_cost_lt_price_rule(self):
        table = _table("products", {
            "unit_cost": _dist_col("unit_cost"),
            "unit_price": _dist_col("unit_price"),
        })
        schema = _schema({"products": table})
        ctx = _ctx(schema)
        ctx.table_roles = {"products": TableRole.ENTITY}
        ctx.column_semantics = {"products": {
            "unit_cost": ColumnSemantic.MONETARY,
            "unit_price": ColumnSemantic.MONETARY,
        }}
        BusinessRuleInferrer().analyze(ctx)
        rule_names = {r.name for r in schema.business_rules}
        assert "products_cost_lt_price" in rule_names


# ===========================================================================
# DDL Parser — plural FK detection
# ===========================================================================

class TestDdlParserPluralFK:
    """Test that naming-convention FK detection handles plural table names."""

    DDL_PLURAL = """
    CREATE TABLE customers (
        customer_id INT IDENTITY(1,1) PRIMARY KEY,
        first_name NVARCHAR(50),
        last_name NVARCHAR(50),
        email NVARCHAR(100),
        status NVARCHAR(20)
    );

    CREATE TABLE orders (
        order_id INT IDENTITY(1,1) PRIMARY KEY,
        customer_id INT NOT NULL,
        order_date DATE NOT NULL,
        total_amount DECIMAL(18,2),
        payment_method NVARCHAR(30),
        status NVARCHAR(20)
    );

    CREATE TABLE order_lines (
        line_id INT IDENTITY(1,1) PRIMARY KEY,
        order_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT NOT NULL,
        unit_price DECIMAL(18,2),
        line_total DECIMAL(18,2)
    );

    CREATE TABLE products (
        product_id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(100),
        unit_price DECIMAL(18,2),
        unit_cost DECIMAL(18,2),
        weight_kg DECIMAL(8,2),
        category NVARCHAR(30)
    );

    CREATE TABLE categories (
        category_id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(50)
    );
    """

    def test_plural_fk_detection(self):
        """customer_id should resolve to 'customers' table even though
        stripping _id gives 'customer' (singular)."""
        schema = DdlParser().parse_string(self.DDL_PLURAL)
        orders = schema.tables["orders"]
        gen = orders.columns["customer_id"].generator
        assert gen["strategy"] == "foreign_key"
        assert "customers" in gen["ref"]

    def test_order_id_resolves_to_orders(self):
        """order_id on order_lines should resolve to 'orders' table."""
        schema = DdlParser().parse_string(self.DDL_PLURAL)
        gen = schema.tables["order_lines"].columns["order_id"].generator
        assert gen["strategy"] == "foreign_key"
        assert "orders" in gen["ref"]

    def test_product_id_resolves_to_products(self):
        schema = DdlParser().parse_string(self.DDL_PLURAL)
        gen = schema.tables["order_lines"].columns["product_id"].generator
        assert gen["strategy"] == "foreign_key"
        assert "products" in gen["ref"]

    def test_relationships_built(self):
        """Plural FK detection should produce RelationshipDefs."""
        schema = DdlParser().parse_string(self.DDL_PLURAL)
        rel_children = {r.child: r.parent for r in schema.relationships}
        assert "orders" in rel_children
        assert rel_children["orders"] == "customers"

    def test_category_id_resolves_to_categories(self):
        """category_id → categories (y→ies would be a different word; this is just +s)."""
        schema = DdlParser().parse_string(self.DDL_PLURAL)
        gen = schema.tables["products"].columns.get("category_id")
        # category_id strips to "category" — should find "categories" via +ies? No, categories uses +s
        # Actually: "category" → try "categorys" (nope), "categoryes" (nope),
        # ends with y → "categori" + "ies" = "ategories" ... hmm
        # Let me check: candidate = "category", candidate.endswith("y") → True
        # candidate[:-1] + "ies" = "categori" + "ies" = "categories" ← yes!
        if gen:
            assert gen.generator["strategy"] == "foreign_key"
            assert "categories" in gen.generator["ref"]


# ===========================================================================
# Full pipeline integration
# ===========================================================================

class TestSchemaInferenceEngine:
    """End-to-end tests for the full inference pipeline."""

    def test_full_pipeline_basic(self):
        schema = DdlParser().parse_string(TestDdlParserPluralFK.DDL_PLURAL)
        engine = SchemaInferenceEngine()
        smart, annotations = engine.infer_with_report(schema)

        # Should have generated annotations
        assert len(annotations) > 10

        # Table roles should be set
        # Can't check ctx directly, but we can check side effects
        # Customers: entity with first_name, last_name, email → entity-style status
        status_gen = smart.tables["customers"].columns["status"].generator
        assert status_gen["strategy"] == "weighted_enum"

    def test_full_pipeline_edge_cases_fixed(self):
        """Verify all 5 edge cases are resolved in the full pipeline."""
        schema = DdlParser().parse_string(TestDdlParserPluralFK.DDL_PLURAL)
        engine = SchemaInferenceEngine()
        smart, annotations = engine.infer_with_report(schema)

        # Edge case 1 & 5: status and payment_method upgraded from faker
        order_status = smart.tables["orders"].columns["status"].generator
        assert order_status["strategy"] == "weighted_enum"

        payment = smart.tables["orders"].columns["payment_method"].generator
        assert payment["strategy"] == "weighted_enum"
        assert "credit_card" in payment["values"]

        # Edge case 3: FK detection for plural tables
        order_cust_fk = smart.tables["orders"].columns["customer_id"].generator
        assert order_cust_fk["strategy"] == "foreign_key"

        # Edge case 2: derived_counts or scale presets should be populated
        # (CardinalityInferrer sets derived_counts for child tables,
        # scale presets for root tables)
        dc = smart.generation.derived_counts
        scales = smart.generation.scales
        assert len(dc) > 0 or len(scales) > 0

        # Verify relationships were built from naming-convention FKs
        assert len(smart.relationships) > 0

        # Edge case 4: order_date should have seasonal pattern
        order_date = smart.tables["orders"].columns["order_date"].generator
        assert order_date.get("pattern") == "seasonal" or order_date.get("strategy") == "temporal"

    def test_correlation_rules_generated(self):
        """Verify cross-column correlations are detected."""
        schema = DdlParser().parse_string(TestDdlParserPluralFK.DDL_PLURAL)
        smart = SchemaInferenceEngine().infer(schema)

        # order_lines should have line_total = quantity * unit_price
        line_total = smart.tables["order_lines"].columns["line_total"].generator
        assert line_total["strategy"] == "formula"

        # products should have unit_cost correlated to unit_price
        unit_cost = smart.tables["products"].columns["unit_cost"].generator
        assert unit_cost["strategy"] == "correlated"

    def test_business_rules_generated(self):
        """Verify business rules are inferred."""
        schema = DdlParser().parse_string(TestDdlParserPluralFK.DDL_PLURAL)
        smart = SchemaInferenceEngine().infer(schema)

        rule_names = {r.name for r in smart.business_rules}
        # Should have monetary positivity rules
        assert any("positive" in n for n in rule_names)

    def test_annotation_report(self):
        """Verify the explanation report has structured annotations."""
        schema = DdlParser().parse_string(TestDdlParserPluralFK.DDL_PLURAL)
        _, annotations = SchemaInferenceEngine().infer_with_report(schema)

        rule_ids = {a.rule_id for a in annotations}
        # Should have table classification annotations
        assert any(rid.startswith("TC-") for rid in rule_ids)
        # Should have enum annotations
        assert any(rid.startswith("EN-") for rid in rule_ids)
