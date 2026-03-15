"""Tests for CompositeDomain and SharedEntityRegistry."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.domains.composite import CompositeDomain
from sqllocks_spindle.domains.shared_registry import (
    DEFAULT_MAPPINGS,
    DomainEntityMapping,
    SharedConcept,
    SharedEntityRegistry,
)
from sqllocks_spindle.schema.parser import (
    ColumnDef,
    GenerationConfig,
    ModelDef,
    RelationshipDef,
    SpindleSchema,
    TableDef,
)


# ---------------------------------------------------------------------------
# Minimal Domain stubs for testing
# ---------------------------------------------------------------------------

def _make_table(
    name: str,
    pk: str | None = None,
    fk_ref: str | None = None,
    fk_col: str | None = None,
) -> TableDef:
    cols: dict[str, ColumnDef] = {
        f"{name}_id": ColumnDef(
            name=f"{name}_id",
            type="integer",
            generator={"strategy": "sequence"},
            nullable=False,
        )
    }
    if fk_col and fk_ref:
        cols[fk_col] = ColumnDef(
            name=fk_col,
            type="integer",
            generator={"strategy": "foreign_key", "ref": fk_ref},
        )
    return TableDef(
        name=name,
        columns=cols,
        primary_key=[pk or f"{name}_id"],
    )


def _make_spindle_schema(
    domain_name: str,
    tables: list[str],
    relationships: list[RelationshipDef] | None = None,
) -> SpindleSchema:
    table_defs = {t: _make_table(t) for t in tables}
    scales = {"small": {t: 10 for t in tables}}
    gen = GenerationConfig(scales=scales)
    return SpindleSchema(
        model=ModelDef(name=domain_name),
        tables=table_defs,
        relationships=relationships or [],
        business_rules=[],
        generation=gen,
    )


class _StubDomain(Domain):
    """Test-only domain that serves a pre-built schema."""

    def __init__(self, domain_name: str, tables: list[str],
                 relationships: list[RelationshipDef] | None = None):
        # Skip Domain.__init__ file-loading: set attributes directly
        self._schema_mode = "3nf"
        self._profile = {}
        self._domain_name = domain_name
        self._schema = _make_spindle_schema(domain_name, tables, relationships)

    @property
    def name(self) -> str:
        return self._domain_name

    @property
    def description(self) -> str:
        return f"Stub domain: {self._domain_name}"

    @property
    def domain_path(self) -> Path:
        return Path("/tmp")

    def _build_schema(self) -> SpindleSchema:
        return self._schema

    def get_schema(self) -> SpindleSchema:
        return self._schema


# ---------------------------------------------------------------------------
# CompositeDomain construction
# ---------------------------------------------------------------------------

class TestCompositeDomainConstruction:
    def test_empty_domains_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            CompositeDomain(domains=[])

    def test_duplicate_domain_names_raises(self):
        d1 = _StubDomain("retail", ["customer"])
        d2 = _StubDomain("retail", ["order"])
        with pytest.raises(ValueError, match="Duplicate"):
            CompositeDomain(domains=[d1, d2])

    def test_name_is_composite_of_sorted_domains(self):
        d1 = _StubDomain("retail", ["customer"])
        d2 = _StubDomain("hr", ["employee"])
        comp = CompositeDomain(domains=[d1, d2])
        assert comp.name == "composite_hr_retail"

    def test_description_lists_domains(self):
        d1 = _StubDomain("retail", ["customer"])
        d2 = _StubDomain("hr", ["employee"])
        comp = CompositeDomain(domains=[d1, d2])
        assert "retail" in comp.description
        assert "hr" in comp.description

    def test_child_domains_property(self):
        d1 = _StubDomain("retail", ["customer"])
        d2 = _StubDomain("hr", ["employee"])
        comp = CompositeDomain(domains=[d1, d2])
        names = [d.name for d in comp.child_domains]
        assert "retail" in names
        assert "hr" in names


# ---------------------------------------------------------------------------
# CompositeDomain.get_schema — table merging
# ---------------------------------------------------------------------------

class TestCompositeDomainSchema:
    def test_tables_are_prefixed(self):
        d1 = _StubDomain("retail", ["customer", "order"])
        d2 = _StubDomain("hr", ["employee"])
        comp = CompositeDomain(domains=[d1, d2])
        schema = comp.get_schema()
        assert "retail_customer" in schema.tables
        assert "retail_order" in schema.tables
        assert "hr_employee" in schema.tables

    def test_no_unprefixed_table_names(self):
        d1 = _StubDomain("retail", ["customer"])
        d2 = _StubDomain("hr", ["employee"])
        comp = CompositeDomain(domains=[d1, d2])
        schema = comp.get_schema()
        assert "customer" not in schema.tables
        assert "employee" not in schema.tables

    def test_table_count_equals_sum_of_child_tables(self):
        d1 = _StubDomain("retail", ["customer", "order"])
        d2 = _StubDomain("hr", ["employee", "department"])
        comp = CompositeDomain(domains=[d1, d2])
        schema = comp.get_schema()
        assert len(schema.tables) == 4

    def test_model_name_is_composite(self):
        d1 = _StubDomain("retail", ["customer"])
        comp = CompositeDomain(domains=[d1])
        schema = comp.get_schema()
        assert "composite" in schema.model.name

    def test_table_description_includes_domain_prefix(self):
        d1 = _StubDomain("retail", ["customer"])
        comp = CompositeDomain(domains=[d1])
        schema = comp.get_schema()
        assert "[retail]" in schema.tables["retail_customer"].description


# ---------------------------------------------------------------------------
# CompositeDomain — relationship merging
# ---------------------------------------------------------------------------

class TestCompositeDomainRelationships:
    def test_intra_domain_rel_is_prefixed(self):
        rel = RelationshipDef(
            name="order_customer",
            parent="customer",
            child="order",
            parent_columns=["customer_id"],
            child_columns=["customer_id"],
            type="many_to_one",
        )
        d1 = _StubDomain("retail", ["customer", "order"], relationships=[rel])
        comp = CompositeDomain(domains=[d1])
        schema = comp.get_schema()
        rel_names = [r.name for r in schema.relationships]
        assert "retail_order_customer" in rel_names

    def test_prefixed_rel_references_correct_tables(self):
        rel = RelationshipDef(
            name="order_customer",
            parent="customer",
            child="order",
            parent_columns=["customer_id"],
            child_columns=["customer_id"],
            type="many_to_one",
        )
        d1 = _StubDomain("retail", ["customer", "order"], relationships=[rel])
        comp = CompositeDomain(domains=[d1])
        schema = comp.get_schema()
        merged_rel = next(r for r in schema.relationships if "order_customer" in r.name)
        assert merged_rel.parent == "retail_customer"
        assert merged_rel.child == "retail_order"


# ---------------------------------------------------------------------------
# CompositeDomain — generation config merging
# ---------------------------------------------------------------------------

class TestCompositeDomainGenerationConfig:
    def test_merged_scales_contain_prefixed_tables(self):
        d1 = _StubDomain("retail", ["customer"])
        d2 = _StubDomain("hr", ["employee"])
        comp = CompositeDomain(domains=[d1, d2])
        schema = comp.get_schema()
        small = schema.generation.scales.get("small", {})
        assert "retail_customer" in small
        assert "hr_employee" in small


# ---------------------------------------------------------------------------
# SharedEntityRegistry — basic properties
# ---------------------------------------------------------------------------

class TestSharedEntityRegistry:
    def test_default_concepts_include_person(self):
        reg = SharedEntityRegistry()
        assert SharedConcept.PERSON in reg.concepts

    def test_get_mappings_returns_list(self):
        reg = SharedEntityRegistry()
        mappings = reg.get_mappings(SharedConcept.PERSON)
        assert len(mappings) > 0
        assert all(isinstance(m, DomainEntityMapping) for m in mappings)

    def test_get_mapping_for_domain_retail(self):
        reg = SharedEntityRegistry()
        mapping = reg.get_mapping_for_domain(SharedConcept.PERSON, "retail")
        assert mapping is not None
        assert mapping.table == "customer"
        assert mapping.pk_column == "customer_id"

    def test_get_mapping_for_domain_unknown_returns_none(self):
        reg = SharedEntityRegistry()
        result = reg.get_mapping_for_domain(SharedConcept.PERSON, "nonexistent_domain")
        assert result is None

    def test_get_domains_for_concept(self):
        reg = SharedEntityRegistry()
        domains = reg.get_domains_for_concept(SharedConcept.PERSON)
        assert "retail" in domains
        assert "hr" in domains

    def test_custom_mappings_override_defaults(self):
        custom = {
            SharedConcept.PERSON: [
                DomainEntityMapping(domain="custom_domain", table="person", pk_column="person_id")
            ]
        }
        reg = SharedEntityRegistry(custom_mappings=custom)
        mappings = reg.get_mappings(SharedConcept.PERSON)
        assert len(mappings) == 1
        assert mappings[0].domain == "custom_domain"


# ---------------------------------------------------------------------------
# SharedEntityRegistry — build_cross_domain_relationships
# ---------------------------------------------------------------------------

class TestBuildCrossdomainRelationships:
    def test_no_shared_entities_uses_defaults(self):
        reg = SharedEntityRegistry()
        d1 = _StubDomain("retail", ["customer"])
        d2 = _StubDomain("hr", ["employee"])
        rels = reg.build_cross_domain_relationships([d1, d2])
        # retail.customer and hr.employee are both PERSON — should produce a cross-domain rel
        assert len(rels) > 0
        assert all(isinstance(r, RelationshipDef) for r in rels)

    def test_explicit_shared_entities_builds_rels(self):
        reg = SharedEntityRegistry()
        d1 = _StubDomain("retail", ["customer"])
        d2 = _StubDomain("hr", ["employee"])
        shared = {
            "person": {
                "primary": "hr.employee",
                "links": {"retail": "customer.employee_id"},
            }
        }
        rels = reg.build_cross_domain_relationships([d1, d2], shared_entities=shared)
        assert len(rels) == 1
        assert rels[0].parent == "hr_employee"
        assert rels[0].child == "retail_customer"
        assert rels[0].parent_columns == ["employee_id"]
        assert rels[0].child_columns == ["employee_id"]

    def test_explicit_rel_name_format(self):
        reg = SharedEntityRegistry()
        d1 = _StubDomain("retail", ["customer"])
        d2 = _StubDomain("hr", ["employee"])
        shared = {
            "person": {
                "primary": "hr.employee",
                "links": {"retail": "customer.employee_id"},
            }
        }
        rels = reg.build_cross_domain_relationships([d1, d2], shared_entities=shared)
        assert "xdomain_person" in rels[0].name

    def test_missing_primary_domain_skipped(self):
        reg = SharedEntityRegistry()
        d1 = _StubDomain("retail", ["customer"])
        shared = {
            "person": {
                "primary": "nonexistent.employee",
                "links": {"retail": "customer.employee_id"},
            }
        }
        rels = reg.build_cross_domain_relationships([d1], shared_entities=shared)
        assert len(rels) == 0

    def test_single_domain_no_default_cross_rels(self):
        reg = SharedEntityRegistry()
        d1 = _StubDomain("retail", ["customer"])
        rels = reg.build_cross_domain_relationships([d1])
        # Only 1 domain — can't have cross-domain rels from defaults
        assert len(rels) == 0


# ---------------------------------------------------------------------------
# SharedEntityRegistry — get_generation_order
# ---------------------------------------------------------------------------

class TestGetGenerationOrder:
    def test_explicit_primary_comes_first(self):
        reg = SharedEntityRegistry()
        d1 = _StubDomain("retail", ["customer", "order"])
        d2 = _StubDomain("hr", ["employee", "department"])
        shared = {
            "person": {
                "primary": "hr.employee",
                "links": {"retail": "customer.employee_id"},
            }
        }
        order = reg.get_generation_order([d1, d2], shared_entities=shared)
        assert order.index("hr_employee") < order.index("retail_customer")

    def test_all_tables_included(self):
        reg = SharedEntityRegistry()
        d1 = _StubDomain("retail", ["customer", "order"])
        d2 = _StubDomain("hr", ["employee"])
        order = reg.get_generation_order([d1, d2])
        assert "retail_customer" in order
        assert "retail_order" in order
        assert "hr_employee" in order

    def test_no_duplicates_in_order(self):
        reg = SharedEntityRegistry()
        d1 = _StubDomain("retail", ["customer", "order"])
        d2 = _StubDomain("hr", ["employee"])
        order = reg.get_generation_order([d1, d2])
        assert len(order) == len(set(order))


# ---------------------------------------------------------------------------
# CompositeDomain — schema_mode propagation
# ---------------------------------------------------------------------------

class TestCompositeDomainSchemaMode:
    def test_schema_mode_stored_on_composite(self):
        d1 = _StubDomain("retail", ["customer"])
        comp = CompositeDomain(domains=[d1], schema_mode="star")
        assert comp._schema_mode == "star"

    def test_default_schema_mode_is_3nf(self):
        d1 = _StubDomain("retail", ["customer"])
        comp = CompositeDomain(domains=[d1])
        assert comp._schema_mode == "3nf"

    def test_schema_mode_reflected_in_model(self):
        d1 = _StubDomain("retail", ["customer"])
        comp = CompositeDomain(domains=[d1], schema_mode="star")
        schema = comp.get_schema()
        assert schema.model.schema_mode == "star"


# ---------------------------------------------------------------------------
# CompositeDomain — bridge FK column injection (default registry)
# ---------------------------------------------------------------------------

class TestBridgeFKColumnInjection:
    """Verify that cross-domain default relationships get bridge columns injected."""

    def test_default_registry_injects_bridge_columns(self):
        """When no explicit shared_entities, bridge FK columns must be created."""
        d1 = _StubDomain("retail", ["customer", "store"])
        d2 = _StubDomain("hr", ["employee", "department"])
        comp = CompositeDomain(domains=[d1, d2])
        schema = comp.get_schema()

        # Default registry: PERSON maps retail.customer (primary) → hr.employee (linked)
        # Should inject shared_person_retail_customer_id into hr_employee
        hr_employee = schema.tables["hr_employee"]
        bridge_col = "shared_person_retail_customer_id"
        assert bridge_col in hr_employee.columns, (
            f"Bridge column {bridge_col} not found in hr_employee. "
            f"Columns: {list(hr_employee.columns.keys())}"
        )
        col_def = hr_employee.columns[bridge_col]
        assert col_def.generator["strategy"] == "foreign_key"
        assert "retail_customer" in col_def.generator["ref"]

    def test_bridge_columns_have_correct_fk_ref(self):
        """Bridge FK ref must point to the prefixed parent table and PK."""
        d1 = _StubDomain("retail", ["customer", "store"])
        d2 = _StubDomain("hr", ["employee", "department"])
        comp = CompositeDomain(domains=[d1, d2])
        schema = comp.get_schema()

        # Check LOCATION concept: retail.store (primary) → hr.department (linked)
        hr_dept = schema.tables["hr_department"]
        bridge_col = "shared_location_retail_store_id"
        assert bridge_col in hr_dept.columns
        assert hr_dept.columns[bridge_col].generator["ref"] == "retail_store.store_id"

    def test_explicit_shared_entities_no_spurious_bridge_cols(self):
        """When explicit shared_entities reference existing columns, no extra cols added."""
        d1 = _StubDomain("retail", ["customer"])
        d2 = _StubDomain("hr", ["employee"])

        # Add the FK column to hr.employee manually (simulating a real domain)
        d2_schema = d2.get_schema()
        d2_schema.tables["employee"].columns["retail_customer_id"] = ColumnDef(
            name="retail_customer_id",
            type="integer",
            generator={"strategy": "foreign_key", "ref": "customer.customer_id"},
        )

        shared = {
            "person": {
                "primary": "retail.customer",
                "links": {"hr": "employee.retail_customer_id"},
            }
        }
        comp = CompositeDomain(domains=[d1, d2], shared_entities=shared)
        schema = comp.get_schema()

        # The explicit config references retail_customer_id which exists,
        # so no bridge column should be injected
        hr_emp = schema.tables["hr_employee"]
        bridge_cols = [c for c in hr_emp.columns if c.startswith("shared_")]
        assert len(bridge_cols) == 0

    def test_bridge_columns_are_nullable_when_optional(self):
        """Default registry relationships are optional=True → bridge cols nullable."""
        d1 = _StubDomain("retail", ["customer"])
        d2 = _StubDomain("hr", ["employee"])
        comp = CompositeDomain(domains=[d1, d2])
        schema = comp.get_schema()

        hr_emp = schema.tables["hr_employee"]
        bridge_col = "shared_person_retail_customer_id"
        assert hr_emp.columns[bridge_col].nullable is True

    def test_cross_domain_rels_reference_injected_columns(self):
        """Each cross-domain rel's child_columns must exist in the child table."""
        d1 = _StubDomain("retail", ["customer", "store"])
        d2 = _StubDomain("hr", ["employee", "department"])
        comp = CompositeDomain(domains=[d1, d2])
        schema = comp.get_schema()

        for rel in schema.relationships:
            if not rel.name.startswith("xdomain_"):
                continue
            child_table = schema.tables.get(rel.child)
            assert child_table is not None, f"Child table {rel.child} not found"
            for c_col in rel.child_columns:
                assert c_col in child_table.columns, (
                    f"Relationship {rel.name}: column {c_col} missing from {rel.child}. "
                    f"Columns: {list(child_table.columns.keys())}"
                )
