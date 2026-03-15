"""Cross-domain composition for Spindle.

CompositeDomain merges multiple domain schemas into a single unified
SpindleSchema that the existing Spindle engine can generate. It handles
table name conflicts via domain-name prefixing, shared entity resolution,
and FK rewiring across domain boundaries.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.domains.shared_registry import SharedEntityRegistry
from sqllocks_spindle.schema.parser import (
    BusinessRuleDef,
    ColumnDef,
    GenerationConfig,
    ModelDef,
    RelationshipDef,
    SpindleSchema,
    TableDef,
)


class CompositeDomain(Domain):
    """A domain composed of multiple child domains with shared entity linkage.

    Merges schemas from all child domains into a single SpindleSchema,
    prefixing table names with the domain name to avoid collisions
    (e.g., ``retail_customer``, ``hr_employee``). Shared entities are
    generated once from a primary domain and referenced by other domains
    via cross-domain FK relationships.

    Args:
        domains: List of Domain instances to compose.
        shared_entities: Explicit shared entity mapping. Format::

            {
                "person": {
                    "primary": "hr.employee",
                    "links": {
                        "retail": "customer.employee_id",
                        "financial": "account.holder_id",
                    },
                },
                "location": {
                    "primary": "retail.store",
                    "links": {
                        "hr": "employee.office_id",
                    },
                },
            }

        registry: Optional SharedEntityRegistry instance. If not provided,
            a default registry is used. Ignored when ``shared_entities``
            is explicitly provided.
        schema_mode: Schema layout variant passed to child domains.
        profile: Profile name passed to child domains.
        overrides: Profile overrides passed to child domains.

    Example::

        from sqllocks_spindle.domains.retail.retail import RetailDomain
        from sqllocks_spindle.domains.hr.hr import HrDomain
        from sqllocks_spindle.domains.composite import CompositeDomain
        from sqllocks_spindle.engine.generator import Spindle

        composite = CompositeDomain(
            domains=[RetailDomain(), HrDomain()],
            shared_entities={
                "person": {
                    "primary": "hr.employee",
                    "links": {"retail": "customer.employee_id"},
                },
            },
        )
        result = Spindle().generate(domain=composite, scale="fabric_demo")
    """

    def __init__(
        self,
        domains: list[Domain],
        shared_entities: dict[str, dict[str, Any]] | None = None,
        registry: SharedEntityRegistry | None = None,
        schema_mode: str = "3nf",
        profile: str = "default",
        overrides: dict[str, Any] | None = None,
    ):
        # Initialize base Domain (profile/overrides are not used at the
        # composite level, but we satisfy the interface)
        super().__init__(schema_mode=schema_mode, profile=profile, overrides=overrides)

        if not domains:
            raise ValueError("CompositeDomain requires at least one child domain")

        self._domains = list(domains)
        self._shared_entities = shared_entities
        self._registry = registry or SharedEntityRegistry()

        # Validate no duplicate domain names
        names = [d.name for d in self._domains]
        if len(names) != len(set(names)):
            dupes = [n for n in names if names.count(n) > 1]
            raise ValueError(
                f"Duplicate domain names in composition: {set(dupes)}. "
                f"Each domain must appear at most once."
            )

    @property
    def name(self) -> str:
        parts = sorted(d.name for d in self._domains)
        return "composite_" + "_".join(parts)

    @property
    def description(self) -> str:
        domain_names = ", ".join(d.name for d in self._domains)
        return f"Cross-domain composition of: {domain_names}"

    @property
    def domain_path(self) -> Path:
        """Composite domains have no single directory — return the domains/ root."""
        return Path(__file__).parent

    @property
    def child_domains(self) -> list[Domain]:
        """The child domains being composed."""
        return list(self._domains)

    def _build_schema(self) -> SpindleSchema:
        """Build a unified schema by merging all child domain schemas."""
        # Collect child schemas
        child_schemas: dict[str, SpindleSchema] = {}
        for domain in self._domains:
            child_schemas[domain.name] = domain.get_schema()

        # Determine which tables are shared (should NOT be prefixed/duplicated)
        shared_tables = self._resolve_shared_tables(child_schemas)

        # Merge tables (with prefixing)
        merged_tables = self._merge_tables(child_schemas, shared_tables)

        # Merge relationships (rewrite table references to prefixed names)
        merged_relationships = self._merge_relationships(child_schemas, shared_tables)

        # Add cross-domain relationships for shared entities
        cross_rels = self._registry.build_cross_domain_relationships(
            self._domains, self._shared_entities
        )
        merged_relationships.extend(cross_rels)

        # Ensure bridge FK columns exist in child tables for cross-domain rels
        self._ensure_bridge_columns(merged_tables, cross_rels)

        # Merge business rules
        merged_rules = self._merge_business_rules(child_schemas, shared_tables)

        # Merge generation config
        merged_generation = self._merge_generation(child_schemas, shared_tables)

        # Build model metadata
        model = ModelDef(
            name=self.name,
            description=self.description,
            domain="composite",
            schema_mode=self._schema_mode,
            locale=self._resolve_locale(child_schemas),
            seed=self._resolve_seed(child_schemas),
            date_range=self._resolve_date_range(child_schemas),
        )

        return SpindleSchema(
            model=model,
            tables=merged_tables,
            relationships=merged_relationships,
            business_rules=merged_rules,
            generation=merged_generation,
        )

    # ── Table merging ──────────────────────────────────────────────────

    def _resolve_shared_tables(
        self,
        child_schemas: dict[str, SpindleSchema],
    ) -> dict[str, tuple[str, str]]:
        """Determine which tables are shared entities.

        Returns:
            Dict mapping prefixed table name -> (source_domain, source_table)
            for the PRIMARY instance of each shared entity. Non-primary
            instances are excluded from the result (they get a bridge FK
            instead of their own copy).
        """
        shared: dict[str, tuple[str, str]] = {}

        if not self._shared_entities:
            return shared

        for concept_name, config in self._shared_entities.items():
            primary_ref = config.get("primary", "")
            if "." not in primary_ref:
                continue

            primary_domain, primary_table = primary_ref.split(".", 1)
            if primary_domain not in child_schemas:
                continue

            prefixed = f"{primary_domain}_{primary_table}"
            shared[prefixed] = (primary_domain, primary_table)

        return shared

    def _merge_tables(
        self,
        child_schemas: dict[str, SpindleSchema],
        shared_tables: dict[str, tuple[str, str]],
    ) -> dict[str, TableDef]:
        """Merge tables from all child schemas, prefixing names to avoid collisions."""
        merged: dict[str, TableDef] = {}

        for domain_name, schema in child_schemas.items():
            for table_name, table_def in schema.tables.items():
                prefixed_name = f"{domain_name}_{table_name}"

                # Rewrite columns: update FK references to use prefixed table names
                new_columns = self._rewrite_columns(
                    table_def.columns, domain_name, child_schemas
                )

                new_table = TableDef(
                    name=prefixed_name,
                    columns=new_columns,
                    primary_key=list(table_def.primary_key),
                    description=f"[{domain_name}] {table_def.description}",
                    cdm_mapping=table_def.cdm_mapping,
                )
                merged[prefixed_name] = new_table

        return merged

    def _rewrite_columns(
        self,
        columns: dict[str, ColumnDef],
        domain_name: str,
        child_schemas: dict[str, SpindleSchema],
    ) -> dict[str, ColumnDef]:
        """Deep-copy columns and rewrite FK references to prefixed table names."""
        new_columns: dict[str, ColumnDef] = {}

        for col_name, col_def in columns.items():
            # Deep-copy to avoid mutating originals
            new_gen = copy.deepcopy(col_def.generator)

            # Rewrite foreign_key ref: "customer.customer_id" -> "retail_customer.customer_id"
            if new_gen.get("strategy") == "foreign_key":
                ref = new_gen.get("ref", "")
                if "." in ref:
                    ref_table, ref_col = ref.split(".", 1)
                    # Only prefix if the table exists in this domain's schema
                    if ref_table in child_schemas[domain_name].tables:
                        new_gen["ref"] = f"{domain_name}_{ref_table}.{ref_col}"

            # Rewrite lookup source_table references
            if new_gen.get("strategy") == "lookup":
                src_table = new_gen.get("source_table", "")
                if src_table in child_schemas[domain_name].tables:
                    new_gen["source_table"] = f"{domain_name}_{src_table}"

            # Rewrite computed child_table references
            if new_gen.get("strategy") == "computed":
                child_table = new_gen.get("child_table", "")
                if child_table in child_schemas[domain_name].tables:
                    new_gen["child_table"] = f"{domain_name}_{child_table}"

            # Rewrite derived source references (e.g., "order.order_date")
            if new_gen.get("strategy") == "derived":
                source = new_gen.get("source", "")
                if "." in source:
                    src_table, src_col = source.split(".", 1)
                    if src_table in child_schemas[domain_name].tables:
                        new_gen["source"] = f"{domain_name}_{src_table}.{src_col}"

            # Rewrite conditional sub-generators
            if new_gen.get("strategy") == "conditional":
                for sub_key in ("true_generator", "false_generator"):
                    sub_gen = new_gen.get(sub_key, {})
                    if isinstance(sub_gen, dict) and sub_gen.get("strategy") == "lookup":
                        src = sub_gen.get("source_table", "")
                        if src in child_schemas[domain_name].tables:
                            sub_gen["source_table"] = f"{domain_name}_{src}"

            new_columns[col_name] = ColumnDef(
                name=col_def.name,
                type=col_def.type,
                generator=new_gen,
                nullable=col_def.nullable,
                null_rate=col_def.null_rate,
                max_length=col_def.max_length,
                precision=col_def.precision,
                scale=col_def.scale,
            )

        return new_columns

    # ── Bridge column injection ──────────────────────────────────────────

    def _ensure_bridge_columns(
        self,
        tables: dict[str, TableDef],
        cross_rels: list[RelationshipDef],
    ) -> None:
        """Add missing bridge FK columns referenced by cross-domain relationships.

        When the SharedEntityRegistry builds default relationships, it creates
        bridge column names (e.g. ``shared_person_hr_employee_id``) that don't
        exist in the child table schema. This method injects those columns as
        ``foreign_key`` generators so the engine populates them during generation.
        """
        for rel in cross_rels:
            if rel.child not in tables:
                continue

            child_table = tables[rel.child]

            for parent_col, bridge_col in zip(rel.parent_columns, rel.child_columns):
                # Skip if the column already exists (explicit config case)
                if bridge_col in child_table.columns:
                    continue

                # Inject a foreign_key column pointing at the parent table's PK
                child_table.columns[bridge_col] = ColumnDef(
                    name=bridge_col,
                    type="integer",
                    generator={
                        "strategy": "foreign_key",
                        "ref": f"{rel.parent}.{parent_col}",
                    },
                    nullable=getattr(rel, "optional", True),
                )

    # ── Relationship merging ───────────────────────────────────────────

    def _merge_relationships(
        self,
        child_schemas: dict[str, SpindleSchema],
        shared_tables: dict[str, tuple[str, str]],
    ) -> list[RelationshipDef]:
        """Merge relationships from all child schemas, rewriting table references."""
        merged: list[RelationshipDef] = []

        for domain_name, schema in child_schemas.items():
            for rel in schema.relationships:
                new_rel = RelationshipDef(
                    name=f"{domain_name}_{rel.name}",
                    parent=f"{domain_name}_{rel.parent}",
                    child=f"{domain_name}_{rel.child}",
                    parent_columns=list(rel.parent_columns),
                    child_columns=list(rel.child_columns),
                    type=rel.type,
                    cardinality=dict(rel.cardinality) if rel.cardinality else {},
                    optional=rel.optional,
                )
                merged.append(new_rel)

        return merged

    # ── Business rules merging ─────────────────────────────────────────

    def _merge_business_rules(
        self,
        child_schemas: dict[str, SpindleSchema],
        shared_tables: dict[str, tuple[str, str]],
    ) -> list[BusinessRuleDef]:
        """Merge business rules, prefixing table references."""
        merged: list[BusinessRuleDef] = []

        for domain_name, schema in child_schemas.items():
            for rule in schema.business_rules:
                # Prefix the table reference if present
                new_table = (
                    f"{domain_name}_{rule.table}" if rule.table else None
                )

                # Rewrite the rule expression to use prefixed table names
                new_rule_expr = rule.rule
                for table_name in schema.tables:
                    # Replace "table.column" references with "domain_table.column"
                    new_rule_expr = new_rule_expr.replace(
                        f"{table_name}.", f"{domain_name}_{table_name}."
                    )

                merged.append(
                    BusinessRuleDef(
                        name=f"{domain_name}_{rule.name}",
                        type=rule.type,
                        rule=new_rule_expr,
                        table=new_table,
                        via=rule.via,
                        when=rule.when,
                    )
                )

        return merged

    # ── Generation config merging ──────────────────────────────────────

    def _merge_generation(
        self,
        child_schemas: dict[str, SpindleSchema],
        shared_tables: dict[str, tuple[str, str]],
    ) -> GenerationConfig:
        """Merge generation configs from all child schemas.

        Scale presets are combined by prefixing table names. The composite
        supports the union of all scale names; missing entries for a domain
        at a given scale fall back to the smallest available scale.
        """
        # Collect all scale names across domains
        all_scale_names: set[str] = set()
        for schema in child_schemas.values():
            all_scale_names.update(schema.generation.scales.keys())

        # Merge scales
        merged_scales: dict[str, dict[str, int]] = {}
        for scale_name in sorted(all_scale_names):
            merged_scales[scale_name] = {}
            for domain_name, schema in child_schemas.items():
                scale_def = schema.generation.scales.get(scale_name, {})
                if not scale_def:
                    # Fallback: use first available scale for this domain
                    for fallback_name, fallback_def in schema.generation.scales.items():
                        scale_def = fallback_def
                        break
                for table_name, count in scale_def.items():
                    merged_scales[scale_name][f"{domain_name}_{table_name}"] = count

        # Merge derived counts
        merged_derived: dict[str, dict[str, Any]] = {}
        for domain_name, schema in child_schemas.items():
            for table_name, derived in schema.generation.derived_counts.items():
                prefixed = f"{domain_name}_{table_name}"
                new_derived = dict(derived)

                # Rewrite per_parent references to prefixed names
                if "per_parent" in new_derived:
                    parent = new_derived["per_parent"]
                    new_derived["per_parent"] = f"{domain_name}_{parent}"

                merged_derived[prefixed] = new_derived

        return GenerationConfig(
            scale="small",
            scales=merged_scales,
            derived_counts=merged_derived,
            output={"format": "dataframe"},
        )

    # ── Model metadata resolution ──────────────────────────────────────

    def _resolve_locale(self, child_schemas: dict[str, SpindleSchema]) -> str:
        """Pick the locale from the first child domain."""
        for schema in child_schemas.values():
            return schema.model.locale
        return "en_US"

    def _resolve_seed(self, child_schemas: dict[str, SpindleSchema]) -> int:
        """Pick the seed from the first child domain."""
        for schema in child_schemas.values():
            return schema.model.seed
        return 42

    def _resolve_date_range(
        self, child_schemas: dict[str, SpindleSchema]
    ) -> dict[str, str]:
        """Compute the widest date range spanning all child domains."""
        starts: list[str] = []
        ends: list[str] = []

        for schema in child_schemas.values():
            dr = schema.model.date_range
            if dr.get("start"):
                starts.append(dr["start"])
            if dr.get("end"):
                ends.append(dr["end"])

        return {
            "start": min(starts) if starts else "2020-01-01",
            "end": max(ends) if ends else "2025-12-31",
        }

    # ── Override domain_path for reference data resolution ─────────────

    def get_schema(self) -> SpindleSchema:
        """Load and return the merged composite schema.

        Overrides the base class to always build programmatically (there is
        no JSON schema file for a composite domain).
        """
        return self._build_schema()
