"""Parse .spindle.json schema files into internal model objects."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ColumnDef:
    """Definition of a single column in a table."""

    name: str
    type: str
    generator: dict[str, Any]
    nullable: bool = False
    null_rate: float = 0.0
    max_length: int | None = None
    precision: int | None = None
    scale: int | None = None

    @property
    def is_foreign_key(self) -> bool:
        return self.generator.get("strategy") == "foreign_key"

    @property
    def fk_ref_table(self) -> str | None:
        if not self.is_foreign_key:
            return None
        ref = self.generator.get("ref", "")
        return ref.split(".")[0] if "." in ref else None

    @property
    def is_computed(self) -> bool:
        return self.generator.get("strategy") in ("computed", "formula")


@dataclass
class TableDef:
    """Definition of a single table."""

    name: str
    columns: dict[str, ColumnDef]
    primary_key: list[str]
    description: str = ""
    cdm_mapping: str | None = None

    @property
    def column_names(self) -> list[str]:
        return list(self.columns.keys())

    @property
    def fk_dependencies(self) -> set[str]:
        deps = set()
        for col in self.columns.values():
            ref_table = col.fk_ref_table
            if ref_table and ref_table != self.name:
                deps.add(ref_table)
        return deps


@dataclass
class RelationshipDef:
    """Definition of a relationship between tables."""

    name: str
    parent: str
    child: str
    parent_columns: list[str]
    child_columns: list[str]
    type: str  # one_to_many, one_to_one, many_to_many, self_referencing
    cardinality: dict[str, Any] = field(default_factory=dict)
    optional: bool = False


@dataclass
class BusinessRuleDef:
    """Definition of a business rule / constraint."""

    name: str
    type: str  # cross_table, cross_column, constraint
    rule: str
    table: str | None = None
    via: str | None = None
    when: str | None = None


@dataclass
class GenerationConfig:
    """Generation scale and output configuration."""

    scale: str = "small"
    scales: dict[str, dict[str, int]] = field(default_factory=dict)
    derived_counts: dict[str, dict[str, Any]] = field(default_factory=dict)
    output: dict[str, Any] = field(default_factory=dict)

    def row_count(self, table_name: str) -> int | None:
        scale_def = self.scales.get(self.scale, {})
        return scale_def.get(table_name)


@dataclass
class ModelDef:
    """Top-level model metadata."""

    name: str
    description: str = ""
    domain: str = ""
    schema_mode: str = "3nf"
    locale: str = "en_US"
    seed: int = 42
    date_range: dict[str, str] = field(default_factory=dict)


@dataclass
class SpindleSchema:
    """Complete parsed schema — the internal representation of a .spindle.json file."""

    model: ModelDef
    tables: dict[str, TableDef]
    relationships: list[RelationshipDef]
    business_rules: list[BusinessRuleDef]
    generation: GenerationConfig

    @property
    def table_names(self) -> list[str]:
        return list(self.tables.keys())

    def get_children(self, table_name: str) -> list[RelationshipDef]:
        return [r for r in self.relationships if r.parent == table_name]

    def get_parents(self, table_name: str) -> list[RelationshipDef]:
        return [r for r in self.relationships if r.child == table_name]

    def get_relationship(self, name: str) -> RelationshipDef | None:
        for r in self.relationships:
            if r.name == name:
                return r
        return None


class SchemaParser:
    """Parse a .spindle.json file into a SpindleSchema."""

    def parse_file(self, path: str | Path) -> SpindleSchema:
        path = Path(path)
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return self.parse_dict(raw)

    def parse_dict(self, raw: dict[str, Any]) -> SpindleSchema:
        model = self._parse_model(raw.get("model", {}))
        tables = self._parse_tables(raw.get("tables", {}))
        relationships = self._parse_relationships(raw.get("relationships", []))
        business_rules = self._parse_business_rules(raw.get("business_rules", []))
        generation = self._parse_generation(raw.get("generation", {}))
        return SpindleSchema(
            model=model,
            tables=tables,
            relationships=relationships,
            business_rules=business_rules,
            generation=generation,
        )

    def _parse_model(self, raw: dict) -> ModelDef:
        return ModelDef(
            name=raw.get("name", "unnamed"),
            description=raw.get("description", ""),
            domain=raw.get("domain", ""),
            schema_mode=raw.get("schema_mode", "3nf"),
            locale=raw.get("locale", "en_US"),
            seed=raw.get("seed", 42),
            date_range=raw.get("date_range", {}),
        )

    def _parse_tables(self, raw: dict) -> dict[str, TableDef]:
        tables = {}
        for table_name, table_raw in raw.items():
            columns = {}
            for col_name, col_raw in table_raw.get("columns", {}).items():
                columns[col_name] = ColumnDef(
                    name=col_name,
                    type=col_raw.get("type", "string"),
                    generator=col_raw.get("generator", {}),
                    nullable=col_raw.get("nullable", False),
                    null_rate=col_raw.get("null_rate", 0.0),
                    max_length=col_raw.get("max_length"),
                    precision=col_raw.get("precision"),
                    scale=col_raw.get("scale"),
                )
            tables[table_name] = TableDef(
                name=table_name,
                columns=columns,
                primary_key=table_raw.get("primary_key", []),
                description=table_raw.get("description", ""),
                cdm_mapping=table_raw.get("cdm_mapping"),
            )
        return tables

    def _parse_relationships(self, raw: list) -> list[RelationshipDef]:
        relationships = []
        for r in raw:
            relationships.append(RelationshipDef(
                name=r.get("name", ""),
                parent=r.get("parent", ""),
                child=r.get("child", ""),
                parent_columns=r.get("parent_columns", []),
                child_columns=r.get("child_columns", []),
                type=r.get("type", "one_to_many"),
                cardinality=r.get("cardinality", {}),
                optional=r.get("optional", False),
            ))
        return relationships

    def _parse_business_rules(self, raw: list) -> list[BusinessRuleDef]:
        rules = []
        for r in raw:
            rules.append(BusinessRuleDef(
                name=r.get("name", ""),
                type=r.get("type", "constraint"),
                rule=r.get("rule", ""),
                table=r.get("table"),
                via=r.get("via"),
                when=r.get("when"),
            ))
        return rules

    def _parse_generation(self, raw: dict) -> GenerationConfig:
        return GenerationConfig(
            scale=raw.get("scale", "small"),
            scales=raw.get("scales", {}),
            derived_counts=raw.get("derived_counts", {}),
            output=raw.get("output", {}),
        )
