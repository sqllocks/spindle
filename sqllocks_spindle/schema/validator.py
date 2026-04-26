"""Validate a SpindleSchema for correctness."""

from __future__ import annotations

from dataclasses import dataclass

from sqllocks_spindle.schema.parser import SpindleSchema


@dataclass
class ValidationError:
    level: str  # "error" or "warning"
    message: str
    location: str  # e.g., "tables.order.columns.customer_id"


# Known strategies and their required config keys
_STRATEGY_REQUIRED_KEYS: dict[str, set[str]] = {
    "sequence": set(),
    "uuid": set(),
    "faker": {"provider"},
    "weighted_enum": {"values"},
    "distribution": {"distribution"},
    "temporal": set(),
    "formula": {"expression"},
    "derived": {"source"},
    "correlated": {"source_column"},
    "foreign_key": {"ref"},
    "lookup": {"source_table", "source_column", "via"},
    "reference_data": {"dataset"},
    "pattern": {"format"},
    "conditional": {"condition"},
    "computed": {"rule", "child_table", "child_column"},
    "lifecycle": {"phases"},
    "self_referencing": {"pk_column"},
    "self_ref_field": {"field"},
    "first_per_parent": {"parent_column"},
    "record_sample": {"dataset", "field"},
    "record_field": {"dataset", "field"},
    "scd2": {"role", "business_key"},
    "composite_foreign_key": {"ref_table", "ref_columns"},
    "composite_fk_field": {"source_column", "ref_column"},
    "native": set(),
}


class SchemaValidator:
    """Validate a parsed SpindleSchema."""

    def validate(self, schema: SpindleSchema) -> list[ValidationError]:
        errors: list[ValidationError] = []
        errors.extend(self._validate_tables(schema))
        errors.extend(self._validate_relationships(schema))
        errors.extend(self._validate_foreign_keys(schema))
        errors.extend(self._validate_business_rules(schema))
        errors.extend(self._validate_generation(schema))
        errors.extend(self._validate_strategy_configs(schema))
        return errors

    def validate_or_raise(self, schema: SpindleSchema) -> None:
        errors = self.validate(schema)
        real_errors = [e for e in errors if e.level == "error"]
        if real_errors:
            msg = "\n".join(f"  [{e.location}] {e.message}" for e in real_errors)
            raise ValueError(f"Schema validation failed:\n{msg}")

    def _validate_tables(self, schema: SpindleSchema) -> list[ValidationError]:
        errors = []
        for table_name, table in schema.tables.items():
            if not table.primary_key:
                errors.append(ValidationError(
                    "warning",
                    "Table has no primary key defined — it cannot be FK-referenced by other tables",
                    f"tables.{table_name}",
                ))
            for pk_col in table.primary_key:
                if pk_col not in table.columns:
                    errors.append(ValidationError(
                        "error",
                        f"Primary key column '{pk_col}' not found in columns",
                        f"tables.{table_name}.primary_key",
                    ))
            if not table.columns:
                errors.append(ValidationError(
                    "error", "Table has no columns",
                    f"tables.{table_name}",
                ))
            for col_name, col in table.columns.items():
                if not col.generator:
                    errors.append(ValidationError(
                        "warning",
                        f"Column '{col_name}' has no generator defined",
                        f"tables.{table_name}.columns.{col_name}",
                    ))
                if col.null_rate < 0 or col.null_rate > 1:
                    errors.append(ValidationError(
                        "error",
                        f"null_rate must be between 0 and 1, got {col.null_rate}",
                        f"tables.{table_name}.columns.{col_name}",
                    ))
        return errors

    def _validate_relationships(self, schema: SpindleSchema) -> list[ValidationError]:
        errors = []
        for rel in schema.relationships:
            if rel.parent not in schema.tables:
                errors.append(ValidationError(
                    "error",
                    f"Parent table '{rel.parent}' not found",
                    f"relationships.{rel.name}",
                ))
            if rel.child not in schema.tables:
                errors.append(ValidationError(
                    "error",
                    f"Child table '{rel.child}' not found",
                    f"relationships.{rel.name}",
                ))
            if rel.parent in schema.tables:
                for col in rel.parent_columns:
                    if col not in schema.tables[rel.parent].columns:
                        errors.append(ValidationError(
                            "error",
                            f"Parent column '{col}' not in table '{rel.parent}'",
                            f"relationships.{rel.name}",
                        ))
            if rel.child in schema.tables:
                for col in rel.child_columns:
                    if col not in schema.tables[rel.child].columns:
                        errors.append(ValidationError(
                            "error",
                            f"Child column '{col}' not in table '{rel.child}'",
                            f"relationships.{rel.name}",
                        ))
        return errors

    def _validate_foreign_keys(self, schema: SpindleSchema) -> list[ValidationError]:
        errors = []
        for table_name, table in schema.tables.items():
            for col_name, col in table.columns.items():
                if not col.is_foreign_key:
                    continue
                ref_table = col.fk_ref_table
                if ref_table and ref_table not in schema.tables:
                    errors.append(ValidationError(
                        "error",
                        f"FK references non-existent table '{ref_table}'",
                        f"tables.{table_name}.columns.{col_name}",
                    ))
                if ref_table and ref_table in schema.tables:
                    ref = col.generator.get("ref", "")
                    ref_col = ref.split(".")[1] if "." in ref else None
                    if ref_col and ref_col not in schema.tables[ref_table].columns:
                        errors.append(ValidationError(
                            "error",
                            f"FK references non-existent column '{ref_table}.{ref_col}'",
                            f"tables.{table_name}.columns.{col_name}",
                        ))
        return errors

    def _validate_business_rules(self, schema: SpindleSchema) -> list[ValidationError]:
        errors = []
        for rule in schema.business_rules:
            if rule.table and rule.table not in schema.tables:
                errors.append(ValidationError(
                    "error",
                    f"Rule references non-existent table '{rule.table}'",
                    f"business_rules.{rule.name}",
                ))
        return errors

    def _validate_generation(self, schema: SpindleSchema) -> list[ValidationError]:
        errors = []
        gen = schema.generation
        if gen.scale and gen.scales and gen.scale not in gen.scales:
            errors.append(ValidationError(
                "warning",
                f"Scale '{gen.scale}' not defined in scales",
                "generation.scale",
            ))
        return errors

    def _validate_strategy_configs(self, schema: SpindleSchema) -> list[ValidationError]:
        """Validate that generator configs have required keys for their strategy."""
        errors = []
        for table_name, table in schema.tables.items():
            for col_name, col in table.columns.items():
                gen = col.generator
                if not gen:
                    continue
                strategy = gen.get("strategy", "")
                if not strategy:
                    continue
                if strategy not in _STRATEGY_REQUIRED_KEYS:
                    errors.append(ValidationError(
                        "warning",
                        f"Unknown strategy '{strategy}'",
                        f"tables.{table_name}.columns.{col_name}.generator",
                    ))
                    continue
                required = _STRATEGY_REQUIRED_KEYS[strategy]
                for key in required:
                    if key not in gen:
                        errors.append(ValidationError(
                            "warning",
                            f"Strategy '{strategy}' expects key '{key}'",
                            f"tables.{table_name}.columns.{col_name}.generator",
                        ))
        return errors
