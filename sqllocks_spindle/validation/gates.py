"""Validation gate framework for Spindle data generation."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from sqllocks_spindle.schema.parser import SpindleSchema


@dataclass
class ValidationContext:
    """Context passed to each validation gate."""

    tables: dict[str, pd.DataFrame] = field(default_factory=dict)
    schema: SpindleSchema | None = None
    file_paths: list[Path] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class GateResult:
    """Result from a single validation gate check."""

    gate_name: str
    passed: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        err_count = len(self.errors)
        warn_count = len(self.warnings)
        return f"GateResult({self.gate_name}: {status}, {err_count} errors, {warn_count} warnings)"


class ValidationGate(ABC):
    """Abstract base class for all validation gates."""

    name: str = "base"

    @abstractmethod
    def check(self, context: ValidationContext) -> GateResult:
        """Run this gate's validation checks against the given context."""
        ...


class ReferentialIntegrityGate(ValidationGate):
    """Check that all FK relationships hold across tables.

    Every FK value in a child column must exist in the referenced parent PK column.
    Reports orphan counts per relationship.
    """

    name = "referential_integrity"

    def check(self, context: ValidationContext) -> GateResult:
        errors: list[str] = []
        warnings: list[str] = []
        orphan_counts: dict[str, int] = {}

        if context.schema is None:
            return GateResult(
                gate_name=self.name,
                passed=False,
                errors=["No schema provided — cannot check referential integrity"],
            )

        for rel in context.schema.relationships:
            if rel.type == "self_referencing":
                continue
            if rel.parent not in context.tables or rel.child not in context.tables:
                warnings.append(
                    f"Skipping relationship '{rel.name}': "
                    f"missing table(s) in context"
                )
                continue

            parent_df = context.tables[rel.parent]
            child_df = context.tables[rel.child]

            for p_col, c_col in zip(rel.parent_columns, rel.child_columns):
                if p_col not in parent_df.columns:
                    errors.append(
                        f"Parent column '{rel.parent}.{p_col}' not found in DataFrame"
                    )
                    continue
                if c_col not in child_df.columns:
                    errors.append(
                        f"Child column '{rel.child}.{c_col}' not found in DataFrame"
                    )
                    continue

                child_vals = child_df[c_col].dropna()
                parent_vals = set(parent_df[p_col])
                orphans = child_vals[~child_vals.isin(parent_vals)]
                orphan_count = len(orphans)

                key = f"{rel.child}.{c_col}->{rel.parent}.{p_col}"
                orphan_counts[key] = orphan_count

                if orphan_count > 0:
                    errors.append(
                        f"{rel.child}.{c_col} has {orphan_count:,} orphan FK values "
                        f"not found in {rel.parent}.{p_col}"
                    )

        return GateResult(
            gate_name=self.name,
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details={"orphan_counts": orphan_counts},
        )


class SchemaConformanceGate(ValidationGate):
    """Check that DataFrames match the expected schema.

    Validates column names are present, data types are compatible, and no
    unexpected columns exist. Uses the SpindleSchema from context or an
    expected_schema dict from config.
    """

    name = "schema_conformance"

    def check(self, context: ValidationContext) -> GateResult:
        errors: list[str] = []
        warnings: list[str] = []
        details: dict[str, Any] = {}

        schema = context.schema
        if schema is None:
            return GateResult(
                gate_name=self.name,
                passed=False,
                errors=["No schema provided — cannot check conformance"],
            )

        for table_name, table_def in schema.tables.items():
            if table_name not in context.tables:
                errors.append(f"Expected table '{table_name}' not found in data")
                continue

            df = context.tables[table_name]
            expected_cols = set(table_def.columns.keys())
            actual_cols = set(df.columns)

            missing = expected_cols - actual_cols
            extra = actual_cols - expected_cols

            if missing:
                errors.append(
                    f"Table '{table_name}' missing columns: {sorted(missing)}"
                )
            if extra:
                warnings.append(
                    f"Table '{table_name}' has unexpected columns: {sorted(extra)}"
                )

            details[table_name] = {
                "expected_columns": sorted(expected_cols),
                "actual_columns": sorted(actual_cols),
                "missing": sorted(missing),
                "extra": sorted(extra),
            }

            # Type compatibility checks
            type_map = {
                "integer": ("int", "Int"),
                "bigint": ("int", "Int"),
                "string": ("object", "string", "str"),
                "float": ("float", "Float"),
                "decimal": ("float", "Float", "object"),
                "date": ("datetime", "object", "date"),
                "datetime": ("datetime", "object"),
                "boolean": ("bool", "Bool", "object"),
                "uuid": ("object", "string", "str"),
            }

            for col_name, col_def in table_def.columns.items():
                if col_name not in actual_cols:
                    continue
                expected_type = col_def.type.lower()
                compatible = type_map.get(expected_type)
                if compatible is None:
                    continue
                actual_dtype = str(df[col_name].dtype)
                if not any(t in actual_dtype for t in compatible):
                    warnings.append(
                        f"Table '{table_name}' column '{col_name}': "
                        f"expected type compatible with '{expected_type}', "
                        f"got '{actual_dtype}'"
                    )

        return GateResult(
            gate_name=self.name,
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details,
        )


class NullConstraintGate(ValidationGate):
    """Check that non-nullable columns have no null values."""

    name = "null_constraint"

    def check(self, context: ValidationContext) -> GateResult:
        errors: list[str] = []
        details: dict[str, Any] = {}

        if context.schema is None:
            return GateResult(
                gate_name=self.name,
                passed=False,
                errors=["No schema provided — cannot check null constraints"],
            )

        for table_name, table_def in context.schema.tables.items():
            if table_name not in context.tables:
                continue

            df = context.tables[table_name]
            table_nulls: dict[str, int] = {}

            for col_name, col_def in table_def.columns.items():
                if col_name not in df.columns:
                    continue
                if col_def.nullable:
                    continue

                null_count = int(df[col_name].isna().sum())
                if null_count > 0:
                    table_nulls[col_name] = null_count
                    errors.append(
                        f"Table '{table_name}' column '{col_name}' is non-nullable "
                        f"but has {null_count:,} null values"
                    )

            if table_nulls:
                details[table_name] = table_nulls

        return GateResult(
            gate_name=self.name,
            passed=len(errors) == 0,
            errors=errors,
            details=details,
        )


class UniqueConstraintGate(ValidationGate):
    """Check that primary key columns have no duplicate values."""

    name = "unique_constraint"

    def check(self, context: ValidationContext) -> GateResult:
        errors: list[str] = []
        details: dict[str, Any] = {}

        if context.schema is None:
            return GateResult(
                gate_name=self.name,
                passed=False,
                errors=["No schema provided — cannot check unique constraints"],
            )

        for table_name, table_def in context.schema.tables.items():
            if table_name not in context.tables:
                continue
            if not table_def.primary_key:
                continue

            df = context.tables[table_name]
            pk_cols = [c for c in table_def.primary_key if c in df.columns]

            if not pk_cols:
                continue

            if len(pk_cols) == 1:
                col = pk_cols[0]
                dup_count = int(df[col].duplicated().sum())
                if dup_count > 0:
                    errors.append(
                        f"Table '{table_name}' PK column '{col}' has "
                        f"{dup_count:,} duplicate values"
                    )
                    details[table_name] = {"column": col, "duplicates": dup_count}
            else:
                dup_count = int(df.duplicated(subset=pk_cols).sum())
                if dup_count > 0:
                    errors.append(
                        f"Table '{table_name}' composite PK {pk_cols} has "
                        f"{dup_count:,} duplicate rows"
                    )
                    details[table_name] = {"columns": pk_cols, "duplicates": dup_count}

        return GateResult(
            gate_name=self.name,
            passed=len(errors) == 0,
            errors=errors,
            details=details,
        )


class RangeConstraintGate(ValidationGate):
    """Check that numeric columns are within expected ranges.

    Configure via context.config with a dict of:
        {
            "ranges": {
                "table_name.column_name": {"min": 0, "max": 100},
                ...
            }
        }
    """

    name = "range_constraint"

    def check(self, context: ValidationContext) -> GateResult:
        errors: list[str] = []
        warnings: list[str] = []
        details: dict[str, Any] = {}

        ranges: dict[str, dict[str, float]] = context.config.get("ranges", {})

        if not ranges:
            return GateResult(
                gate_name=self.name,
                passed=True,
                warnings=["No range constraints configured — nothing to check"],
            )

        for key, bounds in ranges.items():
            parts = key.split(".", 1)
            if len(parts) != 2:
                warnings.append(f"Invalid range key '{key}' — expected 'table.column'")
                continue

            table_name, col_name = parts
            if table_name not in context.tables:
                warnings.append(f"Table '{table_name}' not found in data")
                continue

            df = context.tables[table_name]
            if col_name not in df.columns:
                warnings.append(
                    f"Column '{col_name}' not found in table '{table_name}'"
                )
                continue

            series = pd.to_numeric(df[col_name], errors="coerce").dropna()
            if series.empty:
                continue

            col_min = float(series.min())
            col_max = float(series.max())
            violation_details: dict[str, Any] = {
                "actual_min": col_min,
                "actual_max": col_max,
            }

            min_bound = bounds.get("min")
            max_bound = bounds.get("max")

            if min_bound is not None:
                below = int((series < min_bound).sum())
                if below > 0:
                    errors.append(
                        f"{table_name}.{col_name}: {below:,} values below "
                        f"minimum {min_bound} (actual min: {col_min})"
                    )
                    violation_details["below_min"] = below

            if max_bound is not None:
                above = int((series > max_bound).sum())
                if above > 0:
                    errors.append(
                        f"{table_name}.{col_name}: {above:,} values above "
                        f"maximum {max_bound} (actual max: {col_max})"
                    )
                    violation_details["above_max"] = above

            details[key] = violation_details

        return GateResult(
            gate_name=self.name,
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details,
        )


class TemporalConsistencyGate(ValidationGate):
    """Check temporal consistency of date/datetime columns.

    Validates:
    - Dates are within expected range (configurable)
    - No unexpected future dates
    - Temporal ordering (e.g., end_date >= start_date)

    Configure via context.config:
        {
            "date_range": {"start": "2020-01-01", "end": "2025-12-31"},
            "no_future": ["table.column", ...],
            "ordering": [
                {"table": "orders", "start": "order_date", "end": "ship_date"},
                ...
            ]
        }
    """

    name = "temporal_consistency"

    def check(self, context: ValidationContext) -> GateResult:
        errors: list[str] = []
        warnings: list[str] = []
        details: dict[str, Any] = {}

        config = context.config
        date_range = config.get("date_range", {})
        no_future_cols: list[str] = config.get("no_future", [])
        ordering_rules: list[dict[str, str]] = config.get("ordering", [])

        now = pd.Timestamp.now()

        # Check date range bounds
        if date_range:
            range_start = pd.Timestamp(date_range["start"]) if "start" in date_range else None
            range_end = pd.Timestamp(date_range["end"]) if "end" in date_range else None

            for table_name, df in context.tables.items():
                for col_name in df.columns:
                    if not pd.api.types.is_datetime64_any_dtype(df[col_name]):
                        continue

                    series = df[col_name].dropna()
                    if series.empty:
                        continue

                    key = f"{table_name}.{col_name}"

                    if range_start is not None:
                        before = int((series < range_start).sum())
                        if before > 0:
                            errors.append(
                                f"{key}: {before:,} dates before {date_range['start']}"
                            )
                            details.setdefault(key, {})["before_range"] = before

                    if range_end is not None:
                        after = int((series > range_end).sum())
                        if after > 0:
                            errors.append(
                                f"{key}: {after:,} dates after {date_range['end']}"
                            )
                            details.setdefault(key, {})["after_range"] = after

        # Check no-future constraints
        for spec in no_future_cols:
            parts = spec.split(".", 1)
            if len(parts) != 2:
                continue
            table_name, col_name = parts
            if table_name not in context.tables:
                continue
            df = context.tables[table_name]
            if col_name not in df.columns:
                continue
            if not pd.api.types.is_datetime64_any_dtype(df[col_name]):
                continue

            series = df[col_name].dropna()
            future_count = int((series > now).sum())
            if future_count > 0:
                errors.append(
                    f"{spec}: {future_count:,} values are in the future"
                )
                details.setdefault(spec, {})["future_dates"] = future_count

        # Check temporal ordering
        for rule in ordering_rules:
            table_name = rule.get("table", "")
            start_col = rule.get("start", "")
            end_col = rule.get("end", "")

            if table_name not in context.tables:
                warnings.append(f"Table '{table_name}' not found for ordering check")
                continue

            df = context.tables[table_name]
            if start_col not in df.columns or end_col not in df.columns:
                warnings.append(
                    f"Columns '{start_col}'/'{end_col}' not found in '{table_name}'"
                )
                continue

            mask = df[start_col].notna() & df[end_col].notna()
            subset = df.loc[mask]

            try:
                violations = int((subset[end_col] < subset[start_col]).sum())
            except TypeError:
                warnings.append(
                    f"Cannot compare '{start_col}' and '{end_col}' in '{table_name}' "
                    f"— incompatible types"
                )
                continue

            if violations > 0:
                key = f"{table_name}.{end_col}<{start_col}"
                errors.append(
                    f"{table_name}: {violations:,} rows where "
                    f"'{end_col}' < '{start_col}'"
                )
                details[key] = violations

        return GateResult(
            gate_name=self.name,
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details,
        )


class FileFormatGate(ValidationGate):
    """Validate output files are readable, correct format, and not truncated.

    Checks parquet, CSV, and JSONL files. Takes file paths from context.file_paths.
    """

    name = "file_format"

    def check(self, context: ValidationContext) -> GateResult:
        errors: list[str] = []
        warnings: list[str] = []
        details: dict[str, Any] = {}

        if not context.file_paths:
            return GateResult(
                gate_name=self.name,
                passed=True,
                warnings=["No file paths provided — nothing to check"],
            )

        for file_path in context.file_paths:
            path = Path(file_path)
            file_key = str(path)

            if not path.exists():
                errors.append(f"File not found: {file_key}")
                continue

            if path.stat().st_size == 0:
                errors.append(f"File is empty (0 bytes): {file_key}")
                continue

            suffix = path.suffix.lower()
            file_details: dict[str, Any] = {
                "size_bytes": path.stat().st_size,
                "format": suffix,
            }

            try:
                if suffix == ".parquet":
                    df = pd.read_parquet(path)
                    file_details["rows"] = len(df)
                    file_details["columns"] = len(df.columns)
                elif suffix == ".csv":
                    df = pd.read_csv(path, nrows=0)
                    file_details["columns"] = len(df.columns)
                    # Full read to verify not truncated
                    df = pd.read_csv(path)
                    file_details["rows"] = len(df)
                elif suffix == ".tsv":
                    df = pd.read_csv(path, sep="\t")
                    file_details["rows"] = len(df)
                    file_details["columns"] = len(df.columns)
                elif suffix == ".jsonl":
                    df = pd.read_json(path, lines=True)
                    file_details["rows"] = len(df)
                    file_details["columns"] = len(df.columns)
                else:
                    warnings.append(
                        f"Unknown file format '{suffix}' for {file_key}"
                    )
                    details[file_key] = file_details
                    continue

                file_details["readable"] = True

            except Exception as exc:
                errors.append(f"Failed to read {file_key}: {exc}")
                file_details["readable"] = False

            details[file_key] = file_details

        return GateResult(
            gate_name=self.name,
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details,
        )


class SchemaDriftGate(ValidationGate):
    """Detect schema drift between current data and a baseline schema.

    Detects:
    - Additive changes (new columns, new tables)
    - Breaking changes (removed columns, renamed columns, retyped columns)

    Configure via context.config:
        {
            "baseline": {
                "table_name": {
                    "columns": {"col1": "int64", "col2": "object", ...}
                },
                ...
            }
        }
    """

    name = "schema_drift"

    def check(self, context: ValidationContext) -> GateResult:
        errors: list[str] = []
        warnings: list[str] = []
        details: dict[str, Any] = {
            "additive": [],
            "breaking": [],
        }

        baseline: dict[str, dict] = context.config.get("baseline", {})

        if not baseline:
            return GateResult(
                gate_name=self.name,
                passed=True,
                warnings=["No baseline schema configured — nothing to check"],
            )

        # Check for removed tables
        for table_name in baseline:
            if table_name not in context.tables:
                change = f"Table '{table_name}' removed"
                errors.append(change)
                details["breaking"].append(change)

        for table_name, df in context.tables.items():
            if table_name not in baseline:
                change = f"New table '{table_name}' added"
                warnings.append(change)
                details["additive"].append(change)
                continue

            baseline_table = baseline[table_name]
            baseline_cols: dict[str, str] = baseline_table.get("columns", {})
            actual_cols = {col: str(df[col].dtype) for col in df.columns}

            # Removed columns (breaking)
            for col_name in baseline_cols:
                if col_name not in actual_cols:
                    change = f"Table '{table_name}': column '{col_name}' removed"
                    errors.append(change)
                    details["breaking"].append(change)

            # New columns (additive)
            for col_name in actual_cols:
                if col_name not in baseline_cols:
                    change = f"Table '{table_name}': new column '{col_name}'"
                    warnings.append(change)
                    details["additive"].append(change)

            # Type changes (breaking)
            for col_name, baseline_dtype in baseline_cols.items():
                if col_name not in actual_cols:
                    continue
                actual_dtype = actual_cols[col_name]
                if actual_dtype != baseline_dtype:
                    change = (
                        f"Table '{table_name}': column '{col_name}' type changed "
                        f"from '{baseline_dtype}' to '{actual_dtype}'"
                    )
                    errors.append(change)
                    details["breaking"].append(change)

        return GateResult(
            gate_name=self.name,
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details,
        )


# ---------------------------------------------------------------------------
# Gate registry and runner
# ---------------------------------------------------------------------------

_GATE_REGISTRY: dict[str, type[ValidationGate]] = {
    "referential_integrity": ReferentialIntegrityGate,
    "schema_conformance": SchemaConformanceGate,
    "null_constraint": NullConstraintGate,
    "unique_constraint": UniqueConstraintGate,
    "range_constraint": RangeConstraintGate,
    "temporal_consistency": TemporalConsistencyGate,
    "file_format": FileFormatGate,
    "schema_drift": SchemaDriftGate,
}


class GateRunner:
    """Run validation gates against a context and collect results."""

    def __init__(
        self,
        gates: list[str | ValidationGate] | None = None,
    ) -> None:
        self._gates: list[ValidationGate] = []
        if gates:
            for gate in gates:
                if isinstance(gate, str):
                    cls = _GATE_REGISTRY.get(gate)
                    if cls is None:
                        raise ValueError(
                            f"Unknown gate '{gate}'. "
                            f"Available: {sorted(_GATE_REGISTRY.keys())}"
                        )
                    self._gates.append(cls())
                else:
                    self._gates.append(gate)
        else:
            # Default: all built-in gates
            self._gates = [cls() for cls in _GATE_REGISTRY.values()]

    @staticmethod
    def available_gates() -> list[str]:
        """Return names of all registered gates."""
        return sorted(_GATE_REGISTRY.keys())

    @staticmethod
    def register_gate(name: str, gate_cls: type[ValidationGate]) -> None:
        """Register a custom gate in the global registry."""
        _GATE_REGISTRY[name] = gate_cls

    def run_all(self, context: ValidationContext) -> list[GateResult]:
        """Run all configured gates and return results."""
        return [gate.check(context) for gate in self._gates]

    def run_gate(self, gate_name: str, context: ValidationContext) -> GateResult:
        """Run a single gate by name."""
        cls = _GATE_REGISTRY.get(gate_name)
        if cls is None:
            raise ValueError(
                f"Unknown gate '{gate_name}'. "
                f"Available: {sorted(_GATE_REGISTRY.keys())}"
            )
        return cls().check(context)

    @staticmethod
    def summary(results: list[GateResult]) -> dict[str, Any]:
        """Produce an aggregate summary of gate results."""
        passed = [r for r in results if r.passed]
        failed = [r for r in results if not r.passed]
        total_errors = sum(len(r.errors) for r in results)
        total_warnings = sum(len(r.warnings) for r in results)

        return {
            "total_gates": len(results),
            "passed": len(passed),
            "failed": len(failed),
            "total_errors": total_errors,
            "total_warnings": total_warnings,
            "passed_gates": [r.gate_name for r in passed],
            "failed_gates": [r.gate_name for r in failed],
            "all_passed": len(failed) == 0,
        }
