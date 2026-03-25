"""Main Spindle generator — the public API entry point."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np

logger = logging.getLogger(__name__)
import pandas as pd

from sqllocks_spindle.engine.id_manager import IDManager
from sqllocks_spindle.engine.strategies.base import StrategyRegistry
from sqllocks_spindle.engine.strategies.computed import ComputedStrategy
from sqllocks_spindle.engine.strategies.conditional import ConditionalStrategy
from sqllocks_spindle.engine.strategies.correlated import CorrelatedStrategy
from sqllocks_spindle.engine.strategies.derived import DerivedStrategy
from sqllocks_spindle.engine.strategies.distribution import DistributionStrategy
from sqllocks_spindle.engine.strategies.enum import WeightedEnumStrategy
from sqllocks_spindle.engine.strategies.faker_strategy import FakerStrategy
from sqllocks_spindle.engine.strategies.native import NativeStrategy
from sqllocks_spindle.engine.strategies.first_per_parent import FirstPerParentStrategy
from sqllocks_spindle.engine.strategies.foreign_key import ForeignKeyStrategy
from sqllocks_spindle.engine.strategies.formula import FormulaStrategy
from sqllocks_spindle.engine.strategies.lifecycle import LifecycleStrategy
from sqllocks_spindle.engine.strategies.lookup import LookupStrategy
from sqllocks_spindle.engine.strategies.pattern import PatternStrategy
from sqllocks_spindle.engine.strategies.record_field import RecordFieldStrategy
from sqllocks_spindle.engine.strategies.record_sample import RecordSampleStrategy
from sqllocks_spindle.engine.strategies.scd2 import SCD2Strategy
from sqllocks_spindle.engine.strategies.reference_data import ReferenceDataStrategy
from sqllocks_spindle.engine.strategies.self_referencing import SelfReferencingStrategy, SelfRefFieldStrategy
from sqllocks_spindle.engine.strategies.sequence import SequenceStrategy
from sqllocks_spindle.engine.strategies.temporal import TemporalStrategy
from sqllocks_spindle.engine.strategies.uuid_strategy import UUIDStrategy
from sqllocks_spindle.engine.table_generator import TableGenerator
from sqllocks_spindle.schema.dependency import DependencyResolver
from sqllocks_spindle.schema.parser import SchemaParser, SpindleSchema
from sqllocks_spindle.engine.rules.business_rules import BusinessRulesEngine
from sqllocks_spindle.schema.validator import SchemaValidator


@dataclass
class ColumnLineage:
    """Tracks which strategy produced a column's values."""
    table: str
    column: str
    strategy: str
    config: dict[str, Any]


@dataclass
class GenerationResult:
    """Result of a generation run."""

    tables: dict[str, pd.DataFrame]
    schema: SpindleSchema
    generation_order: list[str]
    elapsed_seconds: float
    row_counts: dict[str, int]
    lineage: list[ColumnLineage] = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.lineage is None:
            self.lineage = []

    def get_lineage(self, table: str, column: str) -> ColumnLineage | None:
        """Look up lineage for a specific column."""
        for entry in self.lineage:
            if entry.table == table and entry.column == column:
                return entry
        return None

    def __getitem__(self, table_name: str) -> pd.DataFrame:
        return self.tables[table_name]

    def __repr__(self) -> str:
        total_rows = sum(self.row_counts.values())
        table_count = len(self.tables)
        return (
            f"GenerationResult({table_count} tables, {total_rows:,} total rows, "
            f"{self.elapsed_seconds:.1f}s)"
        )

    def summary(self) -> str:
        lines = [
            f"Spindle Generation Result",
            f"{'=' * 40}",
            f"Schema: {self.schema.model.name}",
            f"Domain: {self.schema.model.domain}",
            f"Mode:   {self.schema.model.schema_mode}",
            f"Seed:   {self.schema.model.seed}",
            f"Time:   {self.elapsed_seconds:.1f}s",
            f"",
            f"{'Table':<25} {'Rows':>12} {'Columns':>8}",
            f"{'-' * 45}",
        ]
        for table_name in self.generation_order:
            df = self.tables[table_name]
            lines.append(f"{table_name:<25} {len(df):>12,} {len(df.columns):>8}")
        lines.append(f"{'-' * 45}")
        total = sum(self.row_counts.values())
        lines.append(f"{'TOTAL':<25} {total:>12,}")
        return "\n".join(lines)

    def verify_integrity(self) -> list[str]:
        """Verify referential integrity across all tables."""
        errors = []
        for rel in self.schema.relationships:
            if rel.parent not in self.tables or rel.child not in self.tables:
                continue
            if rel.type == "self_referencing":
                continue

            parent_df = self.tables[rel.parent]
            child_df = self.tables[rel.child]

            for p_col, c_col in zip(rel.parent_columns, rel.child_columns):
                if c_col not in child_df.columns or p_col not in parent_df.columns:
                    continue

                child_vals = child_df[c_col].dropna()
                parent_vals = set(parent_df[p_col])
                orphans = child_vals[~child_vals.isin(parent_vals)]

                if len(orphans) > 0:
                    errors.append(
                        f"{rel.child}.{c_col} has {len(orphans)} orphan FK values "
                        f"not found in {rel.parent}.{p_col}"
                    )

        return errors

    # --- Convenience properties ---

    @property
    def table_names(self) -> list[str]:
        """Return list of table names in generation order."""
        return list(self.generation_order)

    def __len__(self) -> int:
        """Return total row count across all tables."""
        return sum(self.row_counts.values())

    def __contains__(self, table_name: str) -> bool:
        """Check if a table exists in the result."""
        return table_name in self.tables

    # --- Export methods ---

    def to_csv(self, output_dir: str | Path, **kwargs: Any) -> list[Path]:
        """Write all tables to CSV files. Returns list of file paths."""
        from sqllocks_spindle.output.pandas_writer import PandasWriter
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        paths = []
        for name, df in self.tables.items():
            path = output_dir / f"{name}.csv"
            df.to_csv(path, index=False, **kwargs)
            paths.append(path)
        return paths

    def to_parquet(self, output_dir: str | Path, **kwargs: Any) -> list[Path]:
        """Write all tables to Parquet files. Requires pyarrow."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        paths = []
        for name, df in self.tables.items():
            path = output_dir / f"{name}.parquet"
            df.to_parquet(path, index=False, **kwargs)
            paths.append(path)
        return paths

    def to_jsonl(self, output_dir: str | Path) -> list[Path]:
        """Write all tables to JSON Lines files."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        paths = []
        for name, df in self.tables.items():
            path = output_dir / f"{name}.jsonl"
            df.to_json(path, orient="records", lines=True)
            paths.append(path)
        return paths

    def to_excel(self, output_path: str | Path) -> Path:
        """Write all tables to a single Excel file (one sheet per table). Requires openpyxl."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for name, df in self.tables.items():
                sheet_name = name[:31]  # Excel sheet name limit
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        return output_path

    def to_sql(self, output_dir: str | Path, **kwargs: Any) -> list[Path]:
        """Write all tables as SQL INSERT files."""
        from sqllocks_spindle.output.pandas_writer import PandasWriter
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        writer = PandasWriter()
        return writer.to_sql_inserts(self.tables, str(output_dir), **kwargs)

    def to_dataframe(self, table_name: str) -> pd.DataFrame:
        """Return the DataFrame for a given table. Alias for self[table_name]."""
        return self.tables[table_name]


class Spindle:
    """Main entry point for Spindle data generation."""

    def __init__(self):
        self._parser = SchemaParser()
        self._validator = SchemaValidator()
        self._resolver = DependencyResolver()
        self._registry = self._build_registry()

    def _build_registry(self) -> StrategyRegistry:
        registry = StrategyRegistry()
        registry.register("sequence", SequenceStrategy())
        registry.register("uuid", UUIDStrategy())
        registry.register("native", NativeStrategy())
        registry.register("faker", FakerStrategy())
        registry.register("weighted_enum", WeightedEnumStrategy())
        registry.register("distribution", DistributionStrategy())
        registry.register("temporal", TemporalStrategy())
        registry.register("formula", FormulaStrategy())
        registry.register("derived", DerivedStrategy())
        registry.register("correlated", CorrelatedStrategy())
        registry.register("foreign_key", ForeignKeyStrategy())
        registry.register("lookup", LookupStrategy())
        registry.register("reference_data", ReferenceDataStrategy())
        registry.register("pattern", PatternStrategy())
        registry.register("conditional", ConditionalStrategy())
        registry.register("computed", ComputedStrategy())
        registry.register("lifecycle", LifecycleStrategy())
        registry.register("self_referencing", SelfReferencingStrategy())
        registry.register("self_ref_field", SelfRefFieldStrategy())
        registry.register("first_per_parent", FirstPerParentStrategy())
        registry.register("record_sample", RecordSampleStrategy())
        registry.register("record_field", RecordFieldStrategy())
        registry.register("scd2", SCD2Strategy())
        # Load any third-party strategy plugins via entrypoints
        registry.load_entrypoint_plugins()
        return registry

    def estimate_memory(
        self,
        domain=None,
        schema: str | Path | dict | SpindleSchema | None = None,
        scale: str | None = None,
        scale_overrides: dict[str, int] | None = None,
    ) -> dict:
        """Estimate RAM usage in bytes per table and total."""
        parsed = self._resolve_schema(domain, schema)
        if scale:
            parsed.generation.scale = scale
        row_counts = self._calculate_row_counts(parsed, scale_overrides)

        estimates: dict[str, int] = {}
        for table_name, table_def in parsed.tables.items():
            count = row_counts.get(table_name, 100)
            bytes_per_row = 0
            for col in table_def.columns.values():
                ctype = (col.type or "").lower()
                if "int" in ctype:
                    bytes_per_row += 8
                elif "float" in ctype or "decimal" in ctype or "numeric" in ctype:
                    bytes_per_row += 8
                elif "date" in ctype or "time" in ctype:
                    bytes_per_row += 8
                elif "bool" in ctype or "bit" in ctype:
                    bytes_per_row += 1
                else:
                    bytes_per_row += max(col.max_length or 50, 50)
            estimates[table_name] = count * bytes_per_row

        return {
            "per_table": estimates,
            "total_bytes": sum(estimates.values()),
            "total_mb": round(sum(estimates.values()) / (1024 * 1024), 1),
        }

    def generate(
        self,
        domain=None,
        schema: str | Path | dict | SpindleSchema | None = None,
        scale: str | None = None,
        scale_overrides: dict[str, int] | None = None,
        seed: int | None = None,
        on_progress: Callable[[str, int, int], None] | None = None,
    ) -> GenerationResult:
        """Generate synthetic data.

        Args:
            domain: A Domain instance (e.g., RetailDomain()) with built-in schema.
            schema: Path to .spindle.json, raw dict, or parsed SpindleSchema.
            scale: Scale preset name (small, medium, large, xlarge).
            scale_overrides: Override row counts for specific tables.
            seed: Random seed for reproducibility.
            on_progress: Optional callback(table_name, tables_done, tables_total).
        """
        # Resolve schema
        parsed = self._resolve_schema(domain, schema)

        # Apply overrides
        if scale:
            parsed.generation.scale = scale
        if seed is not None:
            parsed.model.seed = seed

        # Validate
        self._validator.validate_or_raise(parsed)

        # Resolve generation order
        gen_order = self._resolver.resolve(parsed)

        # Calculate row counts
        row_counts = self._calculate_row_counts(parsed, scale_overrides)

        # Initialize RNG and ID manager
        rng = np.random.default_rng(parsed.model.seed)
        id_manager = IDManager(rng)
        table_gen = TableGenerator(self._registry, id_manager)

        # Build model config
        model_config = {
            "locale": parsed.model.locale,
            "date_range": parsed.model.date_range,
            "seed": parsed.model.seed,
        }
        if domain and hasattr(domain, "domain_path"):
            # For composite domains, collect all child domain paths so
            # reference_data strategy can find datasets from each child.
            if hasattr(domain, "child_domains"):
                model_config["_domain_path"] = [
                    d.domain_path for d in domain.child_domains
                ]
            else:
                model_config["_domain_path"] = domain.domain_path

        # Generate tables in dependency order (parallelize independent tables)
        start_time = time.time()
        tables: dict[str, pd.DataFrame] = {}
        lineage: list[ColumnLineage] = []

        # Group tables by dependency level for parallel generation
        dep_levels = self._group_by_dep_level(gen_order, parsed)
        tables_done = 0
        tables_total = sum(1 for t in gen_order if t in parsed.tables)

        for level_tables in dep_levels:
            level_tables = [t for t in level_tables if t in parsed.tables]
            if not level_tables:
                continue

            # Generate tables in this level sequentially (shared id_manager
            # and RNG are not thread-safe; dependency-level grouping is kept
            # for future parallelization when per-table id_managers are added).
            for table_name in level_tables:
                count = row_counts.get(table_name, 100)
                logger.info("Generating %s (%s rows)", table_name, f"{count:,}")
                df = table_gen.generate(
                    table=parsed.tables[table_name],
                    row_count=count,
                    rng=rng,
                    model_config=model_config,
                    schema=parsed,
                )
                tables[table_name] = df
                tables_done += 1
                if on_progress:
                    on_progress(table_name, tables_done, tables_total)

            # Record lineage for this level
            for table_name in level_tables:
                if table_name not in parsed.tables:
                    continue
                for col_name, col in parsed.tables[table_name].columns.items():
                    lineage.append(ColumnLineage(
                        table=table_name,
                        column=col_name,
                        strategy=col.generator.get("strategy", ""),
                        config=dict(col.generator),
                    ))

        # Compute phase: back-fill computed columns
        self._compute_phase(tables, parsed)

        # Business rules: fix violations (operate on copies to preserve originals)
        if parsed.business_rules:
            rules_engine = BusinessRulesEngine()
            rules_engine.fix_violations(tables, parsed, rng)

        elapsed = time.time() - start_time

        return GenerationResult(
            tables=tables,
            schema=parsed,
            generation_order=gen_order,
            elapsed_seconds=elapsed,
            row_counts={name: len(df) for name, df in tables.items()},
            lineage=lineage,
        )

    def describe(self, domain=None, schema=None) -> SpindleSchema:
        """Parse and return schema without generating data."""
        return self._resolve_schema(domain, schema)

    def _resolve_schema(self, domain, schema) -> SpindleSchema:
        if domain is not None:
            return domain.get_schema()
        if isinstance(schema, SpindleSchema):
            return schema
        if isinstance(schema, dict):
            return self._parser.parse_dict(schema)
        if isinstance(schema, (str, Path)):
            return self._parser.parse_file(schema)
        raise ValueError("Must provide either 'domain' or 'schema'")

    def _calculate_row_counts(
        self,
        schema: SpindleSchema,
        overrides: dict[str, int] | None = None,
    ) -> dict[str, int]:
        counts = {}

        # Start with scale presets for anchor tables
        scale_def = schema.generation.scales.get(schema.generation.scale, {})
        for table_name, count in scale_def.items():
            counts[table_name] = count

        # Calculate derived counts
        for table_name, derived in schema.generation.derived_counts.items():
            if "fixed" in derived:
                if isinstance(derived["fixed"], int):
                    counts[table_name] = derived["fixed"]
            elif "per_parent" in derived:
                parent = derived["per_parent"]
                parent_count = counts.get(parent, 100)
                ratio = derived.get("ratio", derived.get("mean", 1.0))
                counts[table_name] = int(parent_count * ratio)
            elif "per_year" in derived:
                date_range = schema.model.date_range
                if date_range:
                    start_year = int(date_range.get("start", "2022")[:4])
                    end_year = int(date_range.get("end", "2025")[:4])
                    years = end_year - start_year + 1
                    counts[table_name] = derived["per_year"] * years

        # Apply any explicit overrides
        if overrides:
            counts.update(overrides)

        # Default for any table not yet assigned
        for table_name in schema.tables:
            if table_name not in counts:
                counts[table_name] = 100

        return counts

    @staticmethod
    def _group_by_dep_level(
        gen_order: list[str],
        schema: SpindleSchema,
    ) -> list[list[str]]:
        """Group tables into dependency levels for parallel generation."""
        # Build a set of tables each table depends on
        deps: dict[str, set[str]] = {}
        for tname, tdef in schema.tables.items():
            deps[tname] = set(tdef.fk_dependencies) & set(schema.tables.keys())

        assigned: set[str] = set()
        levels: list[list[str]] = []

        remaining = [t for t in gen_order if t in schema.tables]
        while remaining:
            # Tables whose deps are all assigned
            level = [t for t in remaining if deps.get(t, set()).issubset(assigned)]
            if not level:
                # Cycle — fall back to one-at-a-time
                level = [remaining[0]]
            levels.append(level)
            assigned.update(level)
            remaining = [t for t in remaining if t not in assigned]

        return levels

    def _compute_phase(
        self,
        tables: dict[str, pd.DataFrame],
        schema: SpindleSchema,
    ) -> None:
        """Back-fill computed columns from child table data."""
        for table_name, table_def in schema.tables.items():
            if table_name not in tables:
                continue

            for col_name, col in table_def.columns.items():
                if col.generator.get("strategy") != "computed":
                    continue

                config = col.generator
                rule = config.get("rule", "sum_children")
                child_table = config.get("child_table", "")
                child_column = config.get("child_column", "")

                if child_table not in tables:
                    continue

                # Find the FK relationship to determine join columns
                pk_col = table_def.primary_key[0] if table_def.primary_key else None
                if not pk_col:
                    continue

                # Find child FK column that references this table
                child_fk = None
                for c_col in schema.tables[child_table].columns.values():
                    if c_col.fk_ref_table == table_name:
                        child_fk = c_col.name
                        break

                if not child_fk:
                    continue

                ComputedStrategy.backfill(
                    parent_df=tables[table_name],
                    child_df=tables[child_table],
                    parent_pk=pk_col,
                    child_fk=child_fk,
                    child_column=child_column,
                    target_column=col_name,
                    rule=rule,
                )
