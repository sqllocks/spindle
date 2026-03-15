"""Chunked generation engine for billion-row scale.

Two-pass approach:
1. Generate ALL parent/dimension/reference tables fully (in-memory).
2. For each child table in dependency order, yield chunk_size rows at a time.
   Each chunk shares the same IDManager so FK references are valid.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterator

import numpy as np
import pandas as pd

from sqllocks_spindle.engine.generator import (
    ColumnLineage,
    GenerationResult,
    Spindle,
)
from sqllocks_spindle.engine.id_manager import IDManager, RangePKPool
from sqllocks_spindle.engine.strategies.base import StrategyRegistry
from sqllocks_spindle.engine.table_generator import TableGenerator
from sqllocks_spindle.schema.dependency import DependencyResolver
from sqllocks_spindle.schema.parser import SpindleSchema

logger = logging.getLogger(__name__)


@dataclass
class ChunkedGenerationResult:
    """Result of a chunked generation run.

    Parent tables are fully materialized (small). Child tables are available
    only via ``iter_chunks()`` to keep memory bounded.
    """

    parent_tables: dict[str, pd.DataFrame]
    child_table_names: list[str]
    schema: SpindleSchema
    generation_order: list[str]
    row_counts: dict[str, int]

    # Internal state — not part of the public API
    _chunked_spindle: ChunkedSpindle = field(repr=False, default=None)  # type: ignore[assignment]

    def iter_chunks(self, table_name: str) -> Iterator[pd.DataFrame]:
        """Yield DataFrames of ``chunk_size`` rows for a child table.

        Must be called in dependency order. Each table can only be iterated once.
        """
        if table_name not in self.child_table_names:
            raise ValueError(
                f"'{table_name}' is not a chunked child table. "
                f"Chunked tables: {self.child_table_names}"
            )
        return self._chunked_spindle._iter_child_chunks(table_name)

    def write_with(self, writer: Any, **kwargs: Any) -> None:
        """Convenience: write parent tables, then stream child chunks through a writer.

        The writer must implement either:
          - ``write_table(table_name, df, **kwargs)`` for individual DataFrames, or
          - ``stage_chunk(table_name, chunk_df, idx)`` + ``copy_into(table_name)``
            for bulk writers.
        """
        # Write parent tables normally
        for table_name, df in self.parent_tables.items():
            if hasattr(writer, "write_table"):
                writer.write_table(table_name, df, **kwargs)
            elif hasattr(writer, "create_table"):
                writer.create_table(table_name, df)
                writer.stage_chunk(table_name, df, 0)
                writer.copy_into(table_name)

        # Stream child tables
        for table_name in self.child_table_names:
            first_chunk = True
            for idx, chunk_df in enumerate(self.iter_chunks(table_name)):
                if hasattr(writer, "stage_chunk"):
                    if first_chunk:
                        writer.create_table(table_name, chunk_df)
                        first_chunk = False
                    writer.stage_chunk(table_name, chunk_df, idx)
                elif hasattr(writer, "write_table"):
                    mode = kwargs.get("mode", "append")
                    if first_chunk:
                        writer.write_table(table_name, chunk_df, **kwargs)
                        first_chunk = False
                    else:
                        writer.write_table(table_name, chunk_df, mode=mode, **kwargs)

            if hasattr(writer, "copy_into"):
                writer.copy_into(table_name)

    @property
    def all_table_names(self) -> list[str]:
        return list(self.parent_tables.keys()) + self.child_table_names

    @property
    def total_rows(self) -> int:
        return sum(self.row_counts.values())


class ChunkedSpindle:
    """Generate billion-row datasets in bounded memory.

    Uses a two-pass approach:
    1. Parent tables generated fully in-memory (typically small).
    2. Child tables generated in chunks of ``chunk_size`` rows.

    Example::

        cs = ChunkedSpindle()
        result = cs.generate_chunked(
            domain=FinancialDomain(),
            scale="warehouse",
            chunk_size=1_000_000,
        )

        # Parent tables are immediately available
        for name, df in result.parent_tables.items():
            print(f"{name}: {len(df)} rows")

        # Child tables stream via iterator
        for table_name in result.child_table_names:
            for chunk in result.iter_chunks(table_name):
                writer.write(chunk)
    """

    def __init__(self):
        self._spindle = Spindle()

    def generate_chunked(
        self,
        domain=None,
        schema: Any = None,
        scale: str | None = None,
        scale_overrides: dict[str, int] | None = None,
        seed: int | None = None,
        chunk_size: int = 1_000_000,
    ) -> ChunkedGenerationResult:
        """Generate data with chunked child tables.

        Args:
            domain: A Domain instance.
            schema: Path to .spindle.json, raw dict, or parsed SpindleSchema.
            scale: Scale preset name.
            scale_overrides: Override row counts for specific tables.
            seed: Random seed for reproducibility.
            chunk_size: Rows per chunk for child tables.

        Returns:
            ChunkedGenerationResult with parent tables materialized and
            child tables available via iter_chunks().
        """
        # Resolve and configure schema
        parsed = self._spindle._resolve_schema(domain, schema)
        if scale:
            parsed.generation.scale = scale
        if seed is not None:
            parsed.model.seed = seed
        self._spindle._validator.validate_or_raise(parsed)

        # Resolve generation order and row counts
        gen_order = self._spindle._resolver.resolve(parsed)
        row_counts = self._spindle._calculate_row_counts(parsed, scale_overrides)

        # Classify tables: parents (no FK deps or small) vs children (large, have FK deps)
        parent_names, child_names = self._classify_tables(
            parsed, gen_order, row_counts, chunk_size,
        )

        # Initialize RNG and ID manager
        rng = np.random.default_rng(parsed.model.seed)
        id_manager = IDManager(rng)
        table_gen = TableGenerator(self._spindle._registry, id_manager)

        model_config = {
            "locale": parsed.model.locale,
            "date_range": parsed.model.date_range,
            "seed": parsed.model.seed,
        }
        if domain and hasattr(domain, "domain_path"):
            model_config["_domain_path"] = domain.domain_path

        # Pass 1: Generate all parent tables fully
        parent_tables: dict[str, pd.DataFrame] = {}
        for table_name in gen_order:
            if table_name not in parent_names:
                continue
            if table_name not in parsed.tables:
                continue

            count = row_counts.get(table_name, 100)
            logger.info("Generating parent table %s (%s rows)", table_name, f"{count:,}")

            df = table_gen.generate(
                table=parsed.tables[table_name],
                row_count=count,
                rng=rng,
                model_config=model_config,
                schema=parsed,
            )
            parent_tables[table_name] = df

        # Compute phase for parent tables
        self._spindle._compute_phase(parent_tables, parsed)

        # Apply business rules to parent tables
        if parsed.business_rules:
            from sqllocks_spindle.engine.rules.business_rules import BusinessRulesEngine
            rules_engine = BusinessRulesEngine()
            rules_engine.fix_violations(parent_tables, parsed, rng)

        # Store state for lazy child generation
        self._parsed = parsed
        self._gen_order = gen_order
        self._row_counts = row_counts
        self._rng = rng
        self._id_manager = id_manager
        self._table_gen = table_gen
        self._model_config = model_config
        self._chunk_size = chunk_size
        self._child_names_set = set(child_names)
        self._parent_tables = parent_tables

        # Build ordered child list (preserving gen_order)
        ordered_children = [t for t in gen_order if t in self._child_names_set]

        return ChunkedGenerationResult(
            parent_tables=parent_tables,
            child_table_names=ordered_children,
            schema=parsed,
            generation_order=gen_order,
            row_counts=row_counts,
            _chunked_spindle=self,
        )

    def _classify_tables(
        self,
        schema: SpindleSchema,
        gen_order: list[str],
        row_counts: dict[str, int],
        chunk_size: int,
    ) -> tuple[list[str], list[str]]:
        """Classify tables into parent (fully generated) and child (chunked).

        A table is a "child" (chunked) if:
        1. It has FK dependencies, AND
        2. Its row count exceeds chunk_size.

        Everything else is a parent table, generated fully in-memory.
        """
        parents = []
        children = []

        for table_name in gen_order:
            if table_name not in schema.tables:
                continue
            table_def = schema.tables[table_name]
            count = row_counts.get(table_name, 100)

            has_fk_deps = len(table_def.fk_dependencies) > 0
            is_large = count > chunk_size

            if has_fk_deps and is_large:
                children.append(table_name)
            else:
                parents.append(table_name)

        return parents, children

    def _iter_child_chunks(self, table_name: str) -> Iterator[pd.DataFrame]:
        """Generate chunks for a child table."""
        total_rows = self._row_counts.get(table_name, 0)
        table_def = self._parsed.tables[table_name]
        rows_generated = 0
        chunk_idx = 0

        while rows_generated < total_rows:
            chunk_rows = min(self._chunk_size, total_rows - rows_generated)
            logger.info(
                "Generating chunk %d for %s (%s rows, %s/%s total)",
                chunk_idx, table_name, f"{chunk_rows:,}",
                f"{rows_generated:,}", f"{total_rows:,}",
            )

            df = self._table_gen.generate(
                table=table_def,
                row_count=chunk_rows,
                rng=self._rng,
                model_config=self._model_config,
                schema=self._parsed,
                sequence_offset=rows_generated,
            )

            # After generating, append new PKs to IDManager so downstream
            # child-of-child tables can reference them.
            if table_def.primary_key:
                pk_col = table_def.primary_key[0]
                if pk_col in df.columns:
                    new_pks = df[pk_col].values
                    # First chunk already registered by table_gen; for subsequent
                    # chunks, use append_pks.
                    if chunk_idx > 0:
                        self._id_manager.append_pks(table_name, new_pks)

            rows_generated += chunk_rows
            chunk_idx += 1

            yield df
