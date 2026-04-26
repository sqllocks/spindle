"""Subprocess worker function for multi-process chunk generation."""
from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def generate_chunk(
    schema_path: str,
    seed: int,
    offset: int,
    count: int,
) -> dict[str, dict[str, list]]:
    """Generate a chunk of synthetic data for all tables in a schema.

    Designed to run inside a ``ProcessPoolExecutor`` worker.  Must be
    importable at the top level (no closure captures) and return plain
    Python lists so the result pickles cleanly across process boundaries.

    Args:
        schema_path: Path to a .spindle.json file.
        seed: Random seed for this chunk.  Each chunk should use a unique
            seed derived from the base seed and chunk index so that chunks
            are independent but individually reproducible.
        offset: Row offset for PK sequence columns.  A chunk with
            ``offset=N`` and ``count=M`` will produce sequence IDs starting
            at ``start + N * step`` (e.g. offset=100, start=1, step=1 →
            IDs 101-200).
        count: Number of rows to generate per table.

    Returns:
        ``{table_name: {column_name: [values...]}}`` — plain Python lists,
        NOT numpy arrays.
    """
    import numpy as np
    from sqllocks_spindle.engine.id_manager import IDManager
    from sqllocks_spindle.engine.strategies.base import StrategyRegistry
    from sqllocks_spindle.engine.strategies.computed import ComputedStrategy
    from sqllocks_spindle.engine.strategies.conditional import ConditionalStrategy
    from sqllocks_spindle.engine.strategies.correlated import CorrelatedStrategy
    from sqllocks_spindle.engine.strategies.derived import DerivedStrategy
    from sqllocks_spindle.engine.strategies.distribution import DistributionStrategy
    from sqllocks_spindle.engine.strategies.enum import WeightedEnumStrategy
    from sqllocks_spindle.engine.strategies.faker_strategy import FakerStrategy
    from sqllocks_spindle.engine.strategies.first_per_parent import FirstPerParentStrategy
    from sqllocks_spindle.engine.strategies.foreign_key import ForeignKeyStrategy
    from sqllocks_spindle.engine.strategies.formula import FormulaStrategy
    from sqllocks_spindle.engine.strategies.lifecycle import LifecycleStrategy
    from sqllocks_spindle.engine.strategies.lookup import LookupStrategy
    from sqllocks_spindle.engine.strategies.native import NativeStrategy
    from sqllocks_spindle.engine.strategies.pattern import PatternStrategy
    from sqllocks_spindle.engine.strategies.record_field import RecordFieldStrategy
    from sqllocks_spindle.engine.strategies.record_sample import RecordSampleStrategy
    from sqllocks_spindle.engine.strategies.reference_data import ReferenceDataStrategy
    from sqllocks_spindle.engine.strategies.scd2 import SCD2Strategy
    from sqllocks_spindle.engine.strategies.self_referencing import (
        SelfReferencingStrategy,
        SelfRefFieldStrategy,
    )
    from sqllocks_spindle.engine.strategies.sequence import SequenceStrategy
    from sqllocks_spindle.engine.strategies.temporal import TemporalStrategy
    from sqllocks_spindle.engine.strategies.uuid_strategy import UUIDStrategy
    from sqllocks_spindle.engine.strategies.composite_foreign_key import (
        CompositeForeignKeyStrategy,
        CompositeFKFieldStrategy,
    )
    from sqllocks_spindle.engine.table_generator import TableGenerator
    from sqllocks_spindle.schema.dependency import DependencyResolver
    from sqllocks_spindle.schema.parser import SchemaParser

    # Load and parse schema
    schema_dict = json.loads(Path(schema_path).read_text())
    parser = SchemaParser()
    schema = parser.parse_dict(schema_dict)

    # Read static/dynamic classification injected by ScaleRouter
    static_tables: set[str] = set(schema_dict.get("_static_tables", []))
    dynamic_tables: set[str] = set(schema_dict.get("_dynamic_tables", []))
    static_pk_data: dict = schema_dict.get("_static_pk_data", {})
    schema_counts: dict = schema_dict.get("_schema_counts", {})

    # Override seed for this chunk
    schema.model.seed = seed

    # Build a fresh strategy registry (stateless — safe in a subprocess)
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
    registry.register("composite_foreign_key", CompositeForeignKeyStrategy())
    registry.register("composite_fk_field", CompositeFKFieldStrategy())
    registry.load_entrypoint_plugins()

    # Resolve generation order so FK deps are satisfied
    resolver = DependencyResolver()
    gen_order = resolver.resolve(schema)

    import pandas as pd
    from sqllocks_spindle.engine.generator import apply_compute_phase

    rng = np.random.default_rng(seed)
    id_manager = IDManager(rng)
    table_gen = TableGenerator(registry, id_manager)

    model_config: dict = {
        "locale": schema.model.locale,
        "date_range": schema.model.date_range,
        "seed": seed,
    }
    # _domain_path is injected by the caller (e.g. ScaleRouter) before serializing
    # the schema to JSON. reference_data strategy uses it to find data files.
    if "_domain_path" in schema_dict:
        model_config["_domain_path"] = schema_dict["_domain_path"]

    # Pre-load static table PK pools so dynamic tables can FK-reference them.
    # Static table data was generated once by ScaleRouter and broadcast here.
    for sname, col_lists in static_pk_data.items():
        if sname not in schema.tables:
            continue
        sdf = pd.DataFrame({col: vals for col, vals in col_lists.items()})
        pk_cols = schema.tables[sname].primary_key
        id_manager.register_table(sname, sdf, pk_cols)
        logger.debug("chunk_worker: pre-loaded static table '%s' (%d rows)", sname, len(sdf))

    result_dfs: dict[str, pd.DataFrame] = {}

    for table_name in gen_order:
        if table_name not in schema.tables:
            continue

        # Skip static tables — they were generated once by ScaleRouter
        if static_tables and table_name in static_tables:
            logger.debug("chunk_worker: skipping static table '%s'", table_name)
            continue

        table_def = schema.tables[table_name]
        child_rng = np.random.default_rng(seed ^ (hash(table_name) & 0xFFFF_FFFF))

        logger.debug(
            "chunk_worker: generating %s — count=%d offset=%d seed=%d",
            table_name,
            count,
            offset,
            seed,
        )

        df = table_gen.generate(
            table=table_def,
            row_count=count,
            rng=child_rng,
            model_config=model_config,
            schema=schema,
            sequence_offset=offset,
        )

        result_dfs[table_name] = df

    # Apply computed column back-fill (e.g. order.total_amount summed from order_line)
    apply_compute_phase(result_dfs, schema)

    # Apply business rules (e.g. end_date >= start_date) within this chunk
    if schema.business_rules:
        from sqllocks_spindle.engine.rules.business_rules import BusinessRulesEngine
        rules_engine = BusinessRulesEngine()
        rules_engine.fix_violations(result_dfs, schema, rng)

    # Convert DataFrames to plain Python lists — numpy arrays don't pickle
    # reliably across process boundaries (dtype/endianness edge cases).
    result: dict[str, dict[str, list]] = {
        table_name: {
            col: (series.tolist() if hasattr(series, "tolist") else list(series))
            for col, series in df.items()
        }
        for table_name, df in result_dfs.items()
    }

    return result
