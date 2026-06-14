"""Audit 3.0.0 - B3 referential integrity tests for multi-chunk + incremental paths."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def test_table_generator_register_false_appends_to_pool():
    """register=False must append PKs to the parent pool, not replace it.

    Pre-3.0.0 each chunk overwrote the prior PK pool, so child FKs sampled
    from only the LAST chunk's parent slice and produced orphans across chunks.
    """
    from sqllocks_spindle.engine.id_manager import IDManager
    from sqllocks_spindle.engine.strategies.base import StrategyRegistry
    from sqllocks_spindle.engine.strategies.sequence import SequenceStrategy
    from sqllocks_spindle.engine.strategies.pattern import PatternStrategy
    from sqllocks_spindle.engine.table_generator import TableGenerator
    from sqllocks_spindle.schema.parser import ColumnDef, TableDef, SpindleSchema, ModelDef

    rng = np.random.default_rng(1)
    idm = IDManager(rng)
    registry = StrategyRegistry()
    registry.register("sequence", SequenceStrategy())
    registry.register("pattern", PatternStrategy())
    tg = TableGenerator(registry, idm)

    parent = TableDef(
        name="parent",
        primary_key=["parent_id"],
        columns={
            "parent_id": ColumnDef(name="parent_id", type="int", generator={"strategy": "sequence", "start": 1}),
            "label": ColumnDef(name="label", type="string", generator={"strategy": "pattern", "format": "P{seq:5}"}),
        },
    )
    model = ModelDef(name="t", seed=1)
    from sqllocks_spindle.schema.parser import GenerationConfig
    schema = SpindleSchema(
        model=model, tables={"parent": parent},
        relationships=[], business_rules=[], generation=GenerationConfig(),
    )
    model_config = {"locale": "en_US", "date_range": {"start": "2030-01-01", "end": "2030-12-31"}, "seed": 1}

    # Chunk 0: register
    df0 = tg.generate(parent, row_count=100, rng=rng, model_config=model_config, schema=schema, sequence_offset=0, register=True)
    pool_after_chunk0 = idm.get_pool_size("parent")
    # Chunks 1+ : append (would replace pre-fix)
    df1 = tg.generate(parent, row_count=100, rng=rng, model_config=model_config, schema=schema, sequence_offset=100, register=False)
    df2 = tg.generate(parent, row_count=100, rng=rng, model_config=model_config, schema=schema, sequence_offset=200, register=False)

    final_size = idm.get_pool_size("parent")
    assert pool_after_chunk0 == 100
    assert final_size == 300, f"expected pool to grow to 300, got {final_size}"


def test_continue_engine_repeated_calls_yield_disjoint_pks():
    """Two continue_from() calls on the same snapshot must produce disjoint integer PKs."""
    from sqllocks_spindle.incremental.continue_engine import ContinueEngine
    from sqllocks_spindle.incremental.continue_config import ContinueConfig

    existing = {
        "widget": pd.DataFrame({
            "widget_id": np.arange(1, 11),
            "label": [f"w{i}" for i in range(1, 11)],
        }),
    }
    engine = ContinueEngine()
    cfg = ContinueConfig(insert_count=5, update_fraction=0.0, delete_fraction=0.0, seed=7)

    d1 = engine.continue_from(existing, config=cfg)
    d2 = engine.continue_from(existing, config=cfg)

    pks1 = set(d1.inserts["widget"]["widget_id"].astype(int).tolist())
    pks2 = set(d2.inserts["widget"]["widget_id"].astype(int).tolist())
    assert pks1 and pks2, "expected non-empty inserts"
    assert not (pks1 & pks2), f"second call reissued PKs: {pks1 & pks2}"
    assert min(pks2) > max(pks1), "second batch must be strictly above first"


def test_time_travel_same_seed_snapshot0_reproducible():
    """test_time_travel.test_seed_reproducibility-style check that snapshot[0] stays deterministic."""
    from sqllocks_spindle import RetailDomain
    from sqllocks_spindle.incremental.time_travel import TimeTravelEngine, TimeTravelConfig

    cfg = TimeTravelConfig(months=2, seed=123)
    r1 = TimeTravelEngine().generate(domain=RetailDomain(), config=cfg, scale="small")
    r2 = TimeTravelEngine().generate(domain=RetailDomain(), config=cfg, scale="small")
    t = list(r1.snapshots[0].tables.keys())[0]
    assert r1.snapshots[0].tables[t].equals(r2.snapshots[0].tables[t])
