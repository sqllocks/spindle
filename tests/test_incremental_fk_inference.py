"""Regression tests for ContinueEngine FK integrity when the schema has NO
explicit foreign-key generator metadata (the inferred-schema case).

Background: when a schema is produced by SchemaBuilder/DataProfiler over real
data (e.g. the Contoso Day-2 demo), columns do not carry
``generator.strategy == "foreign_key"``.  Before the fix, ``_fk_map`` returned
no FK info for such schemas, so FK columns fell through to ``_perturb_columns``
(integer keys multiplied by a random factor) and produced orphaned keys
(FK valid rate -> 0.0).  These tests pin the two recovery paths:

1. Name-based inference: a column whose name matches another table's PK column.
2. Declared relationships: ``schema.relationships`` child_columns -> parent.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from sqllocks_spindle.incremental import ContinueConfig, ContinueEngine
from sqllocks_spindle.schema.parser import (
    ColumnDef,
    GenerationConfig,
    ModelDef,
    RelationshipDef,
    SpindleSchema,
    TableDef,
)


def _col(name: str, type_: str = "int") -> ColumnDef:
    # Empty generator => is_foreign_key is False (mimics an inferred schema).
    return ColumnDef(name=name, type=type_, generator={})


def _tables():
    """Two-table star: customer (parent) <- sales (child) keyed by CustomerKey.

    Notes on the fixture design:
    - ``Key`` naming (not ``_id``) so the legacy ``_id`` heuristics do not
      accidentally rescue the FK — inference must work on the actual names.
    - Parent keys are SPARSE (multiples of 10).  A perturbed FK (key * a random
      factor in [0.9, 1.1]) almost always lands on a non-multiple-of-10, i.e. a
      real orphan, and cannot be absorbed by the contiguous keys the parent's
      own inserts append above max(key).  This makes the perturbation bug
      observable rather than masked.
    """
    rng = np.random.default_rng(7)
    customer_keys = np.arange(10, 10001, 10, dtype="int64")  # 10, 20, ... 10000
    customer = pd.DataFrame(
        {
            "CustomerKey": customer_keys,
            "Name": [f"cust{k}" for k in customer_keys],
        }
    )
    sales = pd.DataFrame(
        {
            "SalesKey": np.arange(1, 501, dtype="int64"),
            "CustomerKey": rng.choice(customer_keys, size=500).astype("int64"),
            "Amount": rng.uniform(10, 1000, size=500),
        }
    )
    return {"customer": customer, "sales": sales}


def _schema(relationships=None):
    customer = TableDef(
        name="customer",
        columns={"CustomerKey": _col("CustomerKey"), "Name": _col("Name", "string")},
        primary_key=["CustomerKey"],
    )
    sales = TableDef(
        name="sales",
        columns={
            "SalesKey": _col("SalesKey"),
            "CustomerKey": _col("CustomerKey"),
            "Amount": _col("Amount", "float"),
        },
        primary_key=["SalesKey"],
    )
    return SpindleSchema(
        model=ModelDef(name="test"),
        tables={"customer": customer, "sales": sales},
        relationships=relationships or [],
        business_rules=[],
        generation=GenerationConfig(),
    )


def test_name_based_fk_inference_prevents_orphans():
    tables = _tables()
    schema = _schema()
    engine = ContinueEngine()
    config = ContinueConfig(insert_count=200, update_fraction=0.0, delete_fraction=0.0, seed=1)

    delta = engine.continue_from(tables, schema=schema, config=config)

    ins = delta.inserts["sales"]
    assert len(ins) == 200
    valid = set(tables["customer"]["CustomerKey"].tolist())
    valid |= set(delta.inserts["customer"]["CustomerKey"].tolist())
    orphans = set(ins["CustomerKey"].tolist()) - valid
    assert orphans == set(), f"FK column was perturbed/orphaned: {sorted(orphans)[:10]}"


def test_fk_inference_via_declared_relationship_with_renamed_column():
    """FK column name differs from the parent PK; only relationships can catch it."""
    tables = _tables()
    # Rename the child FK column so name-based inference can NOT match it.
    tables["sales"] = tables["sales"].rename(columns={"CustomerKey": "CustKey"})
    schema = _schema(
        relationships=[
            RelationshipDef(
                name="sales_customer",
                parent="customer",
                child="sales",
                parent_columns=["CustomerKey"],
                child_columns=["CustKey"],
                type="one_to_many",
            )
        ]
    )
    # Fix the child schema column name to match the renamed data column.
    schema.tables["sales"].columns = {
        "SalesKey": _col("SalesKey"),
        "CustKey": _col("CustKey"),
        "Amount": _col("Amount", "float"),
    }

    engine = ContinueEngine()
    config = ContinueConfig(insert_count=200, update_fraction=0.0, delete_fraction=0.0, seed=2)
    delta = engine.continue_from(tables, schema=schema, config=config)

    ins = delta.inserts["sales"]
    valid = set(tables["customer"]["CustomerKey"].tolist())
    valid |= set(delta.inserts["customer"]["CustomerKey"].tolist())
    orphans = set(ins["CustKey"].tolist()) - valid
    assert orphans == set(), f"Relationship FK was perturbed/orphaned: {sorted(orphans)[:10]}"
