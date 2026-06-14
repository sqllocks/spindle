"""Regression tests for the 2.14.5 SAFE audit fixes.

Covers the in-process-verifiable fixes. DB-path fixes (copy_into rowcount,
fast_executemany sizing, lakehouse read) are correctness-by-construction and
require a live Fabric SQL/lakehouse to exercise end to end.
"""
from __future__ import annotations

import json
import math
from types import SimpleNamespace

import pytest


def test_translate_distribution_lognormal_uniform_exponential():
    from sqllocks_spindle.inference.safe_profile_adapter import _translate_distribution as x

    # lognormal: scipy (s, loc, scale) -> engine log_normal (mean=ln(scale), sigma=s)
    name, params = x("lognormal", {"s": 0.5, "loc": 0.0, "scale": math.e})
    assert name == "log_normal"
    assert params["sigma"] == 0.5
    assert abs(params["mean"] - 1.0) < 1e-6

    # uniform: scipy (loc, scale) spans [loc, loc+scale] -> engine min/max
    name, params = x("uniform", {"loc": 10.0, "scale": 5.0})
    assert name == "uniform"
    assert params == {"min": 10.0, "max": 15.0}

    # exponential has no faithful engine generator -> None (caller falls back)
    assert x("exponential", {"loc": 0.0, "scale": 3.0}) is None
    assert x(None, None) is None


def test_schema_builder_uses_translation_not_raw_name():
    """A lognormal-fit column must emit engine 'log_normal' (was: crash/uniform)."""
    from sqllocks_spindle.inference.schema_builder import SchemaBuilder

    col = SimpleNamespace(
        name="amount", dtype="float", is_enum=False, is_primary_key=False,
        is_foreign_key=False, fk_ref_table=None, nullable=False,
        fit_score=0.9, quantiles=None,
        distribution="lognormal",
        distribution_params={"s": 0.7, "loc": 0.0, "scale": 100.0},
        mean=100.0, std=20.0, min_value=1.0, max_value=500.0,
        value_counts_ext=None, enum_values=None, pattern=None,
    )
    gen = SchemaBuilder()._column_to_generator(col, None, fit_threshold=0.8)
    assert gen["strategy"] == "distribution"
    assert gen["distribution"] == "log_normal"  # translated, not the raw scipy name
    assert "sigma" in gen["params"]


def test_masker_topo_order_parent_before_child():
    from sqllocks_spindle.inference.masker import DataMasker

    def prof(fks):
        cols = {}
        for cname, ref in fks.items():
            cols[cname] = SimpleNamespace(is_foreign_key=ref is not None, fk_ref_table=ref)
        return SimpleNamespace(columns=cols)

    # order: child references parent; grandchild references child
    profiles = {
        "grandchild": prof({"child_id": "child"}),
        "child": prof({"parent_id": "parent"}),
        "parent": prof({"id": None}),
    }
    order = DataMasker._topo_order(["grandchild", "child", "parent"], profiles)
    assert order.index("parent") < order.index("child") < order.index("grandchild")


def test_masker_topo_order_tolerates_cycle():
    from sqllocks_spindle.inference.masker import DataMasker

    def prof(ref):
        return SimpleNamespace(columns={"fk": SimpleNamespace(is_foreign_key=True, fk_ref_table=ref)})

    profiles = {"a": prof("b"), "b": prof("a")}  # cycle
    order = DataMasker._topo_order(["a", "b"], profiles)
    assert set(order) == {"a", "b"}  # does not hang or drop tables


def test_profile_store_save_emits_valid_json_without_nan(tmp_path):
    """NaN/Inf must serialize to null, not bare NaN (invalid JSON for other consumers)."""
    from sqllocks_spindle.inference.profile_store import ProfileStore

    class _FakeProfile:
        def to_safe_dict(self):
            return {"a": float("nan"), "b": float("inf"), "c": [1.0, float("-inf")], "d": "ok"}

    out = ProfileStore.save(_FakeProfile(), tmp_path / "p.json")
    text = out.read_text(encoding="utf-8")
    assert "NaN" not in text and "Infinity" not in text
    loaded = json.loads(text)  # strict parse must succeed
    assert loaded["a"] is None and loaded["b"] is None and loaded["c"][1] is None
    assert loaded["d"] == "ok"


def test_touched_modules_import():
    import importlib
    for mod in [
        "sqllocks_spindle.engine.generator",
        "sqllocks_spindle.fabric.sql_database_writer",
        "sqllocks_spindle.fabric.warehouse_bulk_writer",
        "sqllocks_spindle.inference.tier3_research",
        "sqllocks_spindle.inference.masker",
        "sqllocks_spindle.inference.schema_builder",
        "sqllocks_spindle.inference.comparator",
        "sqllocks_spindle.inference.profile_store",
        "sqllocks_spindle.inference.lakehouse_profiler",
        "sqllocks_spindle.demo.notebook_gen",
        "sqllocks_spindle.demo.cleanup",
        "sqllocks_spindle.demo.connections",
    ]:
        importlib.import_module(mod)
