"""Audit 3.0.0 - B1 regression tests for config-key tolerant readers.

Verifies the strategy-side fixes from memory id=4788:
- temporal.py accepts top-level start/end as lowest-priority fallback
- temporal.py seasonal accepts top-level month_weights / day_of_week_weights
- derived.py accepts "operation" as alias for "rule", "days N" as add_days uniform
- distribution.py normal accepts sigma/std aliases for std_dev
- distribution.py log_normal accepts std as alias for sigma
- self_referencing.py accepts "max_depth" as alias for "levels"
- foreign_key.py accepts top-level alpha / max_per_parent (nested wins)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.engine.strategies.base import GenerationContext
from sqllocks_spindle.engine.strategies.derived import DerivedStrategy
from sqllocks_spindle.engine.strategies.distribution import DistributionStrategy
from sqllocks_spindle.engine.strategies.self_referencing import SelfReferencingStrategy
from sqllocks_spindle.engine.strategies.temporal import TemporalStrategy
from sqllocks_spindle.schema.parser import ColumnDef


class _Ctx:
    def __init__(self, row_count=100, seed=42):
        self.rng = np.random.default_rng(seed)
        self.row_count = row_count
        self.model_config = {}
        self.current_table = {}
        self.current_table_name = "t"
        self.id_manager = None


def _col(name="x", dtype="datetime"):
    return ColumnDef(name=name, type=dtype, generator={})


# ---------------- temporal ------------------


def test_temporal_top_level_start_end_used_when_no_nested():
    """Top-level start/end should win over the default 2022-2025 window."""
    s = TemporalStrategy()
    cfg = {"start": "2030-01-01", "end": "2030-12-31"}
    out = s.generate(_col(), cfg, _Ctx(row_count=200))
    ts = pd.to_datetime(out)
    assert (ts.year == 2030).all(), "expected all dates in 2030"
    # Disjoint from the legacy default window.
    assert not ((ts.year >= 2022) & (ts.year <= 2025)).any()


def test_temporal_range_ref_still_wins_over_top_level():
    """range_ref=model.date_range must take precedence over top-level start/end."""
    s = TemporalStrategy()
    cfg = {"start": "2030-01-01", "end": "2030-12-31", "range_ref": "model.date_range"}
    ctx = _Ctx(row_count=50)
    ctx.model_config = {"date_range": {"start": "1990-01-01", "end": "1990-12-31"}}
    out = s.generate(_col(), cfg, ctx)
    ts = pd.to_datetime(out)
    assert (ts.year == 1990).all()


def test_temporal_nested_date_range_wins_over_top_level():
    s = TemporalStrategy()
    cfg = {
        "start": "2030-01-01",
        "end": "2030-12-31",
        "date_range": {"start": "1980-01-01", "end": "1980-12-31"},
    }
    out = s.generate(_col(), cfg, _Ctx(row_count=50))
    ts = pd.to_datetime(out)
    assert (ts.year == 1980).all()


def test_temporal_seasonal_top_level_month_weights():
    """Top-level month_weights should feed seasonal pattern when profiles missing month."""
    s = TemporalStrategy()
    cfg = {
        "pattern": "seasonal",
        "date_range": {"start": "2030-01-01", "end": "2030-12-31"},
        "month_weights": {"Jan": 0.9, "Feb": 0.01, "Mar": 0.01, "Apr": 0.01,
                          "May": 0.01, "Jun": 0.01, "Jul": 0.01, "Aug": 0.01,
                          "Sep": 0.01, "Oct": 0.01, "Nov": 0.01, "Dec": 0.01},
    }
    out = s.generate(_col(), cfg, _Ctx(row_count=1000))
    ts = pd.to_datetime(out)
    jan_share = float((ts.month == 1).mean())
    assert jan_share > 0.5, f"expected Jan dominance, got share={jan_share}"


# ---------------- derived ------------------


def test_derived_operation_alias_for_rule():
    """operation key should work where rule is documented."""
    s = DerivedStrategy()
    ctx = _Ctx(row_count=5, seed=1)
    base = pd.to_datetime(["2030-01-01"] * 5).values
    ctx.current_table["src"] = base
    cfg = {"source": "src", "operation": "add_days",
           "params": {"distribution": "uniform", "min": 5, "max": 5}}
    out = s.generate(_col("y"), cfg, ctx)
    ts = pd.to_datetime(out)
    expected = pd.Timestamp("2030-01-06")
    assert (ts == expected).all()


def test_derived_top_level_days_creates_add_days():
    """A top-level days: N should expand to add_days uniform [N, N]."""
    s = DerivedStrategy()
    ctx = _Ctx(row_count=4, seed=1)
    base = pd.to_datetime(["2030-06-01"] * 4).values
    ctx.current_table["src"] = base
    cfg = {"source": "src", "days": 7}
    out = s.generate(_col("y"), cfg, ctx)
    ts = pd.to_datetime(out)
    assert (ts == pd.Timestamp("2030-06-08")).all()


# ---------------- distribution ------------------


def test_normal_sigma_alias_for_std_dev():
    s = DistributionStrategy()
    out = s.generate(
        _col("score", dtype="float"),
        {"distribution": "normal", "params": {"mean": 500, "sigma": 80}},
        _Ctx(row_count=10000, seed=42),
    )
    assert 60 < float(np.std(out)) < 100, f"std={np.std(out)}"


def test_normal_std_alias_for_std_dev():
    s = DistributionStrategy()
    out = s.generate(
        _col("score", dtype="float"),
        {"distribution": "normal", "params": {"mean": 0, "std": 5}},
        _Ctx(row_count=10000, seed=7),
    )
    assert 4.0 < float(np.std(out)) < 6.0


def test_log_normal_std_alias_for_sigma():
    s = DistributionStrategy()
    out = s.generate(
        _col("v", dtype="float"),
        {"distribution": "log_normal", "params": {"mean": 0, "std": 2}},
        _Ctx(row_count=20000, seed=11),
    )
    # log_normal with sigma=2 vs default sigma=1: very different spread.
    assert float(np.std(np.log(out))) > 1.5


# ---------------- self_referencing ------------------


def test_self_referencing_max_depth_alias_for_levels():
    s = SelfReferencingStrategy()
    ctx = _Ctx(row_count=20, seed=3)
    ctx.current_table["category_id"] = np.arange(1, 21)
    cfg = {"pk_column": "category_id", "max_depth": 4, "root_count": 2}
    parents = s.generate(_col("parent_category_id"), cfg, ctx)
    levels = ctx.current_table["_sr_t_level"]
    assert int(levels.max()) == 4, f"expected max level 4, got {levels.max()}"
    assert (parents[:2] == np.array([None, None], dtype=object)).all()


# ---------------- foreign_key top-level alpha (via id_manager) ------------------


def test_foreign_key_top_level_alpha_routes_to_id_manager():
    """A top-level alpha key on a foreign_key generator must reach id_manager params."""
    from sqllocks_spindle.engine.strategies.foreign_key import ForeignKeyStrategy

    captured = {}

    class _IDMgr:
        def get_random_fks(self, table_name, count, distribution, params):
            captured["distribution"] = distribution
            captured["params"] = params
            return np.zeros(count, dtype=int)

    s = ForeignKeyStrategy()
    ctx = _Ctx(row_count=10, seed=1)
    ctx.id_manager = _IDMgr()
    cfg = {"ref": "parent.id", "distribution": "pareto", "alpha": 2.5, "max_per_parent": 100}
    out = s.generate(_col("fk"), cfg, ctx)
    assert captured["distribution"] == "pareto"
    assert captured["params"]["alpha"] == 2.5
    assert captured["params"]["max_per_parent"] == 100


def test_foreign_key_nested_params_wins_over_top_level():
    from sqllocks_spindle.engine.strategies.foreign_key import ForeignKeyStrategy

    captured = {}

    class _IDMgr:
        def get_random_fks(self, table_name, count, distribution, params):
            captured["params"] = params
            return np.zeros(count, dtype=int)

    s = ForeignKeyStrategy()
    ctx = _Ctx(row_count=10, seed=1)
    ctx.id_manager = _IDMgr()
    cfg = {"ref": "p.id", "distribution": "pareto", "alpha": 9.9,
           "params": {"alpha": 1.2, "max_per_parent": 50}}
    s.generate(_col("fk"), cfg, ctx)
    assert captured["params"]["alpha"] == 1.2
    assert captured["params"]["max_per_parent"] == 50
