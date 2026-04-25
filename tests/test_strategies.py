"""Tests for individual generation strategies."""

from __future__ import annotations

import numpy as np
import pytest

from sqllocks_spindle.engine.id_manager import IDManager
from sqllocks_spindle.engine.strategies.base import GenerationContext, StrategyRegistry
from sqllocks_spindle.engine.strategies.conditional import ConditionalStrategy
from sqllocks_spindle.engine.strategies.correlated import CorrelatedStrategy
from sqllocks_spindle.engine.strategies.derived import DerivedStrategy
from sqllocks_spindle.engine.strategies.distribution import DistributionStrategy
from sqllocks_spindle.engine.strategies.enum import WeightedEnumStrategy
from sqllocks_spindle.engine.strategies.foreign_key import ForeignKeyStrategy
from sqllocks_spindle.engine.strategies.formula import FormulaStrategy
from sqllocks_spindle.engine.strategies.lifecycle import LifecycleStrategy
from sqllocks_spindle.engine.strategies.self_referencing import SelfReferencingStrategy, SelfRefFieldStrategy
from sqllocks_spindle.engine.strategies.sequence import SequenceStrategy
from sqllocks_spindle.engine.strategies.uuid_strategy import UUIDStrategy
from sqllocks_spindle.schema.parser import ColumnDef


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(name: str = "col", dtype: str = "integer") -> ColumnDef:
    return ColumnDef(name=name, type=dtype, generator={})


def _ctx(row_count: int = 100, current_table: dict | None = None) -> GenerationContext:
    rng = np.random.default_rng(42)
    id_manager = IDManager(rng)
    ctx = GenerationContext(rng=rng, id_manager=id_manager, model_config={}, row_count=row_count)
    if current_table:
        ctx.current_table = current_table
    return ctx


# ---------------------------------------------------------------------------
# SequenceStrategy
# ---------------------------------------------------------------------------

class TestSequenceStrategy:
    def test_default_start(self):
        strategy = SequenceStrategy()
        result = strategy.generate(_col(), {"start": 1}, _ctx(10))
        assert list(result) == list(range(1, 11))

    def test_custom_start(self):
        strategy = SequenceStrategy()
        result = strategy.generate(_col(), {"start": 100}, _ctx(5))
        assert list(result) == [100, 101, 102, 103, 104]

    def test_length(self):
        strategy = SequenceStrategy()
        result = strategy.generate(_col(), {"start": 1}, _ctx(1000))
        assert len(result) == 1000

    def test_unique_values(self):
        strategy = SequenceStrategy()
        result = strategy.generate(_col(), {"start": 1}, _ctx(500))
        assert len(set(result)) == 500


# ---------------------------------------------------------------------------
# WeightedEnumStrategy
# ---------------------------------------------------------------------------

class TestWeightedEnumStrategy:
    def test_values_in_set(self):
        strategy = WeightedEnumStrategy()
        config = {"values": {"A": 0.5, "B": 0.3, "C": 0.2}}
        result = strategy.generate(_col("x", "string"), config, _ctx(1000))
        assert set(result).issubset({"A", "B", "C"})

    def test_weights_approximately_correct(self):
        strategy = WeightedEnumStrategy()
        config = {"values": {"high": 0.8, "low": 0.2}}
        result = strategy.generate(_col("x", "string"), config, _ctx(10000))
        high_rate = (result == "high").sum() / len(result)
        assert 0.75 < high_rate < 0.85, f"Expected ~0.80, got {high_rate:.3f}"

    def test_single_value(self):
        strategy = WeightedEnumStrategy()
        config = {"values": {"only": 1.0}}
        result = strategy.generate(_col("x", "string"), config, _ctx(50))
        assert all(v == "only" for v in result)


# ---------------------------------------------------------------------------
# DistributionStrategy
# ---------------------------------------------------------------------------

class TestDistributionStrategy:
    def test_uniform_bounds(self):
        strategy = DistributionStrategy()
        config = {"distribution": "uniform", "params": {"min": 10.0, "max": 20.0}}
        result = strategy.generate(_col("x", "decimal"), config, _ctx(1000))
        assert result.min() >= 10.0
        assert result.max() <= 20.0

    def test_log_normal_positive(self):
        strategy = DistributionStrategy()
        config = {"distribution": "log_normal", "params": {"mean": 3.0, "sigma": 1.0, "min": 0.99, "max": 999.99}}
        result = strategy.generate(_col("x", "decimal"), config, _ctx(500))
        assert (result > 0).all()
        assert result.min() >= 0.99
        assert result.max() <= 999.99

    def test_normal_mean(self):
        strategy = DistributionStrategy()
        config = {"distribution": "normal", "params": {"mean": 50.0, "std": 5.0}}
        result = strategy.generate(_col("x", "decimal"), config, _ctx(10000))
        assert abs(result.mean() - 50.0) < 1.0

    def test_geometric_bounds(self):
        strategy = DistributionStrategy()
        config = {"distribution": "geometric", "params": {"p": 0.5, "min": 1, "max": 10}}
        result = strategy.generate(_col("x", "integer"), config, _ctx(500))
        assert result.min() >= 1
        assert result.max() <= 10


# ---------------------------------------------------------------------------
# ForeignKeyStrategy + IDManager (Pareto fix)
# ---------------------------------------------------------------------------

class TestForeignKeyPareto:
    def _make_ctx_with_pool(self, pool_size: int, row_count: int) -> tuple[GenerationContext, IDManager]:
        import pandas as pd
        rng = np.random.default_rng(42)
        id_manager = IDManager(rng)
        df = pd.DataFrame({"customer_id": np.arange(1, pool_size + 1)})
        id_manager.register_table("customer", df, ["customer_id"])
        ctx = GenerationContext(rng=rng, id_manager=id_manager, model_config={}, row_count=row_count)
        return ctx, id_manager

    def test_pareto_max_does_not_dominate(self):
        """No single parent should receive a degenerate share of FK references.

        The pre-fix bug caused nearly all rows to map to index 0 when raw.max()
        was very large (dividing by it collapsed everything to near 0). After the
        fix, even Pareto alpha=1.2 (very heavy-tailed) should not assign more than
        25% of all rows to a single parent.
        """
        ctx, id_manager = self._make_ctx_with_pool(pool_size=1000, row_count=5000)
        result = id_manager.get_random_fks("customer", 5000, "pareto", {"alpha": 1.2})
        import pandas as pd
        counts = pd.Series(result).value_counts()
        # Top parent should not get more than 25% (1250) of 5000 total rows
        assert counts.max() < 1250, f"Pareto too concentrated: top parent got {counts.max()} / 5000"
        # And at least 10 unique parents should be referenced (pre-fix: could be 1-2)
        assert counts.shape[0] >= 10, f"Too few unique parents: {counts.shape[0]}"

    def test_pareto_max_per_parent_enforced(self):
        """max_per_parent must be strictly respected after enforcement."""
        ctx, id_manager = self._make_ctx_with_pool(pool_size=1000, row_count=5000)
        result = id_manager.get_random_fks("customer", 5000, "pareto", {"alpha": 1.2, "max_per_parent": 20})
        import pandas as pd
        counts = pd.Series(result).value_counts()
        assert counts.max() <= 20, f"max_per_parent=20 violated: max={counts.max()}"

    def test_pareto_covers_pool(self):
        """Pareto should reference multiple parents, not just 1-2 (the pre-fix degenerate case).

        With alpha=1.2 and 1000 rows / 100 pool, the distribution is heavy-tailed but
        should still reference a meaningful subset of the pool. The pre-fix bug could
        produce as few as 1-2 unique parents regardless of pool size.
        """
        ctx, id_manager = self._make_ctx_with_pool(pool_size=100, row_count=1000)
        result = id_manager.get_random_fks("customer", 1000, "pareto", {"alpha": 1.2})
        import pandas as pd
        unique_parents = pd.Series(result).nunique()
        # Heavy Pareto alpha=1.2 will concentrate, but should reference at least 15% of pool
        assert unique_parents >= 15, f"Too few unique parents: {unique_parents} (pre-fix bug: could be 1-2)"

    def test_uniform_fk_coverage(self):
        """Uniform distribution should spread evenly across the pool."""
        ctx, id_manager = self._make_ctx_with_pool(pool_size=100, row_count=10000)
        result = id_manager.get_random_fks("customer", 10000, "uniform")
        import pandas as pd
        counts = pd.Series(result).value_counts()
        # Each parent should get roughly 100 ± 50 references
        assert counts.min() >= 30
        assert counts.max() <= 200

    def test_fk_integrity(self):
        """All generated FK values must exist in the parent PK pool."""
        import pandas as pd
        rng = np.random.default_rng(42)
        id_manager = IDManager(rng)
        parent_ids = np.arange(1, 501)
        df = pd.DataFrame({"product_id": parent_ids})
        id_manager.register_table("product", df, ["product_id"])
        result = id_manager.get_random_fks("product", 2000, "zipf", {"alpha": 1.5})
        orphans = set(result) - set(parent_ids)
        assert len(orphans) == 0, f"Orphan FK values found: {orphans}"


# ---------------------------------------------------------------------------
# FormulaStrategy
# ---------------------------------------------------------------------------

class TestFormulaStrategy:
    def test_multiplication(self):
        import pandas as pd
        strategy = FormulaStrategy()
        ctx = _ctx(5)
        ctx.current_table["qty"] = np.array([1, 2, 3, 4, 5])
        ctx.current_table["price"] = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
        result = strategy.generate(_col("total", "decimal"), {"expression": "qty * price"}, ctx)
        expected = np.array([10.0, 40.0, 90.0, 160.0, 250.0])
        np.testing.assert_array_almost_equal(result, expected)


# ---------------------------------------------------------------------------
# UUIDStrategy
# ---------------------------------------------------------------------------

class TestUUIDStrategy:
    def test_produces_unique_values(self):
        strategy = UUIDStrategy()
        result = strategy.generate(_col("id", "string"), {}, _ctx(500))
        assert len(set(result)) == 500

    def test_uuid_format(self):
        import re
        strategy = UUIDStrategy()
        result = strategy.generate(_col("id", "string"), {}, _ctx(10))
        uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
        for val in result:
            assert uuid_re.match(val), f"Not a valid UUID v4: {val}"

    def test_length(self):
        strategy = UUIDStrategy()
        result = strategy.generate(_col("id", "string"), {}, _ctx(100))
        assert len(result) == 100


# ---------------------------------------------------------------------------
# LifecycleStrategy
# ---------------------------------------------------------------------------

class TestLifecycleStrategy:
    def test_values_in_phases(self):
        strategy = LifecycleStrategy()
        config = {"phases": {"active": 0.7, "discontinued": 0.2, "seasonal": 0.1}}
        result = strategy.generate(_col("status", "string"), config, _ctx(1000))
        assert set(result).issubset({"active", "discontinued", "seasonal"})

    def test_weights_approximately_respected(self):
        strategy = LifecycleStrategy()
        config = {"phases": {"active": 0.7, "discontinued": 0.3}}
        result = strategy.generate(_col("status", "string"), config, _ctx(10000))
        active_rate = (result == "active").sum() / len(result)
        assert 0.65 < active_rate < 0.75, f"Expected ~0.70, got {active_rate:.3f}"

    def test_single_phase(self):
        strategy = LifecycleStrategy()
        config = {"phases": {"active": 1.0}}
        result = strategy.generate(_col("status", "string"), config, _ctx(50))
        assert all(v == "active" for v in result)


# ---------------------------------------------------------------------------
# CorrelatedStrategy
# ---------------------------------------------------------------------------

class TestCorrelatedStrategy:
    def test_multiply_within_bounds(self):
        strategy = CorrelatedStrategy()
        ctx = _ctx(1000)
        ctx.current_table["unit_price"] = np.full(1000, 100.0)
        config = {"source_column": "unit_price", "rule": "multiply", "params": {"factor_min": 0.30, "factor_max": 0.70}}
        result = strategy.generate(_col("cost", "decimal"), config, ctx)
        assert (result >= 30.0).all(), "cost should be >= 30 (unit_price * 0.30)"
        assert (result <= 70.0).all(), "cost should be <= 70 (unit_price * 0.70)"

    def test_multiply_proportional(self):
        strategy = CorrelatedStrategy()
        ctx = _ctx(1000)
        prices = np.linspace(10.0, 100.0, 1000)
        ctx.current_table["unit_price"] = prices
        config = {"source_column": "unit_price", "rule": "multiply", "params": {"factor_min": 0.5, "factor_max": 0.5}}
        result = strategy.generate(_col("cost", "decimal"), config, ctx)
        # Strategy rounds to 2 decimal places; allow for that rounding error
        np.testing.assert_array_almost_equal(result, prices * 0.5, decimal=1)

    def test_multiply_result_dtype_float(self):
        strategy = CorrelatedStrategy()
        ctx = _ctx(100)
        ctx.current_table["price"] = np.full(100, 50.0)
        config = {"source_column": "price", "rule": "multiply", "params": {"factor_min": 0.4, "factor_max": 0.6}}
        result = strategy.generate(_col("cost", "decimal"), config, ctx)
        assert result.dtype.kind == "f"


# ---------------------------------------------------------------------------
# DerivedStrategy (same-table add_days)
# ---------------------------------------------------------------------------

class TestDerivedStrategy:
    def test_add_days_same_table(self):
        import pandas as pd
        strategy = DerivedStrategy()
        ctx = _ctx(5)
        base_dates = pd.to_datetime(["2023-01-01"] * 5)
        ctx.current_table["start_date"] = base_dates
        config = {
            "source": "start_date",
            "rule": "add_days",
            "distribution": "uniform",
            "params": {"min": 1, "max": 30},
        }
        result = strategy.generate(_col("end_date", "date"), config, ctx)
        result_dates = pd.to_datetime(result)
        for base, derived in zip(base_dates, result_dates):
            delta = (derived - base).days
            assert 1 <= delta <= 30, f"Expected 1-30 days offset, got {delta}"

    def test_copy_same_table(self):
        import pandas as pd
        strategy = DerivedStrategy()
        ctx = _ctx(5)
        vals = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
        ctx.current_table["base_col"] = vals
        config = {"source": "base_col", "rule": "copy"}
        result = strategy.generate(_col("copy_col", "decimal"), config, ctx)
        np.testing.assert_array_equal(result, vals)


# ---------------------------------------------------------------------------
# ConditionalStrategy
# ---------------------------------------------------------------------------

class TestConditionalStrategy:
    def test_is_not_null_branch(self):
        strategy = ConditionalStrategy()
        ctx = _ctx(6)
        ctx.current_table["promo_id"] = np.array([1, None, 2, None, 3, None], dtype=object)
        config = {
            "condition": "promo_id IS NOT NULL",
            "true_generator": {"fixed": 10.0},
            "false_generator": {"fixed": 0.0},
        }
        result = strategy.generate(_col("discount", "decimal"), config, ctx)
        expected = np.array([10.0, 0.0, 10.0, 0.0, 10.0, 0.0])
        np.testing.assert_array_almost_equal(result, expected)

    def test_equals_branch(self):
        strategy = ConditionalStrategy()
        ctx = _ctx(4)
        ctx.current_table["status"] = np.array(["active", "inactive", "active", "inactive"], dtype=object)
        config = {
            "condition": "status == active",
            "true_generator": {"fixed": 1.0},
            "false_generator": {"fixed": 0.0},
        }
        result = strategy.generate(_col("flag", "integer"), config, ctx)
        expected = np.array([1.0, 0.0, 1.0, 0.0])
        np.testing.assert_array_almost_equal(result, expected)


# ---------------------------------------------------------------------------
# SelfReferencingStrategy
# ---------------------------------------------------------------------------

class TestSelfReferencingStrategy:
    def test_root_rows_have_null_parent(self):
        strategy = SelfReferencingStrategy()
        ctx = _ctx(50)
        ctx.current_table_name = "category"
        ctx.current_table["category_id"] = np.arange(1, 51)
        config = {"pk_column": "category_id", "root_count": 8}
        result = strategy.generate(_col("parent_category_id", "integer"), config, ctx)
        # First 8 rows should have None/NaN parent
        assert all(result[:8] is None or result[i] is None for i in range(8))

    def test_non_root_rows_have_valid_parent(self):
        strategy = SelfReferencingStrategy()
        ctx = _ctx(50)
        ctx.current_table_name = "category"
        pk_vals = np.arange(1, 51)
        ctx.current_table["category_id"] = pk_vals
        config = {"pk_column": "category_id", "root_count": 8}
        result = strategy.generate(_col("parent_id", "integer"), config, ctx)
        # Non-root rows (index 8+) must have a parent that exists in the PK pool
        pk_set = set(pk_vals)
        for i in range(8, 50):
            assert result[i] is not None, f"Row {i} should have a parent"
            assert result[i] in pk_set, f"Row {i} parent {result[i]} not in PK pool"

    def test_level_stashed_in_ctx(self):
        strategy = SelfReferencingStrategy()
        ctx = _ctx(50)
        ctx.current_table_name = "category"
        ctx.current_table["category_id"] = np.arange(1, 51)
        config = {"pk_column": "category_id", "root_count": 8}
        strategy.generate(_col("parent_id", "integer"), config, ctx)
        cache_key = "_sr_category_level"
        assert cache_key in ctx.current_table
        levels = ctx.current_table[cache_key]
        assert set(levels).issubset({1, 2, 3})


# ---------------------------------------------------------------------------
# WeightedEnumStrategy — numeric float keys
# ---------------------------------------------------------------------------

class TestWeightedEnumNumericKeys:
    def test_numeric_keys_return_floats(self):
        strategy = WeightedEnumStrategy()
        config = {"values": {"0.0": 0.70, "5.0": 0.10, "10.0": 0.10, "15.0": 0.05, "20.0": 0.05}}
        result = strategy.generate(_col("discount", "decimal"), config, _ctx(1000))
        assert result.dtype.kind == "f", f"Expected float array, got {result.dtype}"
        assert set(result).issubset({0.0, 5.0, 10.0, 15.0, 20.0})

    def test_numeric_zero_most_common(self):
        strategy = WeightedEnumStrategy()
        config = {"values": {"0.0": 0.70, "10.0": 0.30}}
        result = strategy.generate(_col("discount", "decimal"), config, _ctx(10000))
        zero_rate = (result == 0.0).sum() / len(result)
        assert 0.65 < zero_rate < 0.75, f"Expected ~0.70 zeros, got {zero_rate:.3f}"
