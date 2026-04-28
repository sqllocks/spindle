"""Tests for EmpiricalStrategy."""

from __future__ import annotations

import numpy as np
import pytest

from sqllocks_spindle.engine.strategies.empirical import EmpiricalStrategy
from sqllocks_spindle.engine.strategies.base import GenerationContext
from sqllocks_spindle.engine.id_manager import IDManager
from sqllocks_spindle.schema.parser import ColumnDef


def _make_ctx(n: int = 100) -> GenerationContext:
    rng = np.random.default_rng(42)
    id_mgr = IDManager(rng)
    return GenerationContext(rng=rng, id_manager=id_mgr, model_config={}, row_count=n)


def _make_col(name: str = "val") -> ColumnDef:
    return ColumnDef(name=name, type="decimal", generator={}, nullable=False, null_rate=0.0)


# Quantile fingerprint for a normal(50, 10) distribution
NORMAL_QUANTILES = {
    "p1": 26.7, "p5": 33.6, "p10": 37.2, "p25": 43.3,
    "p50": 50.0, "p75": 56.7, "p90": 62.8, "p95": 66.4, "p99": 73.3,
}


class TestEmpiricalStrategy:
    def test_generates_correct_count(self):
        strategy = EmpiricalStrategy()
        col = _make_col()
        ctx = _make_ctx(n=200)
        config = {"strategy": "empirical", "quantiles": NORMAL_QUANTILES}
        result = strategy.generate(col, config, ctx)
        assert len(result) == 200

    def test_values_within_observed_range(self):
        strategy = EmpiricalStrategy()
        col = _make_col()
        ctx = _make_ctx(n=1000)
        config = {"strategy": "empirical", "quantiles": NORMAL_QUANTILES}
        result = strategy.generate(col, config, ctx)
        # np.interp clamps to boundary values; ±5 allows for floating-point noise at extremes
        assert float(result.min()) >= NORMAL_QUANTILES["p1"] - 5
        assert float(result.max()) <= NORMAL_QUANTILES["p99"] + 5

    def test_median_is_approximate(self):
        strategy = EmpiricalStrategy()
        col = _make_col()
        ctx = _make_ctx(n=2000)
        config = {"strategy": "empirical", "quantiles": NORMAL_QUANTILES}
        result = strategy.generate(col, config, ctx)
        assert abs(float(np.median(result)) - 50.0) < 3.0

    def test_missing_quantiles_raises(self):
        strategy = EmpiricalStrategy()
        col = _make_col()
        ctx = _make_ctx()
        with pytest.raises(ValueError):
            strategy.generate(col, {}, ctx)

    def test_registered_in_spindle(self):
        from sqllocks_spindle import Spindle
        s = Spindle()
        assert s._registry.has("empirical")
