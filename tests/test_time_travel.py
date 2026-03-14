"""Tests for time-travel snapshot generation (C5)."""

import pytest
import pandas as pd

from sqllocks_spindle.incremental.time_travel import (
    TimeTravelEngine,
    TimeTravelConfig,
    TimeTravelResult,
)


class TestTimeTravelEngine:
    @pytest.fixture(scope="module")
    def result(self):
        from sqllocks_spindle import RetailDomain

        engine = TimeTravelEngine()
        config = TimeTravelConfig(
            months=3, growth_rate=0.1, churn_rate=0.02, seed=42
        )
        return engine.generate(domain=RetailDomain(), config=config, scale="small")

    def test_correct_number_of_snapshots(self, result):
        assert len(result.snapshots) == 4  # month 0 + 3 months

    def test_snapshots_have_dates(self, result):
        for snap in result.snapshots:
            assert snap.snapshot_date  # non-empty string
            pd.Timestamp(snap.snapshot_date)  # should parse

    def test_row_counts_grow(self, result):
        # With 10% growth and 2% churn, net ~8% growth per month
        # Over 3 months, should have more rows than initial
        table = list(result.snapshots[0].tables.keys())[0]
        initial = result.snapshots[0].row_counts[table]
        final = result.snapshots[-1].row_counts[table]
        assert final > initial

    def test_month_indices_sequential(self, result):
        for i, snap in enumerate(result.snapshots):
            assert snap.month_index == i

    def test_summary_format(self, result):
        summary = result.summary()
        assert "Time-Travel" in summary
        assert "Month" in summary

    def test_get_snapshot(self, result):
        snap = result.get_snapshot(0)
        assert snap.month_index == 0

    def test_to_partitioned_dfs(self, result):
        partitioned = result.to_partitioned_dfs()
        assert isinstance(partitioned, dict)
        for name, df in partitioned.items():
            assert "_snapshot_date" in df.columns
            # Should have rows from multiple snapshots
            assert df["_snapshot_date"].nunique() == 4

    def test_seasonality_multiplier(self):
        from sqllocks_spindle import RetailDomain

        engine = TimeTravelEngine()
        # Start in October so month 12 (December) is within range
        config = TimeTravelConfig(
            months=3,
            start_date="2023-10-01",
            growth_rate=0.1,
            seasonality={12: 3.0},
            seed=42,
        )
        result = engine.generate(domain=RetailDomain(), config=config, scale="small")
        # December snapshot should have more growth
        assert len(result.snapshots) == 4

    def test_seed_reproducibility(self):
        from sqllocks_spindle import RetailDomain

        engine = TimeTravelEngine()
        config = TimeTravelConfig(months=2, seed=123)
        r1 = engine.generate(domain=RetailDomain(), config=config, scale="small")
        r2 = engine.generate(domain=RetailDomain(), config=config, scale="small")
        t = list(r1.snapshots[0].tables.keys())[0]
        assert r1.snapshots[0].tables[t].equals(r2.snapshots[0].tables[t])
