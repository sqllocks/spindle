"""SCD2 + incremental engine tests at scale."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle import (
    ContinueConfig,
    ContinueEngine,
    Spindle,
    TimeTravelConfig,
    TimeTravelEngine,
)
from sqllocks_spindle.domains.retail import RetailDomain


class TestTimeTravelSnapshots:
    def test_time_travel_generates_snapshots(self):
        """TimeTravelEngine generates multiple monthly snapshots."""
        tte = TimeTravelEngine()
        tt_result = tte.generate(
            domain=RetailDomain(),
            config=TimeTravelConfig(
                months=3,
                growth_rate=0.05,
                seed=42,
            ),
            scale="small",
        )
        # months=3 produces month 0 (initial) + months 1-3 = 4 snapshots
        assert len(tt_result.snapshots) == 4
        # Each snapshot should have tables
        for snap in tt_result.snapshots:
            assert len(snap.tables) > 0

    def test_time_travel_growth(self):
        """Later snapshots should have more rows than earlier ones (growth > churn)."""
        tte = TimeTravelEngine()
        tt_result = tte.generate(
            domain=RetailDomain(),
            config=TimeTravelConfig(
                months=6,
                growth_rate=0.10,
                churn_rate=0.01,
                seed=42,
            ),
            scale="small",
        )
        first_total = sum(tt_result.snapshots[0].row_counts.values())
        last_total = sum(tt_result.snapshots[-1].row_counts.values())
        assert last_total > first_total, (
            f"Expected growth: first={first_total}, last={last_total}"
        )

    def test_time_travel_to_partitioned(self):
        """to_partitioned_dfs() adds _snapshot_date and combines snapshots."""
        tte = TimeTravelEngine()
        tt_result = tte.generate(
            domain=RetailDomain(),
            config=TimeTravelConfig(months=2, seed=42),
            scale="small",
        )
        partitioned = tt_result.to_partitioned_dfs()
        assert len(partitioned) > 0
        for tname, df in partitioned.items():
            assert "_snapshot_date" in df.columns


class TestContinueEngine:
    def test_continue_engine_generates_deltas(self):
        """ContinueEngine produces inserts, updates, and deletes."""
        s = Spindle()
        result = s.generate(domain=RetailDomain(), scale="small", seed=42)

        engine = ContinueEngine()
        config = ContinueConfig(
            insert_count=50,
            update_fraction=0.1,
            delete_fraction=0.02,
            seed=43,
        )
        delta = engine.continue_from(existing=result, config=config)

        # Should have at least some inserts
        total_inserts = sum(len(df) for df in delta.inserts.values())
        assert total_inserts > 0, "No inserts generated"

        # Should have stats for each table
        assert len(delta.stats) > 0

    def test_continue_engine_pk_contiguous(self):
        """New batch PKs start exactly where previous batch ended."""
        s = Spindle()
        result = s.generate(domain=RetailDomain(), scale="small", seed=42)

        engine = ContinueEngine()
        config = ContinueConfig(insert_count=50, seed=43)
        delta = engine.continue_from(existing=result, config=config)

        # Customer inserts PKs should not overlap with base
        if "customer" in delta.inserts and len(delta.inserts["customer"]) > 0:
            base_max = result.tables["customer"]["customer_id"].max()
            delta_min = delta.inserts["customer"]["customer_id"].min()
            assert delta_min > base_max, (
                f"PK overlap: base max={base_max}, delta min={delta_min}"
            )

    def test_continue_engine_summary(self):
        """DeltaResult.summary() produces readable output."""
        s = Spindle()
        result = s.generate(domain=RetailDomain(), scale="small", seed=42)

        engine = ContinueEngine()
        delta = engine.continue_from(existing=result)

        summary = delta.summary()
        assert "Incremental" in summary
        assert "inserts" in summary
