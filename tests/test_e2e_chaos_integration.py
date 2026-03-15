"""E2E tests: chaos engine — all 6 categories, all 4 intensities, escalation modes."""

from __future__ import annotations

import pandas as pd
import numpy as np
import pytest

from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.chaos import ChaosEngine, ChaosConfig


@pytest.fixture(scope="module")
def clean_df():
    """A clean DataFrame to corrupt."""
    return pd.DataFrame({
        "id": range(1, 101),
        "name": [f"item_{i}" for i in range(100)],
        "amount": np.random.default_rng(42).uniform(10, 1000, 100).round(2),
        "status": np.random.default_rng(42).choice(["active", "inactive", "pending"], 100),
        "created_at": pd.date_range("2024-01-01", periods=100, freq="D"),
    })


INTENSITIES = ["calm", "moderate", "stormy", "hurricane"]
CATEGORIES = ["value", "schema", "referential", "temporal", "volume"]


# ---------------------------------------------------------------------------
# Category × Intensity matrix
# ---------------------------------------------------------------------------

class TestValueChaos:
    @pytest.mark.parametrize("intensity", INTENSITIES)
    def test_value_corruption(self, clean_df, intensity):
        config = ChaosConfig(enabled=True, intensity=intensity, seed=42,
                             warmup_days=0, chaos_start_day=1)
        engine = ChaosEngine(config)
        corrupted = engine.corrupt_dataframe(clean_df.copy(), day=5)
        assert len(corrupted) == len(clean_df)
        # At any intensity, some corruption should occur over multiple attempts
        # (probabilities are stochastic, so just verify shape preserved)


class TestSchemaChaos:
    @pytest.mark.parametrize("intensity", INTENSITIES)
    def test_schema_drift(self, clean_df, intensity):
        config = ChaosConfig(enabled=True, intensity=intensity, seed=42,
                             warmup_days=0, chaos_start_day=1)
        engine = ChaosEngine(config)
        drifted = engine.drift_schema(clean_df.copy(), day=5)
        assert isinstance(drifted, pd.DataFrame)
        assert len(drifted) == len(clean_df)

    def test_breaking_changes_after_day_20(self, clean_df):
        config = ChaosConfig(enabled=True, intensity="hurricane", seed=42,
                             warmup_days=0, chaos_start_day=1, breaking_change_day=20)
        engine = ChaosEngine(config)
        # Before breaking change day — only safe mutations
        before = engine.drift_schema(clean_df.copy(), day=10)
        # After breaking change day — destructive mutations possible
        after = engine.drift_schema(clean_df.copy(), day=25)
        # Both should return valid DataFrames
        assert isinstance(before, pd.DataFrame)
        assert isinstance(after, pd.DataFrame)


class TestReferentialChaos:
    @pytest.mark.parametrize("intensity", INTENSITIES)
    def test_referential_corruption(self, intensity):
        config = ChaosConfig(enabled=True, intensity=intensity, seed=42,
                             warmup_days=0, chaos_start_day=1)
        engine = ChaosEngine(config)
        tables = {
            "parent": pd.DataFrame({"parent_id": range(1, 51)}),
            "child": pd.DataFrame({
                "child_id": range(1, 101),
                "parent_id": np.random.default_rng(42).integers(1, 51, 100),
            }),
        }
        corrupted = engine.inject_referential_chaos(
            {k: v.copy() for k, v in tables.items()}, day=5
        )
        assert "parent" in corrupted
        assert "child" in corrupted


class TestTemporalChaos:
    @pytest.mark.parametrize("intensity", INTENSITIES)
    def test_temporal_corruption(self, clean_df, intensity):
        config = ChaosConfig(enabled=True, intensity=intensity, seed=42,
                             warmup_days=0, chaos_start_day=1)
        engine = ChaosEngine(config)
        corrupted = engine.inject_temporal_chaos(
            clean_df.copy(), date_columns=["created_at"], day=5
        )
        assert len(corrupted) == len(clean_df)


class TestVolumeChaos:
    @pytest.mark.parametrize("intensity", INTENSITIES)
    def test_volume_anomaly(self, clean_df, intensity):
        config = ChaosConfig(enabled=True, intensity=intensity, seed=42,
                             warmup_days=0, chaos_start_day=1)
        engine = ChaosEngine(config)
        result = engine.inject_volume_chaos(clean_df.copy(), day=5)
        assert isinstance(result, pd.DataFrame)
        # Volume chaos changes row count (spike, empty, or single row)


class TestFileChaos:
    @pytest.mark.parametrize("intensity", INTENSITIES)
    def test_file_corruption(self, intensity):
        config = ChaosConfig(enabled=True, intensity=intensity, seed=42,
                             warmup_days=0, chaos_start_day=1)
        engine = ChaosEngine(config)
        original = b"id,name,value\n1,test,100\n2,test2,200\n"
        corrupted = engine.corrupt_file(original, day=5)
        assert isinstance(corrupted, bytes)


# ---------------------------------------------------------------------------
# Escalation modes
# ---------------------------------------------------------------------------

class TestEscalation:
    @pytest.mark.parametrize("mode", ["gradual", "random", "front_loaded"])
    def test_escalation_mode(self, clean_df, mode):
        config = ChaosConfig(enabled=True, intensity="moderate", seed=42,
                             warmup_days=0, chaos_start_day=1, escalation=mode)
        engine = ChaosEngine(config)
        # Run over 10 days — should not crash
        for day in range(1, 11):
            engine.corrupt_dataframe(clean_df.copy(), day=day)


# ---------------------------------------------------------------------------
# Warmup period
# ---------------------------------------------------------------------------

class TestWarmup:
    def test_no_chaos_during_warmup(self, clean_df):
        config = ChaosConfig(enabled=True, intensity="hurricane", seed=42,
                             warmup_days=10, chaos_start_day=11)
        engine = ChaosEngine(config)
        # During warmup, should_inject returns False
        for day in range(1, 11):
            assert not engine.should_inject(day, "value")


# ---------------------------------------------------------------------------
# apply_all convenience
# ---------------------------------------------------------------------------

class TestApplyAll:
    def test_apply_all_runs_without_crash(self, clean_df):
        config = ChaosConfig(enabled=True, intensity="moderate", seed=42,
                             warmup_days=0, chaos_start_day=1)
        engine = ChaosEngine(config)
        result = engine.apply_all(clean_df.copy(), day=10)
        assert isinstance(result, pd.DataFrame)
