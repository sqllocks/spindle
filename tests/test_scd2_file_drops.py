"""Tests for SCD Type 2 file drops (E1)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.simulation.scd2_file_drops import (
    SCD2FileDropConfig,
    SCD2FileDropResult,
    SCD2FileDropSimulator,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def customer_table():
    n = 50
    rng = np.random.default_rng(1)
    return {
        "customer": pd.DataFrame({
            "customer_id": range(1, n + 1),
            "name": [f"Customer {i}" for i in range(1, n + 1)],
            "status": rng.choice(["active", "inactive", "pending"], size=n),
            "tier": rng.choice(["gold", "silver", "bronze"], size=n),
            "balance": rng.uniform(100, 10000, size=n).round(2),
        }),
    }


@pytest.fixture
def config(tmp_path):
    return SCD2FileDropConfig(
        domain="retail",
        base_path=str(tmp_path / "landing"),
        business_key_column="customer_id",
        scd2_columns=["status", "tier"],
        initial_load_date="2024-01-01",
        num_delta_days=5,
        daily_change_rate=0.20,
        daily_new_rate=0.10,
        formats=["parquet"],
        manifest_enabled=True,
        seed=42,
    )


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestSCD2FileDropConfig:
    def test_defaults(self):
        cfg = SCD2FileDropConfig()
        assert cfg.domain == "default"
        assert cfg.business_key_column == "id"
        assert cfg.num_delta_days == 30
        assert cfg.seed == 42


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class TestSCD2FileDropSimulator:
    def test_run_returns_result(self, customer_table, config):
        sim = SCD2FileDropSimulator(tables=customer_table, config=config)
        result = sim.run()
        assert isinstance(result, SCD2FileDropResult)

    def test_initial_load_written(self, customer_table, config):
        result = SCD2FileDropSimulator(tables=customer_table, config=config).run()
        assert result.initial_load_path is not None
        assert result.initial_load_path.exists()

    def test_delta_files_written(self, customer_table, config):
        result = SCD2FileDropSimulator(tables=customer_table, config=config).run()
        assert len(result.delta_paths) > 0
        for p in result.delta_paths:
            assert p.exists()

    def test_stats_populated(self, customer_table, config):
        result = SCD2FileDropSimulator(tables=customer_table, config=config).run()
        assert "initial_rows" in result.stats
        assert "total_deltas" in result.stats
        assert result.stats["initial_rows"] > 0
        assert result.stats["days_simulated"] == config.num_delta_days

    def test_initial_load_has_scd2_columns(self, customer_table, config):
        result = SCD2FileDropSimulator(tables=customer_table, config=config).run()
        df = pd.read_parquet(result.initial_load_path)
        assert config.effective_date_column in df.columns
        assert config.end_date_column in df.columns
        assert config.is_current_column in df.columns

    def test_initial_load_all_current(self, customer_table, config):
        result = SCD2FileDropSimulator(tables=customer_table, config=config).run()
        df = pd.read_parquet(result.initial_load_path)
        assert df[config.is_current_column].all()

    def test_delta_has_delta_type(self, customer_table, config):
        result = SCD2FileDropSimulator(tables=customer_table, config=config).run()
        if result.delta_paths:
            df = pd.read_parquet(result.delta_paths[0])
            assert "_delta_type" in df.columns

    def test_manifests_written(self, customer_table, config):
        result = SCD2FileDropSimulator(tables=customer_table, config=config).run()
        assert len(result.manifest_paths) > 0


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_seed_same_output(self, customer_table, tmp_path):
        cfg1 = SCD2FileDropConfig(
            base_path=str(tmp_path / "run1"),
            business_key_column="customer_id",
            scd2_columns=["status"],
            num_delta_days=3,
            seed=42,
        )
        cfg2 = SCD2FileDropConfig(
            base_path=str(tmp_path / "run2"),
            business_key_column="customer_id",
            scd2_columns=["status"],
            num_delta_days=3,
            seed=42,
        )
        r1 = SCD2FileDropSimulator(tables=customer_table, config=cfg1).run()
        r2 = SCD2FileDropSimulator(tables=customer_table, config=cfg2).run()
        assert r1.stats == r2.stats
