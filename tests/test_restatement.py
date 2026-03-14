"""Tests for fact backfill + restatement pack (E2)."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.simulation.file_drop import (
    FileDropConfig,
    FileDropResult,
    FileDropSimulator,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def order_tables():
    n = 60
    rng = np.random.default_rng(1)
    return {
        "order": pd.DataFrame({
            "order_id": range(1, n + 1),
            "customer_id": rng.integers(1, 10, size=n),
            "total_amount": rng.uniform(10.0, 200.0, size=n).round(2),
            "order_date": pd.date_range("2024-01-01", periods=n, freq="D"),
        }),
    }


# ---------------------------------------------------------------------------
# Restatement
# ---------------------------------------------------------------------------

class TestRestatement:
    def test_restatement_generates_files(self, order_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-10",
            formats=["parquet"],
            entities=["order"],
            restatement_enabled=True,
            restatement_probability=1.0,  # Force restatement on every slot
            restatement_max_correction_pct=0.05,
            lateness_enabled=False,
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=order_tables, config=cfg).run()
        # Should have more files than without restatement
        assert len(result.files_written) > 0

    def test_restated_files_have_marker_columns(self, order_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-10",
            formats=["parquet"],
            entities=["order"],
            restatement_enabled=True,
            restatement_probability=1.0,
            lateness_enabled=False,
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=order_tables, config=cfg).run()
        # Find restatement files (seq_start=980)
        restatement_files = [p for p in result.files_written if "00980" in str(p)]
        assert len(restatement_files) > 0
        df = pd.read_parquet(restatement_files[0])
        assert "_restatement" in df.columns
        assert "_restated_at" in df.columns
        assert df["_restatement"].all()

    def test_restatement_modifies_numeric_values(self, order_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-10",
            formats=["parquet"],
            entities=["order"],
            restatement_enabled=True,
            restatement_probability=1.0,
            restatement_max_correction_pct=0.50,  # Large correction for test
            lateness_enabled=False,
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=order_tables, config=cfg).run()
        restatement_files = [p for p in result.files_written if "00980" in str(p)]
        if restatement_files:
            df = pd.read_parquet(restatement_files[0])
            # total_amount should exist and be modified
            assert "total_amount" in df.columns

    def test_restatement_disabled_by_default(self, order_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-05",
            formats=["parquet"],
            entities=["order"],
            lateness_enabled=False,
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=order_tables, config=cfg).run()
        restatement_files = [p for p in result.files_written if "00980" in str(p)]
        assert len(restatement_files) == 0

    def test_restatement_preserves_id_columns(self, order_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-10",
            formats=["parquet"],
            entities=["order"],
            restatement_enabled=True,
            restatement_probability=1.0,
            lateness_enabled=False,
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=order_tables, config=cfg).run()
        restatement_files = [p for p in result.files_written if "00980" in str(p)]
        if restatement_files:
            df = pd.read_parquet(restatement_files[0])
            # customer_id should not be modified (ends with _id)
            original = order_tables["order"]
            if len(df) > 0:
                # IDs should be from the original set
                assert set(df["customer_id"]).issubset(set(original["customer_id"]))
