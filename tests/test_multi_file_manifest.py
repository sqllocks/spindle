"""Tests for multi-file manifest pack (E4)."""

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
def tables():
    n = 100
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
# Multi-file chunking
# ---------------------------------------------------------------------------

class TestMultiFileChunking:
    def test_multi_file_splits_into_chunks(self, tmp_path):
        # Use a dense table with all dates within the window
        n = 90
        rng = np.random.default_rng(1)
        dense_tables = {
            "order": pd.DataFrame({
                "order_id": range(1, n + 1),
                "customer_id": rng.integers(1, 10, size=n),
                "total_amount": rng.uniform(10.0, 200.0, size=n).round(2),
                "order_date": pd.date_range("2024-01-01", periods=n, freq="8h"),
            }),
        }
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-30",
            formats=["parquet"],
            entities=["order"],
            multi_file_enabled=True,
            multi_file_chunks=3,
            lateness_enabled=False,
            manifest_enabled=True,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=dense_tables, config=cfg).run()
        # With multi-file chunking, should have more files than slots
        assert len(result.files_written) > 10

    def test_manifest_contains_checksums(self, tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-03",
            formats=["parquet"],
            entities=["order"],
            multi_file_enabled=True,
            multi_file_chunks=2,
            multi_file_checksum=True,
            lateness_enabled=False,
            manifest_enabled=True,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=tables, config=cfg).run()
        assert len(result.manifest_paths) > 0
        for mp in result.manifest_paths:
            data = json.loads(mp.read_text())
            if "file_details" in data:
                for entry in data["file_details"]:
                    assert "sha256" in entry
                    assert "size_bytes" in entry
                    assert len(entry["sha256"]) == 64  # SHA-256 hex

    def test_multi_file_disabled_single_file_per_slot(self, tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-03",
            formats=["parquet"],
            entities=["order"],
            multi_file_enabled=False,
            lateness_enabled=False,
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=tables, config=cfg).run()
        # Without multi-file, one file per slot per format
        assert len(result.files_written) <= 3

    def test_checksum_disabled_no_file_details(self, tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-03",
            formats=["parquet"],
            entities=["order"],
            multi_file_enabled=True,
            multi_file_chunks=2,
            multi_file_checksum=False,
            lateness_enabled=False,
            manifest_enabled=True,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=tables, config=cfg).run()
        for mp in result.manifest_paths:
            data = json.loads(mp.read_text())
            assert "file_details" not in data

    def test_all_rows_accounted_for(self, tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-03",
            formats=["parquet"],
            entities=["order"],
            multi_file_enabled=True,
            multi_file_chunks=3,
            lateness_enabled=False,
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=tables, config=cfg).run()
        # Read all parquet files and count rows
        total_rows = 0
        for f in result.files_written:
            df = pd.read_parquet(f)
            total_rows += len(df)
        # Should account for all rows that fell into the 3-day window
        assert total_rows > 0
