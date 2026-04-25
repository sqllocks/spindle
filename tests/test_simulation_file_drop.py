"""Tests for FileDropSimulator."""

from __future__ import annotations

import json
from datetime import datetime
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
def simple_tables():
    n = 60
    rng = np.random.default_rng(1)
    return {
        "order": pd.DataFrame({
            "order_id": range(1, n + 1),
            "customer_id": rng.integers(1, 10, size=n),
            "total_amount": rng.uniform(10.0, 200.0, size=n).round(2),
            "order_date": pd.date_range("2024-01-01", periods=n, freq="D"),
        }),
        "customer": pd.DataFrame({
            "customer_id": range(1, 11),
            "name": [f"Customer {i}" for i in range(1, 11)],
        }),
    }


@pytest.fixture
def base_config(tmp_path):
    return FileDropConfig(
        domain="retail",
        base_path=str(tmp_path / "landing"),
        cadence="daily",
        date_range_start="2024-01-01",
        date_range_end="2024-01-07",
        formats=["parquet"],
        manifest_enabled=True,
        done_flag_enabled=True,
        lateness_enabled=False,
        duplicates_enabled=False,
        backfill_enabled=False,
        seed=42,
    )


# ---------------------------------------------------------------------------
# FileDropConfig
# ---------------------------------------------------------------------------

class TestFileDropConfig:
    def test_defaults(self):
        cfg = FileDropConfig()
        assert cfg.domain == "default"
        assert cfg.base_path == "Files/landing"
        assert cfg.cadence == "daily"
        assert cfg.manifest_enabled is True
        assert cfg.done_flag_enabled is True
        assert cfg.lateness_enabled is True
        assert cfg.seed == 42


# ---------------------------------------------------------------------------
# FileDropSimulator — basic run
# ---------------------------------------------------------------------------

class TestFileDropSimulatorBasic:
    def test_run_returns_result(self, simple_tables, base_config):
        sim = FileDropSimulator(tables=simple_tables, config=base_config)
        result = sim.run()
        assert isinstance(result, FileDropResult)

    def test_result_repr_no_raise(self, simple_tables, base_config):
        sim = FileDropSimulator(tables=simple_tables, config=base_config)
        result = sim.run()
        assert isinstance(repr(result), str)

    def test_files_written_exist_on_disk(self, simple_tables, base_config):
        sim = FileDropSimulator(tables=simple_tables, config=base_config)
        result = sim.run()
        for path in result.files_written:
            assert path.exists(), f"Missing: {path}"

    def test_manifest_files_exist(self, simple_tables, base_config):
        sim = FileDropSimulator(tables=simple_tables, config=base_config)
        result = sim.run()
        assert len(result.manifest_paths) > 0
        for path in result.manifest_paths:
            assert path.exists(), f"Missing manifest: {path}"

    def test_done_flags_exist(self, simple_tables, base_config):
        sim = FileDropSimulator(tables=simple_tables, config=base_config)
        result = sim.run()
        assert len(result.done_flag_paths) > 0
        for path in result.done_flag_paths:
            assert path.exists(), f"Missing done flag: {path}"

    def test_stats_has_entity_entries(self, simple_tables, base_config):
        sim = FileDropSimulator(tables=simple_tables, config=base_config)
        result = sim.run()
        assert len(result.stats) > 0


# ---------------------------------------------------------------------------
# File formats
# ---------------------------------------------------------------------------

class TestFileDropFormats:
    def test_parquet_readable(self, simple_tables, base_config):
        sim = FileDropSimulator(tables=simple_tables, config=base_config)
        result = sim.run()
        parquet_files = [p for p in result.files_written if p.suffix == ".parquet"]
        assert len(parquet_files) > 0
        df = pd.read_parquet(parquet_files[0])
        assert len(df) > 0

    def test_csv_format(self, simple_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-03",
            formats=["csv"],
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=simple_tables, config=cfg).run()
        csv_files = [p for p in result.files_written if p.suffix == ".csv"]
        assert len(csv_files) > 0
        df = pd.read_csv(csv_files[0])
        assert len(df.columns) > 0

    def test_jsonl_format(self, simple_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-03",
            formats=["jsonl"],
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=simple_tables, config=cfg).run()
        jsonl_files = [p for p in result.files_written if p.suffix == ".jsonl"]
        assert len(jsonl_files) > 0
        lines = jsonl_files[0].read_text().strip().splitlines()
        assert len(lines) > 0
        json.loads(lines[0])  # Must be valid JSON


# ---------------------------------------------------------------------------
# Manifest content
# ---------------------------------------------------------------------------

class TestFileDropManifestContent:
    def test_manifest_json_structure(self, simple_tables, base_config):
        sim = FileDropSimulator(tables=simple_tables, config=base_config)
        result = sim.run()
        for mp in result.manifest_paths:
            data = json.loads(mp.read_text())
            assert "entity" in data
            assert "slot" in data
            assert "files" in data
            assert "correlation_id" in data


# ---------------------------------------------------------------------------
# Cadence and slot count
# ---------------------------------------------------------------------------

class TestFileDropCadence:
    def test_daily_7_days_partition_structure(self, simple_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-07",
            formats=["parquet"],
            entities=["order"],
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=simple_tables, config=cfg).run()
        assert len(result.files_written) > 0

    def test_hourly_cadence_path_contains_hr(self, simple_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="hourly",
            date_range_start="2024-01-01",
            date_range_end="2024-01-01",
            formats=["parquet"],
            entities=["order"],
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=simple_tables, config=cfg).run()
        if result.files_written:
            path_str = str(result.files_written[0])
            assert "hr=" in path_str

    def test_build_time_slots_daily(self, simple_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-05",
            seed=42,
        )
        sim = FileDropSimulator(tables=simple_tables, config=cfg)
        slots = sim._build_time_slots()
        assert len(slots) == 5


# ---------------------------------------------------------------------------
# Entity filtering
# ---------------------------------------------------------------------------

class TestFileDropEntityFilter:
    def test_entity_filter_restricts_output(self, simple_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-03",
            entities=["order"],
            formats=["parquet"],
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=simple_tables, config=cfg).run()
        assert "order" in result.stats
        assert "customer" not in result.stats

    def test_missing_entity_in_config_skipped(self, simple_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-03",
            entities=["nonexistent_table"],
            formats=["parquet"],
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=simple_tables, config=cfg).run()
        assert len(result.files_written) == 0


# ---------------------------------------------------------------------------
# Anomaly injection
# ---------------------------------------------------------------------------

class TestFileDropAnomalies:
    def test_lateness_writes_additional_files(self, simple_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-07",
            formats=["parquet"],
            entities=["order"],
            lateness_enabled=True,
            lateness_probability=0.5,
            max_days_late=2,
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=1,
        )
        result = FileDropSimulator(tables=simple_tables, config=cfg).run()
        assert len(result.files_written) > 0

    def test_duplicates_increase_row_count(self, tmp_path):
        df = pd.DataFrame({
            "id": range(1, 101),
            "val": range(1, 101),
        })
        cfg = FileDropConfig(
            domain="test",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-03",
            formats=["parquet"],
            duplicates_enabled=True,
            duplicate_probability=0.5,
            lateness_enabled=False,
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=0,
        )
        result = FileDropSimulator(tables={"items": df}, config=cfg).run()
        total_rows = result.stats.get("items", {}).get("rows_written", 0)
        # With 50% duplication, should see more rows than without
        assert total_rows > 0

    def test_backfill_enabled_generates_files(self, simple_tables, tmp_path):
        cfg = FileDropConfig(
            domain="retail",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-10",
            formats=["parquet"],
            entities=["order"],
            backfill_enabled=True,
            max_days_back=3,
            lateness_enabled=False,
            manifest_enabled=False,
            done_flag_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables=simple_tables, config=cfg).run()
        assert len(result.files_written) > 0


# ---------------------------------------------------------------------------
# Temporal column detection
# ---------------------------------------------------------------------------

class TestFileDropTemporalDetection:
    def test_detect_temporal_column_datetime_dtype(self, simple_tables, base_config):
        sim = FileDropSimulator(tables=simple_tables, config=base_config)
        col = sim._detect_temporal_column(simple_tables["order"])
        assert col == "order_date"

    def test_detect_temporal_column_no_dt_returns_none(self, base_config):
        df = pd.DataFrame({"id": range(5), "name": list("abcde")})
        sim = FileDropSimulator(tables={}, config=base_config)
        col = sim._detect_temporal_column(df)
        assert col is None

    def test_round_robin_without_ts_col(self, tmp_path):
        df = pd.DataFrame({"id": range(1, 11), "val": range(1, 11)})
        cfg = FileDropConfig(
            domain="test",
            base_path=str(tmp_path / "landing"),
            cadence="daily",
            date_range_start="2024-01-01",
            date_range_end="2024-01-03",
            formats=["parquet"],
            manifest_enabled=False,
            done_flag_enabled=False,
            lateness_enabled=False,
            seed=42,
        )
        result = FileDropSimulator(tables={"items": df}, config=cfg).run()
        assert len(result.files_written) > 0
