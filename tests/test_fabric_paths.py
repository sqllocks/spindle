"""Tests for OneLakePaths and LakehouseFilesWriter."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import pytest

from sqllocks_spindle.fabric.lakehouse_files_writer import LakehouseFilesWriter
from sqllocks_spindle.fabric.onelake_paths import OneLakePaths


# ---------------------------------------------------------------------------
# OneLakePaths
# ---------------------------------------------------------------------------

class TestOneLakePathsDefaults:
    def test_default_local_base(self):
        paths = OneLakePaths()
        assert "lakehouse_files" in str(paths.base)

    def test_explicit_base_path(self, tmp_path):
        paths = OneLakePaths(base_path=tmp_path / "my_files")
        assert paths.base == tmp_path / "my_files"

    def test_explicit_base_path_string(self, tmp_path):
        paths = OneLakePaths(base_path=str(tmp_path / "files"))
        assert paths.base == tmp_path / "files"


class TestOneLakePathsLandingZone:
    def test_landing_zone_path_no_hour(self, tmp_path):
        paths = OneLakePaths(base_path=tmp_path)
        result = paths.landing_zone_path("retail", "order", "2024-01-15")
        assert result == tmp_path / "landing" / "retail" / "order" / "dt=2024-01-15"

    def test_landing_zone_path_with_hour_int(self, tmp_path):
        paths = OneLakePaths(base_path=tmp_path)
        result = paths.landing_zone_path("retail", "order", "2024-01-15", hour=9)
        assert "hour=09" in str(result)

    def test_landing_zone_path_with_hour_str(self, tmp_path):
        paths = OneLakePaths(base_path=tmp_path)
        result = paths.landing_zone_path("retail", "order", "2024-01-15", hour="23")
        assert "hour=23" in str(result)

    def test_landing_zone_path_hour_zero_padded(self, tmp_path):
        paths = OneLakePaths(base_path=tmp_path)
        result = paths.landing_zone_path("retail", "order", "2024-01-15", hour=5)
        assert "hour=05" in str(result)


class TestOneLakePathsQuarantine:
    def test_quarantine_path_structure(self, tmp_path):
        paths = OneLakePaths(base_path=tmp_path)
        result = paths.quarantine_path("retail", "run-001")
        assert "quarantine" in str(result)
        assert "retail" in str(result)
        assert "run-001" in str(result)


class TestOneLakePathsTables:
    def test_tables_path_sibling_of_files(self, tmp_path):
        paths = OneLakePaths(base_path=tmp_path / "Files")
        result = paths.tables_path("customers")
        assert "Tables" in str(result)
        assert "customers" in str(result)
        # Tables should be alongside Files, not inside it
        assert "Files" not in str(result).split("Tables")[0].split("/")[-2:]


class TestOneLakePathsControl:
    def test_control_path_has_control_suffix(self, tmp_path):
        paths = OneLakePaths(base_path=tmp_path)
        result = paths.control_path("retail", "order")
        assert "_control" in str(result)
        assert "retail" in str(result)
        assert "order" in str(result)

    def test_manifest_path_filename(self, tmp_path):
        paths = OneLakePaths(base_path=tmp_path)
        result = paths.manifest_path("retail", "order", "2024-01-15")
        assert result.name == "manifest_2024-01-15.json"

    def test_done_flag_path_filename(self, tmp_path):
        paths = OneLakePaths(base_path=tmp_path)
        result = paths.done_flag_path("retail", "order", "2024-01-15")
        assert "_SUCCESS_2024-01-15" in result.name


class TestOneLakePathsFabricDetection:
    def test_fabric_runtime_env_var_detected(self, monkeypatch):
        monkeypatch.setenv("FABRIC_RUNTIME", "1")
        assert OneLakePaths._is_fabric_runtime() is True

    def test_no_env_var_not_fabric(self, monkeypatch):
        monkeypatch.delenv("FABRIC_RUNTIME", raising=False)
        monkeypatch.delenv("TRIDENT_RUNTIME_VERSION", raising=False)
        assert OneLakePaths._is_fabric_runtime() is False

    def test_trident_env_var_detected(self, monkeypatch):
        monkeypatch.setenv("TRIDENT_RUNTIME_VERSION", "1.0")
        assert OneLakePaths._is_fabric_runtime() is True

    def test_both_env_vars_detected(self, monkeypatch):
        monkeypatch.setenv("FABRIC_RUNTIME", "1")
        monkeypatch.setenv("TRIDENT_RUNTIME_VERSION", "1.0")
        assert OneLakePaths._is_fabric_runtime() is True


# ---------------------------------------------------------------------------
# LakehouseFilesWriter
# ---------------------------------------------------------------------------

@pytest.fixture
def writer(tmp_path):
    return LakehouseFilesWriter(base_path=tmp_path)


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": range(1, 11),
        "name": [f"Item {i}" for i in range(1, 11)],
        "amount": [float(i) * 1.5 for i in range(1, 11)],
    })


class TestLakehouseFilesWriterBasic:
    def test_paths_property_returns_onelake_paths(self, writer):
        assert isinstance(writer.paths, OneLakePaths)

    def test_unsupported_format_raises(self, writer, sample_df, tmp_path):
        dest = tmp_path / "partition"
        with pytest.raises(ValueError, match="Unsupported"):
            writer.write_partition(sample_df, dest, format="xlsx")

    def test_unsupported_format_on_construction(self, tmp_path):
        with pytest.raises(ValueError, match="Unsupported"):
            LakehouseFilesWriter(base_path=tmp_path, default_format="xlsx")


class TestLakehouseFilesWriterParquet:
    def test_write_partition_parquet_creates_file(self, writer, sample_df, tmp_path):
        dest = tmp_path / "landing" / "retail" / "order" / "dt=2024-01-15"
        path = writer.write_partition(sample_df, dest, format="parquet")
        assert path.exists()
        assert path.suffix == ".parquet"

    def test_parquet_readable(self, writer, sample_df, tmp_path):
        dest = tmp_path / "partition"
        path = writer.write_partition(sample_df, dest, format="parquet")
        loaded = pd.read_parquet(path)
        assert len(loaded) == len(sample_df)
        assert list(loaded.columns) == list(sample_df.columns)

    def test_default_format_parquet(self, writer, sample_df, tmp_path):
        dest = tmp_path / "partition"
        path = writer.write_partition(sample_df, dest)
        assert path.suffix == ".parquet"


class TestLakehouseFilesWriterCSV:
    def test_write_partition_csv_creates_file(self, writer, sample_df, tmp_path):
        dest = tmp_path / "partition"
        path = writer.write_partition(sample_df, dest, format="csv")
        assert path.exists()
        assert path.suffix == ".csv"

    def test_csv_readable(self, writer, sample_df, tmp_path):
        dest = tmp_path / "partition"
        path = writer.write_partition(sample_df, dest, format="csv")
        loaded = pd.read_csv(path)
        assert len(loaded) == len(sample_df)


class TestLakehouseFilesWriterJSONL:
    def test_write_partition_jsonl_creates_file(self, writer, sample_df, tmp_path):
        dest = tmp_path / "partition"
        path = writer.write_partition(sample_df, dest, format="jsonl")
        assert path.exists()

    def test_jsonl_one_record_per_line(self, writer, sample_df, tmp_path):
        dest = tmp_path / "partition"
        path = writer.write_partition(sample_df, dest, format="jsonl")
        lines = path.read_text().strip().splitlines()
        assert len(lines) == len(sample_df)
        json.loads(lines[0])  # Each line must be valid JSON


class TestLakehouseFilesWriterNaming:
    def test_custom_naming_template(self, writer, sample_df, tmp_path):
        dest = tmp_path / "partition"
        path = writer.write_partition(
            sample_df, dest, format="parquet",
            file_naming_template="data_{format}_output.{format}",
        )
        assert "parquet" in path.name

    def test_default_naming_is_part_0001(self, writer, sample_df, tmp_path):
        dest = tmp_path / "partition"
        path = writer.write_partition(sample_df, dest, format="parquet")
        assert path.name == "part-0001.parquet"


class TestLakehouseFilesWriterManifest:
    def test_write_manifest_creates_json_file(self, writer, tmp_path):
        manifest_data = {"entity": "order", "slot": "2024-01-15", "files": ["a.parquet"]}
        dest = tmp_path / "_control" / "manifest_2024-01-15.json"
        path = writer.write_manifest(manifest_data, dest)
        assert path.exists()
        loaded = json.loads(path.read_text())
        assert loaded["entity"] == "order"

    def test_write_manifest_creates_parent_dirs(self, writer, tmp_path):
        dest = tmp_path / "nested" / "dir" / "manifest.json"
        writer.write_manifest({"key": "val"}, dest)
        assert dest.exists()


class TestLakehouseFilesWriterDoneFlag:
    def test_write_done_flag_creates_file(self, writer, tmp_path):
        dest = tmp_path / "_control" / "_SUCCESS_2024-01-15"
        path = writer.write_done_flag(dest)
        assert path.exists()

    def test_done_flag_is_empty(self, writer, tmp_path):
        dest = tmp_path / "_SUCCESS_test"
        path = writer.write_done_flag(dest)
        assert path.read_text() == ""

    def test_done_flag_creates_parent_dirs(self, writer, tmp_path):
        dest = tmp_path / "nested" / "deep" / "_SUCCESS"
        writer.write_done_flag(dest)
        assert dest.exists()
