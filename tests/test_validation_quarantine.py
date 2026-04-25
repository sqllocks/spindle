"""Tests for QuarantineManager."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from sqllocks_spindle.validation.quarantine import (
    META_SUFFIX,
    QuarantineEntry,
    QuarantineManager,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def manager():
    return QuarantineManager(domain="retail")


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "order_id": range(1, 6),
        "amount": [10.0, 20.0, 30.0, 40.0, 50.0],
    })


@pytest.fixture
def source_file(tmp_path):
    p = tmp_path / "part-0001.parquet"
    pd.DataFrame({"id": [1, 2]}).to_parquet(p, index=False)
    return p


# ---------------------------------------------------------------------------
# QuarantineManager.quarantine_file
# ---------------------------------------------------------------------------

class TestQuarantineFile:
    def test_returns_dest_path(self, manager, source_file, tmp_path):
        dest = manager.quarantine_file(
            source_file, tmp_path / "quarantine", "run-001", "bad format"
        )
        assert dest.exists()

    def test_file_is_copied(self, manager, source_file, tmp_path):
        root = tmp_path / "q"
        dest = manager.quarantine_file(source_file, root, "run-001", "test reason")
        # Original still exists
        assert source_file.exists()
        assert dest.exists()

    def test_meta_sidecar_created(self, manager, source_file, tmp_path):
        root = tmp_path / "q"
        dest = manager.quarantine_file(source_file, root, "run-001", "test reason")
        meta_path = dest.parent / f"{dest.name}{META_SUFFIX}"
        assert meta_path.exists()

    def test_meta_sidecar_content(self, manager, source_file, tmp_path):
        root = tmp_path / "q"
        dest = manager.quarantine_file(
            source_file, root, "run-001", "bad schema", gate_name="schema_conformance"
        )
        meta_path = dest.parent / f"{dest.name}{META_SUFFIX}"
        meta = json.loads(meta_path.read_text())
        assert meta["reason"] == "bad schema"
        assert meta["gate_name"] == "schema_conformance"
        assert meta["run_id"] == "run-001"
        assert meta["original_path"] is not None

    def test_domain_and_run_id_in_path(self, manager, source_file, tmp_path):
        root = tmp_path / "q"
        dest = manager.quarantine_file(source_file, root, "run-123", "bad")
        assert "retail" in str(dest)
        assert "run-123" in str(dest)

    def test_creates_parent_dirs(self, manager, source_file, tmp_path):
        root = tmp_path / "deep" / "nested" / "q"
        dest = manager.quarantine_file(source_file, root, "run-001", "test")
        assert dest.exists()


# ---------------------------------------------------------------------------
# QuarantineManager.quarantine_dataframe
# ---------------------------------------------------------------------------

class TestQuarantineDataframe:
    def test_returns_dest_path(self, manager, sample_df, tmp_path):
        dest = manager.quarantine_dataframe(
            sample_df, tmp_path / "q", "run-001", "order", "null check failed"
        )
        assert dest.exists()

    def test_parquet_format(self, manager, sample_df, tmp_path):
        dest = manager.quarantine_dataframe(
            sample_df, tmp_path / "q", "run-001", "order", "test", fmt="parquet"
        )
        assert dest.suffix == ".parquet"
        loaded = pd.read_parquet(dest)
        assert len(loaded) == len(sample_df)

    def test_csv_format(self, manager, sample_df, tmp_path):
        dest = manager.quarantine_dataframe(
            sample_df, tmp_path / "q", "run-001", "order", "test", fmt="csv"
        )
        assert dest.suffix == ".csv"
        loaded = pd.read_csv(dest)
        assert len(loaded) == len(sample_df)

    def test_jsonl_format(self, manager, sample_df, tmp_path):
        dest = manager.quarantine_dataframe(
            sample_df, tmp_path / "q", "run-001", "order", "test", fmt="jsonl"
        )
        assert dest.suffix == ".jsonl"
        lines = dest.read_text().strip().splitlines()
        assert len(lines) == len(sample_df)

    def test_meta_contains_row_count(self, manager, sample_df, tmp_path):
        dest = manager.quarantine_dataframe(
            sample_df, tmp_path / "q", "run-001", "order", "null check"
        )
        meta_path = dest.parent / f"{dest.name}{META_SUFFIX}"
        meta = json.loads(meta_path.read_text())
        assert meta["extra"]["rows"] == len(sample_df)
        assert meta["extra"]["columns"] == len(sample_df.columns)

    def test_table_name_in_meta(self, manager, sample_df, tmp_path):
        dest = manager.quarantine_dataframe(
            sample_df, tmp_path / "q", "run-001", "order", "test"
        )
        meta_path = dest.parent / f"{dest.name}{META_SUFFIX}"
        meta = json.loads(meta_path.read_text())
        assert meta["table_name"] == "order"


# ---------------------------------------------------------------------------
# QuarantineManager.list_quarantined
# ---------------------------------------------------------------------------

class TestListQuarantined:
    def test_empty_root_returns_empty_list(self, manager, tmp_path):
        result = manager.list_quarantined(tmp_path / "q")
        assert result == []

    def test_nonexistent_root_returns_empty(self, manager, tmp_path):
        result = manager.list_quarantined(tmp_path / "does_not_exist")
        assert result == []

    def test_returns_one_entry_per_artifact(self, manager, sample_df, tmp_path):
        root = tmp_path / "q"
        manager.quarantine_dataframe(sample_df, root, "run-001", "order", "test")
        result = manager.list_quarantined(root)
        assert len(result) == 1

    def test_entry_has_quarantine_path(self, manager, sample_df, tmp_path):
        root = tmp_path / "q"
        manager.quarantine_dataframe(sample_df, root, "run-001", "order", "test")
        result = manager.list_quarantined(root)
        assert "quarantine_path" in result[0]
        assert "exists" in result[0]
        assert result[0]["exists"] is True

    def test_multiple_artifacts_listed(self, manager, sample_df, tmp_path):
        root = tmp_path / "q"
        manager.quarantine_dataframe(sample_df, root, "run-001", "order", "test")
        manager.quarantine_dataframe(sample_df, root, "run-001", "customer", "test")
        result = manager.list_quarantined(root)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# QuarantineManager.get_quarantine_report
# ---------------------------------------------------------------------------

class TestGetQuarantineReport:
    def test_empty_report_for_unknown_run(self, manager, tmp_path):
        root = tmp_path / "q"
        root.mkdir()
        report = manager.get_quarantine_report(root, "run-999")
        assert report["total_quarantined"] == 0
        assert report["run_id"] == "run-999"

    def test_report_counts_artifacts(self, manager, sample_df, tmp_path):
        root = tmp_path / "q"
        manager.quarantine_dataframe(
            sample_df, root, "run-001", "order", "null check", gate_name="null_constraint"
        )
        manager.quarantine_dataframe(
            sample_df, root, "run-001", "customer", "schema drift", gate_name="schema_drift"
        )
        report = manager.get_quarantine_report(root, "run-001")
        assert report["total_quarantined"] == 2

    def test_report_aggregates_gates_triggered(self, manager, sample_df, tmp_path):
        root = tmp_path / "q"
        manager.quarantine_dataframe(
            sample_df, root, "run-001", "order", "test", gate_name="null_constraint"
        )
        manager.quarantine_dataframe(
            sample_df, root, "run-001", "customer", "test", gate_name="null_constraint"
        )
        report = manager.get_quarantine_report(root, "run-001")
        assert report["gates_triggered"]["null_constraint"] == 2

    def test_report_domain_field(self, manager, sample_df, tmp_path):
        root = tmp_path / "q"
        report = manager.get_quarantine_report(root, "run-001")
        assert report["domain"] == "retail"
