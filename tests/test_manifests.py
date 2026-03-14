"""Tests for ManifestBuilder and RunManifest."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sqllocks_spindle import __version__
from sqllocks_spindle.manifests.run_manifest import ManifestBuilder, RunManifest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def builder():
    return ManifestBuilder()


@pytest.fixture
def started_builder(builder):
    builder.start(spec=None, pack=None, domain_name="retail", scale="small", seed=42)
    return builder


@pytest.fixture
def full_manifest(started_builder):
    started_builder.record_output("customer", rows=100, columns=5, paths=["customer.parquet"])
    started_builder.record_output("order", rows=500, columns=8)
    started_builder.record_validation("referential_integrity", True)
    started_builder.record_validation("schema_conformance", False)
    started_builder.record_chaos("value", 12)
    started_builder.record_chaos("schema", 3)
    return started_builder.finish()


# ---------------------------------------------------------------------------
# RunManifest
# ---------------------------------------------------------------------------

class TestRunManifest:
    def test_summary_minimal(self):
        m = RunManifest(
            run_id="20240101_000000_retail_small_s42",
            spec_hash="",
            pack_id="test_pack",
            domain="retail",
            scale="small",
            seed=42,
            engine_version="1.0.0",
        )
        summary = m.summary()
        assert "retail" in summary
        assert "small" in summary

    def test_summary_includes_tables_and_rows(self, full_manifest):
        summary = full_manifest.summary()
        assert "600" in summary or "tables" in summary.lower()

    def test_summary_includes_gate_counts(self, full_manifest):
        summary = full_manifest.summary()
        assert "1/2" in summary or "Gates" in summary

    def test_summary_includes_elapsed(self, full_manifest):
        summary = full_manifest.summary()
        assert "Elapsed" in summary or "elapsed" in summary

    def test_summary_no_tables_no_raise(self):
        m = RunManifest(
            run_id="x", spec_hash="", pack_id="", domain="d",
            scale="s", seed=0, engine_version="1.0",
        )
        # Should not raise even with empty tables/validation
        assert isinstance(m.summary(), str)


# ---------------------------------------------------------------------------
# ManifestBuilder.start
# ---------------------------------------------------------------------------

class TestManifestBuilderStart:
    def test_run_id_contains_domain_scale_seed(self, started_builder):
        m = started_builder.finish()
        assert "retail" in m.run_id
        assert "small" in m.run_id
        assert "s42" in m.run_id

    def test_run_id_formatted_with_timestamp(self, started_builder):
        m = started_builder.finish()
        # Format: YYYYMMDD_HHMMSS_domain_scale_sSEED
        parts = m.run_id.split("_")
        assert len(parts) >= 4

    def test_second_start_resets_tables(self, builder):
        builder.start(spec=None, pack=None, domain_name="retail", scale="small", seed=1)
        builder.record_output("customer", rows=100, columns=5)
        builder.start(spec=None, pack=None, domain_name="hr", scale="medium", seed=2)
        m = builder.finish()
        assert m.tables == {}

    def test_second_start_resets_validation(self, builder):
        builder.start(spec=None, pack=None, domain_name="retail", scale="small", seed=1)
        builder.record_validation("gate_a", True)
        builder.start(spec=None, pack=None, domain_name="hr", scale="small", seed=2)
        m = builder.finish()
        assert m.validation == {}

    def test_second_start_resets_chaos(self, builder):
        builder.start(spec=None, pack=None, domain_name="retail", scale="small", seed=1)
        builder.record_chaos("value", 5)
        builder.start(spec=None, pack=None, domain_name="hr", scale="small", seed=2)
        m = builder.finish()
        assert m.chaos == {}

    def test_pack_id_from_pack_object(self, builder):
        class FakePack:
            id = "my_pack_id"
        builder.start(spec=None, pack=FakePack(), domain_name="retail", scale="small", seed=42)
        m = builder.finish()
        assert m.pack_id == "my_pack_id"

    def test_no_pack_empty_pack_id(self, started_builder):
        m = started_builder.finish()
        assert m.pack_id == ""


# ---------------------------------------------------------------------------
# ManifestBuilder.record_*
# ---------------------------------------------------------------------------

class TestManifestBuilderRecord:
    def test_record_output_populates_tables(self, started_builder):
        started_builder.record_output("order", rows=200, columns=6, paths=["a.parquet"])
        m = started_builder.finish()
        assert "order" in m.tables
        assert m.tables["order"]["rows"] == 200
        assert m.tables["order"]["columns"] == 6
        assert m.tables["order"]["file_paths"] == ["a.parquet"]

    def test_record_output_no_paths_defaults_to_empty_list(self, started_builder):
        started_builder.record_output("order", rows=50, columns=4)
        m = started_builder.finish()
        assert m.tables["order"]["file_paths"] == []

    def test_record_validation_true(self, started_builder):
        started_builder.record_validation("referential_integrity", True)
        m = started_builder.finish()
        assert m.validation["referential_integrity"] is True

    def test_record_validation_false(self, started_builder):
        started_builder.record_validation("schema_conformance", False)
        m = started_builder.finish()
        assert m.validation["schema_conformance"] is False

    def test_record_chaos_accumulates(self, started_builder):
        started_builder.record_chaos("value", 5)
        started_builder.record_chaos("value", 3)
        m = started_builder.finish()
        assert m.chaos["value"] == 8

    def test_record_chaos_multiple_categories(self, started_builder):
        started_builder.record_chaos("value", 5)
        started_builder.record_chaos("schema", 2)
        m = started_builder.finish()
        assert m.chaos["value"] == 5
        assert m.chaos["schema"] == 2


# ---------------------------------------------------------------------------
# ManifestBuilder.finish
# ---------------------------------------------------------------------------

class TestManifestBuilderFinish:
    def test_returns_run_manifest(self, started_builder):
        m = started_builder.finish()
        assert isinstance(m, RunManifest)

    def test_engine_version_matches_package(self, started_builder):
        m = started_builder.finish()
        assert m.engine_version == __version__

    def test_timestamps_started_set(self, started_builder):
        m = started_builder.finish()
        assert "started" in m.timestamps
        assert m.timestamps["started"] != ""

    def test_timestamps_finished_set(self, started_builder):
        m = started_builder.finish()
        assert "finished" in m.timestamps
        assert m.timestamps["finished"] != ""

    def test_timestamps_elapsed_seconds_nonnegative(self, started_builder):
        m = started_builder.finish()
        assert m.timestamps["elapsed_seconds"] >= 0

    def test_domain_and_scale_set(self, started_builder):
        m = started_builder.finish()
        assert m.domain == "retail"
        assert m.scale == "small"
        assert m.seed == 42


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

class TestManifestSerialisation:
    def test_to_json_is_valid_json(self, full_manifest):
        json_str = ManifestBuilder.to_json(full_manifest)
        parsed = json.loads(json_str)
        assert parsed["domain"] == "retail"

    def test_to_json_contains_tables(self, full_manifest):
        parsed = json.loads(ManifestBuilder.to_json(full_manifest))
        assert "customer" in parsed["tables"]
        assert "order" in parsed["tables"]

    def test_to_file_and_from_file(self, full_manifest, tmp_path):
        path = tmp_path / "manifest.json"
        ManifestBuilder.to_file(full_manifest, path)
        assert path.exists()
        loaded = ManifestBuilder.from_file(path)
        assert loaded.run_id == full_manifest.run_id
        assert loaded.domain == full_manifest.domain

    def test_from_file_all_fields(self, full_manifest, tmp_path):
        path = tmp_path / "manifest.json"
        ManifestBuilder.to_file(full_manifest, path)
        loaded = ManifestBuilder.from_file(path)
        assert loaded.scale == full_manifest.scale
        assert loaded.seed == full_manifest.seed
        assert loaded.engine_version == full_manifest.engine_version
        assert loaded.validation == full_manifest.validation
        assert loaded.chaos == full_manifest.chaos

    def test_to_file_creates_parent_dirs(self, full_manifest, tmp_path):
        path = tmp_path / "nested" / "dir" / "manifest.json"
        ManifestBuilder.to_file(full_manifest, path)
        assert path.exists()

    def test_hash_path_missing_file(self):
        """_hash_path should return empty string for missing files."""
        result = ManifestBuilder._hash_path(Path("/nonexistent/path/file.yaml"))
        assert result == ""
