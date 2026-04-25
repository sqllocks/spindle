"""Tests for RunManifest enhancements — workspace/lakehouse IDs and SBOM (E14)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

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


# ---------------------------------------------------------------------------
# Workspace / Lakehouse IDs
# ---------------------------------------------------------------------------

class TestFabricIds:
    def test_default_ids_are_empty(self, started_builder):
        m = started_builder.finish()
        assert m.workspace_id == ""
        assert m.lakehouse_id == ""

    def test_set_fabric_ids(self, started_builder):
        started_builder.set_fabric_ids(
            workspace_id="ws-abc-123",
            lakehouse_id="lh-xyz-789",
        )
        m = started_builder.finish()
        assert m.workspace_id == "ws-abc-123"
        assert m.lakehouse_id == "lh-xyz-789"

    def test_fabric_ids_in_json(self, started_builder):
        started_builder.set_fabric_ids(workspace_id="ws-1", lakehouse_id="lh-2")
        m = started_builder.finish()
        json_str = ManifestBuilder.to_json(m)
        parsed = json.loads(json_str)
        assert parsed["workspace_id"] == "ws-1"
        assert parsed["lakehouse_id"] == "lh-2"

    def test_fabric_ids_roundtrip(self, started_builder, tmp_path):
        started_builder.set_fabric_ids(workspace_id="ws-rt", lakehouse_id="lh-rt")
        m = started_builder.finish()
        path = tmp_path / "manifest.json"
        ManifestBuilder.to_file(m, path)
        loaded = ManifestBuilder.from_file(path)
        assert loaded.workspace_id == "ws-rt"
        assert loaded.lakehouse_id == "lh-rt"

    def test_restart_resets_fabric_ids(self, builder):
        builder.start(spec=None, pack=None, domain_name="a", scale="s", seed=1)
        builder.set_fabric_ids(workspace_id="old-ws")
        builder.start(spec=None, pack=None, domain_name="b", scale="s", seed=2)
        m = builder.finish()
        assert m.workspace_id == ""


# ---------------------------------------------------------------------------
# SBOM
# ---------------------------------------------------------------------------

class TestSBOM:
    def test_sbom_populated_on_start(self, started_builder):
        m = started_builder.finish()
        assert isinstance(m.sbom, dict)
        # Should contain at least pandas and numpy (always installed)
        assert "pandas" in m.sbom
        assert "numpy" in m.sbom

    def test_sbom_in_json(self, started_builder):
        m = started_builder.finish()
        parsed = json.loads(ManifestBuilder.to_json(m))
        assert "sbom" in parsed
        assert "pandas" in parsed["sbom"]

    def test_sbom_roundtrip(self, started_builder, tmp_path):
        m = started_builder.finish()
        path = tmp_path / "manifest.json"
        ManifestBuilder.to_file(m, path)
        loaded = ManifestBuilder.from_file(path)
        assert loaded.sbom == m.sbom

    def test_sbom_values_are_version_strings(self, started_builder):
        m = started_builder.finish()
        for pkg, version in m.sbom.items():
            assert isinstance(version, str)
            # Version should look like a version (contains digits)
            assert any(c.isdigit() for c in version), f"{pkg} version {version!r} looks wrong"

    def test_legacy_manifest_without_sbom_loads_empty(self, tmp_path):
        """Manifests from before E14 (no sbom field) should load cleanly."""
        legacy = {
            "run_id": "old_run", "spec_hash": "", "pack_id": "",
            "domain": "retail", "scale": "small", "seed": 42,
            "engine_version": "1.0", "outputs": {}, "tables": {},
            "validation": {}, "chaos": {}, "timestamps": {},
        }
        path = tmp_path / "legacy.json"
        path.write_text(json.dumps(legacy), encoding="utf-8")
        loaded = ManifestBuilder.from_file(path)
        assert loaded.sbom == {}
        assert loaded.workspace_id == ""
        assert loaded.lakehouse_id == ""
