"""Tests for DemoManifest."""
import pytest
from sqllocks_spindle.demo.manifest import DemoManifest, ArtifactRecord


def test_create_and_export_md():
    m = DemoManifest(scenario="retail", mode="inference")
    m.add_artifact("warehouse", "retail.customer", row_count=1000)
    m.finish(True)
    md = m.export("md")
    assert "retail" in md
    assert "inference" in md
    assert "retail.customer" in md
    assert "1,000" in md


def test_create_and_export_html():
    m = DemoManifest(scenario="test", mode="seeding")
    m.add_artifact("lakehouse", "test_file.parquet", row_count=500)
    m.finish(True)
    html = m.export("html")
    assert "<html" in html
    assert "test_file.parquet" in html


def test_save_and_load(tmp_path):
    m = DemoManifest(scenario="retail", mode="seeding")
    m.add_artifact("warehouse", "customer", row_count=100)
    m.finish(True)
    m.save(directory=tmp_path)
    loaded = DemoManifest.load(m.session_id, directory=tmp_path)
    assert loaded.scenario == "retail"
    assert loaded.mode == "seeding"
    assert len(loaded.artifacts) == 1
    assert loaded.artifacts[0].row_count == 100


def test_unknown_format_raises():
    m = DemoManifest()
    with pytest.raises(ValueError):
        m.export("xml")


def test_manifest_records_fabric_run_id(tmp_path):
    from sqllocks_spindle.demo.manifest import DemoManifest
    m = DemoManifest(scenario="retail", mode="seeding")
    m.fabric_run_id = "run-abc-123"
    m.scale_mode = "spark"
    m.workspace_id = "ws-456"
    m.notebook_item_id = "nb-789"
    m.finish(success=True)
    path = m.save(directory=tmp_path)
    assert path.exists()
    loaded = DemoManifest.load(m.session_id, directory=tmp_path)
    assert loaded.fabric_run_id == "run-abc-123"
    assert loaded.scale_mode == "spark"
    assert loaded.workspace_id == "ws-456"
    assert loaded.notebook_item_id == "nb-789"


def test_manifest_defaults_for_local_mode(tmp_path):
    from sqllocks_spindle.demo.manifest import DemoManifest
    m = DemoManifest(scenario="retail", mode="seeding")
    m.scale_mode = "local"
    m.finish(success=True)
    path = m.save(directory=tmp_path)
    loaded = DemoManifest.load(m.session_id, directory=tmp_path)
    assert loaded.scale_mode == "local"
    assert loaded.fabric_run_id is None
    assert loaded.workspace_id is None
    assert loaded.notebook_item_id is None
