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
