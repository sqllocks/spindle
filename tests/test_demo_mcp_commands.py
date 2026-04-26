"""Tests for cmd_demo_status, cmd_demo_cleanup."""
from unittest.mock import MagicMock
import pytest

from sqllocks_spindle.demo.manifest import DemoManifest


def test_cmd_demo_status_unknown_session_returns_error(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "sqllocks_spindle.demo.manifest.DemoManifest.load",
        MagicMock(side_effect=FileNotFoundError("No session 'nope' found")),
    )
    from sqllocks_spindle.mcp_bridge import cmd_demo_status
    result = cmd_demo_status({"session_id": "nope"})
    assert result["error"] == "session_not_found"
    assert result["session_id"] == "nope"


def test_cmd_demo_status_local_returns_manifest(tmp_path, monkeypatch):
    m = DemoManifest(scenario="retail", mode="seeding")
    m.scale_mode = "local"
    m.finish(success=True)
    m.save(directory=tmp_path)

    monkeypatch.setattr(
        "sqllocks_spindle.demo.manifest.DemoManifest.load",
        MagicMock(return_value=m),
    )
    from sqllocks_spindle.mcp_bridge import cmd_demo_status
    result = cmd_demo_status({"session_id": m.session_id})
    assert result["session_id"] == m.session_id
    assert result["manifest"]["scale_mode"] == "local"
    assert result["manifest"]["success"] is True
    assert "fabric" not in result


def test_cmd_demo_status_spark_polls_fabric_tracker(tmp_path, monkeypatch):
    m = DemoManifest(scenario="retail", mode="seeding")
    m.scale_mode = "spark"
    m.fabric_run_id = "run-123"
    m.workspace_id = "ws-1"
    m.notebook_item_id = "nb-456"
    m.finish(success=True)

    fake_tracker = MagicMock()
    fake_tracker.get_status.return_value = {
        "status": "running", "progress_pct": 42, "error": None,
    }
    monkeypatch.setattr(
        "sqllocks_spindle.demo.manifest.DemoManifest.load",
        MagicMock(return_value=m),
    )
    monkeypatch.setattr(
        "sqllocks_spindle.mcp_bridge.FabricJobTracker",
        MagicMock(return_value=fake_tracker),
    )
    from sqllocks_spindle.mcp_bridge import cmd_demo_status
    result = cmd_demo_status({"session_id": m.session_id, "token": "t"})
    assert result["fabric"]["status"] == "running"
    fake_tracker.get_status.assert_called_once_with("ws-1", "nb-456", "run-123")


def test_cmd_demo_cleanup_invokes_cleanup_engine(tmp_path, monkeypatch):
    m = DemoManifest(scenario="retail", mode="seeding")
    m.add_artifact("lakehouse", "Tables/customer", row_count=100)
    m.finish(success=True)

    fake_engine = MagicMock()
    fake_engine.cleanup.return_value = {"lakehouse": ["Tables/customer"]}
    monkeypatch.setattr(
        "sqllocks_spindle.demo.manifest.DemoManifest.load",
        MagicMock(return_value=m),
    )
    monkeypatch.setattr(
        "sqllocks_spindle.mcp_bridge.CleanupEngine",
        MagicMock(return_value=fake_engine),
    )
    from sqllocks_spindle.mcp_bridge import cmd_demo_cleanup
    result = cmd_demo_cleanup({"session_id": m.session_id, "dry_run": True})
    assert result == {"lakehouse": ["Tables/customer"]}
    fake_engine.cleanup.assert_called_once_with(m, dry_run=True)


def test_cmd_demo_cleanup_unknown_session_returns_error(monkeypatch):
    monkeypatch.setattr(
        "sqllocks_spindle.demo.manifest.DemoManifest.load",
        MagicMock(side_effect=FileNotFoundError("No session 'x' found")),
    )
    from sqllocks_spindle.mcp_bridge import cmd_demo_cleanup
    result = cmd_demo_cleanup({"session_id": "x"})
    assert result["error"] == "session_not_found"


def test_cmd_demo_run_passes_scale_mode_through(monkeypatch):
    """Ensure cmd_demo_run forwards scale_mode into DemoParams."""
    from sqllocks_spindle.demo.orchestrator import DemoResult

    captured: dict = {}

    class FakeOrch:
        def run(self, params):
            captured["scale_mode"] = params.scale_mode
            return DemoResult(success=True, session_id="abc",
                              scenario=params.scenario, mode=params.mode)

    monkeypatch.setattr("sqllocks_spindle.mcp_bridge.DemoOrchestrator", FakeOrch)
    from sqllocks_spindle.mcp_bridge import cmd_demo_run
    cmd_demo_run({"scenario": "retail", "mode": "seeding",
                  "rows": 1000, "scale_mode": "spark", "connection": None})
    assert captured["scale_mode"] == "spark"
