"""Tests for DemoOrchestrator."""
import pytest
from sqllocks_spindle.demo.orchestrator import DemoOrchestrator
from sqllocks_spindle.demo.params import DemoParams


def test_run_inference_retail():
    orch = DemoOrchestrator()
    params = DemoParams(
        scenario="retail", mode="inference",
        rows=300, output_formats=["terminal"], seed=42,
    )
    result = orch.run(params)
    assert result.success is True
    assert result.session_id is not None
    assert result.fidelity_score is not None


def test_run_unsupported_mode_raises():
    orch = DemoOrchestrator()
    params = DemoParams(scenario="enterprise", mode="inference", rows=100)
    with pytest.raises(ValueError, match="does not support mode"):
        orch.run(params)


def test_run_unknown_scenario_raises():
    orch = DemoOrchestrator()
    params = DemoParams(scenario="nonexistent_xyz", mode="inference", rows=100)
    with pytest.raises(KeyError):
        orch.run(params)
