"""Tests for InferenceDemoMode — no Fabric connection required."""
import pandas as pd
import pytest
from sqllocks_spindle.demo.params import DemoParams
from sqllocks_spindle.demo.manifest import DemoManifest
from sqllocks_spindle.demo.modes.inference import InferenceDemoMode


def test_inference_domain_defaults():
    """Inference mode with no input file uses domain defaults."""
    params = DemoParams(
        scenario="retail", mode="inference", domain="retail",
        rows=500, output_formats=["terminal"], seed=42,
    )
    manifest = DemoManifest(scenario="retail", mode="inference")
    mode = InferenceDemoMode(params, manifest)
    result = mode.run()
    assert result["success"] is True
    assert "fidelity_score" in result
    assert 0.0 <= result["fidelity_score"] <= 1.0


def test_inference_csv_file(tmp_path):
    """Inference mode with a CSV file."""
    df = pd.DataFrame({
        "id": range(100),
        "name": ["Alice", "Bob"] * 50,
        "amount": [float(i) for i in range(100)],
    })
    csv_path = tmp_path / "test_data.csv"
    df.to_csv(csv_path, index=False)
    params = DemoParams(
        scenario="retail", mode="inference", rows=200,
        input_file=str(csv_path), output_formats=["terminal"], seed=42,
    )
    manifest = DemoManifest(scenario="retail", mode="inference")
    mode = InferenceDemoMode(params, manifest)
    result = mode.run()
    assert result["success"] is True


def test_inference_live_db_without_connection_fails_gracefully():
    params = DemoParams(
        scenario="retail", mode="inference", rows=100,
        input_file="live-db", output_formats=["terminal"],
    )
    manifest = DemoManifest()
    mode = InferenceDemoMode(params, manifest, connection_profile=None)
    result = mode.run()
    assert result["success"] is False
    assert result["error"]
