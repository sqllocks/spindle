"""Tests for `spindle profile capture` and `spindle profile diff` (shape-as-code).

Covers the file-level shape workflow: capture a dataset's shape into a
committable, PII-free JSON, then diff two shapes with a CI-gate threshold.
"""
from __future__ import annotations

import json

import pandas as pd
import pytest
from click.testing import CliRunner

from sqllocks_spindle.cli import main


def _write_parquet(d, df):
    d.mkdir(parents=True, exist_ok=True)
    df.to_parquet(d / "customer.parquet", index=False)


@pytest.fixture
def shapes(tmp_path):
    """Two datasets with the SAME schema but different categorical shape."""
    a = pd.DataFrame({"tier": ["Basic"] * 80 + ["Gold"] * 20})
    b = pd.DataFrame({"tier": ["Basic"] * 40 + ["Gold"] * 60})
    _write_parquet(tmp_path / "a", a)
    _write_parquet(tmp_path / "b", b)
    runner = CliRunner()
    pa, pb = tmp_path / "a.json", tmp_path / "b.json"
    for src, out in ((tmp_path / "a", pa), (tmp_path / "b", pb)):
        r = runner.invoke(main, ["profile", "capture", str(src),
                                 "--format", "parquet", "-o", str(out)])
        assert r.exit_code == 0, r.output
    return runner, pa, pb


def test_capture_writes_pii_free_distribution_artifact(shapes):
    _, pa, _ = shapes
    assert pa.exists()
    prof = json.loads(pa.read_text(encoding="utf-8"))
    # It records the categorical distribution...
    assert "customer.tier" in prof["distributions"]
    assert prof["distributions"]["customer.tier"]["Basic"] == pytest.approx(0.8, abs=0.01)
    # ...and carries no raw rows (only category labels + weights live in it).
    assert prof.get("ratios") == {}
    assert "tables" in prof["metadata"]


def test_diff_reports_drift_and_is_human_readable(shapes):
    runner, pa, pb = shapes
    r = runner.invoke(main, ["profile", "diff", str(pa), str(pb)])
    assert r.exit_code == 0, r.output
    assert "customer.tier" in r.output
    assert "Total shape drift" in r.output


def test_diff_json_drift_score(shapes):
    runner, pa, pb = shapes
    r = runner.invoke(main, ["profile", "diff", str(pa), str(pb), "--json"])
    assert r.exit_code == 0
    report = json.loads(r.output)
    # Basic 0.8->0.4, Gold 0.2->0.6  => TVD = (0.4 + 0.4) / 2 = 0.4
    assert report["total_drift"] == pytest.approx(0.4, abs=0.02)


def test_diff_threshold_is_a_ci_gate(shapes):
    runner, pa, pb = shapes
    # drift ~0.4 exceeds 0.1 -> non-zero exit (build fails)
    assert runner.invoke(main, ["profile", "diff", str(pa), str(pb),
                                "--threshold", "0.1"]).exit_code == 1
    # drift ~0.4 under 0.9 -> passes
    assert runner.invoke(main, ["profile", "diff", str(pa), str(pb),
                                "--threshold", "0.9"]).exit_code == 0


def test_identical_shapes_have_zero_drift(shapes):
    runner, pa, _ = shapes
    r = runner.invoke(main, ["profile", "diff", str(pa), str(pa), "--json"])
    assert r.exit_code == 0
    assert json.loads(r.output)["total_drift"] == 0.0
    # zero drift never trips the gate
    assert runner.invoke(main, ["profile", "diff", str(pa), str(pa),
                                "--threshold", "0.0"]).exit_code == 0
