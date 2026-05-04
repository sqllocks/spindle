# tests/test_e2e_verify.py
"""E2E CLI tests for spindle verify."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest


def _cli(*args) -> tuple[int, str, str]:
    """Run spindle CLI using the installed entry point."""
    import shutil
    spindle_bin = shutil.which("spindle") or str(
        Path(sys.executable).parent / "spindle"
    )
    result = subprocess.run(
        [spindle_bin, *args],
        capture_output=True, text=True,
    )
    return result.returncode, result.stdout, result.stderr


class TestVerifyCLI:
    def test_help(self):
        rc, out, _ = _cli("verify", "--help")
        assert rc == 0
        assert "DATA_PATH" in out
        assert "--schema" in out
        assert "--statistical" in out
        assert "--output" in out
        assert "--strict" in out

    def test_verify_csv_no_schema(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        df.to_csv(tmp_path / "users.csv", index=False)
        rc, out, _ = _cli("verify", str(tmp_path))
        assert rc == 0
        assert "PASS" in out
        assert "users" in out

    def test_verify_missing_path_exits_1(self):
        rc, _, err = _cli("verify", "/no/such/path")
        assert rc == 1
        assert "not found" in err.lower() or "no such" in err.lower()

    def test_verify_json_output(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2, 3]})
        df.to_csv(tmp_path / "t.csv", index=False)
        report_file = tmp_path / "report.json"
        rc, out, _ = _cli("verify", str(tmp_path), "--output", str(report_file))
        assert rc == 0
        assert report_file.exists()
        data = json.loads(report_file.read_text())
        assert "passed" in data
        assert data["row_counts"]["t"] == 3

    def test_verify_markdown_output(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2, 3]})
        df.to_csv(tmp_path / "t.csv", index=False)
        report_file = tmp_path / "report.md"
        rc, _, _ = _cli("verify", str(tmp_path), "--output", str(report_file))
        assert rc == 0
        md = report_file.read_text()
        assert "# Spindle Verify Report" in md
        assert "## Methodology" in md

    def test_verify_exits_0_no_warnings_with_strict(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2, 3]})
        df.to_csv(tmp_path / "t.csv", index=False)
        rc, _, _ = _cli("verify", str(tmp_path), "--strict")
        assert rc == 0  # passes because no warnings without schema
