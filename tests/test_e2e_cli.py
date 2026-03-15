"""E2E tests: all CLI commands via subprocess."""

from __future__ import annotations

import subprocess
import sys

import pytest


def _run_cli(*args, timeout=60):
    """Run a spindle CLI command, return (returncode, stdout, stderr)."""
    cmd = [sys.executable, "-m", "sqllocks_spindle.cli"] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return result.returncode, result.stdout, result.stderr


class TestCliCommands:
    def test_list(self):
        rc, out, err = _run_cli("list")
        assert rc == 0
        assert "retail" in out.lower()

    def test_describe_retail(self):
        rc, out, err = _run_cli("describe", "retail")
        assert rc == 0
        assert "customer" in out.lower()

    def test_generate_retail_summary(self):
        rc, out, err = _run_cli("generate", "retail", "--scale", "small", "--format", "summary")
        assert rc == 0
        assert "customer" in out.lower() or "order" in out.lower()

    def test_generate_retail_csv(self, tmp_path):
        rc, out, err = _run_cli("generate", "retail", "--scale", "small",
                                "--format", "csv", "--output", str(tmp_path))
        assert rc == 0

    def test_validate_nonexistent_file(self):
        rc, out, err = _run_cli("validate", "/nonexistent/file.spindle.json")
        assert rc != 0  # Should fail gracefully

    def test_from_ddl(self, tmp_path):
        ddl_file = tmp_path / "test.sql"
        ddl_file.write_text(
            "CREATE TABLE customer (id INT PRIMARY KEY, name VARCHAR(50));\n"
            "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customer(id));\n"
        )
        output_file = tmp_path / "output.spindle.json"
        rc, out, err = _run_cli("from-ddl", str(ddl_file), "--output", str(output_file))
        assert rc == 0

    def test_to_star(self, tmp_path):
        rc, out, err = _run_cli("to-star", "retail", "--scale", "small",
                                "--output", str(tmp_path), "--format", "csv")
        assert rc == 0

    def test_to_cdm(self, tmp_path):
        rc, out, err = _run_cli("to-cdm", "retail", "--scale", "small",
                                "--output", str(tmp_path))
        assert rc == 0

    def test_export_model(self, tmp_path):
        output_file = tmp_path / "model.bim"
        rc, out, err = _run_cli("export-model", "retail", "--output", str(output_file),
                                "--source-type", "lakehouse")
        assert rc == 0

    def test_presets(self):
        rc, out, err = _run_cli("presets")
        assert rc == 0
        assert "enterprise" in out.lower()

    def test_composite(self, tmp_path):
        rc, out, err = _run_cli("composite", "enterprise", "-s", "small",
                                "--format", "summary")
        assert rc == 0, f"composite failed: {err}"
