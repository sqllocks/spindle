"""Tests for spindle generate accepting a .spindle.json file path as domain_name."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from sqllocks_spindle.cli import main


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def minimal_schema(tmp_path):
    """Write a minimal .spindle.json and return its path."""
    schema = {
        "domain": "test",
        "tables": {
            "item": {
                "rows": 10,
                "columns": {
                    "item_id": {"type": "integer", "strategy": "sequence"},
                    "name": {"type": "string", "strategy": "faker", "faker_method": "word"},
                },
            }
        },
    }
    path = tmp_path / "test_schema.spindle.json"
    path.write_text(json.dumps(schema))
    return str(path)


class TestGenerateSchemaFilePath:
    def test_generate_from_schema_file_summary(self, runner, minimal_schema):
        """generate <path.spindle.json> must succeed and not raise 'Unknown domain'."""
        result = runner.invoke(main, [
            "generate", minimal_schema,
            "--format", "summary",
        ])
        assert result.exit_code == 0, result.output
        assert "Unknown domain" not in result.output
        assert "item" in result.output

    def test_generate_from_schema_file_csv(self, runner, minimal_schema, tmp_path):
        """generate <path.spindle.json> --format csv must write files to output dir."""
        out_dir = tmp_path / "out"
        result = runner.invoke(main, [
            "generate", minimal_schema,
            "--format", "csv",
            "--output", str(out_dir),
        ])
        assert result.exit_code == 0, result.output
        assert "Unknown domain" not in result.output
        csv_files = list(out_dir.rglob("*.csv"))
        assert len(csv_files) >= 1

    def test_generate_from_schema_file_missing(self, runner, tmp_path):
        """generate <nonexistent.spindle.json> must fail with a clear error."""
        missing = str(tmp_path / "nonexistent.spindle.json")
        result = runner.invoke(main, [
            "generate", missing,
            "--format", "summary",
        ])
        assert result.exit_code != 0
        assert "not found" in result.output.lower() or "error" in result.output.lower()

    def test_generate_from_schema_file_dry_run(self, runner, minimal_schema):
        """generate <path.spindle.json> --dry-run must show table plan without generating."""
        result = runner.invoke(main, [
            "generate", minimal_schema,
            "--dry-run",
        ])
        assert result.exit_code == 0, result.output
        assert "item" in result.output
        assert "No data generated" in result.output
