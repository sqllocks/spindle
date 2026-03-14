"""Tests for spindle publish CLI command (E13)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from sqllocks_spindle.cli import main


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def runner():
    return CliRunner()


# ---------------------------------------------------------------------------
# Help and basic invocation
# ---------------------------------------------------------------------------

class TestPublishHelp:
    def test_publish_help(self, runner):
        result = runner.invoke(main, ["publish", "--help"])
        assert result.exit_code == 0
        assert "publish" in result.output.lower()
        assert "--target" in result.output

    def test_publish_requires_target(self, runner):
        result = runner.invoke(main, ["publish", "retail"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()


# ---------------------------------------------------------------------------
# Lakehouse publish (local)
# ---------------------------------------------------------------------------

class TestPublishLakehouse:
    def test_publish_lakehouse_local(self, runner, tmp_path):
        result = runner.invoke(main, [
            "publish", "retail",
            "--target", "lakehouse",
            "--base-path", str(tmp_path / "lakehouse"),
            "--scale", "small",
            "--seed", "42",
            "--format", "parquet",
        ])
        assert result.exit_code == 0, result.output
        assert "Publish complete" in result.output

    def test_publish_lakehouse_writes_files(self, runner, tmp_path):
        base = tmp_path / "lakehouse"
        result = runner.invoke(main, [
            "publish", "retail",
            "--target", "lakehouse",
            "--base-path", str(base),
            "--scale", "small",
            "--seed", "42",
        ])
        assert result.exit_code == 0
        # Should have created files under base/retail/
        parquet_files = list(base.rglob("*.parquet"))
        assert len(parquet_files) > 0

    def test_publish_lakehouse_writes_manifest(self, runner, tmp_path):
        base = tmp_path / "lakehouse"
        result = runner.invoke(main, [
            "publish", "retail",
            "--target", "lakehouse",
            "--base-path", str(base),
            "--scale", "small",
            "--seed", "42",
            "--workspace-id", "test-ws-123",
        ])
        assert result.exit_code == 0
        manifest_files = list(base.rglob("run_manifest.json"))
        assert len(manifest_files) == 1
        data = json.loads(manifest_files[0].read_text())
        assert data["domain"] == "retail"
        assert data["workspace_id"] == "test-ws-123"

    def test_publish_lakehouse_csv_format(self, runner, tmp_path):
        base = tmp_path / "lakehouse"
        result = runner.invoke(main, [
            "publish", "retail",
            "--target", "lakehouse",
            "--base-path", str(base),
            "--format", "csv",
            "--scale", "small",
            "--seed", "42",
        ])
        assert result.exit_code == 0
        csv_files = list(base.rglob("*.csv"))
        assert len(csv_files) > 0

    def test_publish_lakehouse_missing_base_path(self, runner):
        result = runner.invoke(main, [
            "publish", "retail",
            "--target", "lakehouse",
        ])
        assert result.exit_code != 0
        assert "base-path" in result.output.lower() or "required" in result.output.lower()


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

class TestPublishDryRun:
    def test_dry_run_no_files_written(self, runner, tmp_path):
        base = tmp_path / "lakehouse"
        result = runner.invoke(main, [
            "publish", "retail",
            "--target", "lakehouse",
            "--base-path", str(base),
            "--dry-run",
        ])
        assert result.exit_code == 0
        assert "Dry run" in result.output
        # Should NOT create any parquet files
        parquet_files = list(base.rglob("*.parquet"))
        assert len(parquet_files) == 0


# ---------------------------------------------------------------------------
# Credential resolution
# ---------------------------------------------------------------------------

class TestPublishCredentials:
    def test_env_credential_resolved(self, runner, tmp_path, monkeypatch):
        monkeypatch.setenv("SPINDLE_TEST_CONN", "Server=test;Database=db")
        base = tmp_path / "lakehouse"
        # Lakehouse doesn't use connection string, but we can test it doesn't crash
        result = runner.invoke(main, [
            "publish", "retail",
            "--target", "lakehouse",
            "--base-path", str(base),
            "--connection-string", "env://SPINDLE_TEST_CONN",
        ])
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# SQL Database (missing connection)
# ---------------------------------------------------------------------------

class TestPublishSqlDatabase:
    def test_sql_missing_connection_string(self, runner):
        result = runner.invoke(main, [
            "publish", "retail",
            "--target", "sql-database",
        ])
        assert result.exit_code != 0
        assert "connection-string" in result.output.lower() or "required" in result.output.lower()


# ---------------------------------------------------------------------------
# Eventhouse (missing connection)
# ---------------------------------------------------------------------------

class TestPublishEventhouse:
    def test_eventhouse_missing_connection(self, runner):
        result = runner.invoke(main, [
            "publish", "retail",
            "--target", "eventhouse",
        ])
        assert result.exit_code != 0

    def test_eventhouse_missing_database(self, runner):
        result = runner.invoke(main, [
            "publish", "retail",
            "--target", "eventhouse",
            "--connection-string", "https://test.kusto.fabric.microsoft.com",
        ])
        assert result.exit_code != 0
