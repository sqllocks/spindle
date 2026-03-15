"""Tests for the MCP bridge command handlers."""

from __future__ import annotations

import pytest

from sqllocks_spindle.mcp_bridge import (
    cmd_list,
    cmd_describe,
    cmd_generate,
    cmd_dry_run,
    cmd_validate,
    cmd_preview,
    cmd_profile_info,
)


class TestMcpBridgeList:
    def test_list_returns_domains(self):
        result = cmd_list({})
        assert "domains" in result
        assert "count" in result

    def test_list_contains_domains(self):
        result = cmd_list({})
        domains = result["domains"]
        assert len(domains) >= 12
        names = [d["name"] for d in domains]
        assert "retail" in names
        assert "healthcare" in names


class TestMcpBridgeDescribe:
    def test_describe_retail(self):
        result = cmd_describe({"domain": "retail"})
        assert "tables" in result
        assert result["table_count"] > 0

    def test_describe_healthcare(self):
        result = cmd_describe({"domain": "healthcare"})
        assert "tables" in result

    def test_describe_unknown_domain_errors(self):
        with pytest.raises(ValueError, match="Unknown domain"):
            cmd_describe({"domain": "nonexistent_domain_xyz"})


class TestMcpBridgeDryRun:
    def test_dry_run_returns_counts(self):
        result = cmd_dry_run({"domain": "retail", "scale": "small"})
        assert "planned_rows" in result
        assert result["total_rows"] > 0


class TestMcpBridgeGenerate:
    def test_generate_retail_small(self):
        result = cmd_generate({
            "domain": "retail",
            "scale": "small",
            "seed": 42,
            "format": "summary",
        })
        assert result["total_rows"] > 0
        assert result["integrity_pass"] is True

    def test_generate_returns_summary(self):
        result = cmd_generate({
            "domain": "retail",
            "scale": "small",
            "seed": 42,
            "format": "summary",
        })
        assert "tables" in result


class TestMcpBridgePreview:
    def test_preview_returns_rows(self):
        result = cmd_preview({
            "domain": "retail",
            "rows": 5,
            "seed": 42,
        })
        assert "tables" in result
        # Each table should have preview data
        for tname, tdata in result["tables"].items():
            assert "data" in tdata
            assert tdata["preview_rows"] <= 5


class TestMcpBridgeProfileInfo:
    def test_profile_info_retail(self):
        result = cmd_profile_info({"domain": "retail"})
        assert result["domain"] == "retail"
        assert "available_profiles" in result


class TestMcpBridgeValidate:
    def test_validate_missing_file_errors(self):
        with pytest.raises(FileNotFoundError):
            cmd_validate({"schema_path": "/nonexistent/path.spindle.json"})
