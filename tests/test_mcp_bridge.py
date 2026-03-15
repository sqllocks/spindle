"""Tests for the MCP bridge command handlers."""

from __future__ import annotations

import json

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
    def test_list_returns_ok(self):
        result = cmd_list({})
        assert result["status"] == "ok"

    def test_list_contains_domains(self):
        result = cmd_list({})
        domains = result["data"]["domains"]
        assert len(domains) >= 12
        names = [d["name"] for d in domains]
        assert "retail" in names
        assert "healthcare" in names


class TestMcpBridgeDescribe:
    def test_describe_retail(self):
        result = cmd_describe({"domain": "retail"})
        assert result["status"] == "ok"
        assert "tables" in result["data"]

    def test_describe_healthcare(self):
        result = cmd_describe({"domain": "healthcare"})
        assert result["status"] == "ok"

    def test_describe_unknown_domain_errors(self):
        result = cmd_describe({"domain": "nonexistent_domain_xyz"})
        assert result["status"] == "error"


class TestMcpBridgeDryRun:
    def test_dry_run_returns_counts(self):
        result = cmd_dry_run({"domain": "retail", "scale": "small"})
        assert result["status"] == "ok"
        assert "row_counts" in result["data"] or "tables" in result["data"]


class TestMcpBridgeGenerate:
    def test_generate_retail_small(self):
        result = cmd_generate({
            "domain": "retail",
            "scale": "small",
            "seed": 42,
            "format": "summary",
        })
        assert result["status"] == "ok"

    def test_generate_returns_summary(self):
        result = cmd_generate({
            "domain": "retail",
            "scale": "small",
            "seed": 42,
            "format": "summary",
        })
        assert "summary" in result["data"] or "tables" in result["data"]


class TestMcpBridgePreview:
    def test_preview_returns_rows(self):
        result = cmd_preview({
            "domain": "retail",
            "rows": 5,
            "seed": 42,
        })
        assert result["status"] == "ok"
        assert "tables" in result["data"] or "preview" in result["data"]


class TestMcpBridgeProfileInfo:
    def test_profile_info_retail(self):
        result = cmd_profile_info({"domain": "retail"})
        assert result["status"] == "ok"


class TestMcpBridgeValidate:
    def test_validate_missing_file_errors(self):
        result = cmd_validate({"schema_path": "/nonexistent/path.spindle.json"})
        assert result["status"] == "error"
