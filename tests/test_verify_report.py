# tests/test_verify_report.py
"""Tests for VerifyReport rendering."""

from __future__ import annotations

import json

import pytest

from sqllocks_spindle.verify.report import VerifyReport, VerifyResult
from sqllocks_spindle.validation.gates import GateResult


def _make_result(passed: bool = True, errors: list[str] | None = None) -> VerifyResult:
    gate_result = GateResult(
        gate_name="null_constraint",
        passed=passed,
        errors=errors or [],
        warnings=["minor drift"],
    )
    return VerifyResult(
        passed=passed,
        gate_results=[gate_result],
        row_counts={"orders": 1000, "customers": 250},
        run_at="2026-05-04T14:00:00Z",
        data_path="./output/",
        schema_path="retail.spindle.json",
        statistical=True,
        spindle_version="2.13.0",
    )


class TestVerifyReport:
    def test_to_json_is_valid(self):
        result = _make_result()
        report = VerifyReport(result)
        raw = report.to_json()
        data = json.loads(raw)
        assert data["passed"] is True
        assert data["spindle_version"] == "2.13.0"
        assert data["row_counts"]["orders"] == 1000
        assert len(data["gates"]) == 1

    def test_to_json_failed_result(self):
        result = _make_result(passed=False, errors=["orders.id has 3 nulls"])
        report = VerifyReport(result)
        data = json.loads(report.to_json())
        assert data["passed"] is False
        assert data["gates"][0]["errors"] == ["orders.id has 3 nulls"]

    def test_to_markdown_contains_summary_table(self):
        result = _make_result()
        report = VerifyReport(result)
        md = report.to_markdown()
        assert "## Summary" in md
        assert "null_constraint" in md
        assert "PASS" in md

    def test_to_markdown_contains_row_counts(self):
        result = _make_result()
        report = VerifyReport(result)
        md = report.to_markdown()
        assert "Row Counts" in md
        assert "1,000" in md
        assert "250" in md

    def test_to_markdown_contains_methodology(self):
        result = _make_result()
        report = VerifyReport(result)
        md = report.to_markdown()
        assert "## Methodology" in md
        assert "spindle verify" in md
        assert "retail.spindle.json" in md

    def test_to_markdown_overall_pass_banner(self):
        result = _make_result(passed=True)
        report = VerifyReport(result)
        assert "PASS" in report.to_markdown()

    def test_to_markdown_overall_fail_banner(self):
        result = _make_result(passed=False, errors=["something broke"])
        report = VerifyReport(result)
        assert "FAIL" in report.to_markdown()

    def test_reproduce_command_in_markdown(self):
        result = _make_result()
        report = VerifyReport(result)
        md = report.to_markdown()
        assert "spindle verify" in md
        assert "./output/" in md
        assert "--schema retail.spindle.json" in md
        assert "--statistical" in md
