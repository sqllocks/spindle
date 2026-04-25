"""E2E tests: JsonFormatter, RunMetrics, ManifestBuilder."""

from __future__ import annotations

import json
import logging

import pytest

from sqllocks_spindle.observability import JsonFormatter, RunMetrics, configure_logging


class TestJsonFormatter:
    def test_json_formatter_produces_valid_json(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="test.py",
            lineno=1, msg="test message", args=(), exc_info=None,
        )
        output = formatter.format(record)
        data = json.loads(output)
        assert data["message"] == "test message"
        assert "timestamp" in data or "time" in data or "asctime" in data


class TestRunMetrics:
    def test_metrics_lifecycle(self):
        metrics = RunMetrics(run_id="test-run-001")
        metrics.start_table("customer")
        metrics.end_table("customer", rows=1000, columns=10)
        metrics.start_table("order")
        metrics.end_table("order", rows=5000, columns=8)
        summary = metrics.finish()
        assert summary is not None
        assert "customer" in str(summary) or len(summary) > 0

    def test_metrics_to_json(self):
        metrics = RunMetrics(run_id="test-run-001")
        metrics.start_table("test")
        metrics.end_table("test", rows=100, columns=5)
        metrics.finish()
        json_str = metrics.to_json()
        data = json.loads(json_str)
        assert isinstance(data, (dict, list))

    def test_record_event(self):
        metrics = RunMetrics(run_id="test-run-001")
        metrics.record_event("generation_start", domain="retail", scale="small")
        metrics.record_event("generation_end", rows=10000)
        summary = metrics.finish()
        assert summary is not None


class TestConfigureLogging:
    def test_configure_logging_returns_logger(self):
        logger = configure_logging(level="INFO", logger_name="test_spindle")
        assert logger is not None
        assert logger.name == "test_spindle"
