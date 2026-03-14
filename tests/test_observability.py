"""Tests for observability module (E17)."""

from __future__ import annotations

import json
import logging
import time

import pytest

from sqllocks_spindle.observability import (
    JsonFormatter,
    RunMetrics,
    configure_logging,
)


# ---------------------------------------------------------------------------
# JsonFormatter
# ---------------------------------------------------------------------------

class TestJsonFormatter:
    def test_format_produces_valid_json(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello world", args=None, exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["message"] == "hello world"
        assert parsed["level"] == "INFO"

    def test_extra_fields_included(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.WARNING, pathname="", lineno=0,
            msg="event", args=None, exc_info=None,
        )
        record.custom_field = "custom_value"
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["custom_field"] == "custom_value"

    def test_timestamp_is_iso_format(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="ts test", args=None, exc_info=None,
        )
        parsed = json.loads(formatter.format(record))
        assert "T" in parsed["timestamp"]

    def test_exception_included(self):
        formatter = JsonFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys
            record = logging.LogRecord(
                name="test", level=logging.ERROR, pathname="", lineno=0,
                msg="error", args=None, exc_info=sys.exc_info(),
            )
        parsed = json.loads(formatter.format(record))
        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]


# ---------------------------------------------------------------------------
# configure_logging
# ---------------------------------------------------------------------------

class TestConfigureLogging:
    def test_returns_logger(self):
        logger = configure_logging(logger_name="test_obs_config")
        assert isinstance(logger, logging.Logger)
        # Cleanup
        logger.handlers.clear()

    def test_sets_level(self):
        logger = configure_logging(level="DEBUG", logger_name="test_obs_level")
        assert logger.level == logging.DEBUG
        logger.handlers.clear()

    def test_no_duplicate_handlers(self):
        name = "test_obs_dup"
        logger1 = configure_logging(logger_name=name)
        handler_count_1 = len(logger1.handlers)
        logger2 = configure_logging(logger_name=name)
        assert len(logger2.handlers) == handler_count_1
        logger1.handlers.clear()


# ---------------------------------------------------------------------------
# RunMetrics
# ---------------------------------------------------------------------------

class TestRunMetrics:
    def test_start_and_end_table(self):
        metrics = RunMetrics(run_id="test_run")
        metrics.start_table("customer")
        metrics.end_table("customer", rows=100, columns=5)
        summary = metrics.finish()
        assert "customer" in summary["tables"]
        assert summary["tables"]["customer"]["rows"] == 100
        assert summary["tables"]["customer"]["columns"] == 5

    def test_elapsed_seconds_positive(self):
        metrics = RunMetrics(run_id="test_run")
        metrics.start_table("order")
        time.sleep(0.01)
        metrics.end_table("order", rows=50, columns=3)
        summary = metrics.finish()
        assert summary["tables"]["order"]["elapsed_seconds"] > 0

    def test_total_rows_sum(self):
        metrics = RunMetrics(run_id="test_run")
        metrics.start_table("a")
        metrics.end_table("a", rows=100, columns=3)
        metrics.start_table("b")
        metrics.end_table("b", rows=200, columns=5)
        summary = metrics.finish()
        assert summary["total_rows"] == 300
        assert summary["total_tables"] == 2

    def test_record_event(self):
        metrics = RunMetrics(run_id="test_run")
        metrics.record_event("chaos_injected", category="value", count=12)
        summary = metrics.finish()
        assert len(summary["events"]) == 1
        assert summary["events"][0]["type"] == "chaos_injected"
        assert summary["events"][0]["category"] == "value"

    def test_to_json_valid(self):
        metrics = RunMetrics(run_id="test_run")
        metrics.start_table("t")
        metrics.end_table("t", rows=10, columns=2)
        json_str = metrics.to_json()
        parsed = json.loads(json_str)
        assert parsed["run_id"] == "test_run"

    def test_total_elapsed_seconds(self):
        metrics = RunMetrics(run_id="test_run")
        time.sleep(0.01)
        summary = metrics.finish()
        assert summary["total_elapsed_seconds"] > 0

    def test_multiple_events(self):
        metrics = RunMetrics(run_id="test_run")
        metrics.record_event("start", phase="init")
        metrics.record_event("complete", phase="gen")
        summary = metrics.finish()
        assert len(summary["events"]) == 2
