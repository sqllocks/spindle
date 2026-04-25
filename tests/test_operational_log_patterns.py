"""Tests for operational logs / observability patterns (E7)."""

from __future__ import annotations

import pandas as pd
import pytest

from sqllocks_spindle.simulation.operational_log_patterns import (
    OperationalLogConfig,
    OperationalLogResult,
    OperationalLogSimulator,
)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestOperationalLogConfig:
    def test_defaults(self):
        cfg = OperationalLogConfig()
        assert cfg.service_count == 5
        assert cfg.duration_hours == 24.0
        assert cfg.trace_enabled is True
        assert cfg.outage_enabled is True
        assert cfg.latency_spike_enabled is True
        assert cfg.seed == 42


# ---------------------------------------------------------------------------
# Simulator basics
# ---------------------------------------------------------------------------

class TestOperationalLogSimulator:
    @pytest.fixture
    def small_config(self):
        return OperationalLogConfig(
            service_count=3,
            duration_hours=2.0,
            events_per_hour=20.0,
            seed=42,
        )

    def test_run_returns_result(self, small_config):
        result = OperationalLogSimulator(config=small_config).run()
        assert isinstance(result, OperationalLogResult)

    def test_logs_returned(self, small_config):
        result = OperationalLogSimulator(config=small_config).run()
        assert isinstance(result.logs, pd.DataFrame)
        assert len(result.logs) > 0

    def test_traces_returned(self, small_config):
        result = OperationalLogSimulator(config=small_config).run()
        assert isinstance(result.traces, pd.DataFrame)

    def test_service_health_returned(self, small_config):
        result = OperationalLogSimulator(config=small_config).run()
        assert isinstance(result.service_health, pd.DataFrame)
        assert len(result.service_health) == small_config.service_count

    def test_stats_populated(self, small_config):
        result = OperationalLogSimulator(config=small_config).run()
        assert "total_events" in result.stats
        assert result.stats["total_events"] > 0

    def test_deterministic_with_seed(self):
        cfg = OperationalLogConfig(
            service_count=2, duration_hours=1.0, events_per_hour=10.0, seed=99,
        )
        r1 = OperationalLogSimulator(config=cfg).run()
        r2 = OperationalLogSimulator(config=cfg).run()
        assert r1.stats == r2.stats


# ---------------------------------------------------------------------------
# Log events
# ---------------------------------------------------------------------------

class TestLogEvents:
    @pytest.fixture
    def result(self):
        cfg = OperationalLogConfig(
            service_count=3, duration_hours=2.0, events_per_hour=30.0, seed=42,
        )
        return OperationalLogSimulator(config=cfg).run()

    def test_required_columns(self, result):
        required = {
            "log_id", "timestamp", "service", "level",
            "method", "endpoint", "status_code", "latency_ms", "message",
        }
        assert required.issubset(set(result.logs.columns))

    def test_latency_positive(self, result):
        assert (result.logs["latency_ms"] > 0).all()

    def test_status_codes_are_ints(self, result):
        assert result.logs["status_code"].dtype in ("int64", "int32", "Int64")

    def test_log_levels_valid(self, result):
        valid = {"INFO", "WARN", "ERROR", "DEBUG", "FATAL"}
        assert set(result.logs["level"]).issubset(valid)


# ---------------------------------------------------------------------------
# Distributed traces
# ---------------------------------------------------------------------------

class TestTraces:
    def test_traces_have_trace_id(self):
        cfg = OperationalLogConfig(
            service_count=3, duration_hours=2.0, events_per_hour=50.0,
            trace_enabled=True, seed=42,
        )
        result = OperationalLogSimulator(config=cfg).run()
        if len(result.traces) > 0:
            assert "trace_id" in result.traces.columns
            assert "span_id" in result.traces.columns
            assert "parent_span_id" in result.traces.columns

    def test_traces_disabled(self):
        cfg = OperationalLogConfig(
            service_count=2, duration_hours=1.0, events_per_hour=20.0,
            trace_enabled=False, seed=42,
        )
        result = OperationalLogSimulator(config=cfg).run()
        assert len(result.traces) == 0


# ---------------------------------------------------------------------------
# Latency spikes
# ---------------------------------------------------------------------------

class TestLatencySpikes:
    def test_spikes_produce_high_latency(self):
        cfg = OperationalLogConfig(
            service_count=2, duration_hours=4.0, events_per_hour=50.0,
            latency_spike_enabled=True, latency_spike_probability=0.5,
            latency_spike_multiplier=20.0, seed=42,
        )
        result = OperationalLogSimulator(config=cfg).run()
        spike_logs = result.logs[result.logs["is_spike"] == True]
        if len(spike_logs) > 0:
            assert spike_logs["latency_ms"].mean() > cfg.latency_mean_ms


# ---------------------------------------------------------------------------
# Outages
# ---------------------------------------------------------------------------

class TestOutages:
    def test_outage_produces_errors(self):
        cfg = OperationalLogConfig(
            service_count=2, duration_hours=4.0, events_per_hour=50.0,
            outage_enabled=True, outage_probability=0.5,
            outage_error_rate=0.90, seed=42,
        )
        result = OperationalLogSimulator(config=cfg).run()
        outage_logs = result.logs[result.logs["is_outage"] == True]
        if len(outage_logs) > 0:
            error_rate = (outage_logs["status_code"] >= 500).mean()
            assert error_rate > 0.5


# ---------------------------------------------------------------------------
# Service health
# ---------------------------------------------------------------------------

class TestServiceHealth:
    def test_health_has_required_columns(self):
        cfg = OperationalLogConfig(
            service_count=3, duration_hours=2.0, events_per_hour=30.0, seed=42,
        )
        result = OperationalLogSimulator(config=cfg).run()
        required = {"service", "total_requests", "error_count", "error_rate", "p50_latency_ms", "p95_latency_ms"}
        assert required.issubset(set(result.service_health.columns))

    def test_one_row_per_service(self):
        cfg = OperationalLogConfig(
            service_count=4, duration_hours=1.0, events_per_hour=20.0, seed=42,
        )
        result = OperationalLogSimulator(config=cfg).run()
        assert len(result.service_health) == 4


# ---------------------------------------------------------------------------
# Error bursts
# ---------------------------------------------------------------------------

class TestErrorBursts:
    def test_error_burst_increases_errors(self):
        cfg = OperationalLogConfig(
            service_count=2, duration_hours=4.0, events_per_hour=20.0,
            error_burst_enabled=True, error_burst_probability=0.5,
            error_burst_count=100, seed=42,
        )
        result = OperationalLogSimulator(config=cfg).run()
        assert result.stats["total_errors"] > 0
