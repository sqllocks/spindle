"""Tests for the Spindle streaming engine (Phase 2)."""

from __future__ import annotations

import io
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.streaming.config import BurstWindow, StreamConfig, TimePattern
from sqllocks_spindle.streaming.rate_limiter import TokenBucket, poisson_interarrival
from sqllocks_spindle.streaming.sinks.console_sink import ConsoleSink
from sqllocks_spindle.streaming.sinks.file_sink import FileSink
from sqllocks_spindle.streaming.stream_writer import StreamWriter
from sqllocks_spindle.streaming.streamer import SpindleStreamer, StreamResult, _clean_value


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def rng():
    return np.random.default_rng(42)


@pytest.fixture
def simple_tables():
    """Minimal pre-generated tables for streamer tests (no generation needed)."""
    n = 50
    rng = np.random.default_rng(1)
    return {
        "order": pd.DataFrame({
            "order_id": range(1, n + 1),
            "customer_id": rng.integers(1, 10, size=n),
            "total_amount": rng.uniform(10.0, 200.0, size=n).round(2),
            "order_date": pd.date_range("2024-01-01", periods=n, freq="h"),
        }),
        "customer": pd.DataFrame({
            "customer_id": range(1, 11),
            "name": [f"Customer {i}" for i in range(1, 11)],
        }),
    }


# ---------------------------------------------------------------------------
# BurstWindow
# ---------------------------------------------------------------------------

class TestBurstWindow:
    def test_is_active_inside_window(self):
        bw = BurstWindow(start_offset_seconds=10, duration_seconds=30, multiplier=5.0)
        assert bw.is_active(10.0)
        assert bw.is_active(20.0)
        assert bw.is_active(39.9)

    def test_is_not_active_before_window(self):
        bw = BurstWindow(start_offset_seconds=10, duration_seconds=30, multiplier=5.0)
        assert not bw.is_active(9.9)
        assert not bw.is_active(0.0)

    def test_is_not_active_after_window(self):
        bw = BurstWindow(start_offset_seconds=10, duration_seconds=30, multiplier=5.0)
        assert not bw.is_active(40.0)
        assert not bw.is_active(100.0)


# ---------------------------------------------------------------------------
# TimePattern
# ---------------------------------------------------------------------------

class TestTimePattern:
    def test_get_multiplier_defaults_to_one(self):
        tp = TimePattern()
        assert tp.get_multiplier(12, 0) == pytest.approx(1.0)

    def test_hour_multiplier_applied(self):
        tp = TimePattern(hour_multipliers={9: 2.5})
        assert tp.get_multiplier(9, 0) == pytest.approx(2.5)
        assert tp.get_multiplier(10, 0) == pytest.approx(1.0)

    def test_dow_multiplier_applied(self):
        tp = TimePattern(dow_multipliers={6: 0.2})  # Sunday = 6
        assert tp.get_multiplier(12, 6) == pytest.approx(0.2)
        assert tp.get_multiplier(12, 0) == pytest.approx(1.0)

    def test_composite_multiplier(self):
        tp = TimePattern(hour_multipliers={20: 2.0}, dow_multipliers={5: 1.5})
        assert tp.get_multiplier(20, 5) == pytest.approx(3.0)

    def test_business_hours_daytime_higher(self):
        tp = TimePattern.business_hours()
        assert tp.get_multiplier(12, 0) > tp.get_multiplier(3, 0)

    def test_business_hours_weekend_lower(self):
        tp = TimePattern.business_hours()
        assert tp.get_multiplier(12, 6) < tp.get_multiplier(12, 1)

    def test_retail_peak_evening_higher(self):
        tp = TimePattern.retail_peak()
        assert tp.get_multiplier(20, 0) > tp.get_multiplier(4, 0)

    def test_retail_peak_weekend_elevated(self):
        tp = TimePattern.retail_peak()
        assert tp.get_multiplier(12, 6) > tp.get_multiplier(12, 1)


# ---------------------------------------------------------------------------
# StreamConfig
# ---------------------------------------------------------------------------

class TestStreamConfig:
    def test_default_rate_multiplier_is_one(self):
        config = StreamConfig()
        assert config.get_rate_multiplier(0.0) == pytest.approx(1.0)

    def test_burst_window_multiplier(self):
        bw = BurstWindow(start_offset_seconds=5, duration_seconds=20, multiplier=10.0)
        config = StreamConfig(burst_windows=[bw])
        assert config.get_rate_multiplier(10.0) == pytest.approx(10.0)
        assert config.get_rate_multiplier(30.0) == pytest.approx(1.0)

    def test_time_pattern_multiplier_applied(self):
        tp = TimePattern(hour_multipliers={8: 3.0})
        config = StreamConfig(time_pattern=tp)
        assert config.get_rate_multiplier(0.0, wall_hour=8) == pytest.approx(3.0)
        assert config.get_rate_multiplier(0.0, wall_hour=9) == pytest.approx(1.0)

    def test_combined_burst_and_pattern(self):
        bw = BurstWindow(start_offset_seconds=0, duration_seconds=60, multiplier=5.0)
        tp = TimePattern(hour_multipliers={12: 2.0})
        config = StreamConfig(burst_windows=[bw], time_pattern=tp)
        assert config.get_rate_multiplier(5.0, wall_hour=12) == pytest.approx(10.0)

    def test_multiplier_never_zero(self):
        config = StreamConfig(time_pattern=TimePattern(hour_multipliers={0: 0.0}))
        assert config.get_rate_multiplier(0.0, wall_hour=0) > 0


# ---------------------------------------------------------------------------
# TokenBucket
# ---------------------------------------------------------------------------

class TestTokenBucket:
    def test_initial_tokens_available(self):
        """Bucket starts full — first consume should be free."""
        bucket = TokenBucket(rate=10.0)
        assert bucket.consume() == pytest.approx(0.0)

    def test_empty_bucket_returns_positive_wait(self):
        """After draining all tokens, consume returns a positive wait time."""
        # Use a fake clock to control time
        t = [0.0]
        def clock():
            return t[0]

        bucket = TokenBucket(rate=10.0, burst_capacity=2.0, clock=clock)
        # Drain two tokens
        bucket.consume()
        bucket.consume()
        # Now bucket is empty — consume should return > 0
        wait = bucket.consume()
        assert wait > 0.0

    def test_tokens_refill_over_time(self):
        t = [0.0]
        def clock():
            return t[0]

        bucket = TokenBucket(rate=10.0, burst_capacity=10.0, clock=clock)
        # Drain fully
        for _ in range(10):
            bucket.consume()

        # Advance time by 1 second → 10 tokens added
        t[0] = 1.0
        wait = bucket.consume()
        assert wait == pytest.approx(0.0)

    def test_update_rate(self):
        t = [0.0]
        bucket = TokenBucket(rate=10.0, clock=lambda: t[0])
        bucket.update_rate(50.0)
        # Drain all tokens
        for _ in range(int(bucket._capacity)):
            bucket.consume()
        t[0] = 0.1  # 0.1s at 50/s = 5 tokens
        assert bucket.consume() == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# poisson_interarrival
# ---------------------------------------------------------------------------

class TestPoissonInterarrival:
    def test_mean_approximately_correct(self):
        rng = np.random.default_rng(0)
        samples = [poisson_interarrival(10.0, rng) for _ in range(2000)]
        mean = np.mean(samples)
        assert mean == pytest.approx(0.1, rel=0.1)  # 1/10 = 0.1s

    def test_zero_rate_returns_inf(self):
        rng = np.random.default_rng(0)
        assert poisson_interarrival(0.0, rng) == float("inf")

    def test_all_positive(self):
        rng = np.random.default_rng(0)
        samples = [poisson_interarrival(5.0, rng) for _ in range(100)]
        assert all(s > 0 for s in samples)


# ---------------------------------------------------------------------------
# ConsoleSink
# ---------------------------------------------------------------------------

class TestConsoleSink:
    def test_send_writes_json(self):
        buf = io.StringIO()
        sink = ConsoleSink(file=buf)
        sink.send({"key": "value", "num": 42})
        output = buf.getvalue()
        data = json.loads(output.strip())
        assert data["key"] == "value"
        assert data["num"] == 42

    def test_send_batch_writes_multiple_lines(self):
        buf = io.StringIO()
        sink = ConsoleSink(file=buf)
        sink.send_batch([{"a": 1}, {"a": 2}, {"a": 3}])
        lines = [l for l in buf.getvalue().strip().split("\n") if l]
        assert len(lines) == 3

    def test_prefix_prepended(self):
        buf = io.StringIO()
        sink = ConsoleSink(prefix="EVENT: ", file=buf)
        sink.send({"x": 1})
        assert buf.getvalue().startswith("EVENT: ")

    def test_context_manager(self):
        buf = io.StringIO()
        with ConsoleSink(file=buf) as sink:
            sink.send({"ok": True})
        data = json.loads(buf.getvalue().strip())
        assert data["ok"] is True


# ---------------------------------------------------------------------------
# FileSink
# ---------------------------------------------------------------------------

class TestFileSink:
    def test_writes_jsonl(self, tmp_path):
        path = tmp_path / "events.jsonl"
        with FileSink(path, mode="w") as sink:
            sink.send_batch([{"id": 1}, {"id": 2}])
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["id"] == 1

    def test_appends_by_default(self, tmp_path):
        path = tmp_path / "events.jsonl"
        with FileSink(path, mode="w") as sink:
            sink.send({"n": 1})
        with FileSink(path, mode="a") as sink:
            sink.send({"n": 2})
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_overwrite_mode(self, tmp_path):
        path = tmp_path / "events.jsonl"
        with FileSink(path, mode="w") as sink:
            sink.send({"n": 1})
        with FileSink(path, mode="w") as sink:
            sink.send({"n": 2})
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 1
        assert json.loads(lines[0])["n"] == 2

    def test_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "deep" / "nested" / "events.jsonl"
        with FileSink(path, mode="w") as sink:
            sink.send({"ok": True})
        assert path.exists()


# ---------------------------------------------------------------------------
# EventHubSink / KafkaSink — import error tests
# ---------------------------------------------------------------------------

class TestSinkImportErrors:
    def test_eventhub_sink_raises_import_error(self):
        from unittest.mock import patch
        from sqllocks_spindle.streaming.sinks.eventhub_sink import EventHubSink
        import sys
        # Remove cached azure.eventhub modules AND block re-import via builtins
        azure_modules = [k for k in sys.modules if k.startswith("azure.eventhub")]
        saved = {k: sys.modules.pop(k) for k in azure_modules}
        original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__
        def _block_azure_eventhub(name, *args, **kwargs):
            if name == "azure.eventhub" or name.startswith("azure.eventhub."):
                raise ImportError("No module named 'azure.eventhub'")
            return original_import(name, *args, **kwargs)
        try:
            with patch("builtins.__import__", side_effect=_block_azure_eventhub):
                with pytest.raises(ImportError, match="azure-eventhub"):
                    EventHubSink("Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y")
        finally:
            sys.modules.update(saved)

    def test_kafka_sink_raises_import_error(self):
        from unittest.mock import patch
        from sqllocks_spindle.streaming.sinks.kafka_sink import KafkaSink
        import sys
        kafka_modules = [k for k in sys.modules if k.startswith("kafka")]
        saved = {k: sys.modules.pop(k) for k in kafka_modules}
        original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__
        def _block_kafka(name, *args, **kwargs):
            if name == "kafka" or name.startswith("kafka."):
                raise ImportError("No module named 'kafka'")
            return original_import(name, *args, **kwargs)
        try:
            with patch("builtins.__import__", side_effect=_block_kafka):
                with pytest.raises(ImportError, match="kafka-python"):
                    KafkaSink("localhost:9092", "test-topic")
        finally:
            sys.modules.update(saved)


# ---------------------------------------------------------------------------
# SpindleStreamer
# ---------------------------------------------------------------------------

class TestSpindleStreamer:
    def test_stream_to_file(self, tmp_path, simple_tables):
        path = tmp_path / "events.jsonl"
        with FileSink(path, mode="w") as sink:
            result = SpindleStreamer(
                tables=simple_tables,
                sink=sink,
                config=StreamConfig(max_events=20),
            ).stream("order")
        assert result.events_sent == 20
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 20
        first = json.loads(lines[0])
        assert first["_spindle_table"] == "order"
        assert "_spindle_seq" in first

    def test_stream_all(self, tmp_path, simple_tables):
        buf = io.StringIO()
        sink = ConsoleSink(file=buf)
        results = SpindleStreamer(
            tables=simple_tables,
            sink=sink,
        ).stream_all()
        assert len(results) == 2
        table_names = {r.table for r in results}
        assert table_names == {"order", "customer"}

    def test_max_events_respected(self, simple_tables):
        buf = io.StringIO()
        result = SpindleStreamer(
            tables=simple_tables,
            sink=ConsoleSink(file=buf),
            config=StreamConfig(max_events=10),
        ).stream("order")
        assert result.events_sent == 10

    def test_unknown_table_raises(self, simple_tables):
        with pytest.raises(ValueError, match="not found"):
            SpindleStreamer(
                tables=simple_tables,
                sink=ConsoleSink(file=io.StringIO()),
            ).stream("nonexistent_table")

    def test_no_domain_or_tables_raises(self):
        with pytest.raises(ValueError, match="domain.*tables"):
            SpindleStreamer(sink=ConsoleSink(file=io.StringIO()))

    def test_events_contain_metadata(self, simple_tables):
        buf = io.StringIO()
        SpindleStreamer(
            tables=simple_tables,
            sink=ConsoleSink(file=buf),
            config=StreamConfig(max_events=5),
        ).stream("order")
        events = [json.loads(line) for line in buf.getvalue().strip().split("\n") if line]
        for event in events:
            assert "_spindle_table" in event
            assert "_spindle_seq" in event

    def test_datetime_column_produces_event_time(self, simple_tables):
        buf = io.StringIO()
        SpindleStreamer(
            tables=simple_tables,
            sink=ConsoleSink(file=buf),
            config=StreamConfig(max_events=5),
        ).stream("order")
        events = [json.loads(line) for line in buf.getvalue().strip().split("\n") if line]
        assert all("_spindle_event_time" in e for e in events)

    def test_stream_result_fields(self, simple_tables):
        result = SpindleStreamer(
            tables=simple_tables,
            sink=ConsoleSink(file=io.StringIO()),
            config=StreamConfig(max_events=10),
        ).stream("order")
        assert isinstance(result, StreamResult)
        assert result.table == "order"
        assert result.events_sent == 10
        assert result.anomaly_count == 0
        assert result.out_of_order_count == 0
        assert result.elapsed_seconds >= 0
        assert result.events_per_second_actual > 0

    def test_stream_result_repr(self, simple_tables):
        result = SpindleStreamer(
            tables=simple_tables,
            sink=ConsoleSink(file=io.StringIO()),
            config=StreamConfig(max_events=5),
        ).stream("order")
        r = repr(result)
        assert "StreamResult" in r
        assert "order" in r

    def test_out_of_order_reorders_events(self, simple_tables):
        result = SpindleStreamer(
            tables=simple_tables,
            sink=ConsoleSink(file=io.StringIO()),
            config=StreamConfig(out_of_order_fraction=0.3),
        ).stream("order")
        assert result.out_of_order_count > 0

    def test_zero_out_of_order_no_reorder(self, simple_tables):
        result = SpindleStreamer(
            tables=simple_tables,
            sink=ConsoleSink(file=io.StringIO()),
            config=StreamConfig(out_of_order_fraction=0.0),
        ).stream("order")
        assert result.out_of_order_count == 0

    def test_label_anomalies_false_strips_columns(self, simple_tables):
        from sqllocks_spindle.streaming.anomaly import AnomalyRegistry, PointAnomaly
        registry = AnomalyRegistry([PointAnomaly("big", column="total_amount", fraction=0.1)])
        buf = io.StringIO()
        SpindleStreamer(
            tables=simple_tables,
            sink=ConsoleSink(file=buf),
            config=StreamConfig(max_events=20, label_anomalies=False),
            anomaly_registry=registry,
        ).stream("order")
        events = [json.loads(l) for l in buf.getvalue().strip().split("\n") if l]
        for e in events:
            assert "_spindle_is_anomaly" not in e
            assert "_spindle_anomaly_type" not in e

    def test_label_anomalies_true_includes_columns(self, simple_tables):
        from sqllocks_spindle.streaming.anomaly import AnomalyRegistry, PointAnomaly
        registry = AnomalyRegistry([PointAnomaly("big", column="total_amount", fraction=0.2)])
        buf = io.StringIO()
        SpindleStreamer(
            tables=simple_tables,
            sink=ConsoleSink(file=buf),
            config=StreamConfig(label_anomalies=True),
            anomaly_registry=registry,
        ).stream("order")
        events = [json.loads(l) for l in buf.getvalue().strip().split("\n") if l]
        assert all("_spindle_is_anomaly" in e for e in events)

    def test_anomaly_count_in_result(self, simple_tables):
        from sqllocks_spindle.streaming.anomaly import AnomalyRegistry, PointAnomaly
        registry = AnomalyRegistry([PointAnomaly("big", column="total_amount", fraction=0.1)])
        result = SpindleStreamer(
            tables=simple_tables,
            sink=ConsoleSink(file=io.StringIO()),
            anomaly_registry=registry,
        ).stream("order")
        assert result.anomaly_count > 0

    def test_stream_with_retail_domain(self, tmp_path):
        """Integration test: actually generate retail data and stream it."""
        from sqllocks_spindle import RetailDomain
        path = tmp_path / "retail_events.jsonl"
        with FileSink(path, mode="w") as sink:
            result = SpindleStreamer(
                domain=RetailDomain(),
                sink=sink,
                config=StreamConfig(max_events=50),
                scale="small",
                seed=1,
            ).stream("order")
        assert result.events_sent == 50
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 50

    def test_batch_size_respected(self, simple_tables):
        """Verify batches are correctly sized by counting send_batch calls."""
        call_log = []

        class CountingSink(StreamWriter):
            def send_batch(self, events):
                call_log.append(len(events))

        SpindleStreamer(
            tables=simple_tables,
            sink=CountingSink(),
            config=StreamConfig(max_events=25, batch_size=10),
        ).stream("order")

        assert call_log == [10, 10, 5]


# ---------------------------------------------------------------------------
# _clean_value
# ---------------------------------------------------------------------------

class TestCleanValue:
    def test_numpy_int(self):
        assert _clean_value(np.int64(5)) == 5
        assert isinstance(_clean_value(np.int64(5)), int)

    def test_numpy_float(self):
        assert _clean_value(np.float64(3.14)) == pytest.approx(3.14)
        assert isinstance(_clean_value(np.float64(3.14)), float)

    def test_numpy_nan_becomes_none(self):
        assert _clean_value(np.float64("nan")) is None

    def test_python_float_nan_becomes_none(self):
        assert _clean_value(float("nan")) is None

    def test_pandas_timestamp(self):
        ts = pd.Timestamp("2024-06-15 12:30:00")
        result = _clean_value(ts)
        assert isinstance(result, str)
        assert "2024-06-15" in result

    def test_plain_string_unchanged(self):
        assert _clean_value("hello") == "hello"

    def test_none_unchanged(self):
        assert _clean_value(None) is None


# ---------------------------------------------------------------------------
# Package-level import checks
# ---------------------------------------------------------------------------

class TestPackageExports:
    def test_streaming_module_exports(self):
        from sqllocks_spindle import streaming  # noqa: F401
        from sqllocks_spindle.streaming import (
            AnomalyRegistry,
            BurstWindow,
            CollectiveAnomaly,
            ConsoleSink,
            ContextualAnomaly,
            FileSink,
            PointAnomaly,
            SpindleStreamer,
            StreamConfig,
            StreamResult,
            StreamWriter,
            TimePattern,
        )

    def test_lazy_sink_exports(self):
        from sqllocks_spindle.streaming import sinks
        assert hasattr(sinks, "ConsoleSink")
        assert hasattr(sinks, "FileSink")
