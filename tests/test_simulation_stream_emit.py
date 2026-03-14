"""Tests for StreamEmitter, StreamEmitConfig, and helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.simulation.stream_emit import (
    StreamEmitConfig,
    StreamEmitResult,
    StreamEmitter,
    _clean_value,
    _wrap_envelope,
)
from sqllocks_spindle.streaming.sinks.console_sink import ConsoleSink
from sqllocks_spindle.streaming.stream_writer import StreamWriter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_tables():
    n = 30
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


class CaptureSink(StreamWriter):
    """Sink that captures all sent events for inspection."""

    def __init__(self):
        self.sent: list[dict[str, Any]] = []

    def send(self, event: dict[str, Any]) -> None:
        self.sent.append(event)

    def send_batch(self, events: list[dict[str, Any]]) -> None:
        self.sent.extend(events)


# ---------------------------------------------------------------------------
# StreamEmitConfig defaults
# ---------------------------------------------------------------------------

class TestStreamEmitConfig:
    def test_defaults(self):
        cfg = StreamEmitConfig()
        assert cfg.rate_per_sec == pytest.approx(10.0)
        assert cfg.sink_type == "console"
        assert cfg.max_events is None
        assert cfg.realtime is False
        assert cfg.seed == 42

    def test_total_events_property(self):
        result = StreamEmitResult(
            events_sent=100,
            replay_events_sent=20,
            topics_used={"orders"},
            elapsed_seconds=1.0,
            schema_versions={},
        )
        assert result.total_events == 120


# ---------------------------------------------------------------------------
# StreamEmitter — basic emit
# ---------------------------------------------------------------------------

class TestStreamEmitterBasic:
    def test_emit_returns_result(self, simple_tables):
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=StreamEmitConfig(), sink=sink)
        result = emitter.emit()
        assert isinstance(result, StreamEmitResult)

    def test_events_sent_equals_total_rows(self, simple_tables):
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=StreamEmitConfig(), sink=sink)
        result = emitter.emit()
        total_rows = sum(len(df) for df in simple_tables.values())
        assert result.events_sent == total_rows

    def test_sink_receives_all_events(self, simple_tables):
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=StreamEmitConfig(), sink=sink)
        emitter.emit()
        assert len(sink.sent) == sum(len(df) for df in simple_tables.values())

    def test_elapsed_seconds_nonnegative(self, simple_tables):
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=StreamEmitConfig(), sink=sink)
        result = emitter.emit()
        assert result.elapsed_seconds >= 0


# ---------------------------------------------------------------------------
# Topic mapping
# ---------------------------------------------------------------------------

class TestStreamEmitterTopics:
    def test_topic_defaults_to_table_name(self, simple_tables):
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=StreamEmitConfig(), sink=sink)
        result = emitter.emit()
        assert "order" in result.topics_used
        assert "customer" in result.topics_used

    def test_explicit_topics_mapped_positionally(self, simple_tables):
        cfg = StreamEmitConfig(topics=["orders_topic", "customers_topic"])
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=cfg, sink=sink)
        result = emitter.emit()
        assert "orders_topic" in result.topics_used or "customers_topic" in result.topics_used

    def test_single_topic_broadcast_to_all_tables(self, simple_tables):
        cfg = StreamEmitConfig(topics=["all_events"])
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=cfg, sink=sink)
        result = emitter.emit()
        assert "all_events" in result.topics_used

    def test_schema_versions_tracked_per_topic(self, simple_tables):
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=StreamEmitConfig(), sink=sink)
        result = emitter.emit()
        assert len(result.schema_versions) > 0


# ---------------------------------------------------------------------------
# max_events
# ---------------------------------------------------------------------------

class TestStreamEmitterMaxEvents:
    def test_max_events_caps_sent_count(self, simple_tables):
        cfg = StreamEmitConfig(max_events=5)
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=cfg, sink=sink)
        result = emitter.emit()
        assert result.events_sent == 5
        assert len(sink.sent) == 5


# ---------------------------------------------------------------------------
# Out-of-order
# ---------------------------------------------------------------------------

class TestStreamEmitterOutOfOrder:
    def test_out_of_order_shuffles_some_events(self, simple_tables):
        cfg = StreamEmitConfig(out_of_order_probability=0.5, seed=42)
        sink_a = CaptureSink()
        sink_b = CaptureSink()
        emitter_a = StreamEmitter(tables=simple_tables, config=StreamEmitConfig(seed=42), sink=sink_a)
        emitter_b = StreamEmitter(tables=simple_tables, config=cfg, sink=sink_b)
        emitter_a.emit()
        emitter_b.emit()
        ids_a = [e.get("data", {}).get("order_id") for e in sink_a.sent if "order_id" in e.get("data", {})]
        ids_b = [e.get("data", {}).get("order_id") for e in sink_b.sent if "order_id" in e.get("data", {})]
        # Out-of-order emission should produce a different sequence
        assert ids_a != ids_b


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------

class TestStreamEmitterReplay:
    def test_replay_adds_replay_events(self, simple_tables):
        cfg = StreamEmitConfig(
            replay_enabled=True,
            replay_probability=1.0,  # always trigger replay
            replay_burst_size=3,
            seed=0,
        )
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=cfg, sink=sink)
        result = emitter.emit()
        assert result.replay_events_sent > 0

    def test_replay_disabled_no_replay_events(self, simple_tables):
        cfg = StreamEmitConfig(replay_enabled=False, seed=0)
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=cfg, sink=sink)
        result = emitter.emit()
        assert result.replay_events_sent == 0


# ---------------------------------------------------------------------------
# Envelope fields
# ---------------------------------------------------------------------------

class TestStreamEmitterEnvelopeFields:
    def test_envelope_has_required_fields(self, simple_tables):
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=StreamEmitConfig(), sink=sink)
        emitter.emit()
        assert len(sink.sent) > 0
        event = sink.sent[0]
        assert "id" in event
        assert "source" in event
        assert "type" in event
        assert "time" in event
        assert "data" in event

    def test_data_contains_spindle_table_field(self, simple_tables):
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=StreamEmitConfig(), sink=sink)
        emitter.emit()
        event = sink.sent[0]
        assert "_spindle_table" in event["data"]

    def test_data_contains_spindle_seq(self, simple_tables):
        sink = CaptureSink()
        emitter = StreamEmitter(tables=simple_tables, config=StreamEmitConfig(), sink=sink)
        emitter.emit()
        event = sink.sent[0]
        assert "_spindle_seq" in event["data"]


# ---------------------------------------------------------------------------
# Sink construction
# ---------------------------------------------------------------------------

class TestStreamEmitterSinkConstruction:
    def test_console_sink_is_default(self, simple_tables):
        cfg = StreamEmitConfig(sink_type="console")
        emitter = StreamEmitter(tables=simple_tables, config=cfg)
        assert emitter._sink is not None

    def test_file_sink_writes_jsonl(self, simple_tables, tmp_path):
        out_path = str(tmp_path / "events.jsonl")
        cfg = StreamEmitConfig(
            sink_type="file",
            sink_connection={"path": out_path, "mode": "w"},
            max_events=5,
        )
        emitter = StreamEmitter(tables=simple_tables, config=cfg)
        emitter.emit()
        assert Path(out_path).exists()
        lines = Path(out_path).read_text().strip().splitlines()
        assert len(lines) == 5

    def test_unknown_sink_raises_value_error(self, simple_tables):
        cfg = StreamEmitConfig(sink_type="kafka_v999")
        with pytest.raises(ValueError, match="Unknown sink_type"):
            StreamEmitter(tables=simple_tables, config=cfg)


# ---------------------------------------------------------------------------
# _wrap_envelope
# ---------------------------------------------------------------------------

class TestWrapEnvelope:
    def test_all_required_fields_present(self):
        event = {"order_id": 1}
        envelope = _wrap_envelope(event, topic="orders")
        for field in ("id", "source", "type", "time", "specversion", "topic", "data"):
            assert field in envelope

    def test_topic_set(self):
        envelope = _wrap_envelope({}, topic="my_topic")
        assert envelope["topic"] == "my_topic"

    def test_type_includes_topic(self):
        envelope = _wrap_envelope({}, topic="orders")
        assert "orders" in envelope["type"]

    def test_correlation_id_auto_generated(self):
        envelope = _wrap_envelope({}, topic="t")
        assert envelope["correlation_id"] != ""

    def test_correlation_id_explicit(self):
        envelope = _wrap_envelope({}, topic="t", correlation_id="fixed-id")
        assert envelope["correlation_id"] == "fixed-id"

    def test_schema_version_default(self):
        envelope = _wrap_envelope({}, topic="t")
        assert envelope["schema_version"] == "1.0"

    def test_source_override(self):
        envelope = _wrap_envelope({}, topic="t", source="my_service")
        assert envelope["source"] == "my_service"

    def test_data_contains_original_event(self):
        event = {"order_id": 42, "amount": 99.9}
        envelope = _wrap_envelope(event, topic="orders")
        assert envelope["data"]["order_id"] == 42


# ---------------------------------------------------------------------------
# _clean_value
# ---------------------------------------------------------------------------

class TestCleanValue:
    def test_numpy_int64_to_python_int(self):
        assert _clean_value(np.int64(7)) == 7
        assert isinstance(_clean_value(np.int64(7)), int)

    def test_numpy_float64_to_python_float(self):
        result = _clean_value(np.float64(3.14))
        assert isinstance(result, float)

    def test_numpy_nan_to_none(self):
        assert _clean_value(np.float64("nan")) is None

    def test_numpy_bool_to_python_bool(self):
        result = _clean_value(np.bool_(True))
        assert result is True
        assert isinstance(result, bool)

    def test_pandas_timestamp_to_str(self):
        ts = pd.Timestamp("2024-01-15")
        result = _clean_value(ts)
        assert isinstance(result, str)
        assert "2024-01-15" in result

    def test_python_float_nan_to_none(self):
        import math
        assert _clean_value(float("nan")) is None

    def test_regular_string_unchanged(self):
        assert _clean_value("hello") == "hello"

    def test_regular_int_unchanged(self):
        assert _clean_value(42) == 42

    def test_none_passthrough(self):
        assert _clean_value(None) is None


# ---------------------------------------------------------------------------
# Sink routing contract: send() vs send_batch()
# ---------------------------------------------------------------------------

class TestSinkRoutingContract:
    def test_events_routed_through_send_not_send_batch(self, simple_tables):
        """StreamEmitter routes events via send() one at a time, not send_batch()."""
        send_count = 0
        send_batch_count = 0

        class RoutingCaptureSink(StreamWriter):
            def send(self, event):
                nonlocal send_count
                send_count += 1

            def send_batch(self, events):
                nonlocal send_batch_count
                send_batch_count += 1

        cfg = StreamEmitConfig(max_events=5)
        emitter = StreamEmitter(tables=simple_tables, config=cfg, sink=RoutingCaptureSink())
        emitter.emit()
        assert send_count == 5
        assert send_batch_count == 0
