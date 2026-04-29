"""Tests for StreamingMultiWriter — parallel fan-out to multiple streaming sinks."""

from __future__ import annotations

import io
import threading
from collections import defaultdict
from typing import Any, Iterator

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.streaming.multi_writer import (
    SinkResult,
    StreamingMultiWriteResult,
    StreamingMultiWriter,
)
from sqllocks_spindle.streaming.stream_writer import StreamWriter


# ---------------------------------------------------------------------------
# Test sink implementations
# ---------------------------------------------------------------------------


class CaptureSink(StreamWriter):
    """Collects all received events into a list."""

    def __init__(self) -> None:
        self.received: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def send_batch(self, events: list[dict[str, Any]]) -> None:
        with self._lock:
            self.received.extend(events)

    @property
    def event_count(self) -> int:
        return len(self.received)


class FailingSink(StreamWriter):
    """Always raises on send_batch."""

    def send_batch(self, events: list[dict[str, Any]]) -> None:
        raise RuntimeError("Intentional sink failure")


class SlowSink(StreamWriter):
    """Introduces a small delay to test true parallelism."""

    def __init__(self, delay: float = 0.01) -> None:
        self.received: list[dict[str, Any]] = []
        self.delay = delay

    def send_batch(self, events: list[dict[str, Any]]) -> None:
        import time
        time.sleep(self.delay)
        self.received.extend(events)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_stream(n_tables: int = 3, n_rows: int = 50) -> Iterator[tuple[str, pd.DataFrame]]:
    rng = np.random.default_rng(0)
    for i in range(n_tables):
        df = pd.DataFrame({
            "id": np.arange(n_rows),
            "value": rng.normal(0, 1, n_rows),
            "label": rng.choice(["A", "B", "C"], n_rows),
        })
        yield f"table_{i}", df


# ---------------------------------------------------------------------------
# Basic functionality
# ---------------------------------------------------------------------------

def test_smw_streams_to_single_sink():
    sink = CaptureSink()
    smw = StreamingMultiWriter(capture=sink)
    result = smw.stream(_make_stream(n_tables=2, n_rows=50))
    assert result.total_tables == 2
    assert sink.event_count == 100  # 2 tables × 50 rows


def test_smw_streams_to_multiple_sinks():
    sink_a = CaptureSink()
    sink_b = CaptureSink()
    smw = StreamingMultiWriter(a=sink_a, b=sink_b)
    result = smw.stream(_make_stream(n_tables=3, n_rows=20))
    assert sink_a.event_count == 60
    assert sink_b.event_count == 60
    assert len(result.sinks) == 2


def test_smw_parallel_4_sinks():
    """All 4 streaming sink types (mock) receive identical event counts."""
    sinks = {f"sink_{i}": CaptureSink() for i in range(4)}
    smw = StreamingMultiWriter(**sinks)
    result = smw.stream(_make_stream(n_tables=5, n_rows=30))
    event_counts = {name: sinks[name].event_count for name in sinks}
    # All sinks should receive the same count
    assert len(set(event_counts.values())) == 1
    assert all(c == 150 for c in event_counts.values())


def test_smw_result_success():
    sink = CaptureSink()
    smw = StreamingMultiWriter(capture=sink)
    result = smw.stream(_make_stream())
    assert result.success


def test_smw_result_has_elapsed():
    sink = CaptureSink()
    smw = StreamingMultiWriter(capture=sink)
    result = smw.stream(_make_stream())
    assert result.elapsed_seconds >= 0


# ---------------------------------------------------------------------------
# Event format
# ---------------------------------------------------------------------------

def test_smw_events_have_table_key():
    sink = CaptureSink()
    smw = StreamingMultiWriter(capture=sink)
    smw.stream(_make_stream(n_tables=1, n_rows=5))
    for event in sink.received:
        assert "_table" in event


def test_smw_events_contain_original_columns():
    sink = CaptureSink()
    smw = StreamingMultiWriter(capture=sink)
    smw.stream(_make_stream(n_tables=1, n_rows=5))
    for event in sink.received:
        assert "id" in event
        assert "value" in event
        assert "label" in event


def test_smw_table_key_matches_table_name():
    sink = CaptureSink()
    smw = StreamingMultiWriter(capture=sink)
    smw.stream(_make_stream(n_tables=2, n_rows=10))
    tables_seen = {e["_table"] for e in sink.received}
    assert tables_seen == {"table_0", "table_1"}


# ---------------------------------------------------------------------------
# Batch size
# ---------------------------------------------------------------------------

def test_smw_batch_size_respected():
    """Verify send_batch is called with batches ≤ batch_size."""
    batch_sizes_seen: list[int] = []

    class BatchTrackingSink(StreamWriter):
        def send_batch(self, events: list[dict[str, Any]]) -> None:
            batch_sizes_seen.append(len(events))

    sink = BatchTrackingSink()
    smw = StreamingMultiWriter(batch_size=10, capture=sink)
    smw.stream(_make_stream(n_tables=1, n_rows=35))
    assert all(bs <= 10 for bs in batch_sizes_seen)
    assert sum(batch_sizes_seen) == 35


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_smw_partial_failure_continues():
    """One failing sink should not stop a passing sink."""
    good_sink = CaptureSink()
    bad_sink = FailingSink()
    smw = StreamingMultiWriter(good=good_sink, bad=bad_sink, stop_on_sink_error=False)
    result = smw.stream(_make_stream(n_tables=2, n_rows=20))
    assert good_sink.event_count == 40
    assert result.partial_failure


def test_smw_partial_failure_result_not_success():
    good_sink = CaptureSink()
    bad_sink = FailingSink()
    smw = StreamingMultiWriter(good=good_sink, bad=bad_sink)
    result = smw.stream(_make_stream())
    assert not result.success
    assert result.partial_failure


def test_smw_failed_sink_has_errors():
    smw = StreamingMultiWriter(bad=FailingSink(), stop_on_sink_error=False)
    result = smw.stream(_make_stream(n_tables=1, n_rows=5))
    bad_sr = next(s for s in result.sinks if s.sink_name == "bad")
    assert not bad_sr.success
    assert bad_sr.error_count >= 1


def test_smw_all_fail_is_not_partial():
    smw = StreamingMultiWriter(bad1=FailingSink(), bad2=FailingSink())
    result = smw.stream(_make_stream(n_tables=1, n_rows=5))
    assert not result.success
    assert not result.partial_failure  # all failed → not partial


def test_smw_requires_at_least_one_sink():
    with pytest.raises(ValueError, match="one sink"):
        StreamingMultiWriter()


# ---------------------------------------------------------------------------
# Add / remove sinks
# ---------------------------------------------------------------------------

def test_smw_add_sink():
    sink_a = CaptureSink()
    sink_b = CaptureSink()
    smw = StreamingMultiWriter(a=sink_a)
    smw.add_sink("b", sink_b)
    assert "b" in smw.sink_names
    smw.stream(_make_stream(n_tables=1, n_rows=5))
    assert sink_b.event_count == 5


def test_smw_remove_sink():
    sink_a = CaptureSink()
    sink_b = CaptureSink()
    smw = StreamingMultiWriter(a=sink_a, b=sink_b)
    smw.remove_sink("b")
    assert "b" not in smw.sink_names
    smw.stream(_make_stream(n_tables=1, n_rows=5))
    assert sink_b.event_count == 0  # removed, received nothing


# ---------------------------------------------------------------------------
# stream_table API
# ---------------------------------------------------------------------------

def test_smw_stream_table_single():
    sink = CaptureSink()
    smw = StreamingMultiWriter(capture=sink)
    df = pd.DataFrame({"x": np.arange(20)})
    results = smw.stream_table("test", df)
    assert results["capture"] is True
    assert sink.event_count == 20


def test_smw_stream_table_failure_reported():
    smw = StreamingMultiWriter(bad=FailingSink())
    df = pd.DataFrame({"x": [1, 2, 3]})
    results = smw.stream_table("test", df)
    assert results["bad"] is False


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def test_smw_summary_contains_sink_names():
    sink = CaptureSink()
    smw = StreamingMultiWriter(my_sink=sink)
    result = smw.stream(_make_stream(n_tables=2, n_rows=10))
    summary = result.summary()
    assert "my_sink" in summary
    assert "StreamingMultiWriter" in summary


# ---------------------------------------------------------------------------
# Integration with Spindle generate_stream
# ---------------------------------------------------------------------------

@pytest.mark.slow
def test_smw_with_spindle_generate_stream():
    """Integration test: Spindle.generate_stream() → 4 capture sinks."""
    import importlib
    from sqllocks_spindle.cli import _get_domain_registry
    from sqllocks_spindle.engine.generator import Spindle

    registry = _get_domain_registry()
    mod_path, cls_name, _ = registry["retail"]
    module = importlib.import_module(mod_path)
    domain = getattr(module, cls_name)(schema_mode="3nf")

    sinks = {f"sink_{i}": CaptureSink() for i in range(4)}
    smw = StreamingMultiWriter(**sinks)

    spindle = Spindle()
    result = smw.stream(spindle.generate_stream(domain=domain, scale="small", seed=42))

    assert result.success
    assert result.total_tables >= 1
    # All 4 sinks should have received events
    for i in range(4):
        assert sinks[f"sink_{i}"].event_count > 0
    # All sinks should receive the same count
    counts = [sinks[f"sink_{i}"].event_count for i in range(4)]
    assert len(set(counts)) == 1
