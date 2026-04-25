from __future__ import annotations

import threading

import numpy as np
import pytest


class _TrackingSink:
    """Test double that records calls."""

    def __init__(self):
        self.opened = False
        self.closed = False
        self.chunks: list[tuple[str, dict]] = []
        self.lock = threading.Lock()

    def open(self, schema):
        self.opened = True

    def write_chunk(self, table: str, arrays: dict):
        with self.lock:
            self.chunks.append((table, {k: list(v) for k, v in arrays.items()}))

    def close(self):
        self.closed = True


def test_sink_registry_opens_all_sinks():
    from sqllocks_spindle.engine.sink_registry import SinkRegistry

    s1, s2 = _TrackingSink(), _TrackingSink()
    registry = SinkRegistry([s1, s2])
    registry.open(schema=None)
    assert s1.opened and s2.opened


def test_sink_registry_closes_all_sinks():
    from sqllocks_spindle.engine.sink_registry import SinkRegistry

    s1, s2 = _TrackingSink(), _TrackingSink()
    registry = SinkRegistry([s1, s2])
    registry.open(schema=None)
    registry.close()
    assert s1.closed and s2.closed


def test_sink_registry_fan_out_to_all_sinks():
    from sqllocks_spindle.engine.sink_registry import SinkRegistry

    s1, s2 = _TrackingSink(), _TrackingSink()
    registry = SinkRegistry([s1, s2])
    registry.open(schema=None)

    arrays = {"id": np.array([1, 2, 3])}
    registry.write_chunk("orders", arrays)
    registry.close()

    assert len(s1.chunks) == 1
    assert len(s2.chunks) == 1
    assert s1.chunks[0][0] == "orders"
    assert s1.chunks[0][1]["id"] == [1, 2, 3]


def test_sink_registry_continues_on_one_sink_failure():
    from sqllocks_spindle.engine.sink_registry import SinkRegistry, SinkError

    class _FailingSink:
        def open(self, schema): pass
        def write_chunk(self, table, arrays): raise RuntimeError("write failed")
        def close(self): pass

    good = _TrackingSink()
    registry = SinkRegistry([_FailingSink(), good])
    registry.open(schema=None)

    with pytest.raises(SinkError):
        registry.write_chunk("t", {"x": np.array([1])})

    # Good sink still received the write attempt
    assert len(good.chunks) == 1


def test_sink_registry_empty_sinks_no_crash():
    from sqllocks_spindle.engine.sink_registry import SinkRegistry

    registry = SinkRegistry([])
    registry.open(schema=None)
    registry.write_chunk("t", {"x": np.array([1])})
    registry.close()
