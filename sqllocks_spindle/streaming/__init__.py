"""Spindle Phase 2 — Streaming engine."""

from sqllocks_spindle.streaming.config import BurstWindow, StreamConfig, TimePattern
from sqllocks_spindle.streaming.stream_writer import StreamWriter
from sqllocks_spindle.streaming.multi_writer import (
    StreamingMultiWriter,
    StreamingMultiWriteResult,
    SinkResult,
)
from sqllocks_spindle.streaming.anomaly import (
    Anomaly,
    AnomalyRegistry,
    CollectiveAnomaly,
    ContextualAnomaly,
    PointAnomaly,
)
from sqllocks_spindle.streaming.sinks.console_sink import ConsoleSink
from sqllocks_spindle.streaming.sinks.file_sink import FileSink
from sqllocks_spindle.streaming.streamer import SpindleStreamer, StreamResult

__all__ = [
    "BurstWindow",
    "StreamConfig",
    "TimePattern",
    "StreamWriter",
    "Anomaly",
    "AnomalyRegistry",
    "CollectiveAnomaly",
    "ContextualAnomaly",
    "PointAnomaly",
    "ConsoleSink",
    "FileSink",
    "SpindleStreamer",
    "StreamResult",
    "StreamingMultiWriter",
    "StreamingMultiWriteResult",
    "SinkResult",
]


def __getattr__(name: str):
    if name == "EventHubSink":
        from sqllocks_spindle.streaming.sinks.eventhub_sink import EventHubSink
        return EventHubSink
    if name == "KafkaSink":
        from sqllocks_spindle.streaming.sinks.kafka_sink import KafkaSink
        return KafkaSink
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
