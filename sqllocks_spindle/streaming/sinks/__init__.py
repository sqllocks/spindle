"""Streaming sink implementations."""

from sqllocks_spindle.streaming.sinks.console_sink import ConsoleSink
from sqllocks_spindle.streaming.sinks.file_sink import FileSink

__all__ = ["ConsoleSink", "FileSink"]


def __getattr__(name: str):
    if name == "EventHubSink":
        from sqllocks_spindle.streaming.sinks.eventhub_sink import EventHubSink
        return EventHubSink
    if name == "KafkaSink":
        from sqllocks_spindle.streaming.sinks.kafka_sink import KafkaSink
        return KafkaSink
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
