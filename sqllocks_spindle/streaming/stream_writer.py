"""Abstract base class for Spindle streaming sinks."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class StreamWriter(ABC):
    """Abstract base for all streaming sinks.

    Concrete implementations provide the actual transport layer:
    :class:`~sqllocks_spindle.streaming.sinks.console_sink.ConsoleSink`,
    :class:`~sqllocks_spindle.streaming.sinks.file_sink.FileSink`,
    :class:`~sqllocks_spindle.streaming.sinks.eventhub_sink.EventHubSink`,
    :class:`~sqllocks_spindle.streaming.sinks.kafka_sink.KafkaSink`.

    Subclasses must implement :meth:`send_batch`.  :meth:`send` has a default
    implementation that wraps a single event in a list.
    """

    def send(self, event: dict[str, Any]) -> None:
        """Send a single event."""
        self.send_batch([event])

    @abstractmethod
    def send_batch(self, events: list[dict[str, Any]]) -> None:
        """Send a batch of events.

        Args:
            events: List of event dicts to transmit.
        """
        ...

    def close(self) -> None:
        """Close any open connections or file handles (no-op by default)."""
        pass

    def __enter__(self) -> "StreamWriter":
        return self

    def __exit__(self, *_) -> None:
        self.close()
