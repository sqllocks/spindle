"""Apache Kafka streaming sink.

Requires the ``streaming`` extra::

    pip install sqllocks-spindle[streaming]
"""

from __future__ import annotations

import json
from typing import Any

from sqllocks_spindle.streaming.stream_writer import StreamWriter


class KafkaSink(StreamWriter):
    """Send events to an Apache Kafka topic.

    Requires ``kafka-python``::

        pip install sqllocks-spindle[streaming]

    Args:
        bootstrap_servers: Kafka broker address(es), e.g. ``"localhost:9092"``
            or a list ``["broker1:9092", "broker2:9092"]``.
        topic: Target Kafka topic name.
        key_column: Optional column to use as the Kafka message key (ensures
            ordered delivery per entity within a partition).
    """

    def __init__(
        self,
        bootstrap_servers: str | list[str],
        topic: str,
        key_column: str | None = None,
    ) -> None:
        self._topic = topic
        self._key_column = key_column
        self._producer = self._build_producer(bootstrap_servers)

    def _build_producer(self, bootstrap_servers):
        try:
            from kafka import KafkaProducer
        except ImportError:
            raise ImportError(
                "The 'kafka-python' package is required for Kafka output. "
                "Install it with: pip install sqllocks-spindle[streaming]"
            ) from None
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        )

    def send_batch(self, events: list[dict[str, Any]]) -> None:
        for event in events:
            key = (
                str(event[self._key_column])
                if self._key_column and self._key_column in event
                else None
            )
            self._producer.send(self._topic, value=event, key=key)
        self._producer.flush()

    def close(self) -> None:
        self._producer.close()
