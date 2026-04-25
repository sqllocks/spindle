"""Azure Event Hub streaming sink.

Requires the ``streaming`` extra::

    pip install sqllocks-spindle[streaming]
"""

from __future__ import annotations

import json
from typing import Any

from sqllocks_spindle.streaming.stream_writer import StreamWriter


class EventHubSink(StreamWriter):
    """Send events to Azure Event Hub.

    Requires ``azure-eventhub``::

        pip install sqllocks-spindle[streaming]

    Args:
        connection_string: Event Hub namespace connection string.
        eventhub_name: Name of the Event Hub.  Can also be embedded in the
            connection string.
        partition_key_column: Optional column name to use as the Event Hub
            partition key (ensures ordered delivery per entity).
    """

    def __init__(
        self,
        connection_string: str,
        eventhub_name: str | None = None,
        partition_key_column: str | None = None,
    ) -> None:
        self._connection_string = connection_string
        self._eventhub_name = eventhub_name
        self._partition_key_column = partition_key_column
        self._client = self._build_client()

    def _build_client(self):
        try:
            from azure.eventhub import EventHubProducerClient
        except ImportError:
            raise ImportError(
                "The 'azure-eventhub' package is required for Event Hub output. "
                "Install it with: pip install sqllocks-spindle[streaming]"
            ) from None
        kwargs: dict[str, Any] = {"conn_str": self._connection_string}
        if self._eventhub_name:
            kwargs["eventhub_name"] = self._eventhub_name
        return EventHubProducerClient(**kwargs)

    def send_batch(self, events: list[dict[str, Any]]) -> None:
        from azure.eventhub import EventData

        with self._client.create_batch() as batch:
            for event in events:
                body = json.dumps(event, default=str).encode("utf-8")
                ed = EventData(body)
                if self._partition_key_column and self._partition_key_column in event:
                    ed.properties = {
                        "partition_key": str(event[self._partition_key_column])
                    }
                batch.add(ed)
        self._client.send_batch(batch)

    def close(self) -> None:
        self._client.close()
