"""Fabric Eventstream client — wraps azure-eventhub for custom endpoints.

Requires the ``azure-eventhub`` package::

    pip install sqllocks-spindle[streaming]
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from sqllocks_spindle.streaming.stream_writer import StreamWriter

logger = logging.getLogger(__name__)

# Optional dependency — deferred import
try:
    from azure.eventhub import EventData, EventHubProducerClient
    from azure.eventhub.aio import (
        EventHubProducerClient as AsyncEventHubProducerClient,
    )

    _HAS_EVENTHUB = True
except ImportError:
    _HAS_EVENTHUB = False


def _require_eventhub() -> None:
    """Raise a helpful error if azure-eventhub is not installed."""
    if not _HAS_EVENTHUB:
        raise ImportError(
            "The 'azure-eventhub' package is required for EventstreamClient. "
            "Install it with: pip install sqllocks-spindle[streaming]"
        ) from None


class EventstreamClient(StreamWriter):
    """Send event envelopes to a Fabric Eventstream custom endpoint.

    Implements the :class:`~sqllocks_spindle.streaming.stream_writer.StreamWriter`
    protocol so it can be used directly with :class:`SpindleStreamer`.

    Supports both synchronous and asynchronous sending.

    Args:
        connection_string: Event Hub-compatible connection string for the
            Eventstream custom endpoint.  Also accepts a secret reference
            in the pattern ``kv://workspace/secret_name`` (resolution is
            deferred to a key-vault helper — not yet wired).
        eventhub_name: Optional Event Hub / Eventstream name (can be
            embedded in the connection string instead).
        partition_key_column: Optional column name whose value is used as
            the partition key for ordered delivery per entity.
        max_batch_size: Maximum events per wire batch (default 500).
        max_retries: Number of retry attempts on transient failures.
        retry_delay_seconds: Base delay between retries (exponential backoff).
    """

    def __init__(
        self,
        connection_string: str,
        eventhub_name: str | None = None,
        partition_key_column: str | None = None,
        max_batch_size: int = 500,
        max_retries: int = 3,
        retry_delay_seconds: float = 1.0,
    ) -> None:
        _require_eventhub()
        self._connection_string = self._resolve_connection_string(connection_string)
        self._eventhub_name = eventhub_name
        self._partition_key_column = partition_key_column
        self._max_batch_size = max_batch_size
        self._max_retries = max_retries
        self._retry_delay = retry_delay_seconds

        self._client: EventHubProducerClient = self._build_client()
        self._async_client: AsyncEventHubProducerClient | None = None

    # ------------------------------------------------------------------
    # StreamWriter interface (synchronous)
    # ------------------------------------------------------------------

    def send_batch(self, events: list[dict[str, Any]]) -> None:
        """Send a batch of event dicts through the Eventstream endpoint.

        Large batches are automatically chunked to respect ``max_batch_size``.
        """
        for offset in range(0, len(events), self._max_batch_size):
            chunk = events[offset : offset + self._max_batch_size]
            self._send_chunk_with_retry(chunk)

    def close(self) -> None:
        """Close the synchronous producer client."""
        self._client.close()
        if self._async_client is not None:
            # Best-effort close of the async client on the running loop
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._async_client.close())
                else:
                    loop.run_until_complete(self._async_client.close())
            except RuntimeError:
                pass

    # ------------------------------------------------------------------
    # Convenience: single-event methods matching the spec
    # ------------------------------------------------------------------

    def send_event(self, envelope: dict[str, Any]) -> None:
        """Send a single event envelope.

        Args:
            envelope: A dict (typically from ``EnvelopeFactory.to_dict``).
        """
        self.send_batch([envelope])

    # ------------------------------------------------------------------
    # Async API
    # ------------------------------------------------------------------

    async def send_event_async(self, envelope: dict[str, Any]) -> None:
        """Send a single event envelope asynchronously."""
        await self.send_batch_async([envelope])

    async def send_batch_async(self, envelopes: list[dict[str, Any]]) -> None:
        """Send a batch of event envelopes asynchronously.

        Creates or reuses an async ``EventHubProducerClient`` under the hood.
        """
        client = self._ensure_async_client()
        for offset in range(0, len(envelopes), self._max_batch_size):
            chunk = envelopes[offset : offset + self._max_batch_size]
            await self._send_chunk_async_with_retry(client, chunk)

    async def close_async(self) -> None:
        """Close the async producer client."""
        if self._async_client is not None:
            await self._async_client.close()
            self._async_client = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_connection_string(connection_string: str) -> str:
        """Resolve a ``kv://`` secret reference to an actual connection string.

        Currently a placeholder — ``kv://workspace/secret_name`` patterns are
        recognised but not resolved (raises ``NotImplementedError``).  Plain
        connection strings pass through unchanged.
        """
        if connection_string.startswith("kv://"):
            raise NotImplementedError(
                f"Key Vault secret references are not yet supported: {connection_string}. "
                "Pass the raw connection string directly for now."
            )
        return connection_string

    def _build_client(self) -> EventHubProducerClient:
        kwargs: dict[str, Any] = {"conn_str": self._connection_string}
        if self._eventhub_name:
            kwargs["eventhub_name"] = self._eventhub_name
        return EventHubProducerClient(**kwargs)

    def _ensure_async_client(self) -> AsyncEventHubProducerClient:
        if self._async_client is None:
            kwargs: dict[str, Any] = {"conn_str": self._connection_string}
            if self._eventhub_name:
                kwargs["eventhub_name"] = self._eventhub_name
            self._async_client = AsyncEventHubProducerClient(**kwargs)
        return self._async_client

    def _send_chunk_with_retry(self, events: list[dict[str, Any]]) -> None:
        """Send a single chunk with exponential-backoff retry."""
        import time

        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                batch = self._client.create_batch()
                for event in events:
                    body = json.dumps(event, default=str).encode("utf-8")
                    ed = EventData(body)
                    if self._partition_key_column and self._partition_key_column in event:
                        ed.properties = {
                            "partition_key": str(event[self._partition_key_column])
                        }
                    batch.add(ed)
                self._client.send_batch(batch)
                return
            except Exception as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    delay = self._retry_delay * (2 ** attempt)
                    logger.warning(
                        "EventstreamClient send failed (attempt %d/%d), "
                        "retrying in %.1fs: %s",
                        attempt + 1,
                        self._max_retries + 1,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
        raise RuntimeError(
            f"EventstreamClient: failed after {self._max_retries + 1} attempts"
        ) from last_exc

    async def _send_chunk_async_with_retry(
        self,
        client: AsyncEventHubProducerClient,
        events: list[dict[str, Any]],
    ) -> None:
        """Send a single chunk asynchronously with exponential-backoff retry."""
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                batch = await client.create_batch()
                for event in events:
                    body = json.dumps(event, default=str).encode("utf-8")
                    ed = EventData(body)
                    if self._partition_key_column and self._partition_key_column in event:
                        ed.properties = {
                            "partition_key": str(event[self._partition_key_column])
                        }
                    batch.add(ed)
                await client.send_batch(batch)
                return
            except Exception as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    delay = self._retry_delay * (2 ** attempt)
                    logger.warning(
                        "EventstreamClient async send failed (attempt %d/%d), "
                        "retrying in %.1fs: %s",
                        attempt + 1,
                        self._max_retries + 1,
                        delay,
                        exc,
                    )
                    await asyncio.sleep(delay)
        raise RuntimeError(
            f"EventstreamClient: failed after {self._max_retries + 1} attempts"
        ) from last_exc
