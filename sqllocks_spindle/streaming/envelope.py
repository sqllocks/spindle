"""Event envelope dataclass and factory for Spindle streaming."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class EventEnvelope:
    """Standard event envelope for Spindle streaming events.

    All events emitted through Eventstream (or any other sink) are wrapped in
    this envelope to provide consistent metadata across domains and tables.

    Attributes:
        schema_version: Envelope schema version (default ``"1.0"``).
        event_type: Type of the event, typically ``"<domain>.<table>.<action>"``
            (e.g. ``"retail.order.created"``).
        event_time: ISO-8601 timestamp of when the event occurred.
        correlation_id: Unique identifier for tracing (auto-generated UUID).
        tenant_id: Optional tenant identifier for multi-tenant scenarios.
        payload: The actual event data (row dict).
        metadata: Additional metadata key-value pairs.
    """

    schema_version: str
    event_type: str
    event_time: str
    correlation_id: str
    tenant_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


class EnvelopeFactory:
    """Create :class:`EventEnvelope` instances from raw row dicts.

    Provides a consistent way to wrap generated data rows into event envelopes
    with proper correlation IDs, timestamps, and domain context.

    Args:
        default_tenant_id: Optional default tenant ID applied to all envelopes
            unless overridden per call.
        timestamp_column: Name of the column to use as ``event_time``.  If
            *None*, the factory falls back to the current UTC time.

    Example::

        factory = EnvelopeFactory(default_tenant_id="acme-corp")
        envelope = factory.create_envelope(
            row_dict={"order_id": 1, "amount": 99.99, "order_date": "2025-01-15"},
            table_name="order",
            event_type="retail.order.created",
        )
        print(EnvelopeFactory.to_json(envelope))
    """

    def __init__(
        self,
        default_tenant_id: str | None = None,
        timestamp_column: str | None = None,
    ) -> None:
        self._default_tenant_id = default_tenant_id
        self._timestamp_column = timestamp_column

    def create_envelope(
        self,
        row_dict: dict[str, Any],
        table_name: str,
        event_type: str,
        schema_version: str = "1.0",
        tenant_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> EventEnvelope:
        """Create an :class:`EventEnvelope` from a raw row dict.

        Args:
            row_dict: The data payload (a single row as a dict).
            table_name: Name of the source table (added to metadata).
            event_type: Event type string (e.g. ``"retail.order.created"``).
            schema_version: Envelope schema version (default ``"1.0"``).
            tenant_id: Override tenant ID for this envelope.  Falls back to
                the factory default if *None*.
            metadata: Additional metadata to merge into the envelope.

        Returns:
            A populated :class:`EventEnvelope`.
        """
        event_time = self._extract_event_time(row_dict)
        correlation_id = str(uuid.uuid4())

        envelope_metadata: dict[str, Any] = {
            "source_table": table_name,
            "produced_at": datetime.now(timezone.utc).isoformat(),
        }
        if metadata:
            envelope_metadata.update(metadata)

        return EventEnvelope(
            schema_version=schema_version,
            event_type=event_type,
            event_time=event_time,
            correlation_id=correlation_id,
            tenant_id=tenant_id or self._default_tenant_id,
            payload=dict(row_dict),
            metadata=envelope_metadata,
        )

    def _extract_event_time(self, row_dict: dict[str, Any]) -> str:
        """Extract or generate the event timestamp as ISO-8601 string."""
        if self._timestamp_column and self._timestamp_column in row_dict:
            raw = row_dict[self._timestamp_column]
            if isinstance(raw, datetime):
                return raw.isoformat()
            if raw is not None:
                return str(raw)
        return datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Serialisation helpers (static — usable without a factory instance)
    # ------------------------------------------------------------------

    @staticmethod
    def to_dict(envelope: EventEnvelope) -> dict[str, Any]:
        """Convert an :class:`EventEnvelope` to a plain dict.

        Args:
            envelope: The envelope to convert.

        Returns:
            A JSON-serialisable dict.
        """
        return asdict(envelope)

    @staticmethod
    def to_json(envelope: EventEnvelope) -> str:
        """Convert an :class:`EventEnvelope` to a JSON string.

        Args:
            envelope: The envelope to convert.

        Returns:
            A compact JSON string.
        """
        return json.dumps(asdict(envelope), default=str)
