"""Stream emitter — wraps SpindleStreamer with envelope, replay, and schema tracking."""

from __future__ import annotations

import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.streaming.config import BurstWindow, StreamConfig
from sqllocks_spindle.streaming.stream_writer import StreamWriter
from sqllocks_spindle.streaming.streamer import SpindleStreamer


# ---------------------------------------------------------------------------
# Inline envelope wrapper (sqllocks_spindle.streaming.envelope does not yet exist)
# ---------------------------------------------------------------------------

def _wrap_envelope(
    event: dict[str, Any],
    *,
    topic: str,
    schema_version: str = "1.0",
    correlation_id: str | None = None,
    source: str = "spindle",
) -> dict[str, Any]:
    """Wrap a raw event dict in a standard event envelope.

    The envelope follows the CloudEvents-inspired structure used across Spindle
    simulations, making it compatible with the batch side's manifest correlation.

    Returns:
        A new dict containing envelope metadata and the original event as
        ``data``.
    """
    return {
        "id": str(uuid.uuid4()),
        "source": source,
        "type": f"spindle.{topic}",
        "time": datetime.utcnow().isoformat() + "Z",
        "specversion": "1.0",
        "datacontenttype": "application/json",
        "topic": topic,
        "schema_version": schema_version,
        "correlation_id": correlation_id or str(uuid.uuid4()),
        "data": event,
    }


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class StreamEmitConfig:
    """Configuration for :class:`StreamEmitter`.

    Args:
        rate_per_sec: Base events-per-second target.
        jitter_ms: Maximum random jitter added between events (milliseconds).
        burst_windows: List of :class:`BurstWindow` definitions.
        out_of_order_probability: Fraction of events to deliver out of order.
        replay_enabled: Whether to re-emit events from a sliding window.
        replay_window_minutes: Length of the replay window.
        replay_probability: Per-tick probability of triggering a replay burst.
        replay_burst_size: Number of events re-emitted per replay trigger.
        topics: List of topic names (one per table, or explicit mapping).
        envelope_source: Value for the envelope ``source`` field.
        envelope_schema_version: Default schema version string.
        sink_type: Sink identifier — ``"console"``, ``"file"``, ``"eventhub"``,
            ``"eventstream"``.
        sink_connection: Connection info dict (keys depend on ``sink_type``).
        max_events: Stop after this many events (``None`` = no limit).
        realtime: Pace emission in real time (vs. fast-as-possible).
        seed: Random seed.
    """

    rate_per_sec: float = 10.0
    jitter_ms: float = 0.0
    burst_windows: list[BurstWindow] = field(default_factory=list)
    out_of_order_probability: float = 0.0
    replay_enabled: bool = False
    replay_window_minutes: float = 5.0
    replay_probability: float = 0.05
    replay_burst_size: int = 10
    topics: list[str] = field(default_factory=list)
    envelope_source: str = "spindle"
    envelope_schema_version: str = "1.0"
    sink_type: str = "console"
    sink_connection: dict[str, Any] = field(default_factory=dict)
    max_events: int | None = None
    realtime: bool = False
    seed: int = 42


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class StreamEmitResult:
    """Result of a :meth:`StreamEmitter.emit` run.

    Attributes:
        events_sent: Total primary events emitted.
        replay_events_sent: Events re-emitted via the replay window.
        topics_used: Set of topic strings written to.
        elapsed_seconds: Wall-clock duration.
        schema_versions: Mapping of topic -> schema version used.
    """

    events_sent: int
    replay_events_sent: int
    topics_used: set[str]
    elapsed_seconds: float
    schema_versions: dict[str, str]

    @property
    def total_events(self) -> int:
        return self.events_sent + self.replay_events_sent

    def __repr__(self) -> str:
        return (
            f"StreamEmitResult(sent={self.events_sent}, replays={self.replay_events_sent}, "
            f"topics={self.topics_used}, {self.elapsed_seconds:.2f}s)"
        )


# ---------------------------------------------------------------------------
# StreamEmitter
# ---------------------------------------------------------------------------

class StreamEmitter:
    """Emit generated tables as enveloped streaming events.

    Builds on :class:`~sqllocks_spindle.streaming.streamer.SpindleStreamer`
    but adds:

    * **Event envelopes** — each event is wrapped in a CloudEvents-style
      envelope with ``id``, ``time``, ``topic``, ``schema_version``, and
      ``correlation_id``.
    * **Replay window** — a sliding buffer of recently emitted events that
      can be re-sent to simulate consumer-side replays / redeliveries.
    * **Schema version tracking** — each topic carries a schema version
      string so downstream consumers can detect schema evolution.

    Args:
        tables: Pre-generated ``dict[table_name, DataFrame]``.
        config: :class:`StreamEmitConfig`.
        sink: Optional explicit :class:`StreamWriter`.  If ``None``, one is
            built from ``config.sink_type`` / ``config.sink_connection``.

    Example::

        from sqllocks_spindle.simulation import StreamEmitter, StreamEmitConfig

        cfg = StreamEmitConfig(rate_per_sec=50, topics=["orders", "returns"])
        result = StreamEmitter(tables=gen_result.tables, config=cfg).emit()
    """

    def __init__(
        self,
        tables: dict[str, pd.DataFrame],
        config: StreamEmitConfig | None = None,
        sink: StreamWriter | None = None,
    ) -> None:
        self._tables = tables
        self._config = config or StreamEmitConfig()
        self._sink = sink or self._build_sink()
        self._rng = np.random.default_rng(self._config.seed)

        # Schema version registry: topic -> version string
        self._schema_versions: dict[str, str] = {}

        # Replay ring buffer
        self._replay_buffer: deque[dict[str, Any]] = deque(
            maxlen=self._replay_buffer_maxlen(),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def emit(
        self,
        tables: dict[str, pd.DataFrame] | None = None,
        config: StreamEmitConfig | None = None,
    ) -> StreamEmitResult:
        """Emit all rows from *tables* (or the instance tables) as enveloped events.

        Args:
            tables: Override tables for this call.
            config: Override config for this call.

        Returns:
            :class:`StreamEmitResult` with run statistics.
        """
        tables = tables or self._tables
        cfg = config or self._config

        topic_map = self._build_topic_map(tables, cfg)
        events = self._tables_to_enveloped_events(tables, topic_map, cfg)

        # Out-of-order shuffle
        if cfg.out_of_order_probability > 0 and len(events) > 1:
            events = self._shuffle_out_of_order(events, cfg.out_of_order_probability)

        # Honour max_events
        if cfg.max_events is not None:
            events = events[: cfg.max_events]

        start = time.time()
        sent = 0
        replay_sent = 0

        for event in events:
            # Jitter
            if cfg.jitter_ms > 0:
                jitter_s = self._rng.uniform(0, cfg.jitter_ms / 1000.0)
                if cfg.realtime:
                    time.sleep(jitter_s)

            # Send
            self._sink.send(event)
            sent += 1

            # Add to replay buffer
            if cfg.replay_enabled:
                self._replay_buffer.append(event)

                # Probabilistic replay trigger
                if self._rng.random() < cfg.replay_probability and self._replay_buffer:
                    burst = min(cfg.replay_burst_size, len(self._replay_buffer))
                    replay_events = list(self._replay_buffer)[-burst:]
                    for rev in replay_events:
                        replayed = dict(rev)
                        replayed["_replay"] = True
                        replayed["_replay_time"] = datetime.utcnow().isoformat() + "Z"
                        self._sink.send(replayed)
                        replay_sent += 1

            # Rate limiting (simple sleep-based when realtime)
            if cfg.realtime and cfg.rate_per_sec > 0:
                time.sleep(1.0 / cfg.rate_per_sec)

        elapsed = time.time() - start

        return StreamEmitResult(
            events_sent=sent,
            replay_events_sent=replay_sent,
            topics_used=set(topic_map.values()),
            elapsed_seconds=elapsed,
            schema_versions=dict(self._schema_versions),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_sink(self) -> StreamWriter:
        """Construct a sink from config settings."""
        cfg = self._config
        sink_type = cfg.sink_type.lower()

        if sink_type == "file":
            from sqllocks_spindle.streaming.sinks.file_sink import FileSink
            path = cfg.sink_connection.get("path", "spindle_events.jsonl")
            mode = cfg.sink_connection.get("mode", "w")
            return FileSink(path=path, mode=mode)

        if sink_type == "eventhub":
            from sqllocks_spindle.streaming.sinks.eventhub_sink import EventHubSink
            return EventHubSink(
                connection_string=cfg.sink_connection["connection_string"],
                eventhub_name=cfg.sink_connection.get("eventhub_name"),
                partition_key_column=cfg.sink_connection.get("partition_key_column"),
            )

        if sink_type in ("eventstream", "console"):
            from sqllocks_spindle.streaming.sinks.console_sink import ConsoleSink
            return ConsoleSink()

        raise ValueError(
            f"Unknown sink_type {sink_type!r}. "
            f"Supported: console, file, eventhub, eventstream."
        )

    def _build_topic_map(
        self,
        tables: dict[str, pd.DataFrame],
        cfg: StreamEmitConfig,
    ) -> dict[str, str]:
        """Map table names to topic names.

        If ``cfg.topics`` is provided and matches table count, use them
        positionally.  Otherwise default to ``table_name`` as topic.
        """
        table_names = list(tables.keys())
        if cfg.topics and len(cfg.topics) == len(table_names):
            return dict(zip(table_names, cfg.topics))
        if cfg.topics and len(cfg.topics) == 1:
            return {t: cfg.topics[0] for t in table_names}
        return {t: t for t in table_names}

    def _tables_to_enveloped_events(
        self,
        tables: dict[str, pd.DataFrame],
        topic_map: dict[str, str],
        cfg: StreamEmitConfig,
    ) -> list[dict[str, Any]]:
        """Convert all tables to a flat list of enveloped event dicts."""
        all_events: list[dict[str, Any]] = []

        for table_name, df in tables.items():
            topic = topic_map.get(table_name, table_name)
            version = cfg.envelope_schema_version
            self._schema_versions[topic] = version

            records = df.to_dict("records")
            for seq, record in enumerate(records):
                # Clean numpy / pandas types
                cleaned = {k: _clean_value(v) for k, v in record.items()}
                cleaned["_spindle_table"] = table_name
                cleaned["_spindle_seq"] = seq

                envelope = _wrap_envelope(
                    cleaned,
                    topic=topic,
                    schema_version=version,
                    source=cfg.envelope_source,
                )
                all_events.append(envelope)

        return all_events

    def _shuffle_out_of_order(
        self,
        events: list[dict[str, Any]],
        probability: float,
    ) -> list[dict[str, Any]]:
        """Randomly swap adjacent events to simulate out-of-order delivery."""
        events = list(events)
        n = max(1, int(len(events) * probability))
        positions = self._rng.choice(max(1, len(events) - 1), size=min(n, len(events) - 1), replace=False)
        for pos in positions:
            events[pos], events[pos + 1] = events[pos + 1], events[pos]
        return events

    def _replay_buffer_maxlen(self) -> int:
        """Calculate max replay buffer size from config."""
        cfg = self._config
        if not cfg.replay_enabled:
            return 0
        # Estimate: rate * window minutes * 60s
        return max(100, int(cfg.rate_per_sec * cfg.replay_window_minutes * 60))


# ---------------------------------------------------------------------------
# Value cleaner (mirrors streamer._clean_value)
# ---------------------------------------------------------------------------

def _clean_value(v: Any) -> Any:
    """Convert numpy/pandas types to JSON-safe Python natives."""
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        if np.isnan(v):
            return None
        return float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, pd.Timestamp):
        return str(v)
    if isinstance(v, float) and np.isnan(v):
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v
