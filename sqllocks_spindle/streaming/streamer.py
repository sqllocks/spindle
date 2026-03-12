"""SpindleStreamer — main public API for Phase 2 streaming."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.streaming.config import StreamConfig
from sqllocks_spindle.streaming.stream_writer import StreamWriter


@dataclass
class StreamResult:
    """Result of a streaming run.

    Attributes:
        table: Name of the streamed table.
        events_sent: Total events transmitted to the sink.
        anomaly_count: Number of events flagged as anomalous.
        out_of_order_count: Number of events deliberately reordered.
        elapsed_seconds: Wall-clock duration of the streaming run.
        events_per_second_actual: Measured throughput.
    """

    table: str
    events_sent: int
    anomaly_count: int
    out_of_order_count: int
    elapsed_seconds: float

    @property
    def events_per_second_actual(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.events_sent / self.elapsed_seconds

    def __repr__(self) -> str:
        return (
            f"StreamResult(table={self.table!r}, events={self.events_sent:,}, "
            f"anomalies={self.anomaly_count}, ooo={self.out_of_order_count}, "
            f"{self.elapsed_seconds:.2f}s @ {self.events_per_second_actual:,.0f} eps)"
        )


class SpindleStreamer:
    """Stream generated synthetic data row-by-row through a :class:`StreamWriter`.

    Workflow
    --------
    1. Generate a full batch via the Spindle engine (or accept pre-generated tables).
    2. Convert each row to an event dict, adding ``_spindle_table`` and
       ``_spindle_seq`` metadata fields.
    3. Optionally inject anomalies via an :class:`~sqllocks_spindle.streaming.anomaly.AnomalyRegistry`.
    4. Optionally reorder a fraction of events to simulate out-of-order arrival.
    5. Emit in batches through the configured :class:`StreamWriter` sink.
       Rate-limiting (token bucket + Poisson inter-arrivals) is applied only when
       ``StreamConfig(realtime=True)``.

    Args:
        domain: A Domain instance (e.g. ``RetailDomain()``).  Mutually exclusive
            with *tables*.
        tables: Pre-generated ``dict[table_name, DataFrame]`` — skips generation.
        sink: :class:`StreamWriter` to emit events through.  Defaults to
            :class:`~sqllocks_spindle.streaming.sinks.console_sink.ConsoleSink`.
        config: :class:`StreamConfig` controlling rate, bursts, OOO, etc.
        anomaly_registry: Optional :class:`~sqllocks_spindle.streaming.anomaly.AnomalyRegistry`.
        scale: Scale preset for generation (ignored when *tables* is provided).
        seed: Random seed.

    Example::

        from sqllocks_spindle import RetailDomain
        from sqllocks_spindle.streaming import SpindleStreamer, StreamConfig, FileSink

        result = SpindleStreamer(
            domain=RetailDomain(),
            sink=FileSink("events.jsonl", mode="w"),
            config=StreamConfig(max_events=500),
        ).stream("order")

        print(result)
    """

    def __init__(
        self,
        domain=None,
        tables: dict[str, pd.DataFrame] | None = None,
        sink: StreamWriter | None = None,
        config: StreamConfig | None = None,
        anomaly_registry=None,
        scale: str = "small",
        seed: int | None = 42,
    ) -> None:
        if domain is None and tables is None:
            raise ValueError("Provide either 'domain' or 'tables'.")

        self._domain = domain
        self._tables: dict[str, pd.DataFrame] | None = tables
        self._config = config or StreamConfig()
        self._anomaly_registry = anomaly_registry
        self._scale = scale
        self._seed = seed
        self._rng = np.random.default_rng(seed)

        # Lazy-import sink default to avoid circular imports
        if sink is None:
            from sqllocks_spindle.streaming.sinks.console_sink import ConsoleSink
            sink = ConsoleSink()
        self._sink = sink

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def stream(
        self,
        table: str,
        timestamp_column: str | None = None,
    ) -> StreamResult:
        """Stream all rows of *table* through the configured sink.

        Args:
            table: Name of the table to stream.
            timestamp_column: Column to use as ``_spindle_event_time``.  If
                ``None``, the first datetime-typed column is auto-detected.

        Returns:
            :class:`StreamResult` with run statistics.
        """
        tables = self._ensure_generated()
        if table not in tables:
            available = list(tables.keys())
            raise ValueError(
                f"Table '{table}' not found. Available tables: {available}"
            )

        df = tables[table].copy()

        # Anomaly injection
        anomaly_count = 0
        if self._anomaly_registry is not None and len(self._anomaly_registry) > 0:
            df = self._anomaly_registry.inject(df, self._rng)
            if "_spindle_is_anomaly" in df.columns:
                anomaly_count = int(df["_spindle_is_anomaly"].sum())
            if not self._config.label_anomalies:
                df = df.drop(
                    columns=["_spindle_is_anomaly", "_spindle_anomaly_type"],
                    errors="ignore",
                )

        # Convert to event dicts
        events = self._to_events(df, table, timestamp_column)

        # Out-of-order injection
        ooo_count = 0
        if self._config.out_of_order_fraction > 0 and len(events) > 1:
            events, ooo_count = self._inject_out_of_order(events)

        # Honour max_events
        if self._config.max_events is not None:
            events = events[: self._config.max_events]

        # Emit
        start = time.time()
        self._emit(events)
        elapsed = time.time() - start

        return StreamResult(
            table=table,
            events_sent=len(events),
            anomaly_count=anomaly_count,
            out_of_order_count=ooo_count,
            elapsed_seconds=elapsed,
        )

    def stream_all(
        self,
        timestamp_column: str | None = None,
    ) -> list[StreamResult]:
        """Stream every table in generation order.

        Returns:
            List of :class:`StreamResult`, one per table.
        """
        tables = self._ensure_generated()
        return [self.stream(t, timestamp_column) for t in tables]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_generated(self) -> dict[str, pd.DataFrame]:
        if self._tables is None:
            from sqllocks_spindle.engine.generator import Spindle
            result = Spindle().generate(
                domain=self._domain, scale=self._scale, seed=self._seed
            )
            self._tables = result.tables
        return self._tables

    def _to_events(
        self,
        df: pd.DataFrame,
        table_name: str,
        timestamp_column: str | None,
    ) -> list[dict[str, Any]]:
        """Convert a DataFrame to a list of event dicts."""
        ts_col = timestamp_column
        if ts_col is None:
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    ts_col = col
                    break

        records: list[dict[str, Any]] = df.reset_index(drop=True).to_dict("records")

        for seq, event in enumerate(records):
            # Clean numpy / pandas types
            cleaned: dict[str, Any] = {}
            for k, v in event.items():
                cleaned[k] = _clean_value(v)
            cleaned["_spindle_table"] = table_name
            cleaned["_spindle_seq"] = seq
            if ts_col and ts_col in cleaned:
                cleaned["_spindle_event_time"] = str(cleaned[ts_col]) if cleaned[ts_col] is not None else None
            records[seq] = cleaned

        # Sort by event time if we have a timestamp column
        if ts_col:
            records.sort(key=lambda e: str(e.get("_spindle_event_time") or ""))

        return records

    def _inject_out_of_order(
        self,
        events: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], int]:
        """Delay a fraction of events in the stream order.

        Picks ``fraction * len(events)`` positions and moves each event
        forward by 1–``max_delay_slots`` positions, simulating late arrivals.
        """
        n = max(1, int(len(events) * self._config.out_of_order_fraction))
        n = min(n, len(events) - 1)
        max_slots = max(1, self._config.out_of_order_max_delay_slots)

        orig_positions = self._rng.choice(len(events), size=n, replace=False)
        events = list(events)
        ooo_count = 0

        # Process highest positions first to avoid index shifting issues
        for orig_pos in sorted(orig_positions.tolist(), reverse=True):
            delay = int(self._rng.integers(1, max_slots + 1))
            new_pos = min(orig_pos + delay, len(events) - 1)
            if new_pos != orig_pos:
                event = events.pop(orig_pos)
                events.insert(new_pos, event)
                ooo_count += 1

        return events, ooo_count

    def _emit(self, events: list[dict[str, Any]]) -> None:
        """Emit events through the sink, with optional real-time rate limiting."""
        config = self._config
        batch_size = max(1, config.batch_size)

        if not config.realtime:
            # Fast path: emit as quickly as possible (testing / bulk load)
            for i in range(0, len(events), batch_size):
                self._sink.send_batch(events[i : i + batch_size])
            return

        # Real-time path: token bucket + Poisson inter-arrivals
        from sqllocks_spindle.streaming.rate_limiter import TokenBucket, poisson_interarrival

        stream_start = time.monotonic()
        bucket = TokenBucket(config.events_per_second)
        duration_limit = config.duration_seconds

        for i, event in enumerate(events):
            # Check duration limit
            elapsed = time.monotonic() - stream_start
            if duration_limit is not None and elapsed >= duration_limit:
                break

            # Adjust rate for burst / time patterns
            import datetime
            now = datetime.datetime.now()
            mult = config.get_rate_multiplier(elapsed, now.hour, now.weekday())
            effective_rate = config.events_per_second * mult
            bucket.update_rate(effective_rate)

            # Poisson inter-arrival sleep
            wait = poisson_interarrival(effective_rate, self._rng)
            if wait > 0:
                time.sleep(wait)

            # Consume token (may add a small additional wait)
            bucket.wait_and_consume()

            # Send single event (batch_size=1 in realtime mode for accurate pacing)
            self._sink.send_batch([event])


# ---------------------------------------------------------------------------
# Value cleaner
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
