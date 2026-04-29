"""StreamingMultiWriter — concurrent fan-out streaming to multiple sinks.

Accepts a generate_stream() iterator and fans each (table, DataFrame) batch
to all configured StreamWriter sinks in parallel using ThreadPoolExecutor.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Generator, Iterator

import pandas as pd

from sqllocks_spindle.streaming.stream_writer import StreamWriter

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class SinkResult:
    """Per-sink streaming result."""

    sink_name: str
    success: bool
    events_sent: int = 0
    tables_processed: int = 0
    elapsed_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return len(self.errors)


@dataclass
class StreamingMultiWriteResult:
    """Aggregated result from streaming to multiple sinks."""

    sinks: list[SinkResult] = field(default_factory=list)
    total_events: int = 0
    total_tables: int = 0
    elapsed_seconds: float = 0.0

    @property
    def success(self) -> bool:
        return all(s.success for s in self.sinks) and len(self.sinks) > 0

    @property
    def partial_failure(self) -> bool:
        return any(not s.success for s in self.sinks) and any(s.success for s in self.sinks)

    def summary(self) -> str:
        lines = [
            "StreamingMultiWriter Result",
            "=" * 56,
            f"Sinks:         {len(self.sinks)}",
            f"Total events:  {self.total_events:,}",
            f"Total tables:  {self.total_tables}",
            f"Elapsed:       {self.elapsed_seconds:.1f}s",
            f"Status:        {'SUCCESS' if self.success else 'PARTIAL FAILURE' if self.partial_failure else 'FAILURE'}",
            "",
            f"{'Sink':<22} {'Status':<10} {'Events':>10} {'Tables':>8} {'Errors':>7}",
            "-" * 60,
        ]
        for sr in self.sinks:
            status = "OK" if sr.success else "FAIL"
            lines.append(
                f"{sr.sink_name:<22} {status:<10} {sr.events_sent:>10,} "
                f"{sr.tables_processed:>8} {sr.error_count:>7}"
            )
            for err in sr.errors[:3]:
                lines.append(f"  Error: {err}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# StreamingMultiWriter
# ---------------------------------------------------------------------------


class StreamingMultiWriter:
    """Stream generated data to multiple sinks concurrently.

    For each (table_name, DataFrame) batch yielded by ``generate_stream()``,
    converts rows to event dicts and fans out to all registered sinks in
    parallel via ``ThreadPoolExecutor``.

    Usage::

        from sqllocks_spindle.streaming import ConsoleSink, FileSink, StreamingMultiWriter

        smw = StreamingMultiWriter(
            console=ConsoleSink(),
            file=FileSink("events.jsonl"),
        )
        result = smw.stream(spindle.generate_stream(domain=domain, scale="small", seed=42))
        print(result.summary())

    Args:
        max_workers: Number of parallel threads per batch (default: number of sinks).
        batch_size: Max rows per send_batch call (default: 100).
        stop_on_sink_error: If True, abort streaming when any sink errors.
            Default False — partial failures are captured and streaming continues.
        **sinks: Keyword arguments of ``sink_name=StreamWriter`` pairs.
    """

    def __init__(
        self,
        max_workers: int | None = None,
        batch_size: int = 100,
        stop_on_sink_error: bool = False,
        **sinks: StreamWriter,
    ) -> None:
        if not sinks:
            raise ValueError("At least one sink must be provided")
        self._sinks: dict[str, StreamWriter] = sinks
        self._max_workers = max_workers or len(sinks)
        self._batch_size = batch_size
        self._stop_on_sink_error = stop_on_sink_error

    # ---------------------------------------------------------------------------
    # Add / remove sinks
    # ---------------------------------------------------------------------------

    def add_sink(self, name: str, sink: StreamWriter) -> "StreamingMultiWriter":
        """Add a sink and return self for chaining."""
        self._sinks[name] = sink
        return self

    def remove_sink(self, name: str) -> "StreamingMultiWriter":
        """Remove a sink by name."""
        self._sinks.pop(name, None)
        return self

    @property
    def sink_names(self) -> list[str]:
        return list(self._sinks.keys())

    # ---------------------------------------------------------------------------
    # Streaming API
    # ---------------------------------------------------------------------------

    def stream(
        self,
        generator: Iterator[tuple[str, pd.DataFrame]],
    ) -> StreamingMultiWriteResult:
        """Stream (table_name, DataFrame) batches to all sinks in parallel.

        Args:
            generator: Iterator yielding (table_name, DataFrame) tuples.
                Typically ``Spindle.generate_stream()``.

        Returns:
            StreamingMultiWriteResult with per-sink stats.
        """
        start = time.time()
        sink_stats: dict[str, SinkResult] = {
            name: SinkResult(sink_name=name, success=True)
            for name in self._sinks
        }
        tables_seen: set[str] = set()

        for table_name, df in generator:
            tables_seen.add(table_name)
            events = self._df_to_events(table_name, df)

            aborted = self._fan_out(table_name, events, sink_stats)
            if aborted:
                break

        # Close all sinks
        for name, sink in self._sinks.items():
            try:
                sink.close()
            except Exception as e:
                sink_stats[name].errors.append(f"close: {e}")

        elapsed = time.time() - start
        sinks_list = list(sink_stats.values())
        total_events = sum(s.events_sent for s in sinks_list)

        # Set tables_processed on all sinks to the same count
        for s in sinks_list:
            s.tables_processed = len(tables_seen)

        return StreamingMultiWriteResult(
            sinks=sinks_list,
            total_events=total_events // max(1, len(sinks_list)),  # per-sink count
            total_tables=len(tables_seen),
            elapsed_seconds=elapsed,
        )

    def stream_table(
        self,
        table_name: str,
        df: pd.DataFrame,
    ) -> dict[str, bool]:
        """Stream a single DataFrame to all sinks. Returns {sink_name: success}."""
        events = self._df_to_events(table_name, df)
        sink_stats: dict[str, SinkResult] = {
            name: SinkResult(sink_name=name, success=True)
            for name in self._sinks
        }
        self._fan_out(table_name, events, sink_stats)
        return {name: stats.success for name, stats in sink_stats.items()}

    # ---------------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------------

    def _df_to_events(self, table_name: str, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Convert a DataFrame to a list of event dicts."""
        events: list[dict[str, Any]] = []
        records = df.to_dict(orient="records")
        for record in records:
            event: dict[str, Any] = {"_table": table_name}
            event.update({k: v for k, v in record.items()})
            events.append(event)
        return events

    def _fan_out(
        self,
        table_name: str,
        events: list[dict[str, Any]],
        sink_stats: dict[str, SinkResult],
    ) -> bool:
        """Fan out events to all sinks in parallel. Returns True if aborted."""
        if not events:
            return False

        # Chunk events into batches
        batches = [
            events[i:i + self._batch_size]
            for i in range(0, len(events), self._batch_size)
        ]

        def _write_to_sink(sink_name: str, sink: StreamWriter) -> tuple[str, int, str | None]:
            try:
                count = 0
                for batch in batches:
                    sink.send_batch(batch)
                    count += len(batch)
                return sink_name, count, None
            except Exception as e:
                return sink_name, 0, str(e)

        aborted = False
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {
                executor.submit(_write_to_sink, name, sink): name
                for name, sink in self._sinks.items()
            }
            for future in as_completed(futures):
                sink_name, count, error = future.result()
                stats = sink_stats[sink_name]
                if error:
                    stats.success = False
                    stats.errors.append(f"{table_name}: {error}")
                    logger.warning("Sink %s error on %s: %s", sink_name, table_name, error)
                    if self._stop_on_sink_error:
                        aborted = True
                else:
                    stats.events_sent += count

        return aborted
