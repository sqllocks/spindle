"""Operational observability — structured JSON logging and per-run metrics.

Provides a structured logging setup and a lightweight metrics collector for
Spindle generation runs.  All output is JSON-formatted for easy ingestion
by log aggregators (Azure Monitor, Datadog, Splunk, etc.).

Usage::

    from sqllocks_spindle.observability import configure_logging, RunMetrics

    configure_logging(level="INFO")

    metrics = RunMetrics(run_id="20240301_120000_retail_small_s42")
    metrics.start_table("customer")
    # ... generate ...
    metrics.end_table("customer", rows=1000, columns=8)
    metrics.record_event("chaos_injected", category="value", count=12)
    summary = metrics.finish()
"""

from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Structured JSON log formatter
# ---------------------------------------------------------------------------

class JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects.

    Fields: ``timestamp``, ``level``, ``logger``, ``message``, plus any
    ``extra`` keys passed via the ``extra`` parameter of logging calls.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Merge user-supplied extra fields (skip standard LogRecord attrs)
        _STANDARD = logging.LogRecord("", 0, "", 0, None, None, None).__dict__.keys()
        for key, val in record.__dict__.items():
            if key not in _STANDARD and key not in ("message", "msg", "args"):
                log_entry[key] = val

        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def configure_logging(
    *,
    level: str = "INFO",
    stream: Any = None,
    logger_name: str = "sqllocks_spindle",
) -> logging.Logger:
    """Configure structured JSON logging for Spindle.

    Args:
        level: Log level string (``DEBUG``, ``INFO``, ``WARNING``, etc.).
        stream: Output stream (defaults to ``sys.stderr``).
        logger_name: Root logger name to configure.

    Returns:
        The configured :class:`logging.Logger`.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Avoid duplicate handlers on repeated calls
    if not any(isinstance(h, logging.StreamHandler) and isinstance(h.formatter, JsonFormatter)
               for h in logger.handlers):
        handler = logging.StreamHandler(stream or sys.stderr)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    return logger


# ---------------------------------------------------------------------------
# Per-run metrics collector
# ---------------------------------------------------------------------------

@dataclass
class TableMetric:
    """Timing and size metrics for a single table generation."""
    table_name: str
    started: float = 0.0
    elapsed_seconds: float = 0.0
    rows: int = 0
    columns: int = 0


@dataclass
class RunMetrics:
    """Collect per-run operational metrics.

    Args:
        run_id: The generation run identifier.
    """
    run_id: str
    _start_time: float = field(default_factory=time.time, repr=False)
    _table_metrics: dict[str, TableMetric] = field(default_factory=dict, repr=False)
    _events: list[dict[str, Any]] = field(default_factory=list, repr=False)
    _table_starts: dict[str, float] = field(default_factory=dict, repr=False)

    def start_table(self, table_name: str) -> None:
        """Mark the start of table generation."""
        self._table_starts[table_name] = time.time()

    def end_table(self, table_name: str, rows: int = 0, columns: int = 0) -> None:
        """Mark the end of table generation and record metrics."""
        start = self._table_starts.pop(table_name, time.time())
        elapsed = time.time() - start
        self._table_metrics[table_name] = TableMetric(
            table_name=table_name,
            started=start,
            elapsed_seconds=round(elapsed, 4),
            rows=rows,
            columns=columns,
        )

    def record_event(self, event_type: str, **kwargs: Any) -> None:
        """Record an arbitrary operational event."""
        self._events.append({
            "type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        })

    def finish(self) -> dict[str, Any]:
        """Finalize metrics collection and return a summary dict.

        Returns:
            A dict suitable for JSON serialization containing:
            ``run_id``, ``total_elapsed_seconds``, ``tables``, ``events``.
        """
        total_elapsed = time.time() - self._start_time
        return {
            "run_id": self.run_id,
            "total_elapsed_seconds": round(total_elapsed, 4),
            "total_rows": sum(m.rows for m in self._table_metrics.values()),
            "total_tables": len(self._table_metrics),
            "tables": {
                name: {
                    "elapsed_seconds": m.elapsed_seconds,
                    "rows": m.rows,
                    "columns": m.columns,
                }
                for name, m in self._table_metrics.items()
            },
            "events": self._events,
        }

    def to_json(self) -> str:
        """Return the metrics summary as a JSON string."""
        return json.dumps(self.finish(), indent=2, default=str)
