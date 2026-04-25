from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from sqllocks_spindle.engine.sinks.base import Sink
    from sqllocks_spindle.schema.parser import SpindleSchema

logger = logging.getLogger(__name__)


class SinkError(Exception):
    """Raised when one or more sinks fail during write_chunk."""

    def __init__(self, errors: list[tuple[object, Exception]]) -> None:
        self.sink_errors = errors
        msgs = [f"{type(sink).__name__}: {exc}" for sink, exc in errors]
        super().__init__(f"Sink failures: {'; '.join(msgs)}")


class SinkRegistry:
    """Fan-out coordinator — dispatches each chunk to all registered sinks in parallel."""

    def __init__(self, sinks: list[Sink], max_workers: int = 8) -> None:
        self._sinks = sinks
        self._max_workers = max_workers

    def open(self, schema: SpindleSchema | None) -> None:
        if not self._sinks:
            return
        with ThreadPoolExecutor(max_workers=min(self._max_workers, len(self._sinks))) as pool:
            futures = {pool.submit(s.open, schema): s for s in self._sinks}
            errors = []
            for f in as_completed(futures):
                sink = futures[f]
                try:
                    f.result()
                except Exception as exc:
                    errors.append((sink, exc))
                    logger.error("Sink %s.open() failed: %s", type(sink).__name__, exc)
            if errors:
                raise SinkError(errors)

    def write_chunk(self, table: str, arrays: dict[str, np.ndarray]) -> None:
        if not self._sinks:
            return
        with ThreadPoolExecutor(max_workers=min(self._max_workers, len(self._sinks))) as pool:
            futures = {pool.submit(s.write_chunk, table, arrays): s for s in self._sinks}
            errors = []
            for f in as_completed(futures):
                sink = futures[f]
                try:
                    f.result()
                except Exception as exc:
                    errors.append((sink, exc))
                    logger.error(
                        "Sink %s.write_chunk(%s) failed: %s",
                        type(sink).__name__, table, exc,
                    )
            if errors:
                raise SinkError(errors)

    def close(self) -> None:
        if not self._sinks:
            return
        errors = []
        for s in self._sinks:
            try:
                s.close()
            except Exception as exc:
                errors.append((s, exc))
                logger.error("Sink %s.close() failed: %s", type(s).__name__, exc)
        if errors:
            raise SinkError(errors)
