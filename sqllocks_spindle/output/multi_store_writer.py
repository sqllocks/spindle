"""MultiStoreWriter — write Spindle output to multiple targets concurrently.

Wraps any combination of Spindle writers (DeltaWriter, WarehouseBulkWriter,
FabricSqlDatabaseWriter, EventhouseWriter, etc.) and fans out write calls
to all of them in parallel via ThreadPoolExecutor.

Example::

    from sqllocks_spindle.output import DeltaWriter, MultiStoreWriter
    from sqllocks_spindle.fabric import WarehouseBulkWriter

    writer = MultiStoreWriter([
        DeltaWriter(output_dir="./delta"),
        WarehouseBulkWriter(connection_string="..."),
    ])
    results = writer.write_all(result.tables)
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class SpindleWriter(Protocol):
    """Protocol for any Spindle output writer that supports write_all()."""

    def write_all(self, tables: dict[str, pd.DataFrame], **kwargs: Any) -> Any: ...


@dataclass
class MultiStoreResult:
    """Aggregated result from a MultiStoreWriter.write_all() call."""

    results: dict[str, Any] = field(default_factory=dict)
    errors: dict[str, Exception] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def __repr__(self) -> str:
        ok = len(self.results)
        err = len(self.errors)
        return f"MultiStoreResult(writers={ok + err}, ok={ok}, errors={err})"


class MultiStoreWriter:
    """Fan out Spindle table writes to multiple writers in parallel.

    Args:
        writers: List of writer instances. Each must implement ``write_all()``.
        max_workers: Max parallel writer threads. Defaults to number of writers.
        raise_on_error: If True, re-raise the first writer exception after all
            writes complete. If False (default), collect errors in the result.
    """

    def __init__(
        self,
        writers: list[SpindleWriter],
        max_workers: int | None = None,
        raise_on_error: bool = False,
    ) -> None:
        if not writers:
            raise ValueError("MultiStoreWriter requires at least one writer")
        self._writers = writers
        self._max_workers = max_workers or len(writers)
        self._raise_on_error = raise_on_error

    def write_all(
        self,
        tables: dict[str, pd.DataFrame],
        **kwargs: Any,
    ) -> MultiStoreResult:
        """Write all tables to every configured writer in parallel.

        Args:
            tables: Table name -> DataFrame mapping (e.g. ``result.tables``).
            **kwargs: Forwarded to each writer's ``write_all()`` call.

        Returns:
            MultiStoreResult with per-writer results and any errors.
        """
        result = MultiStoreResult()

        def _run(writer: SpindleWriter) -> tuple[str, Any]:
            writer_name = type(writer).__name__
            return writer_name, writer.write_all(tables, **kwargs)

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {pool.submit(_run, w): w for w in self._writers}
            for future in as_completed(futures):
                try:
                    name, output = future.result()
                    result.results[name] = output
                except Exception as exc:
                    writer = futures[future]
                    result.errors[type(writer).__name__] = exc

        if self._raise_on_error and result.errors:
            first_name, first_exc = next(iter(result.errors.items()))
            raise RuntimeError(f"Writer '{first_name}' failed: {first_exc}") from first_exc

        return result

    def __repr__(self) -> str:
        names = ", ".join(type(w).__name__ for w in self._writers)
        return f"MultiStoreWriter([{names}])"
