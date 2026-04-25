"""MultiWriter — concurrent fan-out to multiple Fabric stores."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class StoreResult:
    """Result from a single store write."""

    store: str
    success: bool
    rows_written: int = 0
    tables_written: int = 0
    elapsed_seconds: float = 0.0
    error: str | None = None


@dataclass
class MultiWriteResult:
    """Aggregated result from writing to multiple stores."""

    stores: list[StoreResult] = field(default_factory=list)
    total_rows: int = 0
    total_tables: int = 0
    elapsed_seconds: float = 0.0
    partial_failure: bool = False

    @property
    def success(self) -> bool:
        return all(s.success for s in self.stores) and len(self.stores) > 0

    def summary(self) -> str:
        lines = [
            "MultiWriter Result",
            "=" * 50,
            f"Stores written: {len(self.stores)}",
            f"Total rows:     {self.total_rows:,}",
            f"Elapsed:        {self.elapsed_seconds:.1f}s",
            f"Status:         {'SUCCESS' if self.success else 'PARTIAL FAILURE' if self.partial_failure else 'FAILURE'}",
            "",
            f"{'Store':<20} {'Status':<10} {'Rows':>12} {'Time(s)':>10}",
            "-" * 54,
        ]
        for sr in self.stores:
            status = "OK" if sr.success else "FAIL"
            lines.append(
                f"{sr.store:<20} {status:<10} {sr.rows_written:>12,} {sr.elapsed_seconds:>10.1f}"
            )
            if sr.error:
                lines.append(f"  Error: {sr.error}")
        return "\n".join(lines)


class MultiWriter:
    """Write generated data to multiple Fabric stores concurrently.

    Fans out to all configured writers using ThreadPoolExecutor.
    Each writer runs concurrently. Partial failures are captured
    without aborting other stores.

    Example::

        from sqllocks_spindle.fabric import (
            EventhouseWriter, WarehouseBulkWriter,
            LakehouseFilesWriter, MultiWriter,
        )

        mw = MultiWriter(
            eventhouse=EventhouseWriter(...),
            lakehouse=LakehouseFilesWriter(...),
        )
        result = mw.write(tables)
        print(result.summary())
    """

    def __init__(
        self,
        eventhouse: Any | None = None,
        warehouse: Any | None = None,
        lakehouse: Any | None = None,
    ):
        self._writers: dict[str, Any] = {}
        if eventhouse is not None:
            self._writers["eventhouse"] = eventhouse
        if warehouse is not None:
            self._writers["warehouse"] = warehouse
        if lakehouse is not None:
            self._writers["lakehouse"] = lakehouse

    def write(
        self,
        tables: dict[str, pd.DataFrame],
        **kwargs: Any,
    ) -> MultiWriteResult:
        """Write tables to all configured stores concurrently.

        Args:
            tables: Mapping of table_name -> DataFrame.
            **kwargs: Extra args forwarded to each writer's write_all().

        Returns:
            MultiWriteResult aggregating per-store results.
        """
        start = time.time()
        result = MultiWriteResult()

        if not self._writers:
            result.elapsed_seconds = time.time() - start
            return result

        def _write_one(store_name: str, writer: Any) -> StoreResult:
            t0 = time.time()
            try:
                wr = writer.write_all(tables, **kwargs)
                rows = getattr(wr, "rows_written", 0) or getattr(wr, "total_rows", 0)
                tbls = getattr(wr, "tables_written", 0)
                ok = getattr(wr, "success", True)
                err = None
                if not ok:
                    errors = getattr(wr, "errors", [])
                    err = "; ".join(errors) if errors else "Unknown error"
                return StoreResult(
                    store=store_name,
                    success=ok,
                    rows_written=rows,
                    tables_written=tbls,
                    elapsed_seconds=time.time() - t0,
                    error=err,
                )
            except Exception as exc:
                return StoreResult(
                    store=store_name,
                    success=False,
                    elapsed_seconds=time.time() - t0,
                    error=str(exc),
                )

        with ThreadPoolExecutor(max_workers=len(self._writers)) as pool:
            futures = {
                pool.submit(_write_one, name, writer): name
                for name, writer in self._writers.items()
            }
            for future in as_completed(futures):
                sr = future.result()
                result.stores.append(sr)
                result.total_rows += sr.rows_written
                result.total_tables += sr.tables_written

        any_fail = any(not s.success for s in result.stores)
        any_pass = any(s.success for s in result.stores)
        result.partial_failure = any_fail and any_pass
        result.elapsed_seconds = time.time() - start

        return result
