"""Entry point for multi-process chunked generation with multi-sink fan-out."""
from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import TYPE_CHECKING

import numpy as np

from sqllocks_spindle.engine.chunk_worker import generate_chunk
from sqllocks_spindle.engine.sink_registry import SinkRegistry

if TYPE_CHECKING:
    from sqllocks_spindle.engine.sinks.base import Sink

logger = logging.getLogger(__name__)

_WARN_RAM_FRACTION = 0.80  # warn when estimated working set exceeds this fraction of available RAM


class ScaleRouter:
    """Entry point for multi-process chunked generation with multi-sink fan-out.

    Args:
        schema_path: Path to a .json file containing a serialized SpindleSchema.
        sinks: List of Sink instances to receive generated data.
        chunk_size: Rows per chunk. Default 500_000.
        max_workers: Subprocess count. Default os.cpu_count() - 1. Capped
            automatically if the estimated working set would exceed 80 % of
            available RAM.
    """

    def __init__(
        self,
        schema_path: str,
        sinks: list[Sink],
        chunk_size: int = 500_000,
        max_workers: int | None = None,
    ) -> None:
        self._schema_path = schema_path
        self._registry = SinkRegistry(sinks)
        self._chunk_size = chunk_size
        requested = max_workers or max(1, (os.cpu_count() or 2) - 1)
        self._max_workers = self._cap_workers(requested, chunk_size)

    @staticmethod
    def _cap_workers(requested: int, chunk_size: int) -> int:
        """Cap worker count if estimated RAM working set would exceed 80 % of available."""
        try:
            import psutil
            available = psutil.virtual_memory().available
            # Rough estimate: 8 bytes/row × 10 columns per chunk
            bytes_per_chunk = chunk_size * 8 * 10
            max_by_ram = max(1, int(available * _WARN_RAM_FRACTION / bytes_per_chunk))
            if max_by_ram < requested:
                logger.warning(
                    "Capping max_workers from %d to %d to stay within 80%% of available RAM "
                    "(%.1f GB free, ~%.1f GB per chunk).",
                    requested, max_by_ram,
                    available / 1024 ** 3,
                    bytes_per_chunk / 1024 ** 3,
                )
                return max_by_ram
        except ImportError:
            pass
        return requested

    def run(
        self,
        total_rows: int,
        seed: int = 42,
    ) -> dict:
        """Generate total_rows rows and fan out to all sinks.

        Returns:
            Stats dict: rows_generated, elapsed_seconds, throughput_rows_per_sec,
            memory_peak_gb (estimated).
        """
        import json
        from pathlib import Path
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = json.loads(Path(self._schema_path).read_text())
        parser = SchemaParser()
        schema = parser.parse_dict(schema_dict)

        self._registry.open(schema)

        n_chunks = max(1, (total_rows + self._chunk_size - 1) // self._chunk_size)
        rows_generated = 0
        start = time.perf_counter()

        try:
            with ProcessPoolExecutor(max_workers=min(self._max_workers, n_chunks)) as pool:
                futures = {}
                for i in range(n_chunks):
                    chunk_offset = i * self._chunk_size
                    chunk_count = min(self._chunk_size, total_rows - chunk_offset)
                    chunk_seed = seed ^ i
                    fut = pool.submit(
                        generate_chunk,
                        self._schema_path,
                        chunk_seed,
                        chunk_offset,
                        chunk_count,
                    )
                    futures[fut] = (i, chunk_count)

                for future in as_completed(futures):
                    chunk_idx, chunk_count = futures[future]
                    chunk_data = future.result()

                    for table_name, col_lists in chunk_data.items():
                        arrays = {col: np.array(vals) for col, vals in col_lists.items()}
                        self._registry.write_chunk(table_name, arrays)

                    rows_generated += chunk_count
                    logger.info(
                        "Chunk %d/%d done — %s rows written",
                        chunk_idx + 1, n_chunks, f"{rows_generated:,}",
                    )
        finally:
            self._registry.close()

        elapsed = time.perf_counter() - start
        return {
            "rows_generated": rows_generated,
            "elapsed_seconds": round(elapsed, 2),
            "throughput_rows_per_sec": int(rows_generated / max(elapsed, 0.001)),
            "memory_peak_gb": self._estimate_peak_gb(),
        }

    def _estimate_peak_gb(self) -> float:
        try:
            import psutil
            proc = psutil.Process(os.getpid())
            return round(proc.memory_info().rss / 1024 ** 3, 2)
        except ImportError:
            return 0.0
