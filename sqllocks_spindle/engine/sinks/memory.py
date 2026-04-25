from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from sqllocks_spindle.schema.parser import SpindleSchema

_WARN_THRESHOLD_BYTES = 4 * 1024 ** 3  # 4 GB


class MemorySink:
    """Accumulates all chunks in memory as DataFrames."""

    def __init__(self, max_memory_gb: float | None = None) -> None:
        self._max_bytes = int(max_memory_gb * 1024 ** 3) if max_memory_gb is not None else None
        self._chunks: dict[str, list[pd.DataFrame]] = {}
        self._cumulative_bytes: int = 0

    def open(self, schema: SpindleSchema | None) -> None:
        self._chunks = {}
        self._cumulative_bytes = 0

    def write_chunk(self, table: str, arrays: dict[str, np.ndarray]) -> None:
        chunk_bytes = sum(a.nbytes for a in arrays.values())
        self._cumulative_bytes += chunk_bytes

        if self._max_bytes is not None and self._cumulative_bytes > self._max_bytes:
            raise MemoryError(
                f"MemorySink exceeded max_memory_gb limit "
                f"({self._cumulative_bytes / 1024**3:.1f} GB used)"
            )
        if self._cumulative_bytes > _WARN_THRESHOLD_BYTES:
            warnings.warn(
                f"MemorySink has accumulated {self._cumulative_bytes / 1024**3:.1f} GB. "
                "Consider using ParquetSink or a Fabric sink for large workloads.",
                ResourceWarning,
                stacklevel=2,
            )

        df = pd.DataFrame({col: vals for col, vals in arrays.items()})
        self._chunks.setdefault(table, []).append(df)

    def close(self) -> None:
        pass

    def result(self) -> dict[str, pd.DataFrame]:
        """Return all accumulated data as one DataFrame per table."""
        return {
            table: pd.concat(frames, ignore_index=True)
            for table, frames in self._chunks.items()
        }

    @property
    def _estimate_bytes(self) -> int:
        return self._cumulative_bytes
