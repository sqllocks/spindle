from __future__ import annotations

import threading
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from sqllocks_spindle.schema.parser import SpindleSchema


class ParquetSink:
    """Write chunks as partitioned Parquet files under output_dir/{table}/part-NNNNNN.parquet."""

    def __init__(self, output_dir: str) -> None:
        self._base = Path(output_dir)
        self._counters: dict[str, int] = {}
        self._lock = threading.Lock()

    def open(self, schema: SpindleSchema | None) -> None:
        self._counters = {}

    def write_chunk(self, table: str, arrays: dict[str, np.ndarray]) -> None:
        table_dir = self._base / table
        table_dir.mkdir(parents=True, exist_ok=True)

        with self._lock:
            idx = self._counters.get(table, 0)
            self._counters[table] = idx + 1

        part_path = table_dir / f"part-{idx:06d}.parquet"
        df = pd.DataFrame({col: vals for col, vals in arrays.items()})
        df.to_parquet(part_path, index=False)

    def close(self) -> None:
        pass
