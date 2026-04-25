from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from sqllocks_spindle.schema.parser import SpindleSchema


class LakehouseSink:
    """Delegate sink that writes chunks to a Fabric Lakehouse Files landing zone.

    Thin wrapper around LakehouseFilesWriter.  Accumulates numpy array chunks
    as DataFrames per table, then flushes via ``write_all()`` on close.

    Works locally (``base_path`` is a filesystem path) or against OneLake
    (``base_path`` is an ``abfss://`` URL).  When using an abfss:// path the
    ``azure-storage-file-datalake`` and ``azure-identity`` packages must be
    installed (``pip install sqllocks-spindle[fabric-files]``).

    Args:
        base_path: Root path for the landing zone.  Passed through to
            :class:`~sqllocks_spindle.fabric.LakehouseFilesWriter`.
        format: Output format — ``"parquet"``, ``"csv"``, or ``"jsonl"``
            (default ``"parquet"``).
    """

    def __init__(
        self,
        base_path: str | Path | None = None,
        format: str = "parquet",
    ) -> None:
        self._base_path = base_path
        self._format = format
        self._writer = None
        self._chunks: dict[str, list[pd.DataFrame]] = {}

    def open(self, schema: SpindleSchema | None) -> None:
        from sqllocks_spindle.fabric.lakehouse_files_writer import LakehouseFilesWriter

        self._writer = LakehouseFilesWriter(
            base_path=self._base_path,
            default_format=self._format,
        )
        self._chunks = {}

    def write_chunk(self, table: str, arrays: dict[str, np.ndarray]) -> None:
        df = pd.DataFrame({col: vals for col, vals in arrays.items()})
        self._chunks.setdefault(table, []).append(df)

    def close(self) -> None:
        if self._writer is None or not self._chunks:
            return
        tables = {
            table: pd.concat(frames, ignore_index=True)
            for table, frames in self._chunks.items()
        }
        self._writer.write_all(tables, format=self._format)
        self._chunks = {}
