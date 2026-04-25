from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from sqllocks_spindle.schema.parser import SpindleSchema


class WarehouseSink:
    """Delegate sink that writes chunks to a Fabric Warehouse via COPY INTO.

    Thin wrapper around :class:`~sqllocks_spindle.fabric.WarehouseBulkWriter`.
    Accumulates numpy array chunks as DataFrames per table, then flushes via
    ``write_tables()`` on close.

    Requires ``pip install sqllocks-spindle[fabric-sql,parquet]``.

    Args:
        connection_string: pyodbc connection string for the Warehouse endpoint.
        staging_lakehouse_path: abfss:// path to a Lakehouse Files area for
            staging Parquet files before COPY INTO.
        auth_method: ``"cli"``, ``"msi"``, or ``"spn"`` (default ``"cli"``).
        schema_name: SQL schema for target tables (default ``"dbo"``).
        client_id: Service principal client ID (required when auth_method="spn").
        client_secret: Service principal secret (required when auth_method="spn").
        tenant_id: Azure AD tenant ID (required when auth_method="spn").
        chunk_size: Rows per staged Parquet file (default 1 000 000).
    """

    def __init__(
        self,
        connection_string: str,
        staging_lakehouse_path: str,
        auth_method: str = "cli",
        schema_name: str = "dbo",
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
        chunk_size: int = 1_000_000,
    ) -> None:
        self._connection_string = connection_string
        self._staging_lakehouse_path = staging_lakehouse_path
        self._auth_method = auth_method
        self._schema_name = schema_name
        self._client_id = client_id
        self._client_secret = client_secret
        self._tenant_id = tenant_id
        self._chunk_size = chunk_size
        self._writer = None
        self._chunks: dict[str, list[pd.DataFrame]] = {}

    def open(self, schema: SpindleSchema | None) -> None:
        # Defer import so that missing pyodbc / azure-storage packages don't
        # break local test runs that don't exercise this sink.
        from sqllocks_spindle.fabric.warehouse_bulk_writer import WarehouseBulkWriter

        self._writer = WarehouseBulkWriter(
            connection_string=self._connection_string,
            staging_lakehouse_path=self._staging_lakehouse_path,
            auth_method=self._auth_method,
            schema_name=self._schema_name,
            client_id=self._client_id,
            client_secret=self._client_secret,
            tenant_id=self._tenant_id,
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
        result = self._writer.write_tables(tables=tables, chunk_size=self._chunk_size)
        self._chunks = {}
        if result.errors:
            raise RuntimeError(
                f"WarehouseSink.close() — {len(result.errors)} table(s) failed:\n"
                + "\n".join(result.errors)
            )
