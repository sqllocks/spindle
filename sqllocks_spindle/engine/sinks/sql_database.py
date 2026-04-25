from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from sqllocks_spindle.schema.parser import SpindleSchema


class SQLDatabaseSink:
    """Delegate sink that writes chunks to a Fabric SQL Database (or Azure SQL / Warehouse).

    Thin wrapper around
    :class:`~sqllocks_spindle.fabric.FabricSqlDatabaseWriter`.
    Accumulates numpy array chunks as DataFrames per table, then flushes via
    ``write()`` on close.

    Requires ``pip install sqllocks-spindle[fabric-sql]``.

    Args:
        connection_string: pyodbc connection string for the SQL endpoint.
        auth_method: ``"cli"``, ``"msi"``, ``"spn"``, ``"sql"``,
            ``"device-code"``, or ``"fabric"`` (default ``"cli"``).
        schema_name: Target SQL schema (default ``"dbo"``).
        mode: Write mode — ``"create_insert"``, ``"insert_only"``,
            ``"truncate_insert"``, or ``"append"`` (default ``"create_insert"``).
        batch_size: Rows per INSERT batch (default 5 000).
        staging_lakehouse_path: Optional abfss:// staging path to enable
            COPY INTO for Fabric Warehouse targets (faster bulk load).
        client_id: Service principal client ID (required when auth_method="spn").
        client_secret: Service principal secret (required when auth_method="spn").
        tenant_id: Azure AD tenant ID (required when auth_method="spn").
    """

    def __init__(
        self,
        connection_string: str,
        auth_method: str = "cli",
        schema_name: str = "dbo",
        mode: str = "create_insert",
        batch_size: int = 5_000,
        staging_lakehouse_path: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
    ) -> None:
        self._connection_string = connection_string
        self._auth_method = auth_method
        self._schema_name = schema_name
        self._mode = mode
        self._batch_size = batch_size
        self._staging_lakehouse_path = staging_lakehouse_path
        self._client_id = client_id
        self._client_secret = client_secret
        self._tenant_id = tenant_id
        self._writer = None
        self._chunks: dict[str, list[pd.DataFrame]] = {}

    def open(self, schema: SpindleSchema | None) -> None:
        # Defer import so that missing pyodbc doesn't break local test runs
        # that don't exercise this sink.
        from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter

        self._writer = FabricSqlDatabaseWriter(
            connection_string=self._connection_string,
            auth_method=self._auth_method,
            client_id=self._client_id,
            client_secret=self._client_secret,
            tenant_id=self._tenant_id,
            staging_lakehouse_path=self._staging_lakehouse_path,
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
        result = self._writer.write(
            result=tables,
            schema_name=self._schema_name,
            mode=self._mode,
            batch_size=self._batch_size,
        )
        self._chunks = {}
        if result.errors:
            raise RuntimeError(
                f"SQLDatabaseSink.close() — {len(result.errors)} table(s) failed:\n"
                + "\n".join(result.errors)
            )
