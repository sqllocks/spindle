from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from sqllocks_spindle.schema.parser import SpindleSchema


class KQLSink:
    """Delegate sink that writes chunks to a Fabric Eventhouse (KQL database).

    Thin wrapper around :class:`~sqllocks_spindle.fabric.EventhouseWriter`.
    Accumulates numpy array chunks as DataFrames per table, then ingests via
    ``write()`` on close.

    Requires ``pip install sqllocks-spindle[fabric-kusto]``.

    Args:
        cluster_uri: Eventhouse URI, e.g.
            ``https://my-eventhouse.z0.kusto.fabric.microsoft.com``.
        database: KQL database name.
        auth_method: ``"cli"``, ``"msi"``, ``"spn"``, or ``"fabric"``
            (default ``"cli"``).
        table_prefix: Optional prefix prepended to every KQL table name.
        batch_size: Rows per ingestion batch (default 10 000).
        client_id: Service principal client ID (required when auth_method="spn").
        client_secret: Service principal secret (required when auth_method="spn").
        tenant_id: Azure AD tenant ID (required when auth_method="spn").
    """

    def __init__(
        self,
        cluster_uri: str,
        database: str,
        auth_method: str = "cli",
        table_prefix: str = "",
        batch_size: int = 10_000,
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
    ) -> None:
        self._cluster_uri = cluster_uri
        self._database = database
        self._auth_method = auth_method
        self._table_prefix = table_prefix
        self._batch_size = batch_size
        self._client_id = client_id
        self._client_secret = client_secret
        self._tenant_id = tenant_id
        self._writer = None
        self._chunks: dict[str, list[pd.DataFrame]] = {}

    def open(self, schema: SpindleSchema | None) -> None:
        # Defer import so that missing azure-kusto packages don't break local
        # test runs that don't exercise this sink.
        from sqllocks_spindle.fabric.eventhouse_writer import EventhouseWriter

        self._writer = EventhouseWriter(
            cluster_uri=self._cluster_uri,
            database=self._database,
            auth_method=self._auth_method,
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
        self._writer.write(
            result=tables,
            table_prefix=self._table_prefix,
            batch_size=self._batch_size,
        )
        self._chunks = {}
