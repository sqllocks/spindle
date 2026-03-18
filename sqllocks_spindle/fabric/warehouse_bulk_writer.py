"""Bulk writer for Fabric Warehouse using COPY INTO from staged Parquet.

Pattern: Stage Parquet to OneLake Files → COPY INTO → cleanup.
Uses workspace identity (no SAS tokens needed within the same workspace).

Requires the ``fabric-sql`` and ``parquet`` extras::

    pip install sqllocks-spindle[fabric-sql,parquet]
"""

from __future__ import annotations

import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class BulkWriteResult:
    """Result of a bulk warehouse write operation."""

    tables_written: int = 0
    total_rows: int = 0
    elapsed_seconds: float = 0.0
    per_table: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def summary(self) -> str:
        lines = [
            "Warehouse Bulk Write Result",
            "=" * 50,
            f"Tables written: {self.tables_written}",
            f"Total rows:     {self.total_rows:,}",
            f"Elapsed:        {self.elapsed_seconds:.1f}s",
            f"Throughput:     {self.total_rows / max(self.elapsed_seconds, 0.001):,.0f} rows/sec",
        ]
        if self.per_table:
            lines.append("")
            lines.append(f"{'Table':<30} {'Rows':>12} {'Chunks':>8} {'Time(s)':>10}")
            lines.append("-" * 62)
            for tname, info in self.per_table.items():
                lines.append(
                    f"{tname:<30} {info.get('rows', 0):>12,} "
                    f"{info.get('chunks', 0):>8} "
                    f"{info.get('elapsed', 0):>10.1f}"
                )
        if self.errors:
            lines.append("")
            lines.append(f"Errors ({len(self.errors)}):")
            for err in self.errors:
                lines.append(f"  - {err}")
        return "\n".join(lines)


class WarehouseBulkWriter:
    """Bulk writer for Microsoft Fabric Warehouse using COPY INTO.

    Stages data as Parquet files on a Lakehouse Files path, then uses
    ``COPY INTO`` to load them into Warehouse tables. This is significantly
    faster than row-by-row INSERT for large datasets.

    Args:
        connection_string: pyodbc connection string for the Warehouse endpoint.
        staging_lakehouse_path: abfss:// path to a Lakehouse Files area for staging.
            Example: ``abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Files``
        auth_method: Authentication method for the Warehouse connection
            (``"cli"``, ``"msi"``, ``"spn"``).
        schema_name: SQL schema to create tables in (default ``"dbo"``).
        local_staging_dir: Local directory for staging Parquet files before
            upload (when not running inside a Fabric Notebook). If None,
            uses a temp directory.

    Example::

        writer = WarehouseBulkWriter(
            connection_string="Driver={ODBC Driver 18 for SQL Server};Server=...",
            staging_lakehouse_path="abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files",
            auth_method="cli",
        )
        result = writer.write_chunked(chunked_result)
        print(result.summary())
    """

    def __init__(
        self,
        connection_string: str,
        staging_lakehouse_path: str,
        auth_method: str = "cli",
        schema_name: str = "dbo",
        local_staging_dir: str | Path | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
    ):
        self._connection_string = connection_string
        self._staging_lakehouse_path = staging_lakehouse_path.rstrip("/")
        self._auth_method = auth_method
        self._schema_name = schema_name
        self._local_staging_dir = Path(local_staging_dir) if local_staging_dir else None
        self._client_id = client_id
        self._client_secret = client_secret
        self._tenant_id = tenant_id
        self._run_id = uuid.uuid4().hex[:12]
        # Parse abfss URI components for ADLS Gen2 upload
        self._adls_account_url, self._adls_container, self._adls_base_path = (
            self._parse_abfss_uri(self._staging_lakehouse_path)
        )

    @staticmethod
    def _parse_abfss_uri(abfss_uri: str) -> tuple[str, str, str]:
        """Parse abfss://container@account/path → (https://account, container, path)."""
        # abfss://<container>@<account>/<path>
        import re
        m = re.match(r"abfss://([^@]+)@([^/]+)(.*)", abfss_uri)
        if not m:
            raise ValueError(f"Cannot parse abfss URI: {abfss_uri!r}")
        container = m.group(1)
        account_host = m.group(2)
        path = m.group(3).lstrip("/")
        account_url = f"https://{account_host}"
        return account_url, container, path

    @staticmethod
    def _abfss_to_https(abfss_path: str) -> str:
        """Convert abfss:// OneLake path to https:// for COPY INTO.

        COPY INTO requires https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<rest>
        per Microsoft Learn docs. abfss:// paths are NOT accepted.

        abfss://<workspaceId>@onelake.dfs.fabric.microsoft.com/<lakehouseId>/Files/...
        → https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<lakehouseId>/Files/...
        """
        import re
        m = re.match(r"abfss://([^@]+)@([^/]+)/(.*)", abfss_path)
        if not m:
            return abfss_path  # already https or non-abfss — pass through unchanged
        workspace_id = m.group(1)
        host = m.group(2)
        rest = m.group(3)
        return f"https://{host}/{workspace_id}/{rest}"

    def _get_adls_credential(self):
        """Build an ADLS Gen2 credential matching the configured auth_method."""
        from azure.identity import (
            AzureCliCredential,
            ClientSecretCredential,
            ManagedIdentityCredential,
        )
        if self._auth_method == "cli":
            return AzureCliCredential()
        elif self._auth_method == "msi":
            return ManagedIdentityCredential()
        elif self._auth_method == "spn":
            return ClientSecretCredential(
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret,
            )
        raise ValueError(f"Unsupported auth_method for ADLS upload: {self._auth_method!r}")

    def _upload_to_onelake(self, local_path: Path, remote_adls_path: str) -> None:
        """Upload a local file to OneLake via ADLS Gen2 SDK."""
        try:
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError:
            raise ImportError(
                "azure-storage-file-datalake is required for local->OneLake staging. "
                "Install with: pip install azure-storage-file-datalake"
            )
        credential = self._get_adls_credential()
        service = DataLakeServiceClient(
            account_url=self._adls_account_url,
            credential=credential,
        )
        fs = service.get_file_system_client(self._adls_container)
        file_client = fs.get_file_client(remote_adls_path)
        with open(local_path, "rb") as f:
            file_client.upload_data(f.read(), overwrite=True)
        logger.debug("Uploaded %s -> adls://%s/%s", local_path.name, self._adls_container, remote_adls_path)

    def create_table(self, table_name: str, sample_df: pd.DataFrame) -> None:
        """Create the Warehouse table from a sample DataFrame's schema.

        Drops the table first if it exists.
        """
        from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter

        sql_writer = FabricSqlDatabaseWriter(
            connection_string=self._connection_string,
            auth_method=self._auth_method,
            client_id=self._client_id,
            client_secret=self._client_secret,
            tenant_id=self._tenant_id,
        )
        sql_writer.write(
            {table_name: sample_df.head(0)},
            schema_name=self._schema_name,
            mode="create_insert",
        )

    def stage_chunk(
        self,
        table_name: str,
        chunk_df: pd.DataFrame,
        chunk_idx: int,
    ) -> str:
        """Write a chunk as Parquet and stage it on OneLake for COPY INTO.

        - Inside a Fabric Notebook: writes directly via notebookutils.
        - Outside Fabric (local dev): writes to a temp file then uploads via
          ADLS Gen2 SDK (azure-storage-file-datalake), so COPY INTO always
          reads from the correct abfss:// path.

        Returns the abfss:// remote path.
        """
        import tempfile

        remote_dir = f"{self._adls_base_path}/staging/{self._run_id}/{table_name}"
        filename = f"chunk_{chunk_idx:06d}.parquet"
        remote_path = f"{self._staging_lakehouse_path}/staging/{self._run_id}/{table_name}/{filename}"

        # Fabric Warehouse COPY INTO only supports TIMESTAMP(us) — not TIMESTAMP(ns).
        # pandas defaults to datetime64[ns], which pyarrow writes as TIMESTAMP(ns)
        # (Parquet INT64 without a supported logical type for Fabric).
        # Downcast all datetime64[ns] columns to datetime64[us] before writing.
        chunk_df = chunk_df.copy()
        for col in chunk_df.columns:
            if pd.api.types.is_datetime64_ns_dtype(chunk_df[col]):
                chunk_df[col] = chunk_df[col].astype("datetime64[us]")

        # Inside Fabric Notebook — write Parquet to the mounted lakehouse Files path.
        # notebookutils.fs.put() only accepts strings (not bytes), so we write
        # directly to the /lakehouse/default/Files/ mount provided by Fabric.
        try:
            import notebookutils  # type: ignore[import-not-found]
            import os as _os

            staging_dir = f"/lakehouse/default/Files/staging/{self._run_id}/{table_name}"
            _os.makedirs(staging_dir, exist_ok=True)
            local_path = f"{staging_dir}/{filename}"
            chunk_df.to_parquet(local_path, index=False, engine="pyarrow")
            logger.info(
                "Staged chunk %d for %s on OneLake via lakehouse mount (%d rows)",
                chunk_idx, table_name, len(chunk_df),
            )
            return remote_path
        except ImportError:
            pass

        # Outside Fabric — write to temp file and upload via ADLS Gen2 SDK
        with tempfile.TemporaryDirectory() as tmp:
            local_path = Path(tmp) / filename
            chunk_df.to_parquet(local_path, index=False, engine="pyarrow")  # already ns→us above
            adls_file_path = f"{remote_dir}/{filename}"
            self._upload_to_onelake(local_path, adls_file_path)
            logger.info(
                "Staged chunk %d for %s on OneLake via ADLS upload (%d rows -> %s)",
                chunk_idx, table_name, len(chunk_df), remote_path,
            )
        return remote_path

    def copy_into(self, table_name: str) -> int:
        """Execute COPY INTO from staged Parquet files into the Warehouse table.

        Returns the number of rows loaded (as reported by the command).
        """
        # COPY INTO requires https:// for OneLake — abfss:// is not accepted.
        # Ref: https://learn.microsoft.com/sql/t-sql/statements/copy-into-transact-sql?view=fabric
        staging_abfss = (
            f"{self._staging_lakehouse_path}/staging/{self._run_id}/{table_name}/"
        )
        staging_path = self._abfss_to_https(staging_abfss)
        qualified = f"[{self._schema_name}].[{table_name}]"

        copy_sql = f"""
        COPY INTO {qualified}
        FROM '{staging_path}'
        WITH (
            FILE_TYPE = 'PARQUET'
        )
        """

        from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter
        writer = FabricSqlDatabaseWriter(
            connection_string=self._connection_string,
            auth_method=self._auth_method,
            client_id=self._client_id,
            client_secret=self._client_secret,
            tenant_id=self._tenant_id,
        )
        conn = writer._get_connection()
        conn.timeout = 600  # 10 min for long COPY INTO operations
        try:
            cursor = conn.cursor()
            cursor.execute(copy_sql)
            conn.commit()
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {qualified}")
            row_count = cursor.fetchone()[0]
            logger.info("COPY INTO %s completed (%d rows)", table_name, row_count)
            return row_count
        finally:
            conn.close()

    def cleanup_staging(self, table_name: str | None = None) -> None:
        """Remove staged Parquet files from OneLake.

        If table_name is specified, clean only that table's staging files.
        Otherwise, clean the entire run's staging directory.
        """
        if table_name:
            adls_path = f"{self._adls_base_path}/staging/{self._run_id}/{table_name}"
        else:
            adls_path = f"{self._adls_base_path}/staging/{self._run_id}"

        # Inside Fabric Notebook
        try:
            import notebookutils  # type: ignore[import-not-found]
            staging_abfss = f"{self._staging_lakehouse_path}/staging/{self._run_id}"
            if table_name:
                staging_abfss += f"/{table_name}"
            notebookutils.fs.rm(staging_abfss, recurse=True)
            logger.info("Cleaned OneLake staging (notebookutils): %s", staging_abfss)
            return
        except ImportError:
            pass

        # Outside Fabric — delete via ADLS Gen2 SDK
        try:
            from azure.storage.filedatalake import DataLakeServiceClient
            credential = self._get_adls_credential()
            service = DataLakeServiceClient(
                account_url=self._adls_account_url,
                credential=credential,
            )
            fs = service.get_file_system_client(self._adls_container)
            dir_client = fs.get_directory_client(adls_path)
            dir_client.delete_directory()
            logger.info("Cleaned OneLake staging (ADLS): %s", adls_path)
        except Exception as exc:
            logger.warning("Could not clean OneLake staging %s: %s", adls_path, exc)

    def write_chunked(self, chunked_result: Any) -> BulkWriteResult:
        """Write a ChunkedGenerationResult to the Warehouse.

        Creates tables, stages all chunks as Parquet, executes COPY INTO
        for each table, then cleans up staging files.
        """
        start = time.time()
        result = BulkWriteResult()

        # Write parent tables
        for table_name, df in chunked_result.parent_tables.items():
            table_start = time.time()
            try:
                self.create_table(table_name, df)
                if len(df) > 0:
                    self.stage_chunk(table_name, df, 0)
                    rows = self.copy_into(table_name)
                else:
                    rows = 0
                self.cleanup_staging(table_name)
                result.per_table[table_name] = {
                    "rows": rows,
                    "chunks": 1,
                    "elapsed": time.time() - table_start,
                }
                result.total_rows += rows
                result.tables_written += 1
            except Exception as e:
                result.errors.append(f"{table_name}: {e}")
                logger.error("Error writing %s: %s", table_name, e)

        # Write child tables
        for table_name in chunked_result.child_table_names:
            table_start = time.time()
            chunk_count = 0
            total_chunk_rows = 0
            try:
                first_chunk = True
                for idx, chunk_df in enumerate(chunked_result.iter_chunks(table_name)):
                    if first_chunk:
                        self.create_table(table_name, chunk_df)
                        first_chunk = False
                    self.stage_chunk(table_name, chunk_df, idx)
                    chunk_count += 1
                    total_chunk_rows += len(chunk_df)

                rows = self.copy_into(table_name)
                self.cleanup_staging(table_name)

                result.per_table[table_name] = {
                    "rows": rows,
                    "chunks": chunk_count,
                    "elapsed": time.time() - table_start,
                }
                result.total_rows += rows
                result.tables_written += 1
            except Exception as e:
                result.errors.append(f"{table_name}: {e}")
                logger.error("Error writing %s: %s", table_name, e)

        result.elapsed_seconds = time.time() - start
        return result

    def write_table_chunks(
        self,
        table_name: str,
        chunks: Iterator[pd.DataFrame],
    ) -> BulkWriteResult:
        """Write an arbitrary iterator of DataFrames to a single Warehouse table.

        Useful for manual chunked writes outside of ChunkedGenerationResult.
        """
        start = time.time()
        result = BulkWriteResult()
        chunk_count = 0
        total_rows = 0

        try:
            first_chunk = True
            for idx, chunk_df in enumerate(chunks):
                if first_chunk:
                    self.create_table(table_name, chunk_df)
                    first_chunk = False
                self.stage_chunk(table_name, chunk_df, idx)
                chunk_count += 1
                total_rows += len(chunk_df)

            rows = self.copy_into(table_name)
            self.cleanup_staging(table_name)

            result.per_table[table_name] = {
                "rows": rows,
                "chunks": chunk_count,
                "elapsed": time.time() - start,
            }
            result.total_rows = rows
            result.tables_written = 1
        except Exception as e:
            result.errors.append(f"{table_name}: {e}")
            logger.error("Error writing %s: %s", table_name, e)

        result.elapsed_seconds = time.time() - start
        return result

    def write_tables(
        self,
        tables: dict[str, pd.DataFrame],
        chunk_size: int = 1_000_000,
    ) -> BulkWriteResult:
        """Write a dict of DataFrames to the Warehouse via COPY INTO.

        For each table: DROP/CREATE, stage all chunks as Parquet, then
        execute a single wildcard COPY INTO per table. Tables are loaded
        concurrently via ThreadPoolExecutor (capped at 30 to stay below
        Fabric's 32-concurrent-query limit).

        Args:
            tables: Mapping of table_name -> DataFrame.
            chunk_size: Rows per Parquet chunk file (default 1M).

        Returns:
            BulkWriteResult with per-table row counts and any errors.
        """
        start = time.time()
        result = BulkWriteResult()

        def _load_one_table(table_name: str, df: pd.DataFrame) -> dict:
            """Stage + COPY INTO for a single table. Returns info dict."""
            table_start = time.time()
            self.create_table(table_name, df)
            if df.empty:
                return {"rows": 0, "chunks": 0, "elapsed": time.time() - table_start}

            # Stage in chunks
            chunk_count = 0
            for i in range(0, len(df), chunk_size):
                chunk_df = df.iloc[i : i + chunk_size]
                self.stage_chunk(table_name, chunk_df, chunk_count)
                chunk_count += 1

            # Single COPY INTO with wildcard (Fabric parallelizes internally)
            rows = self.copy_into(table_name)
            self.cleanup_staging(table_name)
            return {"rows": rows, "chunks": chunk_count, "elapsed": time.time() - table_start}

        # Run tables concurrently (max 4 to avoid COPY INTO timeouts when
        # the warehouse queues long-running loads)
        max_workers = min(len(tables), 4)
        with ThreadPoolExecutor(max_workers=max(max_workers, 1)) as pool:
            futures = {
                pool.submit(_load_one_table, tname, df): tname
                for tname, df in tables.items()
            }
            for future in as_completed(futures):
                tname = futures[future]
                try:
                    info = future.result()
                    result.per_table[tname] = info
                    result.total_rows += info["rows"]
                    result.tables_written += 1
                    logger.info(
                        "Bulk wrote %s: %d rows, %d chunks (%.1fs)",
                        tname, info["rows"], info["chunks"], info["elapsed"],
                    )
                except Exception as e:
                    result.errors.append(f"{tname}: {e}")
                    logger.error("Error writing %s: %s", tname, e)

        result.elapsed_seconds = time.time() - start
        return result
