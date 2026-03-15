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
        """Write a chunk as a Parquet file to the staging area.

        Returns the staging path (abfss:// or local).
        """
        staging_dir = f"{self._staging_lakehouse_path}/staging/{self._run_id}/{table_name}"
        filename = f"chunk_{chunk_idx:06d}.parquet"

        if self._local_staging_dir is not None:
            local_dir = self._local_staging_dir / "staging" / self._run_id / table_name
            local_dir.mkdir(parents=True, exist_ok=True)
            local_path = local_dir / filename
            chunk_df.to_parquet(local_path, index=False, engine="pyarrow")
            logger.info(
                "Staged chunk %d for %s locally (%d rows, %s)",
                chunk_idx, table_name, len(chunk_df), local_path,
            )
            return str(local_path)

        # In Fabric Notebook context, write directly to OneLake via notebookutils
        try:
            import notebookutils  # type: ignore[import-not-found]
            # Write Parquet to OneLake staging path
            remote_path = f"{staging_dir}/{filename}"
            # Convert to parquet bytes and write via mssparkutils
            parquet_bytes = chunk_df.to_parquet(index=False, engine="pyarrow")
            notebookutils.fs.put(remote_path, parquet_bytes, overwrite=True)
            logger.info(
                "Staged chunk %d for %s on OneLake (%d rows)",
                chunk_idx, table_name, len(chunk_df),
            )
            return remote_path
        except ImportError:
            # Fallback: use local staging
            import tempfile
            local_dir = Path(tempfile.mkdtemp()) / "staging" / self._run_id / table_name
            local_dir.mkdir(parents=True, exist_ok=True)
            local_path = local_dir / filename
            chunk_df.to_parquet(local_path, index=False, engine="pyarrow")
            logger.info(
                "Staged chunk %d for %s locally (fallback: %s)",
                chunk_idx, table_name, local_path,
            )
            return str(local_path)

    def copy_into(self, table_name: str) -> int:
        """Execute COPY INTO from staged Parquet files into the Warehouse table.

        Returns the number of rows loaded (as reported by the command).
        """
        staging_path = (
            f"{self._staging_lakehouse_path}/staging/{self._run_id}/{table_name}/"
        )
        qualified = f"[{self._schema_name}].[{table_name}]"

        copy_sql = f"""
        COPY INTO {qualified}
        FROM '{staging_path}'
        WITH (
            FILE_TYPE = 'PARQUET',
            CREDENTIAL = ()
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
        """Remove staged Parquet files.

        If table_name is specified, clean only that table's staging files.
        Otherwise, clean the entire run's staging directory.
        """
        if table_name:
            staging_path = (
                f"{self._staging_lakehouse_path}/staging/{self._run_id}/{table_name}"
            )
        else:
            staging_path = f"{self._staging_lakehouse_path}/staging/{self._run_id}"

        try:
            import notebookutils  # type: ignore[import-not-found]
            notebookutils.fs.rm(staging_path, recurse=True)
            logger.info("Cleaned staging: %s", staging_path)
        except ImportError:
            if self._local_staging_dir:
                import shutil
                local_path = self._local_staging_dir / "staging" / self._run_id
                if table_name:
                    local_path = local_path / table_name
                if local_path.exists():
                    shutil.rmtree(local_path)
                    logger.info("Cleaned local staging: %s", local_path)

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
