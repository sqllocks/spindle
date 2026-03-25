"""Write generated data to SQL databases via pyodbc.

Supports Fabric SQL Database, Fabric Warehouse, Azure SQL Database,
and SQL Server with Entra ID (Azure AD) and SQL authentication.

Requires the ``fabric-sql`` extra::

    pip install sqllocks-spindle[fabric-sql]
"""

from __future__ import annotations

import logging
import struct
import time
import warnings
from dataclasses import dataclass, field

from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class WriteResult:
    """Result of a database write operation."""

    tables_written: int
    total_rows: int
    elapsed_seconds: float
    per_table: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def summary(self) -> str:
        lines = [
            "SQL Database Write Result",
            "=" * 40,
            f"Tables written: {self.tables_written}",
            f"Total rows:     {self.total_rows:,}",
            f"Elapsed:        {self.elapsed_seconds:.1f}s",
        ]
        if self.per_table:
            lines.append("")
            lines.append(f"{'Table':<30} {'Rows':>10}")
            lines.append("-" * 40)
            for tname, count in self.per_table.items():
                lines.append(f"{tname:<30} {count:>10,}")
        if self.errors:
            lines.append("")
            lines.append(f"Errors ({len(self.errors)}):")
            for err in self.errors:
                lines.append(f"  - {err}")
        return "\n".join(lines)


class FabricSqlDatabaseWriter:
    """Write generated data to SQL databases via pyodbc.

    Supports:
        - Fabric SQL Database (\\*.database.fabric.microsoft.com)
        - Fabric Warehouse (\\*.datawarehouse.fabric.microsoft.com)
        - Azure SQL Database (\\*.database.windows.net)
        - SQL Server (on-prem or VM)

    Authentication methods:
        - ``"cli"`` — Azure CLI (``az login``), default for local dev
        - ``"msi"`` — Managed Identity, for Fabric Notebooks / Azure VMs
        - ``"spn"`` — Service Principal, for CI/CD pipelines
        - ``"sql"`` — SQL authentication (username/password), for SQL Server on-prem

    Usage::

        writer = FabricSqlDatabaseWriter(
            connection_string="Driver={ODBC Driver 18 for SQL Server};Server=...",
            auth_method="cli",
        )
        writer.write(result, schema_name="dbo", mode="create_insert")
    """

    # pyodbc attribute constant for access token
    _SQL_COPT_SS_ACCESS_TOKEN = 1256

    def __init__(
        self,
        connection_string: str,
        auth_method: str = "cli",
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
        staging_lakehouse_path: str | None = None,
    ):
        self._connection_string = connection_string
        self._auth_method = auth_method
        self._client_id = client_id
        self._client_secret = client_secret
        self._tenant_id = tenant_id
        self._is_warehouse = ".datawarehouse.fabric.microsoft.com" in connection_string
        self._staging_lakehouse_path = staging_lakehouse_path

        # Validate auth method
        valid_methods = ("cli", "msi", "spn", "sql", "device-code")
        if auth_method not in valid_methods:
            raise ValueError(f"auth_method must be one of {valid_methods}, got '{auth_method}'")

        if auth_method == "spn" and not all([client_id, client_secret, tenant_id]):
            raise ValueError("auth_method='spn' requires client_id, client_secret, and tenant_id")

        # Auto-build bulk writer for Warehouse + staging path (COPY INTO is 100x faster)
        self._bulk_writer = None
        if self._is_warehouse and staging_lakehouse_path:
            try:
                from sqllocks_spindle.fabric.warehouse_bulk_writer import WarehouseBulkWriter
                self._bulk_writer = WarehouseBulkWriter(
                    connection_string=connection_string,
                    staging_lakehouse_path=staging_lakehouse_path,
                    auth_method=auth_method,
                    client_id=client_id,
                    client_secret=client_secret,
                    tenant_id=tenant_id,
                )
                logger.info("Warehouse + staging path detected — will use COPY INTO for bulk writes")
            except Exception as e:
                logger.warning("Could not initialize bulk writer, falling back to INSERT: %s", e)

    def test_connection(self) -> bool:
        """Test the database connection. Returns True if successful."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

    def write(
        self,
        result: Any,
        schema_name: str = "dbo",
        mode: str = "create_insert",
        batch_size: int = 5000,
        table_order: list[str] | None = None,
        on_table_complete: Any | None = None,
    ) -> WriteResult:
        """Write all tables from a GenerationResult or dict of DataFrames.

        Args:
            result: A GenerationResult or dict[str, DataFrame].
            schema_name: SQL schema prefix (default "dbo").
            mode: Write mode — one of:
                - ``"create_insert"``: DROP + CREATE + INSERT (full reset)
                - ``"insert_only"``: INSERT into existing tables (no DDL)
                - ``"truncate_insert"``: TRUNCATE + INSERT (keep schema, reset data)
                - ``"append"``: INSERT without truncating (for Day 2 loads)
            batch_size: Rows per INSERT batch (default 5000).
            table_order: Explicit table write order. If None, uses
                GenerationResult.generation_order or dict key order.
            on_table_complete: Optional callback ``(table_name, row_count) -> None``
                invoked after each table is written. Use for progress reporting.

        Returns:
            WriteResult with per-table row counts and any errors.
        """
        valid_modes = ("create_insert", "insert_only", "truncate_insert", "append")
        if mode not in valid_modes:
            raise ValueError(f"mode must be one of {valid_modes}, got '{mode}'")

        # Normalize input
        if isinstance(result, dict):
            tables = result
            order = table_order or list(tables.keys())
            schema_obj = None
        else:
            tables = result.tables
            order = table_order or result.generation_order
            schema_obj = result.schema

        # Delegate to COPY INTO for Warehouse bulk writes
        if self._bulk_writer is not None:
            strategy = "COPY INTO (bulk)"
            logger.info("Write strategy: %s for %d tables", strategy, len(order))
            return self._write_via_bulk(tables, order, schema_name, mode, schema_obj)

        logger.info("Write strategy: INSERT (fast_executemany) for %d tables", len(order))

        start = time.time()
        write_result = WriteResult(tables_written=0, total_rows=0, elapsed_seconds=0)

        try:
            conn = self._get_connection()
        except Exception as e:
            write_result.errors.append(f"Connection failed: {e}")
            write_result.elapsed_seconds = time.time() - start
            return write_result

        try:
            cursor = conn.cursor()

            # Ensure target schema exists (Fabric Warehouse needs explicit CREATE SCHEMA)
            self._ensure_schema(cursor, schema_name)
            conn.commit()

            # DROP phase (reverse order for FK constraints)
            # Commit after each DDL — Fabric Warehouse can hang if DDL is batched
            # in a single uncommitted transaction.
            if mode == "create_insert":
                for tname in reversed(order):
                    if tname in tables:
                        self._drop_table(cursor, tname, schema_name)
                        conn.commit()

            # CREATE + INSERT per table (dependency order)
            # Commit after each table to avoid large uncommitted transactions
            # which are slow on Fabric Warehouse (distributed Delta storage).
            for tname in order:
                if tname not in tables:
                    continue

                df = tables[tname]
                try:
                    if mode == "create_insert":
                        self._create_table(cursor, tname, df, schema_name, schema_obj)
                        conn.commit()

                    if mode == "truncate_insert":
                        self._truncate_table(cursor, tname, schema_name)
                        conn.commit()

                    rows = self._insert_rows(cursor, tname, df, schema_name, batch_size, schema_obj)
                    conn.commit()
                    write_result.per_table[tname] = rows
                    write_result.total_rows += rows
                    write_result.tables_written += 1
                    logger.info("Wrote %d rows to %s.%s", rows, schema_name, tname)
                    if on_table_complete:
                        on_table_complete(tname, rows)
                except Exception as e:
                    write_result.errors.append(f"{tname}: {e}")
                    logger.error("Error writing %s: %s", tname, e)
                    try:
                        conn.rollback()
                    except Exception:
                        pass

        except Exception as e:
            write_result.errors.append(f"Transaction error: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

        write_result.elapsed_seconds = time.time() - start
        return write_result

    def write_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        schema_name: str = "dbo",
        mode: str = "create_insert",
        batch_size: int = 5000,
    ) -> int:
        """Write a single DataFrame to the database. Returns rows written."""
        result = self.write(
            {table_name: df},
            schema_name=schema_name,
            mode=mode,
            batch_size=batch_size,
        )
        if result.errors:
            raise RuntimeError(f"Write failed: {'; '.join(result.errors)}")
        return result.per_table.get(table_name, 0)

    def create_ddl(
        self,
        result: Any,
        schema_name: str = "dbo",
        dialect: str = "tsql",
    ) -> str:
        """Generate CREATE TABLE DDL as a string without executing.

        Args:
            result: A GenerationResult or dict[str, DataFrame].
            schema_name: SQL schema prefix.
            dialect: SQL dialect for DDL generation.

        Returns:
            SQL DDL string for all tables.
        """
        from sqllocks_spindle.output.pandas_writer import _generate_create_table_ddl

        if isinstance(result, dict):
            tables = result
            schema_obj = None
            order = list(tables.keys())
        else:
            tables = result.tables
            schema_obj = result.schema
            order = result.generation_order

        parts = []
        for tname in order:
            if tname not in tables:
                continue
            df = tables[tname]
            meta, pk = self._get_table_meta(tname, schema_obj)
            ddl = _generate_create_table_ddl(
                table_name=tname,
                df=df,
                schema_name=schema_name,
                sql_dialect=dialect,
                include_drop=True,
                include_go=dialect == "tsql",
                schema_meta=meta,
                primary_key=pk,
            )
            parts.append(ddl)

        return "\n\n".join(parts)

    # ----- internal: connection -----

    def _get_connection(self, _retries: int = 3, _delay: float = 2.0):
        """Build a pyodbc connection with appropriate auth.

        Retries on transient failures (e.g. IMDS cold-start in Fabric Spark).
        """
        try:
            import pyodbc
        except ImportError:
            raise ImportError(
                "pyodbc is required for SQL database output. "
                "Install with: pip install sqllocks-spindle[fabric-sql]"
            )

        if self._auth_method == "sql":
            return pyodbc.connect(self._connection_string, autocommit=False, timeout=120)

        # Entra ID token-based auth with retry
        last_err = None
        for attempt in range(1, _retries + 1):
            try:
                token_bytes = self._get_access_token()
                conn = pyodbc.connect(
                    self._connection_string,
                    attrs_before={self._SQL_COPT_SS_ACCESS_TOKEN: token_bytes},
                    autocommit=False,
                    timeout=120,
                )
                if attempt > 1:
                    logger.info("Connection succeeded on attempt %d", attempt)
                return conn
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "Connection attempt %d/%d failed: %s", attempt, _retries, exc
                )
                if attempt < _retries:
                    time.sleep(_delay * attempt)
        raise last_err

    def _get_access_token(self) -> bytes:
        """Acquire an Entra ID access token for the database resource."""
        try:
            from azure.identity import (
                AzureCliCredential,
                ClientSecretCredential,
                ManagedIdentityCredential,
            )
        except ImportError:
            raise ImportError(
                "azure-identity is required for Entra ID auth. "
                "Install with: pip install sqllocks-spindle[fabric-sql]"
            )

        resource = "https://database.windows.net/.default"

        if self._auth_method == "device-code":
            from azure.identity import DeviceCodeCredential
            credential = DeviceCodeCredential(
                tenant_id=self._tenant_id or None,
                prompt_callback=lambda uri, code, exp: print(
                    f"\n  Auth required: go to {uri} and enter code {code}\n", flush=True
                ),
            )
        elif self._auth_method == "cli":
            credential = AzureCliCredential()
        elif self._auth_method == "msi":
            # In Fabric Spark notebooks, prefer mssparkutils over IMDS
            # (ManagedIdentityCredential uses IMDS which is flaky in streaming)
            try:
                try:
                    from notebookutils import mssparkutils as _msu  # type: ignore[import-not-found]
                except ImportError:
                    import mssparkutils as _msu  # type: ignore[import-not-found]
                _token_str = _msu.credentials.getToken("https://database.windows.net/")
                if not _token_str or len(_token_str) < 50:
                    raise ValueError(
                        f"mssparkutils returned unusable token (len={len(_token_str) if _token_str else 0})"
                    )
                logger.info("Acquired SQL token via mssparkutils (len=%d)", len(_token_str))
                token_bytes = _token_str.encode("utf-16-le")
                return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
            except ImportError:
                logger.debug("mssparkutils not available, falling back to ManagedIdentityCredential")
            except Exception as exc:
                logger.warning("mssparkutils token failed (%s), falling back to ManagedIdentityCredential", exc)
            credential = ManagedIdentityCredential()
        elif self._auth_method == "spn":
            credential = ClientSecretCredential(
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret,
            )
        else:
            raise ValueError(f"Cannot acquire token for auth_method='{self._auth_method}'")

        token = credential.get_token(resource)
        # Encode as UTF-16-LE with DWORD length prefix for pyodbc ACCESSTOKEN struct
        token_bytes = token.token.encode("utf-16-le")
        return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    # ----- internal: DDL operations -----

    def _ensure_schema(self, cursor, schema_name: str):
        """Create the target schema if it doesn't exist.

        Uses dynamic SQL because ``CREATE SCHEMA`` must be the only statement
        in a batch on SQL Server / Fabric Warehouse.
        """
        if schema_name.lower() == "dbo":
            return  # dbo always exists
        try:
            cursor.execute(
                f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = ?) "
                f"EXEC('CREATE SCHEMA [{schema_name}]')",
                schema_name,
            )
        except Exception as e:
            logger.warning("Could not ensure schema '%s': %s", schema_name, e)

    def _drop_table(self, cursor, table_name: str, schema_name: str):
        """Drop a table if it exists.

        Uses ``DROP TABLE IF EXISTS`` instead of ``IF OBJECT_ID(...)`` because
        Fabric Warehouse does not support ``OBJECT_ID()`` — it hangs indefinitely.
        """
        qualified = f"[{schema_name}].[{table_name}]"
        cursor.execute(f"DROP TABLE IF EXISTS {qualified}")

    def _create_table(self, cursor, table_name: str, df: pd.DataFrame, schema_name: str, schema_obj):
        """Create a table based on DataFrame columns and optional schema metadata."""
        meta, pk = self._get_table_meta(table_name, schema_obj)
        qualified = f"[{schema_name}].[{table_name}]"

        col_defs = []
        for col_name in df.columns:
            col_meta = meta.get(col_name, {})
            sql_type = self._infer_sql_type(col_name, df[col_name].dtype, col_meta)
            # If the schema declares a narrow VARCHAR (e.g. state VARCHAR(2)) but the
            # actual data has longer values, widen the column to fit the data so that
            # SQL Server doesn't reject the INSERT with HY000 right-truncation.
            if "VARCHAR(" in sql_type and col_name in df.columns:
                series = df[col_name].dropna()
                if not series.empty:
                    try:
                        actual_max = int(series.astype(str).str.len().max())
                        import re as _re
                        m = _re.search(r"VARCHAR\((\d+)\)", sql_type)
                        if m and actual_max > int(m.group(1)):
                            sql_type = _re.sub(r"VARCHAR\(\d+\)", f"VARCHAR({actual_max})", sql_type)
                    except Exception:
                        pass
            nullable = col_meta.get("nullable", True)
            null_str = "NULL" if nullable else "NOT NULL"
            col_defs.append(f"    [{col_name}] {sql_type} {null_str}")

        create_sql = f"CREATE TABLE {qualified} (\n"
        create_sql += ",\n".join(col_defs)
        create_sql += "\n)"

        cursor.execute(create_sql)

    def _truncate_table(self, cursor, table_name: str, schema_name: str):
        """Truncate a table."""
        qualified = f"[{schema_name}].[{table_name}]"
        cursor.execute(f"TRUNCATE TABLE {qualified}")

    def _write_via_bulk(
        self,
        tables: dict[str, pd.DataFrame],
        order: list[str],
        schema_name: str,
        mode: str,
        schema_obj: Any,
    ) -> WriteResult:
        """Write tables via COPY INTO (Warehouse with staging path only)."""
        start = time.time()
        write_result = WriteResult(tables_written=0, total_rows=0, elapsed_seconds=0)

        self._bulk_writer._schema_name = schema_name

        for tname in order:
            if tname not in tables:
                continue
            df = tables[tname]
            try:
                if mode == "create_insert":
                    self._bulk_writer.create_table(tname, df)
                elif mode == "truncate_insert":
                    conn = self._get_connection()
                    cursor = conn.cursor()
                    self._truncate_table(cursor, tname, schema_name)
                    conn.commit()
                    conn.close()

                if not df.empty:
                    self._bulk_writer.stage_chunk(tname, df, 0)
                    rows = self._bulk_writer.copy_into(tname)
                    self._bulk_writer.cleanup_staging(tname)
                else:
                    rows = 0

                write_result.per_table[tname] = rows
                write_result.total_rows += rows
                write_result.tables_written += 1
                logger.info("Bulk wrote %d rows to %s.%s", rows, schema_name, tname)
            except Exception as e:
                write_result.errors.append(f"{tname}: {e}")
                logger.error("Bulk write error for %s: %s", tname, e)

        write_result.elapsed_seconds = time.time() - start
        return write_result

    @staticmethod
    def _coerce_df_for_insert(df: pd.DataFrame) -> pd.DataFrame:
        """Vectorized type coercion for pyodbc compatibility.

        Converts numpy/pandas types to Python natives at the column level,
        replacing the per-cell isinstance loop with ~10 vectorized ops
        regardless of row count.
        """
        out = df.copy()
        for col in out.columns:
            series = out[col]
            dtype = series.dtype

            if pd.api.types.is_bool_dtype(dtype):
                out[col] = series.astype(object).where(series.notna(), None)
            elif pd.api.types.is_integer_dtype(dtype):
                out[col] = series.where(series.notna(), None)
            elif pd.api.types.is_float_dtype(dtype):
                out[col] = series.where(series.notna(), None)
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    out[col] = series.dt.floor("us").astype(object).where(series.notna(), None)
            elif isinstance(dtype, pd.StringDtype):
                # pd.StringDtype (pandas nullable string) — convert to object so that
                # is_object_dtype() returns True downstream and the fast_executemany
                # cover-row buffer-sizing logic correctly detects string columns.
                out[col] = series.astype(object).where(series.notna(), None)
            elif isinstance(dtype, pd.CategoricalDtype):
                # pd.CategoricalDtype — convert to object so string categories are
                # visible to is_object_dtype() and the cover-row algorithm. Without
                # this, categorical string columns (e.g. state codes that may vary
                # in length across composite domains) bypass buffer-size correction.
                out[col] = series.astype(object).where(series.notna(), None)
            else:
                out[col] = series.where(series.notna(), None)

        return out

    def _insert_rows(
        self, cursor, table_name: str, df: pd.DataFrame,
        schema_name: str, batch_size: int, schema_obj=None,
    ) -> int:
        """Insert rows using fast parameterized queries. Returns row count."""
        import math
        import numpy as np

        if df.empty:
            return 0

        qualified = f"[{schema_name}].[{table_name}]"
        columns = ", ".join(f"[{c}]" for c in df.columns)
        placeholders = ", ".join(["?"] * len(df.columns))
        insert_sql = f"INSERT INTO {qualified} ({columns}) VALUES ({placeholders})"

        cursor.fast_executemany = True

        coerced = self._coerce_df_for_insert(df)

        # Convert "date"-typed datetime columns from Timestamp → datetime.date.
        # fast_executemany overflows when a Timestamp (datetime+time) is sent to
        # a SQL DATE column that has no time component.
        # Also convert boolean string columns ("true"/"false") → Python bool.
        # When a schema column is type="boolean" the DB column is BIT, but
        # Spindle's weighted_enum generator may produce string values "true"/"false".
        # pyodbc's fast_executemany sizes the BIT parameter buffer from SQLDescribeParam
        # (ColumnSize=1), which for SQL_C_WCHAR = 2 bytes — too small for "false"
        # (10 bytes), causing HY000 right-truncation.
        meta, _ = self._get_table_meta(table_name, schema_obj)
        date_cols = {col for col, m in meta.items() if m.get("type") == "date"}
        bool_cols = {col for col, m in meta.items() if m.get("type") == "boolean"}
        for col in date_cols:
            if col in coerced.columns and pd.api.types.is_object_dtype(coerced[col]):
                coerced[col] = coerced[col].apply(
                    lambda v: v.date() if hasattr(v, "date") and v is not None else v
                )
        for col in bool_cols:
            if col in coerced.columns and pd.api.types.is_object_dtype(coerced[col]):
                # Convert "true"/"false" strings (and any other truthy/falsy strings)
                # to Python bool so pyodbc sends SQL_C_BIT to the BIT column.
                def _to_bool(v):
                    if v is None:
                        return None
                    if isinstance(v, bool):
                        return v
                    if isinstance(v, str):
                        return v.strip().lower() not in ("false", "0", "no", "f", "")
                    return bool(v)
                coerced[col] = coerced[col].apply(_to_bool)

        # pyodbc.cursor.setinputsizes() is a DB-API 2.0 no-op — it does nothing.
        # pyodbc fast_executemany sizes its parameter buffers from the FIRST ROW.
        # If later rows have longer strings, the driver silently right-truncates.
        # Fix: identify per-column max string lengths, find (or synthesise) a
        # "max row" to put first so that buffers are allocated to the right size.

        # Identify string-column positions and pre-compute max lengths once (W5)
        str_col_idxs = [
            i for i, col in enumerate(coerced.columns)
            if pd.api.types.is_object_dtype(coerced[col])
            and coerced[col].dropna().apply(lambda v: isinstance(v, str)).any()
        ]
        # Pre-compute global max string length per string column
        global_max_lens: dict[int, int] = {}
        for j in str_col_idxs:
            col = coerced.iloc[:, j]
            str_vals = col[col.apply(lambda v: isinstance(v, str))]
            global_max_lens[j] = int(str_vals.str.len().max()) if len(str_vals) > 0 else 0

        rows_written = 0
        for batch_start in range(0, len(coerced), batch_size):
            batch = coerced.iloc[batch_start : batch_start + batch_size]
            # pandas .values.tolist() on a mixed-dtype DataFrame returns:
            #   - float('nan') for None in object columns  → replace with None
            #   - numpy.int64 for integer columns          → convert to Python int
            # Both confuse pyodbc's fast_executemany type inference.
            raw = batch.values.tolist()
            params = [
                [
                    None if (isinstance(v, float) and math.isnan(v))
                    else int(v) if isinstance(v, np.integer)
                    else v
                    for v in row
                ]
                for row in raw
            ]

            # fast_executemany sizes its parameter buffers from the FIRST ROW.
            # Build a synthetic cover row at params[0] by replacing each string
            # column's value with the max-length string from anywhere in the batch.
            # All replacement values are real data — just assembled column-by-column
            # from different rows — so fast_executemany can stay enabled.
            if str_col_idxs and len(params) > 1:
                row0 = list(params[0])
                for j in str_col_idxs:
                    needed = global_max_lens.get(j, 0)
                    current_len = len(row0[j]) if isinstance(row0[j], str) else 0
                    if current_len < needed:
                        # Pull the max-length string for this column from any row in batch
                        row0[j] = max(
                            (r[j] for r in params if isinstance(r[j], str)),
                            key=len,
                            default=row0[j],
                        )
                params[0] = row0

            try:
                cursor.executemany(insert_sql, params)
            except Exception as _exc:
                if "right truncation" in str(_exc).lower():
                    col_names = list(coerced.columns)
                    logger.error(
                        "TRUNCATION DIAGNOSIS for %s.%s (fast_executemany=%s):",
                        table_name, schema_name, cursor.fast_executemany,
                    )
                    for _j, _col in enumerate(col_names):
                        _str_vals = [r[_j] for r in params if isinstance(r[_j], str)]
                        if _str_vals:
                            _first = params[0][_j]
                            _first_len = len(_first) if isinstance(_first, str) else f"non-str({type(_first).__name__})"
                            _max_len = max(len(v) for v in _str_vals)
                            logger.error(
                                "  col[%d] %-30s first_len=%-6s max_len=%d",
                                _j, _col, _first_len, _max_len,
                            )
                raise
            rows_written += len(params)

        return rows_written

    # ----- internal: metadata helpers -----

    def _get_table_meta(self, table_name: str, schema_obj) -> tuple[dict, list[str]]:
        """Extract column metadata and primary key from SpindleSchema."""
        if schema_obj is None:
            return {}, []

        tdef = schema_obj.tables.get(table_name)
        if tdef is None:
            return {}, []

        meta = {}
        for cname, cdef in tdef.columns.items():
            meta[cname] = {
                "type": cdef.type,
                "nullable": cdef.nullable,
                "max_length": cdef.max_length,
                "precision": cdef.precision,
                "scale": cdef.scale,
            }
        return meta, tdef.primary_key

    def _infer_sql_type(self, col_name: str, dtype, col_meta: dict) -> str:
        """Infer a T-SQL type from Spindle type or pandas dtype.

        Fabric Warehouse does not support NVARCHAR or bare DATETIME2 —
        uses VARCHAR and DATETIME2(6) instead.
        """
        from sqllocks_spindle.output.pandas_writer import _sql_type_for_column
        sql_type = _sql_type_for_column(col_name, dtype, "tsql", col_meta if col_meta else None)
        if self._is_warehouse:
            sql_type = sql_type.replace("NVARCHAR", "VARCHAR")
            if sql_type == "DATETIME2":
                sql_type = "DATETIME2(6)"
        return sql_type
