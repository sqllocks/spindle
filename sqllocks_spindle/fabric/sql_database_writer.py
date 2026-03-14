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
from dataclasses import dataclass, field
from itertools import chain, repeat
from typing import Any

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
    ):
        self._connection_string = connection_string
        self._auth_method = auth_method
        self._client_id = client_id
        self._client_secret = client_secret
        self._tenant_id = tenant_id

        # Validate auth method
        valid_methods = ("cli", "msi", "spn", "sql")
        if auth_method not in valid_methods:
            raise ValueError(f"auth_method must be one of {valid_methods}, got '{auth_method}'")

        if auth_method == "spn" and not all([client_id, client_secret, tenant_id]):
            raise ValueError("auth_method='spn' requires client_id, client_secret, and tenant_id")

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
        batch_size: int = 1000,
        table_order: list[str] | None = None,
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
            batch_size: Rows per INSERT batch.
            table_order: Explicit table write order. If None, uses
                GenerationResult.generation_order or dict key order.

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

            # DROP phase (reverse order for FK constraints)
            if mode == "create_insert":
                for tname in reversed(order):
                    if tname in tables:
                        self._drop_table(cursor, tname, schema_name)

            # CREATE + INSERT per table (dependency order)
            for tname in order:
                if tname not in tables:
                    continue

                df = tables[tname]
                try:
                    if mode == "create_insert":
                        self._create_table(cursor, tname, df, schema_name, schema_obj)

                    if mode == "truncate_insert":
                        self._truncate_table(cursor, tname, schema_name)

                    rows = self._insert_rows(cursor, tname, df, schema_name, batch_size)
                    write_result.per_table[tname] = rows
                    write_result.total_rows += rows
                    write_result.tables_written += 1
                    logger.info("Wrote %d rows to %s.%s", rows, schema_name, tname)
                except Exception as e:
                    write_result.errors.append(f"{tname}: {e}")
                    logger.error("Error writing %s: %s", tname, e)

            conn.commit()

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
        batch_size: int = 1000,
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

    def _get_connection(self):
        """Build a pyodbc connection with appropriate auth."""
        try:
            import pyodbc
        except ImportError:
            raise ImportError(
                "pyodbc is required for SQL database output. "
                "Install with: pip install sqllocks-spindle[fabric-sql]"
            )

        if self._auth_method == "sql":
            return pyodbc.connect(self._connection_string, autocommit=False)

        # Entra ID token-based auth
        token_bytes = self._get_access_token()
        return pyodbc.connect(
            self._connection_string,
            attrs_before={self._SQL_COPT_SS_ACCESS_TOKEN: token_bytes},
            autocommit=False,
        )

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

        if self._auth_method == "cli":
            credential = AzureCliCredential()
        elif self._auth_method == "msi":
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
        token_bytes = bytes(token.token, "UTF-8")
        # Encode as UTF-16-LE with length prefix for pyodbc
        encoded = bytes(chain.from_iterable(zip(token_bytes, repeat(0))))
        return struct.pack("<i", len(encoded)) + encoded

    # ----- internal: DDL operations -----

    def _drop_table(self, cursor, table_name: str, schema_name: str):
        """Drop a table if it exists."""
        qualified = f"[{schema_name}].[{table_name}]"
        cursor.execute(
            f"IF OBJECT_ID('{qualified}', 'U') IS NOT NULL DROP TABLE {qualified}"
        )

    def _create_table(self, cursor, table_name: str, df: pd.DataFrame, schema_name: str, schema_obj):
        """Create a table based on DataFrame columns and optional schema metadata."""
        meta, pk = self._get_table_meta(table_name, schema_obj)
        qualified = f"[{schema_name}].[{table_name}]"

        col_defs = []
        for col_name in df.columns:
            col_meta = meta.get(col_name, {})
            sql_type = self._infer_sql_type(col_name, df[col_name].dtype, col_meta)
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

    def _insert_rows(
        self, cursor, table_name: str, df: pd.DataFrame,
        schema_name: str, batch_size: int,
    ) -> int:
        """Insert rows using parameterized queries. Returns row count."""
        if df.empty:
            return 0

        qualified = f"[{schema_name}].[{table_name}]"
        columns = ", ".join(f"[{c}]" for c in df.columns)
        placeholders = ", ".join(["?"] * len(df.columns))
        insert_sql = f"INSERT INTO {qualified} ({columns}) VALUES ({placeholders})"

        rows_written = 0
        for batch_start in range(0, len(df), batch_size):
            batch = df.iloc[batch_start : batch_start + batch_size]
            params = []
            for _, row in batch.iterrows():
                row_vals = []
                for val in row:
                    if pd.isna(val):
                        row_vals.append(None)
                    else:
                        row_vals.append(val)
                params.append(row_vals)

            cursor.executemany(insert_sql, params)
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
        """Infer a T-SQL type from Spindle type or pandas dtype."""
        from sqllocks_spindle.output.pandas_writer import _sql_type_for_column
        return _sql_type_for_column(col_name, dtype, "tsql", col_meta if col_meta else None)
