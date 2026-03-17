"""Pandas DataFrame output writer."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Spindle type → SQL DDL type mappings per dialect
# ---------------------------------------------------------------------------

_TSQL_TYPE_MAP: dict[str, str] = {
    "integer": "INT",
    "string": "NVARCHAR({length})",
    "decimal": "DECIMAL({precision},{scale})",
    "timestamp": "DATETIME2",
    "boolean": "BIT",
    "uuid": "UNIQUEIDENTIFIER",
    "float": "FLOAT",
    "date": "DATE",
    "time": "TIME",
}

_POSTGRES_TYPE_MAP: dict[str, str] = {
    "integer": "INTEGER",
    "string": "VARCHAR({length})",
    "decimal": "NUMERIC({precision},{scale})",
    "timestamp": "TIMESTAMPTZ",
    "boolean": "BOOLEAN",
    "uuid": "UUID",
    "float": "DOUBLE PRECISION",
    "date": "DATE",
    "time": "TIME",
}

_MYSQL_TYPE_MAP: dict[str, str] = {
    "integer": "INT",
    "string": "VARCHAR({length})",
    "decimal": "DECIMAL({precision},{scale})",
    "timestamp": "DATETIME",
    "boolean": "TINYINT(1)",
    "uuid": "CHAR(36)",
    "float": "DOUBLE",
    "date": "DATE",
    "time": "TIME",
}

_DIALECT_MAPS = {
    "tsql": _TSQL_TYPE_MAP,
    "postgres": _POSTGRES_TYPE_MAP,
    "mysql": _MYSQL_TYPE_MAP,
}


def _quote_ident(name: str, dialect: str) -> str:
    """Quote a SQL identifier based on dialect."""
    if dialect == "postgres":
        return f'"{name}"'
    elif dialect == "mysql":
        return f"`{name}`"
    return f"[{name}]"


def _sql_type_for_column(
    col_name: str,
    dtype,
    dialect: str,
    schema_meta: dict | None = None,
) -> str:
    """Infer SQL type from a pandas dtype and optional schema metadata."""
    type_map = _DIALECT_MAPS.get(dialect, _TSQL_TYPE_MAP)

    # If schema metadata is available, use the Spindle type
    if schema_meta:
        spindle_type = schema_meta.get("type", "string")
        template = type_map.get(spindle_type, type_map.get("string", "NVARCHAR(255)"))
        length = schema_meta.get("max_length") or 255
        precision = schema_meta.get("precision") or 18
        scale = schema_meta.get("scale") or 2
        return template.format(length=length, precision=precision, scale=scale)

    # Fallback: infer from pandas dtype
    dtype_str = str(dtype)
    if "int" in dtype_str:
        return type_map.get("integer", "INT")
    elif "float" in dtype_str:
        return type_map.get("float", "FLOAT")
    elif "bool" in dtype_str:
        return type_map.get("boolean", "BIT")
    elif "datetime" in dtype_str:
        return type_map.get("timestamp", "DATETIME2")
    else:
        template = type_map.get("string", "NVARCHAR(255)")
        return template.format(length=255, precision=18, scale=2)


def _format_sql_value(val, dialect: str) -> str:
    """Format a Python value as a SQL literal."""
    if pd.isna(val):
        return "NULL"
    elif isinstance(val, bool):
        if dialect == "postgres":
            return "TRUE" if val else "FALSE"
        return "1" if val else "0"
    elif isinstance(val, (int, float)):
        return str(val)
    elif isinstance(val, str):
        escaped = val.replace("'", "''")
        return f"N'{escaped}'" if dialect == "tsql" else f"'{escaped}'"
    else:
        escaped = str(val).replace("'", "''")
        return f"N'{escaped}'" if dialect == "tsql" else f"'{escaped}'"


def _generate_create_table_ddl(
    table_name: str,
    df: pd.DataFrame,
    schema_name: str | None,
    sql_dialect: str,
    include_drop: bool,
    include_go: bool,
    schema_meta: dict | None = None,
    primary_key: list[str] | None = None,
    is_fabric_warehouse: bool = False,
) -> str:
    """Generate CREATE TABLE DDL for a single table."""
    lines = []
    qi = lambda name: _quote_ident(name, sql_dialect)
    go = "\nGO\n" if include_go and sql_dialect == "tsql" else "\n"

    prefix = f"{qi(schema_name)}." if schema_name else ""
    qualified = f"{prefix}{qi(table_name)}"

    # DROP IF EXISTS
    if include_drop:
        if sql_dialect == "tsql":
            lines.append(f"IF OBJECT_ID('{qualified}', 'U') IS NOT NULL")
            lines.append(f"    DROP TABLE {qualified};")
            lines.append("GO")
        elif sql_dialect == "postgres":
            lines.append(f"DROP TABLE IF EXISTS {qualified} CASCADE;")
        elif sql_dialect == "mysql":
            lines.append(f"DROP TABLE IF EXISTS {qualified};")
        lines.append("")

    # Fabric Warehouse note
    if is_fabric_warehouse and primary_key:
        pk_cols = ", ".join(qi(c) for c in primary_key)
        lines.append(f"-- NOTE: Fabric Warehouse does not enforce PRIMARY KEY constraints.")
        lines.append(f"-- Column(s) {pk_cols} are the logical primary key.")

    # CREATE TABLE
    lines.append(f"CREATE TABLE {qualified} (")

    col_defs = []
    for col_name in df.columns:
        col_meta = (schema_meta or {}).get(col_name, {})
        sql_type = _sql_type_for_column(col_name, df[col_name].dtype, sql_dialect, col_meta if col_meta else None)
        nullable = col_meta.get("nullable", True)
        null_str = "NULL" if nullable else "NOT NULL"
        col_defs.append(f"    {qi(col_name):<30} {sql_type:<20} {null_str}")

    # Add PK constraint (unless Fabric Warehouse)
    if primary_key and not is_fabric_warehouse:
        pk_cols = ", ".join(qi(c) for c in primary_key)
        if sql_dialect == "mysql":
            col_defs.append(f"    PRIMARY KEY ({pk_cols})")
        else:
            col_defs.append(f"    CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_cols})")

    lines.append(",\n".join(col_defs))
    lines.append(");")
    lines.append(go.rstrip())

    return "\n".join(lines)


class PandasWriter:
    """Write generated tables to various local formats."""

    def to_csv(
        self,
        tables: dict[str, pd.DataFrame],
        output_dir: str | Path,
        separator: str = ",",
    ) -> list[Path]:
        """Write all tables as CSV files."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        written = []
        for table_name, df in tables.items():
            path = output_dir / f"{table_name}.csv"
            df.to_csv(path, index=False, sep=separator)
            written.append(path)

        return written

    def to_tsv(
        self,
        tables: dict[str, pd.DataFrame],
        output_dir: str | Path,
    ) -> list[Path]:
        """Write all tables as tab-delimited text files."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        written = []
        for table_name, df in tables.items():
            path = output_dir / f"{table_name}.tsv"
            df.to_csv(path, index=False, sep="\t")
            written.append(path)

        return written

    def to_jsonl(
        self,
        tables: dict[str, pd.DataFrame],
        output_dir: str | Path,
    ) -> list[Path]:
        """Write all tables as JSON Lines files (one JSON object per line)."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        written = []
        for table_name, df in tables.items():
            path = output_dir / f"{table_name}.jsonl"
            df.to_json(path, orient="records", lines=True, date_format="iso")
            written.append(path)

        return written

    def to_parquet(
        self,
        tables: dict[str, pd.DataFrame],
        output_dir: str | Path,
    ) -> list[Path]:
        """Write all tables as Parquet files (requires pyarrow)."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        written = []
        for table_name, df in tables.items():
            path = output_dir / f"{table_name}.parquet"
            df.to_parquet(path, index=False)
            written.append(path)

        return written

    def to_excel(
        self,
        tables: dict[str, pd.DataFrame],
        output_dir: str | Path,
        single_workbook: bool = True,
    ) -> list[Path]:
        """Write tables as Excel files (requires openpyxl).

        If single_workbook=True, writes one .xlsx with a sheet per table.
        If False, writes one .xlsx per table.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        written = []
        if single_workbook:
            path = output_dir / "spindle_output.xlsx"
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                for table_name, df in tables.items():
                    # Excel sheet names max 31 chars
                    sheet = table_name[:31]
                    df.to_excel(writer, sheet_name=sheet, index=False)
            written.append(path)
        else:
            for table_name, df in tables.items():
                path = output_dir / f"{table_name}.xlsx"
                df.to_excel(path, index=False, engine="openpyxl")
                written.append(path)

        return written

    def to_sql_inserts(
        self,
        tables: dict[str, pd.DataFrame],
        output_dir: str | Path,
        schema_name: str | None = None,
        batch_size: int = 1000,
        include_ddl: bool = True,
        include_drop: bool = True,
        include_go: bool = True,
        sql_dialect: str = "tsql",
        schema_meta: dict[str, dict[str, dict]] | None = None,
        primary_keys: dict[str, list[str]] | None = None,
        domain_name: str | None = None,
        scale: str | None = None,
        seed: int | None = None,
    ) -> list[Path]:
        """Write tables as SQL scripts with optional CREATE TABLE DDL.

        Args:
            tables: Dict of table_name -> DataFrame.
            output_dir: Directory for .sql files.
            schema_name: SQL schema prefix (e.g. "dbo").
            batch_size: Rows per INSERT batch.
            include_ddl: Prepend CREATE TABLE before INSERTs.
            include_drop: Include DROP IF EXISTS before CREATE.
            include_go: Include GO batch separators (T-SQL only).
            sql_dialect: "tsql", "postgres", or "mysql".
            schema_meta: Per-table column metadata from SpindleSchema.
                Format: {table_name: {col_name: {type, nullable, max_length, ...}}}
            primary_keys: Per-table primary key columns. {table_name: [col1, ...]}
            domain_name: Domain name for header comment.
            scale: Scale preset for header comment.
            seed: Seed for header comment.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        qi = lambda name: _quote_ident(name, sql_dialect)
        go_sep = "\nGO\n\n" if include_go and sql_dialect == "tsql" else "\n\n"
        is_fabric = sql_dialect in ("tsql-fabric-warehouse", "tsql-fabric")
        effective_dialect = "tsql" if is_fabric else sql_dialect

        written = []
        for table_name, df in tables.items():
            path = output_dir / f"{table_name}.sql"
            prefix = f"{qi(schema_name)}." if schema_name else ""
            qualified = f"{prefix}{qi(table_name)}"
            columns = ", ".join(qi(c) for c in df.columns)

            table_meta = (schema_meta or {}).get(table_name, {})
            table_pk = (primary_keys or {}).get(table_name, [])

            with open(path, "w", encoding="utf-8") as f:
                # Header
                from sqllocks_spindle import __version__
                f.write(f"-- Generated by Spindle v{__version__} (sqllocks-spindle)\n")
                if domain_name:
                    parts = [f"Domain: {domain_name}"]
                    if scale:
                        parts.append(f"Scale: {scale}")
                    if seed is not None:
                        parts.append(f"Seed: {seed}")
                    f.write(f"-- {' | '.join(parts)}\n")
                f.write(f"-- Generated: {datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')}\n\n")

                # Section header
                f.write(f"-- {'=' * 60}\n")
                f.write(f"-- Table: {table_name} ({len(df):,} rows)\n")
                f.write(f"-- {'=' * 60}\n\n")

                # DDL
                if include_ddl:
                    ddl = _generate_create_table_ddl(
                        table_name=table_name,
                        df=df,
                        schema_name=schema_name,
                        sql_dialect=effective_dialect,
                        include_drop=include_drop,
                        include_go=include_go,
                        schema_meta=table_meta,
                        primary_key=table_pk,
                        is_fabric_warehouse=is_fabric,
                    )
                    f.write(ddl)
                    f.write("\n\n")

                # INSERT statements
                for batch_start in range(0, len(df), batch_size):
                    batch = df.iloc[batch_start : batch_start + batch_size]
                    f.write(f"INSERT INTO {qualified} ({columns})\nVALUES\n")

                    rows = []
                    for _, row in batch.iterrows():
                        vals = [_format_sql_value(val, effective_dialect) for val in row]
                        rows.append(f"  ({', '.join(vals)})")

                    f.write(",\n".join(rows))
                    f.write(";")
                    f.write(go_sep)

            written.append(path)

        return written
