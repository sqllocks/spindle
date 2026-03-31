"""Database profiler -- read schema metadata from SQL Server catalog views.

Connects to SQL Server, Azure SQL, Fabric Warehouse, or Fabric SQL Database
and reads exact PKs, FKs, column metadata from sys catalog views.
Then samples data for distribution profiling.

Requires the fabric-sql extra::

    pip install sqllocks-spindle[fabric-sql]
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.inference.profiler import (
    ColumnProfile,
    DatasetProfile,
    TableProfile,
)

logger = logging.getLogger(__name__)


_TABLES_QUERY = """
SELECT s.name AS schema_name, t.name AS table_name, t.object_id
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = ?
ORDER BY t.name
"""

_COLUMNS_QUERY = """
SELECT c.name AS column_name, tp.name AS type_name,
    c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, c.column_id
FROM sys.columns c
JOIN sys.types tp ON c.user_type_id = tp.user_type_id
WHERE c.object_id = ?
ORDER BY c.column_id
"""

_PK_QUERY = """
SELECT col.name AS column_name
FROM sys.key_constraints kc
JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
WHERE kc.type = 'PK' AND kc.parent_object_id = ?
ORDER BY ic.key_ordinal
"""

_FK_QUERY = """
SELECT fk.name AS fk_name,
    OBJECT_NAME(fk.parent_object_id) AS child_table,
    cp.name AS child_column,
    OBJECT_NAME(fk.referenced_object_id) AS parent_table,
    cr.name AS parent_column,
    fkc.constraint_column_id AS ordinal
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
JOIN sys.tables t ON fk.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = ?
ORDER BY fk.name, fkc.constraint_column_id
"""

_ROW_COUNT_QUERY = """
SELECT s.name AS schema_name, t.name AS table_name, SUM(p.rows) AS row_count
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
WHERE s.name = ?
GROUP BY s.name, t.name
ORDER BY t.name
"""


def _sql_type_to_spindle(type_name: str) -> str:
    t = type_name.lower()
    if t in ("int", "bigint", "smallint", "tinyint"): return "integer"
    if t in ("float", "real", "decimal", "numeric", "money", "smallmoney"): return "float"
    if t in ("date",): return "date"
    if t in ("datetime", "datetime2", "smalldatetime", "datetimeoffset"): return "datetime"
    if t in ("bit",): return "boolean"
    return "string"


class DatabaseProfiler:
    """Profile a SQL database by reading catalog metadata + sampling data."""

    def __init__(self, connection_string: str, auth_method: str = "cli",
                 client_id: str | None = None, client_secret: str | None = None,
                 tenant_id: str | None = None):
        self._connection_string = connection_string
        self._auth_method = auth_method
        self._client_id = client_id
        self._client_secret = client_secret
        self._tenant_id = tenant_id

    def _get_connection(self):
        import pyodbc
        if self._auth_method == "sql":
            return pyodbc.connect(self._connection_string, timeout=30)
        import struct
        token_bytes = self._get_token()
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        return pyodbc.connect(self._connection_string, attrs_before={1256: token_struct}, timeout=30)

    def _get_token(self) -> bytes:
        scope = "https://database.windows.net/.default"
        if self._auth_method == "cli":
            from azure.identity import AzureCliCredential
            credential = AzureCliCredential()
        elif self._auth_method == "msi":
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential(exclude_managed_identity_credential=False)
        elif self._auth_method == "spn":
            from azure.identity import ClientSecretCredential
            credential = ClientSecretCredential(
                tenant_id=self._tenant_id, client_id=self._client_id,
                client_secret=self._client_secret)
        elif self._auth_method == "fabric":
            try:
                try:
                    from notebookutils import mssparkutils as _msu
                except ImportError:
                    import mssparkutils as _msu
                token_str = _msu.credentials.getToken("https://database.windows.net/")
                return token_str.encode("utf-16-le")
            except ImportError:
                raise RuntimeError("auth_method='fabric' requires mssparkutils (Fabric Notebooks only)")
        else:
            raise ValueError(f"Unsupported auth_method: {self._auth_method}")
        token = credential.get_token(scope)
        return token.token.encode("utf-16-le")

    def profile(self, schema: str = "dbo", sample_rows: int = 1000,
                tables: list[str] | None = None) -> DatasetProfile:
        """Profile a database schema with exact PKs, FKs, types, and sampled distributions."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            return self._profile_impl(conn, cursor, schema, sample_rows, tables)
        finally:
            cursor.close()
            conn.close()

    def _profile_impl(self, conn, cursor, schema, sample_rows, filter_tables):
        cursor.execute(_TABLES_QUERY, (schema,))
        table_rows = cursor.fetchall()
        if filter_tables:
            table_rows = [r for r in table_rows if r.table_name in filter_tables]
        logger.info("Found %d tables in schema %s", len(table_rows), schema)

        cursor.execute(_ROW_COUNT_QUERY, (schema,))
        row_counts = {r.table_name: (r.row_count or 0) for r in cursor.fetchall()}

        cursor.execute(_FK_QUERY, (schema,))
        fk_rows = cursor.fetchall()
        fk_map: dict[str, dict] = {}
        for r in fk_rows:
            if r.fk_name not in fk_map:
                fk_map[r.fk_name] = {"child_table": r.child_table, "parent_table": r.parent_table,
                                      "child_columns": [], "parent_columns": []}
            fk_map[r.fk_name]["child_columns"].append(r.child_column)
            fk_map[r.fk_name]["parent_columns"].append(r.parent_column)
        logger.info("Found %d foreign key constraints", len(fk_map))

        table_profiles: dict[str, TableProfile] = {}
        for trow in table_rows:
            tname = trow.table_name
            obj_id = trow.object_id
            cursor.execute(_COLUMNS_QUERY, (obj_id,))
            col_rows = cursor.fetchall()
            cursor.execute(_PK_QUERY, (obj_id,))
            pk_cols = [r.column_name for r in cursor.fetchall()]

            # Heuristic PK inference when catalog returns nothing (Fabric Warehouse)
            if not pk_cols:
                col_names = [cr.column_name for cr in col_rows]
                # Check for identity column
                identity_cols = [cr.column_name for cr in col_rows if cr.is_identity]
                if identity_cols:
                    pk_cols = [identity_cols[0]]
                else:
                    # Convention: table_name + _id/_key, or just 'id'
                    tname_lower = tname.lower().replace('dim', '').replace('fact', '')
                    for cname in col_names:
                        cl = cname.lower()
                        if cl in (tname_lower + '_id', tname_lower + 'id',
                                  tname_lower + '_key', tname_lower + 'key', 'id'):
                            pk_cols = [cname]
                            break
                    # Last resort: first *_id or *Key column
                    if not pk_cols:
                        for cname in col_names:
                            cl = cname.lower()
                            if cl.endswith('_id') or cl.endswith('id') or cl.endswith('key'):
                                pk_cols = [cname]
                                break
                    # Absolute fallback: first column
                    if not pk_cols and col_names:
                        pk_cols = [col_names[0]]

            table_fks: dict[str, str] = {}
            for fk_info in fk_map.values():
                if fk_info["child_table"] == tname:
                    for child_col in fk_info["child_columns"]:
                        table_fks[child_col] = fk_info["parent_table"]

            sample_df = None
            if sample_rows > 0:
                try:
                    sample_df = pd.read_sql(f"SELECT TOP {sample_rows} * FROM [{schema}].[{tname}]", conn)
                except Exception as e:
                    logger.warning("Could not sample %s: %s", tname, e)

            columns: dict[str, ColumnProfile] = {}
            for cr in col_rows:
                col_name = cr.column_name
                spindle_type = _sql_type_to_spindle(cr.type_name)
                is_pk = col_name in pk_cols
                is_fk = col_name in table_fks
                cardinality = None
                null_count = 0
                enum_values = min_val = max_val = mean_val = std_val = pattern = None

                if sample_df is not None and col_name in sample_df.columns:
                    series = sample_df[col_name]
                    cardinality = int(series.nunique())
                    null_count = int(series.isna().sum())
                    if spindle_type in ("integer", "float"):
                        numeric = pd.to_numeric(series, errors="coerce").dropna()
                        if len(numeric) > 0:
                            min_val, max_val = float(numeric.min()), float(numeric.max())
                            mean_val, std_val = float(numeric.mean()), float(numeric.std())
                    if spindle_type == "string" and cardinality is not None and cardinality <= 50:
                        enum_values = sorted(str(v) for v in series.dropna().unique().tolist())

                row_count_for_ratios = row_counts.get(tname, 0) or (len(sample_df) if sample_df is not None else 1)
                null_rate = null_count / max(row_count_for_ratios, 1)
                card = cardinality if cardinality is not None else 0
                cardinality_ratio = card / max(row_count_for_ratios, 1)
                is_unique = cardinality_ratio > 0.99 if card > 0 else False
                is_enum = (card > 0 and card <= 50) or (cardinality_ratio < 0.05 and card > 0)
                enum_dict = None
                if is_enum and enum_values:
                    prob = 1.0 / len(enum_values) if enum_values else 0
                    enum_dict = {v: prob for v in enum_values}

                columns[col_name] = ColumnProfile(
                    name=col_name, dtype=spindle_type, null_count=null_count,
                    null_rate=null_rate, cardinality=card,
                    cardinality_ratio=cardinality_ratio, is_unique=is_unique,
                    is_enum=is_enum, enum_values=enum_dict,
                    min_value=min_val, max_value=max_val, mean=mean_val, std=std_val,
                    distribution=None, distribution_params=None, pattern=pattern,
                    is_primary_key=is_pk, is_foreign_key=is_fk,
                    fk_ref_table=table_fks.get(col_name))

            table_profiles[tname] = TableProfile(
                name=tname, row_count=row_counts.get(tname, 0),
                columns=columns, primary_key=pk_cols, detected_fks=table_fks)
            logger.info("Profiled %s: %d cols, PK=%s, %d FKs, %d rows",
                        tname, len(columns), pk_cols, len(table_fks), row_counts.get(tname, 0))

        # If catalog returned no FKs, fall back to heuristic FK detection
        if not fk_map:
            from sqllocks_spindle.inference.profiler import DataProfiler
            sampled = {}
            for tname, tp in table_profiles.items():
                # Build minimal DataFrames for heuristic detection
                if sample_rows > 0:
                    try:
                        sampled[tname] = pd.read_sql(
                            f"SELECT TOP {sample_rows} * FROM [{schema}].[{tname}]", conn)
                    except Exception:
                        pass
            if sampled:
                dp = DataProfiler()
                heuristic_profile = dp.profile_dataset(sampled)
                # If heuristic also found nothing (empty tables), fall through to name-based
                if not any(tp.detected_fks for tp in heuristic_profile.tables.values()):
                    sampled = {}  # trigger name-based below
            if not sampled:
                # Empty tables: infer FKs from naming convention only
                heuristic_profile = None
                table_names_lower = {t.lower(): t for t in table_profiles}
                for tname, tp in table_profiles.items():
                    for col_name in tp.columns:
                        cl = col_name.lower()
                        candidate = None
                        if cl.endswith('_id'):
                            candidate = cl.rsplit('_id', 1)[0]
                        elif cl.endswith('id') and len(cl) > 2:
                            candidate = cl[:-2]
                        elif cl.endswith('key') and len(cl) > 3:
                            candidate = cl[:-3]
                            if candidate.endswith('_'):
                                candidate = candidate[:-1]
                        if candidate and candidate != tname.lower().replace('dim','').replace('fact',''):
                            # Check for matching table
                            for lookup in (candidate, 'dim' + candidate, candidate.capitalize(),
                                          'Dim' + candidate.capitalize()):
                                if lookup.lower() in table_names_lower:
                                    parent = table_names_lower[lookup.lower()]
                                    tp.detected_fks[col_name] = parent
                                    fk_map[f'fk_{tname}_{col_name}'] = {
                                        'child_table': tname, 'parent_table': parent,
                                        'child_columns': [col_name],
                                        'parent_columns': table_profiles[parent].primary_key[:1] if table_profiles[parent].primary_key else [col_name]}
                                    break
                logger.info('Name-based FK inference found %d relationships', len(fk_map))
                # Merge heuristic FKs into table_profiles
                for tname, tp in (heuristic_profile.tables.items() if heuristic_profile else []):
                    if tname in table_profiles and tp.detected_fks:
                        table_profiles[tname].detected_fks.update(tp.detected_fks)
                        for col_name, parent in tp.detected_fks.items():
                            if col_name in table_profiles[tname].columns:
                                old_cp = table_profiles[tname].columns[col_name]
                                table_profiles[tname].columns[col_name] = ColumnProfile(
                                    name=old_cp.name, dtype=old_cp.dtype,
                                    null_count=old_cp.null_count, null_rate=old_cp.null_rate,
                                    cardinality=old_cp.cardinality,
                                    cardinality_ratio=old_cp.cardinality_ratio,
                                    is_unique=old_cp.is_unique, is_enum=old_cp.is_enum,
                                    enum_values=old_cp.enum_values,
                                    min_value=old_cp.min_value, max_value=old_cp.max_value,
                                    mean=old_cp.mean, std=old_cp.std,
                                    distribution=old_cp.distribution,
                                    distribution_params=old_cp.distribution_params,
                                    pattern=old_cp.pattern,
                                    is_primary_key=old_cp.is_primary_key,
                                    is_foreign_key=True, fk_ref_table=parent)
                # Use heuristic relationships (only if heuristic ran)
                if heuristic_profile is not None:
                    fk_map = {}
                    for rel in heuristic_profile.relationships:
                        fk_map[rel["name"]] = rel
                    logger.info("Heuristic FK detection found %d relationships", len(fk_map))

        relationships = []
        profiled_names = {t.table_name for t in table_rows}
        for fk_name, fk_info in fk_map.items():
            # Handle both catalog format (dict with child_table) and heuristic format (dict with child)
            child = fk_info.get("child_table") or fk_info.get("child", "")
            parent = fk_info.get("parent_table") or fk_info.get("parent", "")
            if child not in profiled_names:
                continue
            relationships.append({
                "name": fk_name, "parent": parent, "child": child,
                "parent_columns": fk_info.get("parent_columns", []),
                "child_columns": fk_info.get("child_columns", []),
                "type": "one_to_many"})

        logger.info("Database profile complete: %d tables, %d relationships",
                     len(table_profiles), len(relationships))
        return DatasetProfile(tables=table_profiles, relationships=relationships)
