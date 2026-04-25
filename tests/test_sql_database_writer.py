"""Tests for the FabricSqlDatabaseWriter (mocked — no live database required)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter


@pytest.fixture()
def sample_tables():
    return {
        "customer": pd.DataFrame({
            "customer_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["a@x.com", "b@x.com", "c@x.com"],
        }),
        "order": pd.DataFrame({
            "order_id": [10, 20],
            "customer_id": [1, 2],
            "total": [99.99, 49.50],
        }),
    }


class TestFabricSqlDatabaseWriterInit:
    def test_creates_instance(self):
        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        assert writer is not None

    def test_stores_connection_string(self):
        # ODBC-format strings pass through unchanged; non-ODBC get normalized
        odbc_cs = "Driver={ODBC Driver 18 for SQL Server};Server=test;Database=db;"
        writer = FabricSqlDatabaseWriter(connection_string=odbc_cs)
        assert writer._connection_string == odbc_cs


class TestFabricSqlDatabaseWriterDDL:
    def _get_create_sql(self, df, table_name="test_table"):
        """Helper: call _create_table with a mock cursor and return executed SQL."""
        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        mock_cursor = MagicMock()
        writer._create_table(mock_cursor, table_name, df, "dbo", None)
        return mock_cursor.execute.call_args[0][0]

    def test_build_create_table_sql(self):
        df = pd.DataFrame({
            "id": [1, 2],
            "name": ["a", "b"],
            "value": [1.5, 2.5],
            "active": [True, False],
        })
        sql = self._get_create_sql(df)
        assert "CREATE TABLE" in sql
        assert "test_table" in sql

    def test_create_table_maps_int_columns(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        sql = self._get_create_sql(df)
        sql_upper = sql.upper()
        assert "INT" in sql_upper or "BIGINT" in sql_upper

    def test_create_table_maps_string_columns(self):
        df = pd.DataFrame({"name": ["alice", "bob"]})
        sql = self._get_create_sql(df)
        sql_upper = sql.upper()
        assert "NVARCHAR" in sql_upper or "VARCHAR" in sql_upper

    def test_create_table_maps_float_columns(self):
        df = pd.DataFrame({"price": [1.99, 2.50]})
        sql = self._get_create_sql(df)
        sql_upper = sql.upper()
        assert "FLOAT" in sql_upper or "DECIMAL" in sql_upper or "NUMERIC" in sql_upper


class TestFabricSqlDatabaseWriterInsert:
    def _get_insert_sql(self, df, table_name="my_table"):
        """Helper: call _insert_rows with a mock cursor and return the INSERT SQL."""
        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        mock_cursor = MagicMock()
        writer._insert_rows(mock_cursor, table_name, df, "dbo", 1000)
        return mock_cursor.executemany.call_args[0][0]

    def test_build_insert_sql(self):
        df = pd.DataFrame({"id": [1], "name": ["test"]})
        sql = self._get_insert_sql(df)
        assert "INSERT" in sql.upper()
        assert "my_table" in sql

    def test_insert_includes_all_columns(self):
        df = pd.DataFrame({"col_a": [1], "col_b": ["x"], "col_c": [1.5]})
        sql = self._get_insert_sql(df)
        assert "col_a" in sql
        assert "col_b" in sql
        assert "col_c" in sql


class TestFabricSqlDatabaseWriterWrite:
    @patch.object(FabricSqlDatabaseWriter, "_get_connection")
    def test_write_calls_execute(self, mock_get_conn, sample_tables):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        result = writer.write(sample_tables, mode="create_insert")

        assert mock_cursor.execute.called
        assert result is not None

    @patch.object(FabricSqlDatabaseWriter, "_get_connection")
    def test_write_processes_all_tables(self, mock_get_conn, sample_tables):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        result = writer.write(sample_tables, mode="create_insert")

        executed_sql = " ".join(str(c) for c in mock_cursor.execute.call_args_list)
        assert "customer" in executed_sql.lower()
        assert "order" in executed_sql.lower()
