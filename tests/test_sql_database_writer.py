"""Tests for the FabricSqlDatabaseWriter (mocked — no live database required)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

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
        writer = FabricSqlDatabaseWriter(connection_string="test://conn")
        assert writer.connection_string == "test://conn"


class TestFabricSqlDatabaseWriterDDL:
    def test_build_create_table_sql(self):
        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        df = pd.DataFrame({
            "id": [1, 2],
            "name": ["a", "b"],
            "value": [1.5, 2.5],
            "active": [True, False],
        })
        sql = writer._build_create_table("test_table", df)
        assert "CREATE TABLE" in sql
        assert "test_table" in sql

    def test_create_table_maps_int_columns(self):
        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        df = pd.DataFrame({"id": [1, 2, 3]})
        sql = writer._build_create_table("t", df)
        sql_upper = sql.upper()
        assert "INT" in sql_upper or "BIGINT" in sql_upper

    def test_create_table_maps_string_columns(self):
        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        df = pd.DataFrame({"name": ["alice", "bob"]})
        sql = writer._build_create_table("t", df)
        sql_upper = sql.upper()
        assert "NVARCHAR" in sql_upper or "VARCHAR" in sql_upper

    def test_create_table_maps_float_columns(self):
        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        df = pd.DataFrame({"price": [1.99, 2.50]})
        sql = writer._build_create_table("t", df)
        sql_upper = sql.upper()
        assert "FLOAT" in sql_upper or "DECIMAL" in sql_upper or "NUMERIC" in sql_upper


class TestFabricSqlDatabaseWriterInsert:
    def test_build_insert_sql(self):
        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        df = pd.DataFrame({"id": [1], "name": ["test"]})
        sql = writer._build_insert("my_table", df)
        assert "INSERT" in sql.upper()
        assert "my_table" in sql

    def test_insert_includes_all_columns(self):
        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        df = pd.DataFrame({"col_a": [1], "col_b": ["x"], "col_c": [1.5]})
        sql = writer._build_insert("t", df)
        assert "col_a" in sql
        assert "col_b" in sql
        assert "col_c" in sql


class TestFabricSqlDatabaseWriterWrite:
    @patch("sqllocks_spindle.fabric.sql_database_writer.pyodbc")
    def test_write_calls_execute(self, mock_pyodbc, sample_tables):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        result = writer.write(sample_tables, write_mode="create_insert")

        assert mock_cursor.execute.called
        assert result is not None

    @patch("sqllocks_spindle.fabric.sql_database_writer.pyodbc")
    def test_write_processes_all_tables(self, mock_pyodbc, sample_tables):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        writer = FabricSqlDatabaseWriter(connection_string="test://fake")
        result = writer.write(sample_tables, write_mode="create_insert")

        executed_sql = " ".join(str(c) for c in mock_cursor.execute.call_args_list)
        assert "customer" in executed_sql.lower()
        assert "order" in executed_sql.lower()
