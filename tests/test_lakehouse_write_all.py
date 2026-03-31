"""Unit tests for LakehouseFilesWriter.write_all()."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from sqllocks_spindle.fabric.lakehouse_files_writer import LakehouseFilesWriter, LakehouseWriteResult


@pytest.fixture
def sample_tables():
    return {
        "users": pd.DataFrame({"id": range(100), "name": [f"user_{i}" for i in range(100)]}),
        "orders": pd.DataFrame({"id": range(200), "user_id": [i % 100 for i in range(200)]}),
        "items": pd.DataFrame({"id": range(50), "price": [9.99] * 50}),
    }


class TestLakehouseWriteAll:
    def test_write_all_parquet(self, sample_tables):
        """Writes 3 tables, verifies parquet files on disk with correct row counts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = LakehouseFilesWriter(base_path=tmpdir)
            result = writer.write_all(sample_tables)

            assert result.success
            assert result.tables_written == 3

            for table_name, df in sample_tables.items():
                parquet_file = Path(tmpdir) / table_name / "part-0001.parquet"
                assert parquet_file.exists(), f"Missing parquet for {table_name}"

                read_back = pd.read_parquet(parquet_file)
                assert len(read_back) == len(df), (
                    f"{table_name}: expected {len(df)} rows, got {len(read_back)}"
                )

    def test_write_all_returns_result(self, sample_tables):
        """Result has correct tables_written, rows_written, .success."""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = LakehouseFilesWriter(base_path=tmpdir)
            result = writer.write_all(sample_tables)

            assert isinstance(result, LakehouseWriteResult)
            assert result.success
            assert result.tables_written == 3
            assert result.rows_written == 350  # 100 + 200 + 50
            assert result.per_table == {"users": 100, "orders": 200, "items": 50}
            assert result.elapsed_seconds >= 0

    def test_write_all_empty_tables(self):
        """Handles empty dict gracefully (success=True, rows=0)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = LakehouseFilesWriter(base_path=tmpdir)
            result = writer.write_all({})

            assert result.success
            assert result.tables_written == 0
            assert result.rows_written == 0
