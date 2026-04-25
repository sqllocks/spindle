"""E2E tests: all output formats — CSV, TSV, JSONL, Parquet, Excel, SQL — with round-trip read-back."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from sqllocks_spindle import Spindle, RetailDomain


@pytest.fixture(scope="module")
def retail_result():
    return Spindle().generate(domain=RetailDomain(), scale="small", seed=42)


class TestCsvOutput:
    def test_to_csv_creates_files(self, retail_result, tmp_path):
        files = retail_result.to_csv(str(tmp_path))
        assert len(files) > 0
        for f in files:
            assert Path(f).exists()
            df = pd.read_csv(f)
            assert len(df) > 0


class TestTsvOutput:
    def test_to_tsv_creates_files(self, retail_result, tmp_path):
        from sqllocks_spindle.output import PandasWriter
        writer = PandasWriter()
        files = writer.to_tsv(retail_result.tables, str(tmp_path))
        assert len(files) > 0
        for f in files:
            df = pd.read_csv(f, sep="\t")
            assert len(df) > 0


class TestJsonlOutput:
    def test_to_jsonl_creates_files(self, retail_result, tmp_path):
        files = retail_result.to_jsonl(str(tmp_path))
        assert len(files) > 0
        for f in files:
            with open(f) as fh:
                lines = fh.readlines()
            assert len(lines) > 0
            # Each line should be valid JSON
            json.loads(lines[0])


class TestParquetOutput:
    def test_to_parquet_creates_files(self, retail_result, tmp_path):
        files = retail_result.to_parquet(str(tmp_path))
        assert len(files) > 0
        for f in files:
            df = pd.read_parquet(f)
            assert len(df) > 0


class TestExcelOutput:
    def test_to_excel_creates_file(self, retail_result, tmp_path):
        output_file = tmp_path / "retail_data.xlsx"
        result_path = retail_result.to_excel(str(output_file))
        assert Path(result_path).exists()


class TestSqlOutput:
    def test_to_sql_creates_files(self, retail_result, tmp_path):
        files = retail_result.to_sql(str(tmp_path))
        assert len(files) > 0
        for f in files:
            content = Path(f).read_text()
            assert "CREATE TABLE" in content or "INSERT" in content


class TestOutputRowCountMatch:
    def test_csv_row_counts_match(self, retail_result, tmp_path):
        files = retail_result.to_csv(str(tmp_path / "csv"))
        for f in files:
            table_name = Path(f).stem
            if table_name in retail_result.tables:
                df = pd.read_csv(f)
                expected = len(retail_result.tables[table_name])
                assert len(df) == expected, (
                    f"{table_name}: CSV has {len(df)} rows, expected {expected}"
                )

    def test_parquet_row_counts_match(self, retail_result, tmp_path):
        files = retail_result.to_parquet(str(tmp_path / "pq"))
        for f in files:
            table_name = Path(f).stem
            if table_name in retail_result.tables:
                df = pd.read_parquet(f)
                expected = len(retail_result.tables[table_name])
                assert len(df) == expected, (
                    f"{table_name}: Parquet has {len(df)} rows, expected {expected}"
                )
