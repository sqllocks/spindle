"""Tests for the shared data loader."""

from __future__ import annotations

import pandas as pd
import pytest

from sqllocks_spindle.verify.loader import load_tables


class TestLoadTables:
    def test_single_csv_file(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        p = tmp_path / "orders.csv"
        df.to_csv(p, index=False)
        tables = load_tables(str(p), "csv")
        assert "orders" in tables
        assert len(tables["orders"]) == 3
        assert list(tables["orders"].columns) == ["id", "name"]

    def test_directory_of_csvs(self, tmp_path):
        for name in ("customers", "orders"):
            pd.DataFrame({"x": [1, 2]}).to_csv(tmp_path / f"{name}.csv", index=False)
        tables = load_tables(str(tmp_path), "csv")
        assert set(tables.keys()) == {"customers", "orders"}

    def test_single_parquet_file(self, tmp_path):
        pytest.importorskip("pyarrow")
        df = pd.DataFrame({"val": [10, 20]})
        p = tmp_path / "facts.parquet"
        df.to_parquet(p, index=False)
        tables = load_tables(str(p), "parquet")
        assert "facts" in tables
        assert len(tables["facts"]) == 2

    def test_single_jsonl_file(self, tmp_path):
        p = tmp_path / "events.jsonl"
        p.write_text('{"id": 1}\n{"id": 2}\n')
        tables = load_tables(str(p), "jsonl")
        assert "events" in tables
        assert len(tables["events"]) == 2

    def test_path_not_found_raises(self):
        with pytest.raises(FileNotFoundError, match="/no/such/path"):
            load_tables("/no/such/path", "csv")

    def test_empty_directory_returns_empty(self, tmp_path):
        tables = load_tables(str(tmp_path), "csv")
        assert tables == {}

    def test_invalid_format_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Unsupported format"):
            load_tables(str(tmp_path), "xlsx")
