"""Tests for Eventhouse/KQL writer (E16)."""

from __future__ import annotations

import pytest

from sqllocks_spindle.fabric.eventhouse_writer import EventhouseWriteResult


# ---------------------------------------------------------------------------
# EventhouseWriteResult
# ---------------------------------------------------------------------------

class TestEventhouseWriteResult:
    def test_summary(self):
        result = EventhouseWriteResult(
            tables_written=5,
            rows_written=1000,
            errors=[],
            elapsed_seconds=2.5,
        )
        summary = result.summary()
        assert "5" in summary
        assert "1000" in summary or "1,000" in summary

    def test_summary_with_errors(self):
        result = EventhouseWriteResult(
            tables_written=3,
            rows_written=500,
            errors=["Table X failed: timeout"],
            elapsed_seconds=10.0,
        )
        summary = result.summary()
        assert "error" in summary.lower() or "1" in summary


# ---------------------------------------------------------------------------
# EventhouseWriter — import and instantiation
# ---------------------------------------------------------------------------

class TestEventhouseWriterImport:
    def test_import_succeeds(self):
        from sqllocks_spindle.fabric.eventhouse_writer import EventhouseWriter
        assert EventhouseWriter is not None

    def test_lazy_import_via_fabric_package(self):
        from sqllocks_spindle.fabric import EventhouseWriter
        assert EventhouseWriter is not None

    def test_lazy_import_result_via_fabric_package(self):
        from sqllocks_spindle.fabric import EventhouseWriteResult
        assert EventhouseWriteResult is not None


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------

class TestKqlTypeMapping:
    def test_pandas_dtype_mappings(self):
        from sqllocks_spindle.fabric.eventhouse_writer import EventhouseWriter

        writer = EventhouseWriter.__new__(EventhouseWriter)
        # Test the type mapping method exists and works
        assert hasattr(writer, "_pandas_dtype_to_kql")

        import pandas as pd
        import numpy as np

        # Build a test DataFrame with various types
        df = pd.DataFrame({
            "int_col": pd.array([1, 2, 3], dtype="int64"),
            "float_col": pd.array([1.0, 2.0, 3.0], dtype="float64"),
            "str_col": ["a", "b", "c"],
            "bool_col": [True, False, True],
            "dt_col": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        })

        for col in df.columns:
            kql_type = writer._pandas_dtype_to_kql(df[col].dtype)
            assert isinstance(kql_type, str)
            assert kql_type in ("string", "long", "real", "datetime", "bool", "dynamic")
