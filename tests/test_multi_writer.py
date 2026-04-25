"""Unit tests for MultiWriter — mocked concurrent fan-out."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from unittest.mock import MagicMock

import pandas as pd
import pytest

from sqllocks_spindle.fabric.multi_writer import MultiWriter, MultiWriteResult


@dataclass
class MockWriteResult:
    success: bool = True
    rows_written: int = 0
    tables_written: int = 0
    errors: list[str] = field(default_factory=list)


def _make_mock_writer(rows_per_table: int = 100, delay: float = 0.0, fail: bool = False):
    writer = MagicMock()

    def _write_all(tables, **kwargs):
        if delay:
            time.sleep(delay)
        if fail:
            return MockWriteResult(success=False, rows_written=0, errors=["boom"])
        total = sum(len(df) for df in tables.values())
        return MockWriteResult(
            success=True,
            rows_written=total,
            tables_written=len(tables),
        )

    writer.write_all = _write_all
    return writer


@pytest.fixture
def sample_tables():
    return {
        "t1": pd.DataFrame({"a": range(100)}),
        "t2": pd.DataFrame({"b": range(200)}),
        "t3": pd.DataFrame({"c": range(50)}),
    }


class TestMultiWriter:
    def test_all_writers_called(self, sample_tables):
        """All 3 writers receive data."""
        w_eh = _make_mock_writer()
        w_wh = _make_mock_writer()
        w_lh = _make_mock_writer()

        mw = MultiWriter(eventhouse=w_eh, warehouse=w_wh, lakehouse=w_lh)
        result = mw.write(sample_tables)

        assert len(result.stores) == 3
        assert result.success

    def test_partial_failure_returns_result(self, sample_tables):
        """One writer fails, others succeed, result marks partial."""
        w_ok = _make_mock_writer()
        w_fail = _make_mock_writer(fail=True)

        mw = MultiWriter(eventhouse=w_ok, warehouse=w_fail, lakehouse=w_ok)
        result = mw.write(sample_tables)

        assert result.partial_failure
        assert not result.success
        failed = [s for s in result.stores if not s.success]
        assert len(failed) == 1

    def test_none_writers_skipped(self, sample_tables):
        """Only configured writers called."""
        w_lh = _make_mock_writer()
        mw = MultiWriter(lakehouse=w_lh)
        result = mw.write(sample_tables)

        assert len(result.stores) == 1
        assert result.stores[0].store == "lakehouse"
        assert result.success

    def test_result_aggregates_row_counts(self, sample_tables):
        """Total rows = sum of per-store rows."""
        w1 = _make_mock_writer()
        w2 = _make_mock_writer()
        mw = MultiWriter(eventhouse=w1, lakehouse=w2)
        result = mw.write(sample_tables)

        total_per_table = sum(len(df) for df in sample_tables.values())
        assert result.total_rows == total_per_table * 2  # 2 stores

    def test_parallel_execution(self, sample_tables):
        """Writers execute concurrently — total time < sum of individual times."""
        w1 = _make_mock_writer(delay=0.1)
        w2 = _make_mock_writer(delay=0.1)
        w3 = _make_mock_writer(delay=0.1)

        mw = MultiWriter(eventhouse=w1, warehouse=w2, lakehouse=w3)
        t0 = time.time()
        result = mw.write(sample_tables)
        elapsed = time.time() - t0

        # Sequential would be ~0.3s; parallel should be ~0.1s
        assert elapsed < 0.25, f"Parallel execution took {elapsed:.2f}s (expected <0.25s)"
        assert result.success
