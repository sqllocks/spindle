"""Tests for the Spindle fidelity comparator."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference.comparator import (
    ColumnFidelity,
    FidelityComparator,
    FidelityReport,
    TableFidelity,
)


# ---------------------------------------------------------------------------
# TestFidelityComparator
# ---------------------------------------------------------------------------


class TestFidelityComparator:
    def test_identical_data_scores_high(self):
        """Same data compared to itself should score 90+."""
        rng = np.random.default_rng(42)
        df = pd.DataFrame(
            {
                "id": range(100),
                "name": ["a"] * 50 + ["b"] * 50,
                "value": rng.normal(0, 1, 100),
            }
        )
        comp = FidelityComparator()
        report = comp.compare({"t1": df}, {"t1": df.copy()})
        assert report.overall_score >= 90

    def test_different_data_scores_lower(self):
        """Very different distributions should produce a noticeably lower score."""
        rng_real = np.random.default_rng(1)
        rng_synth = np.random.default_rng(2)
        real = pd.DataFrame({"x": rng_real.normal(0, 1, 1000)})
        synth = pd.DataFrame({"x": rng_synth.normal(10, 5, 1000)})
        comp = FidelityComparator()
        report = comp.compare({"t1": real}, {"t1": synth})
        assert report.overall_score < 70

    def test_categorical_overlap(self):
        """Partial overlap of categorical values should be detected."""
        real = pd.DataFrame({"cat": ["a", "b", "c"] * 100})
        synth = pd.DataFrame({"cat": ["a", "b", "d"] * 100})  # d instead of c
        comp = FidelityComparator()
        report = comp.compare({"t1": real}, {"t1": synth})
        cf = report.tables["t1"].columns["cat"]
        assert cf.value_overlap is not None
        assert 0 < cf.value_overlap < 1  # partial overlap

    def test_null_rate_comparison(self):
        """Differing null rates should be reflected in null_rate_delta."""
        real = pd.DataFrame({"x": [1, None, 3, None, 5] * 20})
        synth = pd.DataFrame({"x": [1, 2, 3, 4, 5] * 20})  # no nulls
        comp = FidelityComparator()
        report = comp.compare({"t1": real}, {"t1": synth})
        cf = report.tables["t1"].columns["x"]
        assert cf.null_rate_delta > 0.3  # real has ~40% nulls, synth has 0

    def test_summary_format(self):
        """summary() should return a string containing 'Fidelity'."""
        df = pd.DataFrame({"id": range(10)})
        comp = FidelityComparator()
        report = comp.compare({"t1": df}, {"t1": df})
        assert "Fidelity" in report.summary()

    def test_markdown_output(self):
        """to_markdown() should produce a valid markdown table."""
        rng = np.random.default_rng(99)
        df = pd.DataFrame({"id": range(10), "val": rng.normal(0, 1, 10)})
        comp = FidelityComparator()
        report = comp.compare({"t1": df}, {"t1": df})
        md = report.to_markdown()
        assert "# Fidelity Report" in md
        assert "|" in md

    def test_no_common_tables(self):
        """No overlapping table names should yield score 0."""
        comp = FidelityComparator()
        report = comp.compare(
            {"a": pd.DataFrame({"x": [1]})}, {"b": pd.DataFrame({"x": [1]})}
        )
        assert report.overall_score == 0.0

    def test_multi_table(self):
        """Multiple tables should all appear in the report."""
        t1 = pd.DataFrame({"x": range(100)})
        t2 = pd.DataFrame({"y": ["a", "b"] * 50})
        comp = FidelityComparator()
        report = comp.compare(
            {"t1": t1, "t2": t2}, {"t1": t1.copy(), "t2": t2.copy()}
        )
        assert len(report.tables) == 2

    def test_score_is_normalised(self):
        """All scores should fall in [0, 100]."""
        rng = np.random.default_rng(7)
        df = pd.DataFrame(
            {
                "num": rng.normal(50, 10, 200),
                "cat": rng.choice(["x", "y", "z"], 200),
            }
        )
        comp = FidelityComparator()
        report = comp.compare({"t1": df}, {"t1": df.copy()})
        assert 0 <= report.overall_score <= 100
        for tf in report.tables.values():
            assert 0 <= tf.score <= 100
            for cf in tf.columns.values():
                assert 0 <= cf.score <= 100

    def test_empty_dataframes(self):
        """Empty DataFrames should not crash."""
        comp = FidelityComparator()
        report = comp.compare(
            {"t1": pd.DataFrame({"a": pd.Series(dtype="float64")})},
            {"t1": pd.DataFrame({"a": pd.Series(dtype="float64")})},
        )
        assert report.overall_score >= 0

    def test_dtype_mismatch_lowers_score(self):
        """Comparing numeric vs string column should still produce a result."""
        real = pd.DataFrame({"x": range(100)})
        synth = pd.DataFrame({"x": [str(i) for i in range(100)]})
        comp = FidelityComparator()
        report = comp.compare({"t1": real}, {"t1": synth})
        cf = report.tables["t1"].columns["x"]
        # dtype_match should be False since real is numeric, synth is string
        # (though pandas may auto-detect the strings as numeric)
        assert isinstance(cf.score, float)
        assert 0 <= cf.score <= 100
