"""Tests for Phase 3B FidelityReport enhancements."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference.comparator import FidelityReport, FidelityComparator


def _make_report(real: dict[str, pd.DataFrame], synth: dict[str, pd.DataFrame]) -> FidelityReport:
    return FidelityComparator().compare(real, synth)


class TestFidelityReportV2:
    def test_failing_columns_returns_list(self):
        real = {"t": pd.DataFrame({"a": np.arange(100, dtype=float)})}
        synth = {"t": pd.DataFrame({"a": np.arange(200, 300, dtype=float)})}  # very different
        report = _make_report(real, synth)
        failing = report.failing_columns(threshold=90.0)
        assert isinstance(failing, list)

    def test_to_dict_is_serializable(self):
        import json
        real = {"t": pd.DataFrame({"x": [1.0, 2.0, 3.0]})}
        synth = {"t": pd.DataFrame({"x": [1.0, 2.0, 3.0]})}
        report = _make_report(real, synth)
        d = report.to_dict()
        assert isinstance(d, dict)
        # Must be JSON-serializable
        json.dumps(d)

    def test_to_dataframe_has_expected_columns(self):
        real = {"t": pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": ["a", "b", "c"]})}
        synth = {"t": pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": ["a", "b", "c"]})}
        report = _make_report(real, synth)
        df = report.to_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert "table" in df.columns
        assert "column" in df.columns
        assert "score" in df.columns

    def test_score_classmethod_returns_report(self):
        real = pd.DataFrame({"x": np.random.default_rng(0).normal(0, 1, 100)})
        synth = pd.DataFrame({"x": np.random.default_rng(1).normal(0, 1, 100)})
        report = FidelityReport.score(real, synth)
        assert isinstance(report, FidelityReport)
        assert 0.0 <= report.overall_score <= 100.0

    def test_score_classmethod_accepts_table_name(self):
        real = pd.DataFrame({"v": [1.0, 2.0, 3.0]})
        synth = pd.DataFrame({"v": [1.0, 2.0, 3.0]})
        report = FidelityReport.score(real, synth, table_name="my_table")
        assert "my_table" in report.tables

    def test_perfect_match_scores_high(self):
        df = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": ["x", "y", "z"]})
        report = FidelityReport.score(df, df.copy())
        assert report.overall_score >= 85.0
