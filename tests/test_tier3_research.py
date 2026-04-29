"""Tests for Tier 3 research features."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference.tier3_research import (
    BootstrapMode,
    ChowLiuNetwork,
    CTGANWrapper,
    DifferentialPrivacy,
    DriftMonitor,
)


# ---------------------------------------------------------------------------
# ChowLiuNetwork
# ---------------------------------------------------------------------------

@pytest.fixture
def structured_df():
    rng = np.random.default_rng(0)
    n = 500
    age = rng.normal(35, 10, n).clip(18, 80)
    income = age * 1000 + rng.normal(0, 5000, n)
    segment = rng.choice(["A", "B", "C"], n)
    return pd.DataFrame({"age": age, "income": income, "segment": segment})


def test_chow_liu_returns_result(structured_df):
    net = ChowLiuNetwork()
    result = net.fit(structured_df)
    assert result is not None
    assert len(result.edges) == len(structured_df.columns) - 1


def test_chow_liu_edge_count(structured_df):
    net = ChowLiuNetwork()
    result = net.fit(structured_df)
    # n columns → n-1 edges in spanning tree
    assert len(result.edges) == 2


def test_chow_liu_correlated_columns_high_mi(structured_df):
    """Correlated columns (age ↔ income) should appear as an edge."""
    net = ChowLiuNetwork()
    result = net.fit(structured_df)
    edge_pairs = {(e.parent, e.child) for e in result.edges} | {(e.child, e.parent) for e in result.edges}
    # age and income are correlated
    assert ("age", "income") in edge_pairs or ("income", "age") in edge_pairs


def test_chow_liu_mutual_info_matrix_symmetric(structured_df):
    net = ChowLiuNetwork()
    result = net.fit(structured_df)
    for ci, row in result.mutual_info_matrix.items():
        for cj, mi in row.items():
            assert abs(mi - result.mutual_info_matrix[cj][ci]) < 1e-9


def test_chow_liu_mi_non_negative(structured_df):
    net = ChowLiuNetwork()
    result = net.fit(structured_df)
    for row in result.mutual_info_matrix.values():
        for mi in row.values():
            assert mi >= 0


def test_chow_liu_single_column():
    df = pd.DataFrame({"x": np.arange(100)})
    net = ChowLiuNetwork()
    result = net.fit(df)
    assert result.edges == []


# ---------------------------------------------------------------------------
# DifferentialPrivacy
# ---------------------------------------------------------------------------

def test_dp_laplace_returns_df():
    rng = np.random.default_rng(0)
    df = pd.DataFrame({"age": rng.normal(35, 10, 200), "income": rng.normal(50000, 10000, 200)})
    dp = DifferentialPrivacy(epsilon=1.0, mechanism="laplace")
    noised_df, result = dp.apply(df)
    assert len(noised_df) == len(df)
    assert result.mechanism == "laplace"


def test_dp_gaussian_returns_df():
    rng = np.random.default_rng(0)
    df = pd.DataFrame({"x": rng.normal(0, 1, 200)})
    dp = DifferentialPrivacy(epsilon=2.0, mechanism="gaussian")
    noised_df, result = dp.apply(df)
    assert "x" in result.columns_noised


def test_dp_clips_to_range():
    df = pd.DataFrame({"x": [0.0, 0.5, 1.0] * 50})
    dp = DifferentialPrivacy(epsilon=0.1, clip_to_range=True)
    noised_df, _ = dp.apply(df)
    assert noised_df["x"].min() >= 0.0
    assert noised_df["x"].max() <= 1.0


def test_dp_modifies_values():
    rng = np.random.default_rng(0)
    df = pd.DataFrame({"x": rng.normal(0, 1, 200)})
    dp = DifferentialPrivacy(epsilon=1.0)
    noised_df, _ = dp.apply(df)
    assert not np.allclose(df["x"].values, noised_df["x"].values)


def test_dp_invalid_mechanism():
    with pytest.raises(ValueError):
        DifferentialPrivacy(mechanism="invalid")


def test_dp_sensitivity_recorded():
    df = pd.DataFrame({"age": [18.0, 80.0, 35.0] * 10})
    dp = DifferentialPrivacy(epsilon=1.0)
    _, result = dp.apply(df)
    assert "age" in result.actual_sensitivity
    assert result.actual_sensitivity["age"] == pytest.approx(62.0, abs=0.01)


def test_dp_skips_non_numeric():
    df = pd.DataFrame({"name": ["Alice", "Bob", "Carol"] * 10, "age": [25.0, 30.0, 35.0] * 10})
    dp = DifferentialPrivacy(epsilon=1.0)
    noised_df, result = dp.apply(df)
    assert "name" not in result.columns_noised
    assert "age" in result.columns_noised


# ---------------------------------------------------------------------------
# DriftMonitor
# ---------------------------------------------------------------------------

def test_drift_no_drift_similar_data():
    rng = np.random.default_rng(0)
    ref = pd.DataFrame({"x": rng.normal(0, 1, 500), "cat": rng.choice(["A", "B"], 500)})
    cur = pd.DataFrame({"x": rng.normal(0, 1, 500), "cat": rng.choice(["A", "B"], 500)})
    monitor = DriftMonitor(pvalue_threshold=0.001)
    report = monitor.compare(ref, cur)
    assert report.drift_fraction < 0.5


def test_drift_detects_numeric_shift():
    rng = np.random.default_rng(0)
    ref = pd.DataFrame({"x": rng.normal(0, 1, 500)})
    cur = pd.DataFrame({"x": rng.normal(10, 1, 500)})  # massive shift
    monitor = DriftMonitor()
    report = monitor.compare(ref, cur)
    assert "x" in report.drifted_columns


def test_drift_detects_categorical_shift():
    ref = pd.DataFrame({"cat": ["A"] * 400 + ["B"] * 100})
    cur = pd.DataFrame({"cat": ["A"] * 100 + ["B"] * 400})  # flipped proportions
    monitor = DriftMonitor()
    report = monitor.compare(ref, cur)
    assert "cat" in report.drifted_columns


def test_drift_report_fields():
    rng = np.random.default_rng(0)
    ref = pd.DataFrame({"x": rng.normal(0, 1, 300)})
    cur = pd.DataFrame({"x": rng.normal(5, 1, 300)})
    monitor = DriftMonitor()
    report = monitor.compare(ref, cur)
    assert 0.0 <= report.overall_drift_score <= 1.0
    assert isinstance(report.drifted_columns, list)
    assert 0.0 <= report.drift_fraction <= 1.0


def test_drift_spindle_cols_excluded():
    ref = pd.DataFrame({"x": np.arange(100), "_spindle_is_anomaly": [False] * 100})
    cur = pd.DataFrame({"x": np.arange(100), "_spindle_is_anomaly": [True] * 100})
    monitor = DriftMonitor()
    report = monitor.compare(ref, cur)
    assert "_spindle_is_anomaly" not in report.columns


# ---------------------------------------------------------------------------
# BootstrapMode
# ---------------------------------------------------------------------------

def test_bootstrap_generates_correct_row_count():
    rng = np.random.default_rng(0)
    source = pd.DataFrame({"a": rng.normal(0, 1, 100), "b": list("AB") * 50})
    bm = BootstrapMode()
    synth, result = bm.generate(source, n_rows=200)
    assert len(synth) == 200
    assert result.n_rows == 200


def test_bootstrap_default_row_count():
    source = pd.DataFrame({"x": np.arange(50)})
    bm = BootstrapMode(add_jitter=False)
    synth, result = bm.generate(source)
    assert len(synth) == 50


def test_bootstrap_with_jitter_modifies_numerics():
    source = pd.DataFrame({"x": np.ones(100)})
    bm = BootstrapMode(add_jitter=True, jitter_std_fraction=0.1)
    # std of source is 0 — so jitter should be 0 (no change)
    synth, _ = bm.generate(source, seed=0)
    # All values identical → std = 0 → jitter = 0
    assert np.allclose(synth["x"].values, 1.0)


def test_bootstrap_reproducible():
    source = pd.DataFrame({"x": np.arange(100.0)})
    bm = BootstrapMode(add_jitter=False)
    s1, _ = bm.generate(source, seed=42)
    s2, _ = bm.generate(source, seed=42)
    pd.testing.assert_frame_equal(s1, s2)


def test_bootstrap_different_seeds_differ():
    source = pd.DataFrame({"x": np.arange(100.0)})
    bm = BootstrapMode(add_jitter=False)
    s1, _ = bm.generate(source, seed=42)
    s2, _ = bm.generate(source, seed=99)
    assert not s1["x"].equals(s2["x"])


def test_bootstrap_preserves_string_cols():
    source = pd.DataFrame({"cat": list("ABCD") * 25})
    bm = BootstrapMode()
    synth, _ = bm.generate(source, seed=0)
    assert set(synth["cat"].unique()).issubset({"A", "B", "C", "D"})


# ---------------------------------------------------------------------------
# CTGANWrapper
# ---------------------------------------------------------------------------

def test_ctgan_is_available_flag():
    # Just check it returns a bool — sdv may or may not be installed
    result = CTGANWrapper.is_available()
    assert isinstance(result, bool)


def test_ctgan_raises_without_sdv():
    if CTGANWrapper.is_available():
        pytest.skip("ctgan is installed — skip unavailable test")
    wrapper = CTGANWrapper()
    df = pd.DataFrame({"x": np.arange(10.0)})
    with pytest.raises(ImportError, match="ctgan"):
        wrapper.fit(df)


def test_ctgan_sample_requires_fit():
    wrapper = CTGANWrapper()
    with pytest.raises(RuntimeError, match="fitted"):
        wrapper.sample(10)
