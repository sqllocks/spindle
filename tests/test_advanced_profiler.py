"""Tests for AdvancedProfiler — Tier 1 fidelity features."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference.advanced_profiler import AdvancedProfiler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_real():
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "age": rng.normal(35, 10, 500).clip(18, 80),
        "revenue": rng.exponential(200, 500),
        "segment": rng.choice(["A", "B", "C"], 500, p=[0.5, 0.3, 0.2]),
        "region": rng.choice(["North", "South", "East", "West"], 500),
        "signup_date": pd.date_range("2020-01-01", periods=500, freq="D"),
    })


@pytest.fixture
def simple_synth():
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "age": rng.normal(35, 10, 500).clip(18, 80),
        "revenue": rng.exponential(200, 500),
        "segment": rng.choice(["A", "B", "C"], 500, p=[0.5, 0.3, 0.2]),
        "region": rng.choice(["North", "South", "East", "West"], 500),
        "signup_date": pd.date_range("2020-01-01", periods=500, freq="D"),
    })


# ---------------------------------------------------------------------------
# profile_pair returns AdvancedTableProfile
# ---------------------------------------------------------------------------

def test_profile_pair_returns_result(simple_real, simple_synth):
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(simple_real, simple_synth, table_name="users")
    assert result is not None
    assert result.table_name == "users"
    assert result.row_count == 500


def test_profile_pair_has_gmm_fits(simple_real, simple_synth):
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(simple_real, simple_synth)
    # GMM should be fit for numeric columns (age, revenue)
    assert len(result.gmm_fits) >= 1


def test_gmm_fit_fields(simple_real, simple_synth):
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(simple_real, simple_synth)
    for col, fit in result.gmm_fits.items():
        assert fit.n_components >= 1
        assert len(fit.means) == fit.n_components
        assert len(fit.weights) == fit.n_components
        assert abs(sum(fit.weights) - 1.0) < 0.01
        assert fit.bic < np.inf


def test_conditional_profiles_present(simple_real, simple_synth):
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(simple_real, simple_synth)
    assert len(result.conditional_profiles) >= 1


def test_conditional_profile_structure(simple_real, simple_synth):
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(simple_real, simple_synth)
    for cp in result.conditional_profiles:
        assert cp.primary_col
        assert cp.conditioned_on
        assert len(cp.stats_by_value) >= 1
        for val, stats in cp.stats_by_value.items():
            assert "mean" in stats
            assert "std" in stats
            assert "count" in stats


# ---------------------------------------------------------------------------
# Adversarial validator
# ---------------------------------------------------------------------------

def test_adversarial_result_present(simple_real, simple_synth):
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(simple_real, simple_synth)
    # sklearn installed → adversarial should run
    assert result.adversarial is not None


def test_adversarial_auc_in_range(simple_real, simple_synth):
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(simple_real, simple_synth)
    if result.adversarial:
        assert 0.0 <= result.adversarial.auc_roc <= 1.0


def test_adversarial_similar_data_low_auc():
    """Near-identical distributions → AUC should be below distinguishability threshold."""
    rng = np.random.default_rng(0)
    real = pd.DataFrame({"x": rng.normal(0, 1, 800), "y": rng.normal(5, 2, 800)})
    synth = pd.DataFrame({"x": rng.normal(0, 1, 800), "y": rng.normal(5, 2, 800)})
    profiler = AdvancedProfiler(adversarial_threshold=0.75)
    result = profiler.profile_pair(real, synth)
    if result.adversarial:
        # With same distribution, AUC should be near 0.5
        assert result.adversarial.auc_roc < 0.8


def test_adversarial_distinguishable_data_high_auc():
    """Very different distributions → AUC should be above threshold."""
    rng = np.random.default_rng(0)
    real = pd.DataFrame({"x": rng.normal(0, 1, 800)})
    synth = pd.DataFrame({"x": rng.normal(100, 1, 800)})  # completely different mean
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(real, synth)
    if result.adversarial:
        assert result.adversarial.auc_roc > 0.9


def test_adversarial_distinguishability_score(simple_real, simple_synth):
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(simple_real, simple_synth)
    if result.adversarial:
        score = result.adversarial.distinguishability_score
        assert -100 <= score <= 100


def test_adversarial_top_features_not_empty(simple_real, simple_synth):
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(simple_real, simple_synth)
    if result.adversarial:
        assert len(result.adversarial.top_features) >= 1
        for name, importance in result.adversarial.top_features:
            assert isinstance(name, str)
            assert importance >= 0


# ---------------------------------------------------------------------------
# Temporal profiler
# ---------------------------------------------------------------------------

def test_temporal_profile_for_datetime_col(simple_real, simple_synth):
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(simple_real, simple_synth)
    assert "signup_date" in result.temporal_profiles


def test_temporal_profile_fields(simple_real, simple_synth):
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(simple_real, simple_synth)
    tp = result.temporal_profiles.get("signup_date")
    if tp:
        assert tp.mean_gap_seconds is not None
        assert tp.mean_gap_seconds > 0
        assert tp.min_gap_seconds is not None
        assert tp.max_gap_seconds is not None


def test_no_temporal_profile_for_non_datetime():
    rng = np.random.default_rng(0)
    df = pd.DataFrame({"value": rng.normal(0, 1, 100)})
    profiler = AdvancedProfiler()
    result = profiler.profile_single(df)
    assert result.temporal_profiles == {}


# ---------------------------------------------------------------------------
# Periodicity detection (FFT)
# ---------------------------------------------------------------------------

def test_periodic_signal_detected():
    """A known sinusoidal signal should be detected as periodic."""
    t = np.linspace(0, 4 * np.pi, 256)
    signal = np.sin(t * 4) + np.random.default_rng(0).normal(0, 0.1, 256)
    df = pd.DataFrame({"signal": signal})
    profiler = AdvancedProfiler()
    result = profiler.profile_single(df)
    if "signal" in result.periodicity:
        assert result.periodicity["signal"].is_periodic


def test_noise_not_periodic():
    """Pure noise should not be flagged as periodic."""
    rng = np.random.default_rng(99)
    df = pd.DataFrame({"noise": rng.standard_normal(256)})
    profiler = AdvancedProfiler()
    result = profiler.profile_single(df)
    if "noise" in result.periodicity:
        assert not result.periodicity["noise"].is_periodic


def test_periodicity_result_fields(simple_real, simple_synth):
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(simple_real, simple_synth)
    for col, pr in result.periodicity.items():
        assert pr.column == col
        assert pr.dominant_power is not None
        assert len(pr.top_periods) >= 1


# ---------------------------------------------------------------------------
# profile_single (no adversarial)
# ---------------------------------------------------------------------------

def test_profile_single_no_adversarial(simple_real):
    profiler = AdvancedProfiler()
    result = profiler.profile_single(simple_real, table_name="single")
    assert result.adversarial is None
    assert result.table_name == "single"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_empty_numeric_columns_ok():
    df_a = pd.DataFrame({"cat": list("ABC") * 100})
    df_b = pd.DataFrame({"cat": list("ABC") * 100})
    profiler = AdvancedProfiler()
    result = profiler.profile_pair(df_a, df_b)
    assert result.gmm_fits == {}


def test_small_dataframe_ok():
    rng = np.random.default_rng(0)
    df = pd.DataFrame({"v": rng.normal(0, 1, 15)})
    profiler = AdvancedProfiler()
    result = profiler.profile_single(df)
    assert result is not None
