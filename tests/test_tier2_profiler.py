"""Tests for Tier 2 fidelity improvements."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference.tier2_profiler import (
    AnomalyRateResult,
    CardinalityConstraintChecker,
    FormatPreservationAnalyzer,
    StringSimilarityAnalyzer,
    Tier2Report,
    check_anomaly_rates,
    run_tier2,
)


# ---------------------------------------------------------------------------
# Format preservation
# ---------------------------------------------------------------------------

def test_email_format_detected():
    real = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(200)]})
    synth = pd.DataFrame({"email": [f"test{i}@domain.org" for i in range(200)]})
    analyzer = FormatPreservationAnalyzer()
    results = analyzer.analyze(real, synth)
    assert "email" in results
    assert results["email"].detected_format == "email"
    assert results["email"].real_format_rate > 0.95
    assert results["email"].synth_format_rate > 0.95
    assert results["email"].passed


def test_format_mismatch_flagged():
    real = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(200)]})
    # Synth has mostly non-emails
    synth = pd.DataFrame({"email": [f"notanemail{i}" for i in range(200)]})
    analyzer = FormatPreservationAnalyzer(threshold=0.10)
    results = analyzer.analyze(real, synth)
    if "email" in results:
        assert not results["email"].passed


def test_uuid_format_detected():
    import uuid
    real = pd.DataFrame({"id": [str(uuid.uuid4()) for _ in range(200)]})
    synth = pd.DataFrame({"id": [str(uuid.uuid4()) for _ in range(200)]})
    analyzer = FormatPreservationAnalyzer()
    results = analyzer.analyze(real, synth)
    assert "id" in results
    assert results["id"].detected_format == "uuid"


def test_no_format_for_random_strings():
    rng = np.random.default_rng(0)
    words = ["apple", "banana", "cherry", "date", "elderberry"]
    real = pd.DataFrame({"word": rng.choice(words, 200)})
    synth = pd.DataFrame({"word": rng.choice(words, 200)})
    analyzer = FormatPreservationAnalyzer()
    results = analyzer.analyze(real, synth)
    # No clear format should match >50%
    assert "word" not in results


def test_format_ignores_numeric_cols():
    real = pd.DataFrame({"n": np.arange(100), "email": [f"u{i}@x.com" for i in range(100)]})
    synth = pd.DataFrame({"n": np.arange(100), "email": [f"u{i}@x.com" for i in range(100)]})
    results = FormatPreservationAnalyzer().analyze(real, synth)
    assert "n" not in results


# ---------------------------------------------------------------------------
# String similarity
# ---------------------------------------------------------------------------

def test_identical_string_cols_max_similarity():
    vals = [f"customer_{i}" for i in range(200)]
    real = pd.DataFrame({"name": vals})
    synth = pd.DataFrame({"name": vals})
    analyzer = StringSimilarityAnalyzer(ngram_n=3)
    results = analyzer.analyze(real, synth)
    assert "name" in results
    assert results["name"].cosine_similarity > 0.99


def test_different_string_cols_lower_similarity():
    real = pd.DataFrame({"name": [f"customer_{i}" for i in range(200)]})
    synth = pd.DataFrame({"name": [f"XXXXXXXX_{i}" for i in range(200)]})
    analyzer = StringSimilarityAnalyzer(ngram_n=3)
    results = analyzer.analyze(real, synth)
    if "name" in results:
        assert results["name"].cosine_similarity < 0.9


def test_string_similarity_score_in_range():
    real = pd.DataFrame({"col": [f"abc_{i}" for i in range(100)]})
    synth = pd.DataFrame({"col": [f"abc_{i}" for i in range(100)]})
    results = StringSimilarityAnalyzer().analyze(real, synth)
    for col, r in results.items():
        assert 0 <= r.score <= 100


def test_string_similarity_ignores_numeric():
    real = pd.DataFrame({"val": np.arange(100), "name": [f"x_{i}" for i in range(100)]})
    synth = pd.DataFrame({"val": np.arange(100), "name": [f"x_{i}" for i in range(100)]})
    results = StringSimilarityAnalyzer().analyze(real, synth)
    assert "val" not in results


# ---------------------------------------------------------------------------
# Cardinality constraint checker
# ---------------------------------------------------------------------------

def test_cardinality_pass_when_close():
    rng = np.random.default_rng(0)
    real = pd.DataFrame({"cat": rng.choice(list("ABCDE"), 500)})
    synth = pd.DataFrame({"cat": rng.choice(list("ABCDE"), 500)})
    checker = CardinalityConstraintChecker(max_deviation=0.20)
    results = checker.analyze(real, synth)
    assert "cat" in results
    assert results["cat"].passed


def test_cardinality_fail_when_far_off():
    real = pd.DataFrame({"id": list(range(500))})
    synth = pd.DataFrame({"id": list(range(50))})  # 10x fewer unique values
    checker = CardinalityConstraintChecker(max_deviation=0.20)
    results = checker.analyze(real, synth)
    assert "id" in results
    assert not results["id"].passed


def test_cardinality_ratio_correct():
    real = pd.DataFrame({"x": list(range(100))})
    synth = pd.DataFrame({"x": list(range(100))})
    results = CardinalityConstraintChecker().analyze(real, synth)
    assert abs(results["x"].ratio - 1.0) < 0.01


def test_cardinality_skips_spindle_internal_cols():
    real = pd.DataFrame({"a": [1, 2, 3], "_spindle_is_anomaly": [False, False, True]})
    synth = pd.DataFrame({"a": [1, 2, 3], "_spindle_is_anomaly": [False, False, False]})
    results = CardinalityConstraintChecker().analyze(real, synth)
    assert "_spindle_is_anomaly" not in results


# ---------------------------------------------------------------------------
# Anomaly rate checker
# ---------------------------------------------------------------------------

def test_anomaly_rate_no_anomaly_col():
    df = pd.DataFrame({"x": [1, 2, 3]})
    result = check_anomaly_rates(df)
    assert result is None


def test_anomaly_rate_zero_expected():
    df = pd.DataFrame({"_spindle_is_anomaly": [False] * 100})
    result = check_anomaly_rates(df, expected_fractions=None)
    assert result is not None
    assert result.actual_fraction == 0.0
    assert result.passed


def test_anomaly_rate_matches_expectation():
    n = 1000
    flags = [True] * 50 + [False] * 950
    df = pd.DataFrame({"_spindle_is_anomaly": flags})
    result = check_anomaly_rates(df, expected_fractions={"point": 0.05}, tolerance=0.02)
    assert result is not None
    assert result.passed


def test_anomaly_rate_fails_when_too_high():
    df = pd.DataFrame({"_spindle_is_anomaly": [True] * 200 + [False] * 800})
    result = check_anomaly_rates(df, expected_fractions={"point": 0.01}, tolerance=0.05)
    assert result is not None
    assert not result.passed


# ---------------------------------------------------------------------------
# run_tier2 composite
# ---------------------------------------------------------------------------

def test_run_tier2_returns_report():
    rng = np.random.default_rng(0)
    real = pd.DataFrame({
        "age": rng.normal(35, 10, 200),
        "name": [f"user_{i}" for i in range(200)],
        "segment": rng.choice(["A", "B", "C"], 200),
    })
    synth = pd.DataFrame({
        "age": rng.normal(35, 10, 200),
        "name": [f"user_{i}" for i in range(200)],
        "segment": rng.choice(["A", "B", "C"], 200),
    })
    report = run_tier2(real, synth)
    assert isinstance(report, Tier2Report)
    assert report.passing_rate() >= 0.0


def test_tier2_summary_contains_sections():
    rng = np.random.default_rng(0)
    real = pd.DataFrame({"n": rng.normal(0, 1, 100)})
    synth = pd.DataFrame({"n": rng.normal(0, 1, 100)})
    report = run_tier2(real, synth)
    summary = report.summary()
    assert "Tier 2 Fidelity Report" in summary
    assert "Cardinality" in summary


def test_passing_rate_all_pass():
    # Identical DataFrames should all pass
    df = pd.DataFrame({"a": list("ABCD") * 25, "b": np.arange(100)})
    report = run_tier2(df, df.copy())
    assert report.passing_rate() >= 0.8
