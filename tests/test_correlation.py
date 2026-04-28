"""Tests for GaussianCopula post-pass."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.engine.correlation import GaussianCopula


def _correlated_df(n: int = 2000, target_r: float = 0.8, seed: int = 42) -> tuple[pd.DataFrame, float]:
    """Create a DataFrame with two columns having ~target_r correlation."""
    rng = np.random.default_rng(seed)
    x = rng.normal(0, 1, n)
    noise = rng.normal(0, 1, n)
    y = target_r * x + np.sqrt(1 - target_r**2) * noise
    return pd.DataFrame({"x": x, "y": y}), target_r


class TestGaussianCopula:
    def test_apply_preserves_row_count(self):
        df, _ = _correlated_df()
        copula = GaussianCopula({"x": {"y": 0.8}, "y": {"x": 0.8}})
        result = copula.apply(df)
        assert len(result) == len(df)

    def test_apply_preserves_column_set(self):
        df, _ = _correlated_df()
        copula = GaussianCopula({"x": {"y": 0.8}, "y": {"x": 0.8}})
        result = copula.apply(df)
        assert set(result.columns) == {"x", "y"}

    def test_marginals_unchanged(self):
        """Each column's sorted values must be identical after copula."""
        df, _ = _correlated_df(n=500)
        copula = GaussianCopula({"x": {"y": 0.8}, "y": {"x": 0.8}})
        result = copula.apply(df)
        np.testing.assert_array_equal(
            np.sort(df["x"].values), np.sort(result["x"].values)
        )
        np.testing.assert_array_equal(
            np.sort(df["y"].values), np.sort(result["y"].values)
        )

    def test_below_threshold_columns_skipped(self):
        """Pairs with |r| < threshold are not reordered."""
        df = pd.DataFrame({"a": np.arange(100, dtype=float), "b": np.arange(100, dtype=float)})
        copula = GaussianCopula({"a": {"b": 0.3}}, threshold=0.5)
        result = copula.apply(df)
        pd.testing.assert_frame_equal(df, result)

    def test_empty_correlation_matrix_is_noop(self):
        df = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
        copula = GaussianCopula({})
        result = copula.apply(df)
        pd.testing.assert_frame_equal(df, result)

    def test_seed_produces_reproducible_output(self):
        """Same seed should produce identical output."""
        df, _ = _correlated_df(n=200)
        copula1 = GaussianCopula({"x": {"y": 0.8}, "y": {"x": 0.8}}, seed=0)
        copula2 = GaussianCopula({"x": {"y": 0.8}, "y": {"x": 0.8}}, seed=0)
        r1 = copula1.apply(df)
        r2 = copula2.apply(df)
        pd.testing.assert_frame_equal(r1, r2)

    def test_different_seeds_produce_different_output(self):
        """Different seeds should produce different outputs."""
        df, _ = _correlated_df(n=500)
        copula1 = GaussianCopula({"x": {"y": 0.8}, "y": {"x": 0.8}}, seed=1)
        copula2 = GaussianCopula({"x": {"y": 0.8}, "y": {"x": 0.8}}, seed=2)
        r1 = copula1.apply(df)
        r2 = copula2.apply(df)
        # With different seeds, at least some values should differ
        assert not (r1["x"].values == r2["x"].values).all()
