"""Gaussian copula post-pass — enforce column correlations without changing marginals."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _probit(u: np.ndarray) -> np.ndarray:
    """Inverse normal CDF (probit). Uses rational approximation (Abramowitz & Stegun)."""
    # Clamp u away from 0 and 1 to avoid -inf/+inf
    u = np.clip(u, 1e-10, 1 - 1e-10)

    # Rational approximation for probit (Abramowitz & Stegun 26.2.17)
    # Accurate to ~1e-4 across the full range
    c = np.array([2.515517, 0.802853, 0.010328])
    d = np.array([1.432788, 0.189269, 0.001308])

    t = np.sqrt(-2.0 * np.log(np.minimum(u, 1 - u)))
    num = c[0] + c[1]*t + c[2]*t**2
    den = 1 + d[0]*t + d[1]*t**2 + d[2]*t**3
    x = t - num/den
    return np.where(u < 0.5, -x, x)


class GaussianCopula:
    """Reorder column values to achieve target Pearson correlations.

    Algorithm (rank-based Gaussian copula):
    1. For each numeric column, map values to ranks, then to uniform [0,1].
    2. Apply inverse normal CDF (probit) → correlated Gaussian space.
    3. Cholesky decompose target correlation matrix → apply linear transform.
    4. Map back to uniform via normal CDF → back to original values via rank lookup.

    This preserves each column's marginal distribution exactly while inducing
    the target pairwise correlations.

    Args:
        correlation_matrix: dict of {col_a: {col_b: r}} pairs.
        threshold: Skip pairs where |r| < threshold (default 0.5).
    """

    def __init__(
        self,
        correlation_matrix: dict[str, dict[str, float]],
        threshold: float = 0.5,
    ):
        self.correlation_matrix = correlation_matrix
        self.threshold = threshold

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply the copula reordering to a DataFrame. Returns a new DataFrame."""
        if not self.correlation_matrix:
            return df

        # Find numeric columns that appear in the correlation matrix
        cols = [
            c for c in df.columns
            if c in self.correlation_matrix
            and pd.api.types.is_numeric_dtype(df[c])
        ]
        if len(cols) < 2:
            return df

        # Filter to pairs that exceed threshold
        active_pairs: set[tuple[str, str]] = set()
        for col_a, row in self.correlation_matrix.items():
            for col_b, r in row.items():
                if abs(r) >= self.threshold and col_a in cols and col_b in cols:
                    pair = tuple(sorted([col_a, col_b]))
                    active_pairs.add(pair)  # type: ignore[arg-type]

        if not active_pairs:
            return df

        active_cols = sorted({c for pair in active_pairs for c in pair})
        n = len(df)

        # Build target correlation matrix for active_cols
        k = len(active_cols)
        target = np.eye(k)
        col_idx = {c: i for i, c in enumerate(active_cols)}
        for col_a, col_b in active_pairs:
            r = self.correlation_matrix.get(col_a, {}).get(col_b, 0.0)
            i, j = col_idx[col_a], col_idx[col_b]
            target[i, j] = r
            target[j, i] = r

        # Ensure positive semi-definiteness (clip eigenvalues)
        eigvals, eigvecs = np.linalg.eigh(target)
        eigvals = np.clip(eigvals, 1e-8, None)
        target = eigvecs @ np.diag(eigvals) @ eigvecs.T

        try:
            L = np.linalg.cholesky(target)
        except np.linalg.LinAlgError:
            return df  # fall back if decomposition fails

        # Step 1: rank-based uniform CDF for each column
        result = df.copy()
        uniform_block = np.zeros((n, k))
        for idx, col in enumerate(active_cols):
            vals = df[col].values.astype(float)
            ranks = np.argsort(np.argsort(vals))  # tie-preserving ranks (0-based)
            # Map ranks to (0, 1) open interval using (rank + 0.5) / n
            uniform_block[:, idx] = (ranks + 0.5) / n

        # Step 2: probit transform → Gaussian space
        gaussian_block = _probit(uniform_block)

        # Step 3: apply Cholesky transform to induce target correlations
        z_raw = np.random.default_rng(42).standard_normal((n, k))
        z_corr = z_raw @ L.T

        # Step 4: use the RANK ORDER from z_corr to reorder original column values
        for idx, col in enumerate(active_cols):
            original_sorted = np.sort(df[col].values)
            new_ranks = np.argsort(np.argsort(z_corr[:, idx]))
            result[col] = original_sorted[new_ranks]

        return result
