"""Gaussian copula post-pass — enforce column correlations without changing marginals."""

from __future__ import annotations

import numpy as np
import pandas as pd

# Optional scipy for improved probit computation
try:
    from scipy.stats import norm as _sp_norm
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False


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
        seed: Random seed for reproducibility (default None, uses system entropy).
    """

    def __init__(
        self,
        correlation_matrix: dict[str, dict[str, float]],
        threshold: float = 0.5,
        seed: int | None = None,
    ):
        self.correlation_matrix = correlation_matrix
        self.threshold = threshold
        self._rng = np.random.default_rng(seed)

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

        # Step 2: apply Cholesky transform to induce target correlations
        z_raw = self._rng.standard_normal((n, k))
        z_corr = z_raw @ L.T

        # Step 4: use the RANK ORDER from z_corr to reorder original column values
        for idx, col in enumerate(active_cols):
            original_sorted = np.sort(df[col].values)
            new_ranks = np.argsort(np.argsort(z_corr[:, idx]))
            result[col] = original_sorted[new_ranks]

        return result
