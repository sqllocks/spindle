"""Empirical strategy — quantile-interpolation-based numeric generation."""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef

try:
    from scipy.interpolate import interp1d as _interp1d
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

# Fixed percentiles that match the DataProfiler fingerprint
_PERCENTILE_KEYS = ["p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99"]
_PERCENTILE_VALUES = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]


class EmpiricalStrategy(Strategy):
    """Generate numeric values by interpolating a stored quantile fingerprint.

    Requires scipy for cubic interpolation; falls back to numpy linear
    interpolation when scipy is absent.

    Schema config:
        strategy: "empirical"
        quantiles: {p1: float, p5: float, ..., p99: float}
        interpolation: "linear" | "cubic"  (default "linear")
    """

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        quantiles = config.get("quantiles")
        if not quantiles:
            raise ValueError(
                f"empirical strategy requires 'quantiles' dict for column '{column.name}'"
            )
        interpolation = config.get("interpolation", "linear")

        # Build (cdf_value, quantile_value) mapping
        q_values = np.array([quantiles[k] for k in _PERCENTILE_KEYS], dtype=float)
        p_values = np.array(_PERCENTILE_VALUES, dtype=float)

        # Draw uniform samples, then map through the quantile function
        u = ctx.rng.uniform(0.0, 1.0, size=ctx.row_count)

        if HAS_SCIPY and interpolation == "cubic":
            interp_fn = _interp1d(p_values, q_values, kind="cubic", bounds_error=False,
                                   fill_value=(q_values[0], q_values[-1]))
            result = interp_fn(u).astype(float)
        else:
            result = np.interp(u, p_values, q_values)

        return result
