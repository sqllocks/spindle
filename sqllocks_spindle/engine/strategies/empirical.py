"""Empirical strategy — quantile-interpolation-based numeric generation."""

from __future__ import annotations

import warnings
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
        min: float  (optional) — clip lower bound (winsorized ``lo``, ADR-002)
        max: float  (optional) — clip upper bound (winsorized ``hi``, ADR-002)
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
        # Validate that all required percentile keys are present
        missing = [k for k in _PERCENTILE_KEYS if k not in quantiles]
        if missing:
            raise ValueError(
                f"empirical strategy for column '{column.name}' is missing quantile keys: {missing}"
            )

        # Tail anchors (STORY-019 / ADR-016): when the persisted fingerprint
        # carries the winsorization-widening endpoints p0_5 / p99_5, include them
        # as OUTER interpolation anchors so the empirical inverse-CDF can reach
        # into the widened tail. With default bounds the post-clip still clamps at
        # [p1, p99]; with widened bounds (lo=p0_5, hi=p99_5) the heavy-tail mass is
        # recovered THROUGH the empirical path — this is what reconciles STORY-019
        # empirical-first with the STORY-006 / ADR-002 winsorization-widening lever.
        # Anchors are only added when monotonic (p0_5 <= p1, p99_5 >= p99) so the
        # quantile function stays non-decreasing for interpolation.
        p_list = list(_PERCENTILE_VALUES)
        q_list = [quantiles[k] for k in _PERCENTILE_KEYS]
        lo_anchor = quantiles.get("p0_5")
        if lo_anchor is not None and float(lo_anchor) <= q_list[0]:
            p_list.insert(0, 0.005)
            q_list.insert(0, float(lo_anchor))
        hi_anchor = quantiles.get("p99_5")
        if hi_anchor is not None and float(hi_anchor) >= q_list[-1]:
            p_list.append(0.995)
            q_list.append(float(hi_anchor))

        q_values = np.array(q_list, dtype=float)
        p_values = np.array(p_list, dtype=float)

        # Draw uniform samples, then map through the quantile function
        u = ctx.rng.uniform(0.0, 1.0, size=ctx.row_count)

        if HAS_SCIPY and interpolation == "cubic":
            interp_fn = _interp1d(p_values, q_values, kind="cubic", bounds_error=False,
                                   fill_value=(q_values[0], q_values[-1]))
            result = interp_fn(u).astype(float)
        else:
            if interpolation == "cubic" and not HAS_SCIPY:
                warnings.warn(
                    f"scipy not available; falling back to linear interpolation for column '{column.name}'",
                    ImportWarning,
                    stacklevel=2,
                )
            result = np.interp(u, p_values, q_values)

        # Clip to the winsorized bounds when provided (ADR-002 / STORY-006).
        # Interpolation clamps to the anchored quantile span ([p1, p99], or
        # [p0_5, p99_5] when widening anchors are present); explicit min/max then
        # enforce the configured window so no value escapes [lo, hi].
        min_val = config.get("min")
        max_val = config.get("max")
        if min_val is not None:
            result = np.maximum(result, float(min_val))
        if max_val is not None:
            result = np.minimum(result, float(max_val))

        return result
