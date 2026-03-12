"""Distribution strategy — statistical distributions for numeric/date columns."""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class DistributionStrategy(Strategy):
    """Generate values from statistical distributions."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        dist_name = config.get("distribution", "uniform")
        # Support both nested "params" key and top-level params for backwards
        # compatibility with schemas that write min/max/mean/sigma at the top level.
        params = config.get("params") or config

        generator = getattr(self, f"_dist_{dist_name}", None)
        if generator is None:
            raise ValueError(
                f"Unknown distribution '{dist_name}'. "
                f"Available: uniform, normal, log_normal, pareto, zipf, geometric, poisson"
            )

        values = generator(ctx.rng, ctx.row_count, params)
        return self._apply_bounds(values, params, column)

    def _apply_bounds(
        self,
        values: np.ndarray,
        params: dict,
        column: ColumnDef,
    ) -> np.ndarray:
        min_val = params.get("min")
        max_val = params.get("max")
        if min_val is not None:
            values = np.maximum(values, min_val)
        if max_val is not None:
            values = np.minimum(values, max_val)

        # Apply decimal precision
        if column.scale is not None:
            values = np.round(values, column.scale)

        return values

    def _dist_uniform(
        self, rng: np.random.Generator, count: int, params: dict
    ) -> np.ndarray:
        low = params.get("min", 0)
        high = params.get("max", 1)
        return rng.uniform(low, high, size=count)

    def _dist_normal(
        self, rng: np.random.Generator, count: int, params: dict
    ) -> np.ndarray:
        mean = params.get("mean", 0)
        std_dev = params.get("std_dev", 1)
        return rng.normal(mean, std_dev, size=count)

    def _dist_log_normal(
        self, rng: np.random.Generator, count: int, params: dict
    ) -> np.ndarray:
        mean = params.get("mean", 0)
        sigma = params.get("sigma", 1)
        return rng.lognormal(mean, sigma, size=count)

    def _dist_pareto(
        self, rng: np.random.Generator, count: int, params: dict
    ) -> np.ndarray:
        alpha = params.get("alpha", 1.5)
        min_val = params.get("min", 1)
        return (rng.pareto(alpha, size=count) + 1) * min_val

    def _dist_zipf(
        self, rng: np.random.Generator, count: int, params: dict
    ) -> np.ndarray:
        alpha = params.get("alpha", 1.5)
        max_val = params.get("max", 1000)
        values = rng.zipf(alpha, size=count * 2)
        values = values[values <= max_val]
        while len(values) < count:
            more = rng.zipf(alpha, size=count)
            values = np.concatenate([values, more[more <= max_val]])
        return values[:count].astype(float)

    def _dist_geometric(
        self, rng: np.random.Generator, count: int, params: dict
    ) -> np.ndarray:
        p = params.get("p", 0.5)
        return rng.geometric(p, size=count).astype(float)

    def _dist_poisson(
        self, rng: np.random.Generator, count: int, params: dict
    ) -> np.ndarray:
        lam = params.get("lambda", 5)
        return rng.poisson(lam, size=count).astype(float)

    def _dist_bernoulli(
        self, rng: np.random.Generator, count: int, params: dict
    ) -> np.ndarray:
        probability = params.get("probability", 0.5)
        return rng.binomial(1, probability, size=count).astype(float)
