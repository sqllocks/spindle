"""Faker strategy — realistic fake data from Faker providers."""

from __future__ import annotations

from typing import Any

import numpy as np
from faker import Faker

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class FakerStrategy(Strategy):
    """Generate realistic fake data using the Faker library."""

    def __init__(self):
        self._fakers: dict[str, Faker] = {}

    def _get_faker(self, locale: str) -> Faker:
        if locale not in self._fakers:
            self._fakers[locale] = Faker(locale)
        return self._fakers[locale]

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        locale = ctx.model_config.get("locale", "en_US")
        fake = self._get_faker(locale)
        # Seed Faker for reproducibility within this column
        fake.seed_instance(int(ctx.rng.integers(0, 2**31)))

        provider = config.get("provider", "word")
        provider_args = config.get("args", {})

        # Get the Faker method
        if hasattr(fake, provider):
            method = getattr(fake, provider)
        else:
            raise ValueError(f"Unknown Faker provider: '{provider}'")

        # Pool-and-sample: generate min(row_count, pool_size) unique values, then sample
        pool_size = min(ctx.row_count, 50_000)
        pool = np.array([method(**provider_args) for _ in range(pool_size)], dtype=object)
        if ctx.row_count <= pool_size:
            values = pool[:ctx.row_count]
        else:
            indices = ctx.rng.integers(0, pool_size, size=ctx.row_count)
            values = pool[indices]

        # Apply max_length truncation
        if column.max_length:
            values = np.array(
                [str(v)[:column.max_length] if v is not None else v for v in values],
                dtype=object,
            )

        return values
