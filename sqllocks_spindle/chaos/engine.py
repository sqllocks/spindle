"""Chaos Engine — the orchestrator that decides *when* and *what* to inject.

The engine owns the chaos RNG, evaluates per-day injection probabilities
(respecting warmup, escalation curves, per-category weights, and intensity
presets), and delegates to category-specific mutators.

Usage::

    from sqllocks_spindle.chaos.config import ChaosConfig
    from sqllocks_spindle.chaos.engine import ChaosEngine

    cfg = ChaosConfig(enabled=True, intensity="stormy", seed=99)
    engine = ChaosEngine(cfg)

    if engine.should_inject(day=12, category="value"):
        df = engine.corrupt_dataframe(df, day=12)
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.chaos.categories import (
    ChaosMutator,
    FileChaosMutator,
    ReferentialChaosMutator,
    SchemaChaosMutator,
    TemporalChaosMutator,
    ValueChaosMutator,
    VolumeChaosMutator,
)
from sqllocks_spindle.chaos.config import ChaosCategory, ChaosConfig


class ChaosEngine:
    """Orchestrates chaos injection across all categories.

    Args:
        config: A :class:`ChaosConfig` instance controlling behaviour.
        seed: If provided, overrides ``config.seed``.
    """

    def __init__(self, config: ChaosConfig | None = None, seed: int | None = None) -> None:
        self._config = config or ChaosConfig()
        effective_seed = seed if seed is not None else self._config.seed
        self._rng = np.random.RandomState(effective_seed)

        # Instantiate mutators
        self._mutators: dict[str, ChaosMutator] = {
            ChaosCategory.SCHEMA.value: SchemaChaosMutator(
                breaking_change_day=self._config.breaking_change_day,
            ),
            ChaosCategory.VALUE.value: ValueChaosMutator(),
            ChaosCategory.FILE.value: FileChaosMutator(),
            ChaosCategory.REFERENTIAL.value: ReferentialChaosMutator(),
            ChaosCategory.TEMPORAL.value: TemporalChaosMutator(),
            ChaosCategory.VOLUME.value: VolumeChaosMutator(),
        }

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def config(self) -> ChaosConfig:
        return self._config

    @property
    def rng(self) -> np.random.RandomState:
        return self._rng

    # ------------------------------------------------------------------
    # Injection decision
    # ------------------------------------------------------------------

    def should_inject(self, day: int, category: str) -> bool:
        """Decide whether chaos should fire on *day* for *category*.

        Returns ``False`` immediately if the engine is disabled, the day is
        within the warmup window, or the category is disabled.  Otherwise
        draws against the effective probability (base weight * intensity
        multiplier * escalation factor).
        """
        if not self._config.enabled:
            return False
        if day < self._config.chaos_start_day:
            return False
        if not self._config.is_category_enabled(category):
            return False

        # Check for explicit overrides — they bypass probability
        for ov in self._config.overrides_for_day(day):
            if ov.category == category:
                return True

        base_weight = self._config.category_weight(category)
        intensity = self._config.intensity_multiplier
        escalation = self._escalation_factor(day)

        probability = min(base_weight * intensity * escalation, 1.0)
        return float(self._rng.uniform()) < probability

    def _escalation_factor(self, day: int) -> float:
        """Return a 0-1 escalation multiplier for the given day.

        - ``gradual``: linear ramp from 0 to 1 over 30 chaos-days.
        - ``random``: uniform random draw each call.
        - ``front-loaded``: starts at 1.0 and decays exponentially.
        """
        chaos_day = day - self._config.chaos_start_day
        if chaos_day < 0:
            return 0.0

        mode = self._config.escalation
        if mode == "gradual":
            return min(chaos_day / 30.0, 1.0)
        elif mode == "random":
            return float(self._rng.uniform())
        elif mode == "front-loaded":
            # Exponential decay: starts at 1.0, halves every 15 days
            return max(0.95 ** chaos_day, 0.1)
        else:
            return 1.0

    # ------------------------------------------------------------------
    # Public mutation methods
    # ------------------------------------------------------------------

    def corrupt_dataframe(self, df: pd.DataFrame, day: int) -> pd.DataFrame:
        """Apply value-level chaos to a DataFrame.

        Injects nulls, out-of-range values, wrong types, encoding issues,
        future dates, and negative amounts.
        """
        mutator = self._mutators[ChaosCategory.VALUE.value]
        return mutator.mutate(df, day, self._rng, self._config.intensity_multiplier)

    def drift_schema(self, df: pd.DataFrame, day: int) -> pd.DataFrame:
        """Apply schema-level chaos: add/remove/rename/reorder/retype columns.

        Destructive mutations (drop, rename) only fire after
        ``config.breaking_change_day``.
        """
        mutator = self._mutators[ChaosCategory.SCHEMA.value]
        return mutator.mutate(df, day, self._rng, self._config.intensity_multiplier)

    def corrupt_file(self, file_bytes: bytes, day: int) -> bytes:
        """Corrupt raw file bytes: truncation, encoding damage, partial
        writes, zero-byte, garbage headers.
        """
        mutator = self._mutators[ChaosCategory.FILE.value]
        return mutator.mutate(
            file_bytes, day, self._rng, self._config.intensity_multiplier
        )

    def inject_referential_chaos(
        self,
        tables_dict: dict[str, pd.DataFrame],
        day: int,
    ) -> dict[str, pd.DataFrame]:
        """Corrupt referential integrity: orphan FKs, duplicate PKs."""
        mutator = self._mutators[ChaosCategory.REFERENTIAL.value]
        return mutator.mutate(
            tables_dict, day, self._rng, self._config.intensity_multiplier
        )

    def inject_temporal_chaos(
        self,
        df: pd.DataFrame,
        date_columns: list[str],
        day: int,
    ) -> pd.DataFrame:
        """Corrupt temporal columns: late arrivals, out-of-order, timezone
        mismatches, DST boundary issues.
        """
        mutator: TemporalChaosMutator = self._mutators[ChaosCategory.TEMPORAL.value]  # type: ignore[assignment]
        return mutator.mutate(
            df, day, self._rng, self._config.intensity_multiplier,
            date_columns=date_columns,
        )

    def inject_volume_chaos(self, df: pd.DataFrame, day: int) -> pd.DataFrame:
        """Alter data volume: 10x spike, empty batch, or single-row."""
        mutator = self._mutators[ChaosCategory.VOLUME.value]
        return mutator.mutate(df, day, self._rng, self._config.intensity_multiplier)

    # ------------------------------------------------------------------
    # Convenience: apply all applicable chaos for a day
    # ------------------------------------------------------------------

    def apply_all(
        self,
        df: pd.DataFrame,
        day: int,
        *,
        tables_dict: dict[str, pd.DataFrame] | None = None,
        date_columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Run through every category and inject chaos where
        :meth:`should_inject` returns ``True``.

        This is a convenience wrapper — callers who need fine-grained
        control should call individual methods directly.
        """
        if not self._config.enabled:
            return df

        if self.should_inject(day, ChaosCategory.SCHEMA.value):
            df = self.drift_schema(df, day)

        if self.should_inject(day, ChaosCategory.VALUE.value):
            df = self.corrupt_dataframe(df, day)

        if self.should_inject(day, ChaosCategory.TEMPORAL.value):
            cols = date_columns or df.select_dtypes(include="datetime").columns.tolist()
            if cols:
                df = self.inject_temporal_chaos(df, cols, day)

        if self.should_inject(day, ChaosCategory.VOLUME.value):
            df = self.inject_volume_chaos(df, day)

        # Referential chaos operates on the full tables dict
        if tables_dict is not None and self.should_inject(
            day, ChaosCategory.REFERENTIAL.value
        ):
            tables_dict = self.inject_referential_chaos(tables_dict, day)

        return df
