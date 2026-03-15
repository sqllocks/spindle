"""Temporal strategy — time-aware date/timestamp generation."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class TemporalStrategy(Strategy):
    """Generate dates and timestamps with temporal patterns."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        pattern = config.get("pattern", "uniform")

        # Resolve date range (check both "date_range" and "range" keys)
        date_range = config.get("date_range", config.get("range", {}))
        if config.get("range_ref") == "model.date_range":
            date_range = ctx.model_config.get("date_range", {})

        start_str = date_range.get("start", "2022-01-01")
        end_str = date_range.get("end", "2025-12-31")
        start = pd.Timestamp(start_str)
        end = pd.Timestamp(end_str)

        if pattern == "uniform":
            return self._uniform(start, end, ctx)
        elif pattern == "seasonal":
            profiles = config.get("profiles", {})
            return self._seasonal(start, end, profiles, ctx)
        else:
            return self._uniform(start, end, ctx)

    def _uniform(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        ctx: GenerationContext,
    ) -> np.ndarray:
        start_ns = start.value
        end_ns = end.value
        random_ns = ctx.rng.integers(start_ns, end_ns, size=ctx.row_count)
        return pd.to_datetime(random_ns).values

    def _seasonal(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        profiles: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        # Generate base uniform dates, then apply seasonal weighting via rejection sampling
        month_weights = profiles.get("month", {})
        dow_weights = profiles.get("day_of_week", {})
        hour_profile = profiles.get("hour_of_day", {})

        # Generate more than needed, then filter with acceptance probability
        oversample = 3
        candidates = self._uniform(start, end, type("Ctx", (), {
            "rng": ctx.rng,
            "row_count": ctx.row_count * oversample,
        })())
        candidate_ts = pd.DatetimeIndex(candidates)

        # Calculate acceptance probabilities
        acceptance = np.ones(len(candidates))

        if month_weights:
            month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            base_weight = 1.0 / 12.0
            month_probs = np.array([
                month_weights.get(m, base_weight) for m in month_names
            ])
            month_probs = month_probs / month_probs.sum()
            # Relative to uniform: how much more/less likely
            month_factor = month_probs * 12  # normalize so uniform = 1.0
            acceptance *= month_factor[candidate_ts.month - 1]

        if dow_weights:
            dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            base_weight = 1.0 / 7.0
            dow_probs = np.array([
                dow_weights.get(d, base_weight) for d in dow_names
            ])
            dow_probs = dow_probs / dow_probs.sum()
            dow_factor = dow_probs * 7
            acceptance *= dow_factor[candidate_ts.dayofweek]

        # Normalize acceptance to [0, 1]
        acceptance = acceptance / acceptance.max()

        # Rejection sampling
        keep = ctx.rng.random(len(candidates)) < acceptance
        kept = candidates[keep]

        if len(kept) >= ctx.row_count:
            result = kept[:ctx.row_count]
        else:
            # Fallback: pad with uniform if rejection removed too many
            shortfall = ctx.row_count - len(kept)
            extra = self._uniform(start, end, type("Ctx", (), {
                "rng": ctx.rng,
                "row_count": shortfall,
            })())
            result = np.concatenate([kept, extra])

        # Apply hour-of-day profile if specified
        if hour_profile:
            result = self._apply_hour_profile(result, hour_profile, ctx)

        return result[:ctx.row_count]

    def _apply_hour_profile(
        self,
        timestamps: np.ndarray,
        hour_profile: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        ts = pd.DatetimeIndex(timestamps)

        if hour_profile.get("distribution") == "bimodal":
            peaks = hour_profile.get("peaks", [12, 18])
            std_dev = hour_profile.get("std_dev", 2)
            # Mix of two normals
            n = len(ts)
            peak_choice = ctx.rng.choice(len(peaks), size=n)
            hours = np.zeros(n)
            for i, peak in enumerate(peaks):
                mask = peak_choice == i
                hours[mask] = ctx.rng.normal(peak, std_dev, size=mask.sum())
            hours = np.clip(hours, 0, 23.99).astype(int)
            minutes = ctx.rng.integers(0, 60, size=n)
            seconds = ctx.rng.integers(0, 60, size=n)
        else:
            hours = ctx.rng.integers(0, 24, size=len(ts))
            minutes = ctx.rng.integers(0, 60, size=len(ts))
            seconds = ctx.rng.integers(0, 60, size=len(ts))

        # Reconstruct timestamps with new times
        dates = ts.normalize()  # midnight
        offsets = pd.to_timedelta(hours * 3600 + minutes * 60 + seconds, unit="s")
        return (dates + offsets).values
