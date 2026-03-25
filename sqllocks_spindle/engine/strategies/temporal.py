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
        month_weights = profiles.get("month", {})
        dow_weights = profiles.get("day_of_week", {})
        hour_profile = profiles.get("hour_of_day", {})

        # Fast path: no seasonal weighting — just uniform
        if not month_weights and not dow_weights:
            result = self._uniform(start, end, ctx)
            if hour_profile:
                result = self._apply_hour_profile(result, hour_profile, ctx)
            return result

        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

        # Build month probability array (12 values)
        base_month = 1.0 / 12.0
        month_probs = np.array([month_weights.get(m, base_month) for m in month_names])
        month_probs = month_probs / month_probs.sum()

        # Build DOW probability array (7 values)
        base_dow = 1.0 / 7.0
        dow_probs = np.array([dow_weights.get(d, base_dow) for d in dow_names])
        dow_probs = dow_probs / dow_probs.sum()

        # Allocate row counts per (month, dow) bucket via multinomial.
        # Combined prob matrix: 12 x 7, flattened to 84 buckets.
        combined_probs = (month_probs[:, None] * dow_probs[None, :]).ravel()
        combined_probs = combined_probs / combined_probs.sum()
        bucket_counts = ctx.rng.multinomial(ctx.row_count, combined_probs)

        # Generate uniform dates within each (year-month, dow) bucket
        # and collect into a result array.
        result_parts: list[np.ndarray] = []
        total_days = int((end - start).days) + 1

        # Pre-compute all valid days in the range as a DatetimeIndex for fast lookup
        all_days = pd.date_range(start, end, freq="D")
        all_months = all_days.month - 1   # 0-indexed
        all_dows = all_days.dayofweek     # 0=Mon

        for bucket_idx, count in enumerate(bucket_counts):
            if count == 0:
                continue
            m_idx = bucket_idx // 7
            d_idx = bucket_idx % 7
            valid_mask = (all_months == m_idx) & (all_dows == d_idx)
            valid_days = all_days[valid_mask]
            if len(valid_days) == 0:
                # This (month, dow) combo doesn't exist in range — redistribute uniformly
                result_parts.append(self._uniform(start, end, type("_Ctx", (), {
                    "rng": ctx.rng, "row_count": int(count),
                })()))
                continue
            # Sample with replacement from valid days, then randomise time-of-day
            chosen = valid_days[ctx.rng.integers(0, len(valid_days), size=int(count))]
            # Add random microsecond offset within each day (unit-matches pandas DatetimeIndex)
            us_offsets = pd.to_timedelta(
                ctx.rng.integers(0, 86_400_000_000, size=int(count)), unit="us"
            )
            result_parts.append((chosen + us_offsets).values)

        if not result_parts:
            result = self._uniform(start, end, ctx)
        else:
            combined = np.concatenate(result_parts)
            # Shuffle so buckets don't appear in sorted order
            ctx.rng.shuffle(combined)
            result = combined[:ctx.row_count]
            if len(result) < ctx.row_count:
                shortfall = ctx.row_count - len(result)
                extra = self._uniform(start, end, type("_Ctx", (), {
                    "rng": ctx.rng, "row_count": shortfall,
                })())
                result = np.concatenate([result, extra])

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
