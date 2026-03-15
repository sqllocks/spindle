"""Sequence strategy — auto-incrementing integers."""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class SequenceStrategy(Strategy):
    """Generate auto-incrementing integer sequences."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        start = config.get("start", 1)
        step = config.get("step", 1)
        # sequence_offset shifts the start for chunked generation so each
        # chunk produces contiguous, non-overlapping PKs.
        offset = getattr(ctx, "sequence_offset", 0)
        effective_start = start + offset * step
        return np.arange(effective_start, effective_start + ctx.row_count * step, step, dtype=np.int64)
