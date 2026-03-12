"""UUID strategy — generate UUID v4 values as alternative primary keys."""

from __future__ import annotations

import uuid
from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class UUIDStrategy(Strategy):
    """Generate UUID v4 strings."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        return np.array([str(uuid.uuid4()) for _ in range(ctx.row_count)], dtype=object)
