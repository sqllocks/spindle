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
        # Deterministic UUIDs from seeded RNG — reproducible + 10x faster
        raw_bytes = ctx.rng.integers(0, 256, size=(ctx.row_count, 16), dtype=np.uint8)
        # Set version 4 bits (byte 6: high nibble = 0100) and variant bits (byte 8: high bits = 10)
        raw_bytes[:, 6] = (raw_bytes[:, 6] & 0x0F) | 0x40
        raw_bytes[:, 8] = (raw_bytes[:, 8] & 0x3F) | 0x80

        def _format_uuid(b):
            h = b.tobytes().hex()
            return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

        values = np.array([_format_uuid(row) for row in raw_bytes], dtype=object)
        return values
