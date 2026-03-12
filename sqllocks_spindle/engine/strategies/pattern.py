"""Pattern strategy — generate formatted strings with tokens."""

from __future__ import annotations

import re
from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef


class PatternStrategy(Strategy):
    """Generate values from a format pattern with tokens.

    Supports tokens like:
        {seq:6}       → zero-padded sequence (e.g., "000042")
        {random:4}    → random alphanumeric (e.g., "A3F1")
        {column_name} → value from another column in current row
    """

    TOKEN_RE = re.compile(r"\{(\w+)(?::(\d+))?\}")

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        fmt = config.get("format", "")
        if not fmt:
            raise ValueError(
                f"pattern strategy requires 'format' for column '{column.name}'"
            )

        results = []
        for i in range(ctx.row_count):
            value = self._render(fmt, i, ctx)
            results.append(value)

        return np.array(results, dtype=object)

    def _render(self, fmt: str, row_index: int, ctx: GenerationContext) -> str:
        def replace_token(match: re.Match) -> str:
            token = match.group(1)
            width = int(match.group(2)) if match.group(2) else 0

            if token == "seq":
                val = str(row_index + 1)
                return val.zfill(width) if width else val

            if token == "random":
                chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                length = width or 4
                return "".join(
                    ctx.rng.choice(list(chars)) for _ in range(length)
                )

            # Try to look up from current table columns
            if token in ctx.current_table:
                val = ctx.current_table[token]
                if hasattr(val, "__len__") and row_index < len(val):
                    v = str(val[row_index])
                    return v.zfill(width) if width else v

            return match.group(0)  # Leave unresolved tokens as-is

        return self.TOKEN_RE.sub(replace_token, fmt)
