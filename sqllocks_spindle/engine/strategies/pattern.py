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
    _CHARS = np.array(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"))

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

        # Pre-scan for token types to batch-generate where possible
        tokens = list(self.TOKEN_RE.finditer(fmt))
        has_column_ref = any(
            t.group(1) not in ("seq", "random") for t in tokens
        )

        # If column references exist, fall back to per-row rendering
        if has_column_ref:
            results = []
            for i in range(ctx.row_count):
                value = self._render(fmt, i, ctx)
                results.append(value)
            return np.array(results, dtype=object)

        # Batch path: pre-generate all random/seq tokens, then assemble
        # Split the format string into literal parts and token specs
        parts = []
        last_end = 0
        for m in tokens:
            if m.start() > last_end:
                parts.append(("literal", fmt[last_end:m.start()]))
            token = m.group(1)
            width = int(m.group(2)) if m.group(2) else 0
            parts.append(("token", token, width))
            last_end = m.end()
        if last_end < len(fmt):
            parts.append(("literal", fmt[last_end:]))

        # Pre-generate batch arrays for each token type
        token_arrays: dict[int, np.ndarray] = {}
        for idx, part in enumerate(parts):
            if part[0] != "token":
                continue
            token, width = part[1], part[2]
            if token == "seq":
                seqs = np.arange(1, ctx.row_count + 1)
                if width:
                    token_arrays[idx] = np.array(
                        [str(s).zfill(width) for s in seqs], dtype=object
                    )
                else:
                    token_arrays[idx] = seqs.astype(str).astype(object)
            elif token == "random":
                length = width or 4
                rand_indices = ctx.rng.integers(
                    0, len(self._CHARS), size=(ctx.row_count, length)
                )
                token_arrays[idx] = np.array(
                    ["".join(self._CHARS[row]) for row in rand_indices],
                    dtype=object,
                )

        # Assemble results
        result = np.full(ctx.row_count, "", dtype=object)
        for idx, part in enumerate(parts):
            if part[0] == "literal":
                result = np.char.add(result.astype(str), part[1])
            else:
                result = np.char.add(result.astype(str), token_arrays[idx].astype(str))

        return result

    def _render(self, fmt: str, row_index: int, ctx: GenerationContext) -> str:
        def replace_token(match: re.Match) -> str:
            token = match.group(1)
            width = int(match.group(2)) if match.group(2) else 0

            if token == "seq":
                val = str(row_index + 1)
                return val.zfill(width) if width else val

            if token == "random":
                length = width or 4
                indices = ctx.rng.integers(0, len(self._CHARS), size=length)
                return "".join(self._CHARS[indices])

            # Try to look up from current table columns
            if token in ctx.current_table:
                val = ctx.current_table[token]
                if hasattr(val, "__len__") and row_index < len(val):
                    v = str(val[row_index])
                    return v.zfill(width) if width else v

            return match.group(0)  # Leave unresolved tokens as-is

        return self.TOKEN_RE.sub(replace_token, fmt)
