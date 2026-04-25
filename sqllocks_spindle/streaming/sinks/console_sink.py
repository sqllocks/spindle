"""Console (stdout) streaming sink — no external dependencies."""

from __future__ import annotations

import json
import sys
from typing import Any

from sqllocks_spindle.streaming.stream_writer import StreamWriter


class ConsoleSink(StreamWriter):
    """Prints events as JSON Lines to stdout.

    Useful for local development and quick debugging.  No external dependencies.

    Args:
        indent: JSON indent level (``None`` for compact single-line output,
            ``2`` for pretty-printed).
        prefix: Optional string prepended before each JSON line.
        file: Output file object (defaults to ``sys.stdout``).  Injectable for
            testing.
    """

    def __init__(
        self,
        indent: int | None = None,
        prefix: str = "",
        file=None,
    ) -> None:
        self._indent = indent
        self._prefix = prefix
        self._file = file or sys.stdout

    def send_batch(self, events: list[dict[str, Any]]) -> None:
        for event in events:
            line = json.dumps(event, default=str, indent=self._indent)
            print(f"{self._prefix}{line}", file=self._file)
