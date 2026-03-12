"""File (JSONL) streaming sink — no external dependencies."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqllocks_spindle.streaming.stream_writer import StreamWriter


class FileSink(StreamWriter):
    """Writes events as JSON Lines (one JSON object per line) to a file.

    Args:
        path: Output file path.  Parent directories are created automatically.
        mode: File open mode — ``"a"`` (append, default) or ``"w"`` (overwrite).
    """

    def __init__(self, path: str | Path, mode: str = "a") -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self._path, mode, encoding="utf-8")

    def send_batch(self, events: list[dict[str, Any]]) -> None:
        for event in events:
            self._file.write(json.dumps(event, default=str) + "\n")
        self._file.flush()

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()
