from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class StreamState:
    stream_id: str
    chunks_written: int = 0
    rows_written: int = 0
    running: bool = True
    error: str | None = None
    stop_event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = field(default=None, repr=False)


class StreamManager:
    """Manages background streaming jobs. One instance per process (singleton)."""

    _instance: StreamManager | None = None
    _class_lock = threading.Lock()

    @classmethod
    def instance(cls) -> StreamManager:
        with cls._class_lock:
            if cls._instance is None:
                cls._instance = StreamManager()
            return cls._instance

    def __init__(self) -> None:
        self._streams: dict[str, StreamState] = {}
        self._lock = threading.Lock()

    def start(self, run_fn) -> str:
        """Start a background streaming job. run_fn(state) is called in a daemon thread."""
        stream_id = str(uuid.uuid4())
        state = StreamState(stream_id=stream_id)
        t = threading.Thread(target=self._run, args=(run_fn, state), daemon=True)
        state.thread = t
        with self._lock:
            self._streams[stream_id] = state
        t.start()
        return stream_id

    def _run(self, run_fn, state: StreamState) -> None:
        try:
            run_fn(state)
        except Exception as exc:
            state.error = str(exc)
            logger.error("Stream %s failed: %s", state.stream_id, exc)
        finally:
            state.running = False

    def status(self, stream_id: str) -> dict:
        with self._lock:
            state = self._streams.get(stream_id)
        if state is None:
            return {"error": f"Unknown stream_id: {stream_id}"}
        return {
            "stream_id": stream_id,
            "chunks_written": state.chunks_written,
            "rows_written": state.rows_written,
            "running": state.running,
            "error": state.error,
        }

    def stop(self, stream_id: str) -> None:
        with self._lock:
            state = self._streams.get(stream_id)
        if state is None:
            return
        state.stop_event.set()
        if state.thread and state.thread.is_alive():
            state.thread.join(timeout=5.0)
