"""In-process registry for submitted Fabric Spark generation jobs."""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class JobRecord:
    """Tracks one submitted Fabric notebook run."""

    job_id: str
    fabric_run_id: str
    workspace_id: str
    notebook_item_id: str
    schema_temp_path: str
    lakehouse_id: str
    token: str
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "submitted"


class AsyncJobStore:
    """Thread-safe in-process store for JobRecords."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, JobRecord] = {}

    def put(self, record: JobRecord) -> None:
        with self._lock:
            self._jobs[record.job_id] = record

    def get(self, job_id: str) -> JobRecord | None:
        with self._lock:
            return self._jobs.get(job_id)

    def update_status(self, job_id: str, status: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.status = status
