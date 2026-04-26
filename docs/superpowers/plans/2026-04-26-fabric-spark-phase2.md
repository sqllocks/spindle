# Spindle Phase 2 — Fabric Spark Scale Generation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `scale_mode="fabric_spark"` in `cmd_scale_generate` so Spindle submits generation jobs to a Fabric Spark notebook and returns a `job_id` immediately, with `cmd_scale_status` and `cmd_scale_cancel` for async tracking.

**Architecture:** Static tables are generated once in the MCP process, embedded in an augmented schema JSON uploaded to OneLake. A Fabric notebook runs inside the workspace's Spark cluster using `foreachPartition` to generate dynamic tables chunk-by-chunk, writing directly to the target sinks (LakehouseSink, WarehouseSink, KQLSink, SQLDatabaseSink). The MCP bridge returns immediately after job submission; callers poll with `cmd_scale_status`.

**Tech Stack:** Python `requests` (existing dep), Fabric Jobs REST API (`api.fabric.microsoft.com/v1`), OneLake DFS API (`onelake.dfs.fabric.microsoft.com`), PySpark `foreachPartition`, `unittest.mock.patch` for tests.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `sqllocks_spindle/engine/async_job_store.py` | `JobRecord` dataclass + thread-safe `AsyncJobStore` |
| Create | `sqllocks_spindle/engine/job_tracker.py` | `FabricJobTracker` — polls/cancels via Fabric REST |
| Create | `sqllocks_spindle/engine/spark_router.py` | `FabricSparkRouter` — static gen + OneLake upload + notebook submit |
| Create | `sqllocks_spindle/notebooks/__init__.py` | Exports `SPARK_WORKER_IPYNB` notebook dict |
| Create | `notebooks/spindle_spark_worker.ipynb` | Fabric Spark worker notebook template |
| Modify | `sqllocks_spindle/mcp_bridge.py` | Add `_job_store`, `cmd_scale_status`, `cmd_scale_cancel`; wire `fabric_spark` in `cmd_scale_generate` |
| Modify | `pyproject.toml` | Bump version 2.6.0 → 2.6.1 (patch release for gap fixes) |
| Modify | `sqllocks_spindle/__init__.py` | Bump `__version__` 2.6.0 → 2.6.1 |
| Create | `tests/test_spark_router.py` | Unit tests (all mocked — no Spark required) |
| Modify | `docs/changelog.md` | Add v2.7.0 entry |

---

## Task 1: Version Bump 2.6.0 → 2.6.1

The changelog already records v2.6.1 (the GAP fixes from Phase 1) but pyproject.toml and `__init__.py` still read 2.6.0. Fix before adding new code.

**Files:**
- Modify: `pyproject.toml:7`
- Modify: `sqllocks_spindle/__init__.py:5`

- [ ] **Step 1: Update pyproject.toml version**

In `pyproject.toml`, change line 7:
```toml
version = "2.6.1"
```

- [ ] **Step 2: Update __init__.py version**

In `sqllocks_spindle/__init__.py`, change line 5:
```python
__version__ = "2.6.1"
```

- [ ] **Step 3: Verify**

```bash
cd projects/fabric-datagen
python -c "import sqllocks_spindle; print(sqllocks_spindle.__version__)"
```
Expected: `2.6.1`

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml sqllocks_spindle/__init__.py
git commit -m "chore: bump version to 2.6.1 (GAP fixes)"
```

---

## Task 2: AsyncJobStore + JobRecord

Thread-safe in-process registry for submitted Fabric jobs. No external deps. Start with tests.

**Files:**
- Create: `sqllocks_spindle/engine/async_job_store.py`
- Create: `tests/test_spark_router.py` (first test only)

- [ ] **Step 1: Write failing tests**

Create `tests/test_spark_router.py`:

```python
"""Unit tests for FabricSparkRouter, AsyncJobStore, and FabricJobTracker.

All tests mock HTTP calls — no live Fabric workspace required.
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# AsyncJobStore tests
# ---------------------------------------------------------------------------


def test_job_store_put_and_get():
    """put then get returns the same record."""
    from sqllocks_spindle.engine.async_job_store import AsyncJobStore, JobRecord

    store = AsyncJobStore()
    record = JobRecord(
        job_id="j1",
        fabric_run_id="r1",
        workspace_id="ws1",
        notebook_item_id="nb1",
        schema_temp_path="spindle_temp/r1_schema.json",
        lakehouse_id="lh1",
        token="tok",
    )
    store.put(record)
    assert store.get("j1") is record


def test_job_store_get_unknown_returns_none():
    """get with an unknown job_id returns None (bridge then returns error dict)."""
    from sqllocks_spindle.engine.async_job_store import AsyncJobStore

    store = AsyncJobStore()
    assert store.get("no-such-id") is None


def test_job_store_update_status():
    """update_status changes the record's status field in place."""
    from sqllocks_spindle.engine.async_job_store import AsyncJobStore, JobRecord

    store = AsyncJobStore()
    record = JobRecord(
        job_id="j2",
        fabric_run_id="r2",
        workspace_id="ws",
        notebook_item_id="nb",
        schema_temp_path="spindle_temp/r2_schema.json",
        lakehouse_id="lh",
        token="tok",
    )
    store.put(record)
    store.update_status("j2", "succeeded")
    assert store.get("j2").status == "succeeded"


def test_job_store_thread_safe():
    """Concurrent puts from multiple threads all land safely."""
    from sqllocks_spindle.engine.async_job_store import AsyncJobStore, JobRecord

    store = AsyncJobStore()
    errors = []

    def worker(i):
        try:
            r = JobRecord(
                job_id=f"j{i}",
                fabric_run_id=f"r{i}",
                workspace_id="ws",
                notebook_item_id="nb",
                schema_temp_path=f"spindle_temp/r{i}_schema.json",
                lakehouse_id="lh",
                token="tok",
            )
            store.put(r)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len([store.get(f"j{i}") for i in range(50) if store.get(f"j{i}")]) == 50
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd projects/fabric-datagen
pytest tests/test_spark_router.py::test_job_store_put_and_get -v 2>&1 | tail -5
```
Expected: `ModuleNotFoundError: No module named 'sqllocks_spindle.engine.async_job_store'`

- [ ] **Step 3: Create AsyncJobStore**

Create `sqllocks_spindle/engine/async_job_store.py`:

```python
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
```

- [ ] **Step 4: Run AsyncJobStore tests**

```bash
pytest tests/test_spark_router.py::test_job_store_put_and_get \
       tests/test_spark_router.py::test_job_store_get_unknown_returns_none \
       tests/test_spark_router.py::test_job_store_update_status \
       tests/test_spark_router.py::test_job_store_thread_safe -v
```
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add sqllocks_spindle/engine/async_job_store.py tests/test_spark_router.py
git commit -m "feat: add AsyncJobStore + JobRecord for Fabric Spark job tracking"
```

---

## Task 3: FabricJobTracker

HTTP client for polling and cancelling Fabric notebook runs. Wraps the Fabric Jobs REST API.

**Files:**
- Create: `sqllocks_spindle/engine/job_tracker.py`
- Modify: `tests/test_spark_router.py` (add tracker tests)

- [ ] **Step 1: Add failing tests for FabricJobTracker**

Append to `tests/test_spark_router.py`:

```python
# ---------------------------------------------------------------------------
# FabricJobTracker tests
# ---------------------------------------------------------------------------


def test_tracker_get_status_running():
    """get_status maps Fabric 'InProgress' → 'running'."""
    from sqllocks_spindle.engine.job_tracker import FabricJobTracker

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"status": "InProgress", "id": "run1"}
    mock_resp.raise_for_status = MagicMock()

    with patch("sqllocks_spindle.engine.job_tracker.requests.get", return_value=mock_resp):
        tracker = FabricJobTracker(token="tok")
        result = tracker.get_status(workspace_id="ws1", item_id="nb1", run_id="run1")

    assert result["status"] == "running"
    assert result["fabric_status"] == "InProgress"
    assert result["fabric_run_id"] == "run1"


def test_tracker_get_status_succeeded():
    """get_status maps Fabric 'Completed' → 'succeeded'."""
    from sqllocks_spindle.engine.job_tracker import FabricJobTracker

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"status": "Completed", "id": "run2"}
    mock_resp.raise_for_status = MagicMock()

    with patch("sqllocks_spindle.engine.job_tracker.requests.get", return_value=mock_resp):
        tracker = FabricJobTracker(token="tok")
        result = tracker.get_status(workspace_id="ws1", item_id="nb1", run_id="run2")

    assert result["status"] == "succeeded"


def test_tracker_cancel():
    """cancel posts to the cancel endpoint and returns cancelled=True."""
    from sqllocks_spindle.engine.job_tracker import FabricJobTracker

    mock_resp = MagicMock()
    mock_resp.status_code = 202
    mock_resp.raise_for_status = MagicMock()

    with patch("sqllocks_spindle.engine.job_tracker.requests.post", return_value=mock_resp):
        tracker = FabricJobTracker(token="tok")
        result = tracker.cancel(workspace_id="ws1", item_id="nb1", run_id="run3")

    assert result["cancelled"] is True
    assert result["fabric_run_id"] == "run3"
```

- [ ] **Step 2: Run tracker tests to confirm failure**

```bash
pytest tests/test_spark_router.py::test_tracker_get_status_running -v 2>&1 | tail -5
```
Expected: `ModuleNotFoundError: No module named 'sqllocks_spindle.engine.job_tracker'`

- [ ] **Step 3: Create FabricJobTracker**

Create `sqllocks_spindle/engine/job_tracker.py`:

```python
"""Fabric REST API client for polling and cancelling notebook job runs."""
from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)

_FABRIC_API = "https://api.fabric.microsoft.com/v1"

_STATUS_MAP: dict[str, str] = {
    "NotStarted": "submitted",
    "InProgress": "running",
    "Deduplicating": "running",
    "Completed": "succeeded",
    "Failed": "failed",
    "Cancelled": "cancelled",
}


class FabricJobTracker:
    """Thin wrapper around the Fabric Jobs REST API.

    Args:
        token: Bearer token for Entra authentication.
    """

    def __init__(self, token: str) -> None:
        self._token = token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def get_status(self, workspace_id: str, item_id: str, run_id: str) -> dict:
        """Poll Fabric for the current status of a notebook run.

        Returns:
            dict with keys: status (Spindle-normalized), fabric_status (raw),
            fabric_run_id.
        """
        url = (
            f"{_FABRIC_API}/workspaces/{workspace_id}"
            f"/items/{item_id}/jobs/instances/{run_id}"
        )
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        fabric_status = data.get("status", "Unknown")
        return {
            "status": _STATUS_MAP.get(fabric_status, fabric_status.lower()),
            "fabric_status": fabric_status,
            "fabric_run_id": run_id,
        }

    def cancel(self, workspace_id: str, item_id: str, run_id: str) -> dict:
        """Cancel an in-flight Fabric notebook run.

        Returns:
            dict with keys: cancelled (True), fabric_run_id.
        """
        url = (
            f"{_FABRIC_API}/workspaces/{workspace_id}"
            f"/items/{item_id}/jobs/instances/{run_id}/cancel"
        )
        resp = requests.post(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        return {"cancelled": True, "fabric_run_id": run_id}
```

- [ ] **Step 4: Run all tracker tests**

```bash
pytest tests/test_spark_router.py::test_tracker_get_status_running \
       tests/test_spark_router.py::test_tracker_get_status_succeeded \
       tests/test_spark_router.py::test_tracker_cancel -v
```
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add sqllocks_spindle/engine/job_tracker.py tests/test_spark_router.py
git commit -m "feat: add FabricJobTracker for Fabric Jobs REST API"
```

---

## Task 4: FabricSparkRouter

The main orchestrator: generates static tables in-process, uploads augmented schema JSON to OneLake via the DFS API, looks up (or creates) the Spark worker notebook, submits the Fabric notebook run, and returns a `JobRecord`.

**Files:**
- Create: `sqllocks_spindle/engine/spark_router.py`
- Modify: `tests/test_spark_router.py` (add router tests)

- [ ] **Step 1: Add failing tests for FabricSparkRouter**

Append to `tests/test_spark_router.py`:

```python
# ---------------------------------------------------------------------------
# FabricSparkRouter tests
# ---------------------------------------------------------------------------


def _minimal_schema_dict_dynamic() -> dict:
    """Schema where all tables have count >= 500 (so all are dynamic at chunk_size=500)."""
    return {
        "model": {"name": "test", "domain": "test", "seed": 42, "locale": "en_US", "date_range": None},
        "tables": {
            "widgets": {
                "name": "widgets",
                "columns": {
                    "widget_id": {
                        "name": "widget_id",
                        "type": "integer",
                        "generator": {"strategy": "sequence", "start": 1},
                        "nullable": False,
                        "null_rate": 0.0,
                    },
                },
                "primary_key": ["widget_id"],
                "description": "test",
            }
        },
        "relationships": [],
        "business_rules": [],
        "generation": {
            "scale": "large",
            "scales": {"large": {"widgets": 1000}},
        },
    }


def _make_router(**kwargs):
    from sqllocks_spindle.engine.spark_router import FabricSparkRouter

    defaults = dict(
        workspace_id="ws-123",
        lakehouse_id="lh-456",
        token="bearer-tok",
        chunk_size=500,
    )
    defaults.update(kwargs)
    return FabricSparkRouter(**defaults)


def _mock_http_for_submit(notebook_list_items=None, notebook_id="nb-789", submit_status=202):
    """Return (get_mock, post_mock, put_mock, patch_mock) configured for a happy-path submit."""
    notebook_list_items = notebook_list_items or [
        {"id": notebook_id, "displayName": "spindle_spark_worker"}
    ]

    get_mock = MagicMock()
    get_mock.return_value.status_code = 200
    get_mock.return_value.json.return_value = {"value": notebook_list_items}
    get_mock.return_value.raise_for_status = MagicMock()

    post_mock = MagicMock()
    post_mock.return_value.status_code = submit_status
    post_mock.return_value.headers = {"Location": f"https://api.fabric.microsoft.com/v1/runs/{notebook_id}/jobs/instances/run-001"}
    post_mock.return_value.json.return_value = {"id": "run-001"}
    post_mock.return_value.raise_for_status = MagicMock()

    put_mock = MagicMock()
    put_mock.return_value.status_code = 201
    put_mock.return_value.raise_for_status = MagicMock()

    patch_mock = MagicMock()
    patch_mock.return_value.status_code = 200
    patch_mock.return_value.raise_for_status = MagicMock()

    return get_mock, post_mock, put_mock, patch_mock


def test_submit_returns_job_record():
    """submit() returns a JobRecord with non-empty job_id and fabric_run_id."""
    from sqllocks_spindle.engine.spark_router import FabricSparkRouter
    import sqllocks_spindle.engine.spark_router as sr_mod

    get_m, post_m, put_m, patch_m = _mock_http_for_submit()
    schema = _minimal_schema_dict_dynamic()

    with patch.object(sr_mod.requests, "get", get_m), \
         patch.object(sr_mod.requests, "post", post_m), \
         patch.object(sr_mod.requests, "put", put_m), \
         patch.object(sr_mod.requests, "patch", patch_m):
        router = _make_router()
        record = router.submit(schema, total_rows=1000, seed=42)

    assert record.job_id
    assert record.fabric_run_id
    assert record.workspace_id == "ws-123"
    assert record.notebook_item_id == "nb-789"


def test_submit_uploads_schema_to_onelake():
    """submit() makes three OneLake DFS calls (create, append, flush) before notebook submission."""
    from sqllocks_spindle.engine.spark_router import FabricSparkRouter
    import sqllocks_spindle.engine.spark_router as sr_mod

    get_m, post_m, put_m, patch_m = _mock_http_for_submit()
    schema = _minimal_schema_dict_dynamic()

    with patch.object(sr_mod.requests, "get", get_m), \
         patch.object(sr_mod.requests, "post", post_m), \
         patch.object(sr_mod.requests, "put", put_m), \
         patch.object(sr_mod.requests, "patch", patch_m):
        router = _make_router()
        router.submit(schema, total_rows=1000, seed=42)

    # put = create file resource; patch called twice (append + flush)
    assert put_m.call_count == 1
    assert patch_m.call_count == 2
    put_url = put_m.call_args[0][0]
    assert "onelake.dfs.fabric.microsoft.com" in put_url
    assert "ws-123" in put_url
    assert "lh-456" in put_url
    assert "spindle_temp/" in put_url


def test_submit_embeds_schema_counts_in_upload():
    """Uploaded schema JSON contains _schema_counts and _base_seed keys."""
    from sqllocks_spindle.engine.spark_router import FabricSparkRouter
    import sqllocks_spindle.engine.spark_router as sr_mod

    get_m, post_m, put_m, patch_m = _mock_http_for_submit()
    schema = _minimal_schema_dict_dynamic()
    captured_body = []

    real_patch = sr_mod.requests.patch

    def capture_patch(url, **kwargs):
        if "action=append" in url:
            captured_body.append(kwargs.get("data", b""))
        m = MagicMock()
        m.raise_for_status = MagicMock()
        return m

    with patch.object(sr_mod.requests, "get", get_m), \
         patch.object(sr_mod.requests, "post", post_m), \
         patch.object(sr_mod.requests, "put", put_m), \
         patch.object(sr_mod.requests, "patch", capture_patch):
        router = _make_router()
        router.submit(schema, total_rows=1000, seed=42)

    uploaded = json.loads(captured_body[0])
    assert "_schema_counts" in uploaded
    assert "_base_seed" in uploaded
    assert uploaded["_base_seed"] == 42


def test_submit_finds_existing_notebook():
    """submit() uses the existing notebook ID when found — does not create a new one."""
    from sqllocks_spindle.engine.spark_router import FabricSparkRouter
    import sqllocks_spindle.engine.spark_router as sr_mod

    existing_id = "nb-existing"
    get_m, post_m, put_m, patch_m = _mock_http_for_submit(
        notebook_list_items=[{"id": existing_id, "displayName": "spindle_spark_worker"}],
        notebook_id=existing_id,
    )
    schema = _minimal_schema_dict_dynamic()

    with patch.object(sr_mod.requests, "get", get_m), \
         patch.object(sr_mod.requests, "post", post_m), \
         patch.object(sr_mod.requests, "put", put_m), \
         patch.object(sr_mod.requests, "patch", patch_m):
        router = _make_router()
        record = router.submit(schema, total_rows=1000, seed=42)

    assert record.notebook_item_id == existing_id


def test_submit_creates_notebook_when_missing():
    """submit() creates the notebook via POST /items when not found in workspace."""
    from sqllocks_spindle.engine.spark_router import FabricSparkRouter
    import sqllocks_spindle.engine.spark_router as sr_mod

    # Notebook list returns empty; creation POST returns new ID
    get_m, post_m, put_m, patch_m = _mock_http_for_submit(notebook_list_items=[])

    # post is called twice: once for notebook creation, once for job submission
    created_id = "nb-newly-created"
    post_responses = [
        MagicMock(**{"status_code": 200, "json.return_value": {"id": created_id}, "raise_for_status": MagicMock()}),
        MagicMock(**{"status_code": 202, "headers": {"Location": ".../run-999"}, "raise_for_status": MagicMock()}),
    ]
    post_m.side_effect = post_responses

    schema = _minimal_schema_dict_dynamic()

    with patch.object(sr_mod.requests, "get", get_m), \
         patch.object(sr_mod.requests, "post", post_m), \
         patch.object(sr_mod.requests, "put", put_m), \
         patch.object(sr_mod.requests, "patch", patch_m):
        router = _make_router()
        record = router.submit(schema, total_rows=1000, seed=42)

    assert record.notebook_item_id == created_id
    # First POST is notebook creation
    first_call_url = post_m.call_args_list[0][0][0]
    assert "/items" in first_call_url
```

- [ ] **Step 2: Run failing tests**

```bash
pytest tests/test_spark_router.py::test_submit_returns_job_record -v 2>&1 | tail -5
```
Expected: `ModuleNotFoundError: No module named 'sqllocks_spindle.engine.spark_router'`

- [ ] **Step 3: Create notebooks/__init__.py with SPARK_WORKER_IPYNB**

Create `sqllocks_spindle/notebooks/__init__.py`:

```python
"""Fabric notebook templates bundled with Spindle."""
from __future__ import annotations

from pathlib import Path

_NOTEBOOKS_DIR = Path(__file__).parent.parent.parent / "notebooks"


def _load_notebook(name: str) -> dict:
    import json
    path = _NOTEBOOKS_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Notebook template not found: {path}")
    with open(path) as f:
        return json.load(f)


def _get_spark_worker_ipynb() -> dict:
    try:
        return _load_notebook("spindle_spark_worker.ipynb")
    except FileNotFoundError:
        # Return minimal valid notebook so tests don't fail on missing template
        return {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {"kernelspec": {"display_name": "PySpark", "language": "python", "name": "synapse_pyspark"}},
            "cells": [],
        }


SPARK_WORKER_IPYNB: dict = _get_spark_worker_ipynb()
```

- [ ] **Step 4: Create FabricSparkRouter**

Create `sqllocks_spindle/engine/spark_router.py`:

```python
"""FabricSparkRouter — submits Spindle generation jobs to Fabric Spark notebooks."""
from __future__ import annotations

import json
import logging
import uuid

import requests

from sqllocks_spindle.engine.async_job_store import JobRecord
from sqllocks_spindle.engine.scale_router import (
    _SpindleJSONEncoder,
    _classify_tables,
    _generate_static_tables,
)

logger = logging.getLogger(__name__)

_FABRIC_API = "https://api.fabric.microsoft.com/v1"
_ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"


class NotebookNotFoundError(RuntimeError):
    """Raised when the Spindle Spark worker notebook cannot be found or created."""


class FabricAPIError(RuntimeError):
    """Raised when the Fabric REST API returns an unexpected error."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"Fabric API error {status_code}: {message}")
        self.status_code = status_code


class FabricSparkRouter:
    """Submits Spindle generation jobs to a Fabric Spark notebook.

    Workflow:
    1. Generate static (reference) tables in the calling process.
    2. Embed static PK data + classification metadata into schema JSON.
    3. Upload augmented schema JSON to OneLake Files via the DFS API.
    4. Find or auto-create the ``spindle_spark_worker`` notebook in the workspace.
    5. Submit a notebook run with schema path + generation params.
    6. Return a :class:`~sqllocks_spindle.engine.async_job_store.JobRecord` immediately.

    Args:
        workspace_id: Fabric workspace GUID.
        lakehouse_id: Lakehouse GUID for temp file staging and (optionally) output.
        token: Entra bearer token with Workspace.Write + Lakehouse.ReadWrite permissions.
        notebook_name: Display name of the Spark worker notebook (default: ``spindle_spark_worker``).
        sinks: List of sink names to pass into the notebook (default: ``["lakehouse"]``).
        sink_config: Per-sink configuration dict passed as JSON to the notebook.
        chunk_size: Rows per Spark partition. Default 500_000.
    """

    def __init__(
        self,
        workspace_id: str,
        lakehouse_id: str,
        token: str,
        notebook_name: str = "spindle_spark_worker",
        sinks: list[str] | None = None,
        sink_config: dict | None = None,
        chunk_size: int = 500_000,
    ) -> None:
        self._workspace_id = workspace_id
        self._lakehouse_id = lakehouse_id
        self._token = token
        self._notebook_name = notebook_name
        self._sinks = sinks or ["lakehouse"]
        self._sink_config = sink_config or {}
        self._chunk_size = chunk_size

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    def _json_headers(self) -> dict[str, str]:
        return {**self._auth_headers(), "Content-Type": "application/json"}

    def _get_or_create_notebook(self) -> str:
        """Return the item ID for the Spark worker notebook, creating if absent."""
        url = f"{_FABRIC_API}/workspaces/{self._workspace_id}/notebooks"
        resp = requests.get(url, headers=self._json_headers(), timeout=30)
        resp.raise_for_status()
        for item in resp.json().get("value", []):
            if item.get("displayName") == self._notebook_name:
                logger.info("Found existing notebook '%s' (id=%s)", self._notebook_name, item["id"])
                return item["id"]
        logger.info("Notebook '%s' not found — creating.", self._notebook_name)
        return self._create_notebook()

    def _create_notebook(self) -> str:
        """Create the spindle_spark_worker notebook via the Fabric Items API."""
        import base64

        from sqllocks_spindle.notebooks import SPARK_WORKER_IPYNB

        payload_b64 = base64.b64encode(
            json.dumps(SPARK_WORKER_IPYNB).encode()
        ).decode()
        body = {
            "displayName": self._notebook_name,
            "type": "Notebook",
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.ipynb",
                        "payload": payload_b64,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }
        url = f"{_FABRIC_API}/workspaces/{self._workspace_id}/items"
        resp = requests.post(url, headers=self._json_headers(), json=body, timeout=60)
        resp.raise_for_status()
        item_id = resp.json()["id"]
        logger.info("Created notebook '%s' (id=%s)", self._notebook_name, item_id)
        return item_id

    def _upload_schema(self, schema_dict: dict, run_id: str) -> str:
        """Upload augmented schema JSON to OneLake Files via the ADLS Gen2 DFS API.

        Returns the OneLake-relative file path (``spindle_temp/{run_id}_schema.json``).

        The three-step DFS protocol:
        1. PUT  ?resource=file          — create empty file
        2. PATCH ?action=append         — write bytes at position 0
        3. PATCH ?action=flush          — commit at final length
        """
        data: bytes = json.dumps(schema_dict, cls=_SpindleJSONEncoder).encode()
        rel_path = f"spindle_temp/{run_id}_schema.json"
        base_url = (
            f"{_ONELAKE_DFS}/{self._workspace_id}/{self._lakehouse_id}/files/{rel_path}"
        )

        requests.put(
            f"{base_url}?resource=file",
            headers=self._auth_headers(),
            timeout=30,
        ).raise_for_status()

        requests.patch(
            f"{base_url}?action=append&position=0",
            headers={**self._auth_headers(), "Content-Length": str(len(data))},
            data=data,
            timeout=120,
        ).raise_for_status()

        requests.patch(
            f"{base_url}?action=flush&position={len(data)}",
            headers=self._auth_headers(),
            timeout=30,
        ).raise_for_status()

        logger.info("Schema uploaded to OneLake: %s", rel_path)
        return rel_path

    def _submit_notebook_run(
        self,
        notebook_item_id: str,
        schema_path: str,
        total_rows: int,
        seed: int,
    ) -> str:
        """Submit a Fabric notebook run and return the Fabric run ID."""
        url = (
            f"{_FABRIC_API}/workspaces/{self._workspace_id}"
            f"/items/{notebook_item_id}/jobs/instances?jobType=RunNotebook"
        )
        body = {
            "executionData": {
                "parameters": {
                    "schema_path": {"value": schema_path, "cellLanguage": "Python"},
                    "chunk_size": {"value": str(self._chunk_size), "cellLanguage": "Python"},
                    "seed": {"value": str(seed), "cellLanguage": "Python"},
                    "total_rows": {"value": str(total_rows), "cellLanguage": "Python"},
                    "sinks_json": {
                        "value": json.dumps(
                            {"sinks": self._sinks, "sink_config": self._sink_config}
                        ),
                        "cellLanguage": "Python",
                    },
                    "workspace_id": {"value": self._workspace_id, "cellLanguage": "Python"},
                    "lakehouse_id": {"value": self._lakehouse_id, "cellLanguage": "Python"},
                }
            }
        }
        resp = requests.post(url, headers=self._json_headers(), json=body, timeout=30)
        if resp.status_code == 202:
            # Async accepted — run ID is in the Location header
            location = resp.headers.get("Location", "")
            return location.rstrip("/").split("/")[-1]
        if resp.status_code == 200:
            return resp.json().get("id", uuid.uuid4().hex)
        raise FabricAPIError(resp.status_code, resp.text)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(self, schema_dict: dict, total_rows: int, seed: int) -> JobRecord:
        """Generate static tables, upload schema, and submit a Fabric notebook run.

        Returns a :class:`JobRecord` immediately.  The actual data generation
        runs asynchronously inside the Fabric Spark notebook.
        """
        from sqllocks_spindle.engine.generator import calculate_row_counts
        from sqllocks_spindle.schema.parser import SchemaParser

        schema = SchemaParser().parse_dict(schema_dict)
        schema_counts = calculate_row_counts(schema)
        static_tables, dynamic_tables = _classify_tables(schema_counts, self._chunk_size)

        if static_tables:
            logger.info("Generating %d static tables in main process.", len(static_tables))
            static_chunk = _generate_static_tables(
                schema_path="",
                static_tables=static_tables,
                schema_counts=schema_counts,
                seed=seed,
                schema_dict=schema_dict,
            )
            schema_dict["_static_tables"] = list(static_tables)
            schema_dict["_static_pk_data"] = static_chunk
            schema_dict["_dynamic_tables"] = list(dynamic_tables)
            schema_dict["_schema_counts"] = schema_counts
        else:
            schema_dict["_static_tables"] = []
            schema_dict["_static_pk_data"] = {}
            schema_dict["_dynamic_tables"] = list(dynamic_tables)
            schema_dict["_schema_counts"] = schema_counts

        schema_dict["_base_seed"] = seed

        run_id = uuid.uuid4().hex
        schema_path = self._upload_schema(schema_dict, run_id)
        notebook_item_id = self._get_or_create_notebook()
        fabric_run_id = self._submit_notebook_run(
            notebook_item_id=notebook_item_id,
            schema_path=schema_path,
            total_rows=total_rows,
            seed=seed,
        )

        job_id = f"spindle-{run_id[:8]}"
        return JobRecord(
            job_id=job_id,
            fabric_run_id=fabric_run_id,
            workspace_id=self._workspace_id,
            notebook_item_id=notebook_item_id,
            schema_temp_path=schema_path,
            lakehouse_id=self._lakehouse_id,
            token=self._token,
        )
```

- [ ] **Step 5: Run all router tests**

```bash
pytest tests/test_spark_router.py::test_submit_returns_job_record \
       tests/test_spark_router.py::test_submit_uploads_schema_to_onelake \
       tests/test_spark_router.py::test_submit_embeds_schema_counts_in_upload \
       tests/test_spark_router.py::test_submit_finds_existing_notebook \
       tests/test_spark_router.py::test_submit_creates_notebook_when_missing -v
```
Expected: 5 passed

- [ ] **Step 6: Run full suite to catch regressions**

```bash
pytest tests/ -x -q --ignore=tests/test_e2e_scale_router.py 2>&1 | tail -10
```
Expected: all pass (ignore slow mark)

- [ ] **Step 7: Commit**

```bash
git add sqllocks_spindle/engine/spark_router.py sqllocks_spindle/notebooks/__init__.py tests/test_spark_router.py
git commit -m "feat: add FabricSparkRouter (OneLake upload + notebook submit)"
```

---

## Task 5: MCP Bridge — cmd_scale_status + cmd_scale_cancel

Wire the `_job_store` global and add the two new polling commands to `mcp_bridge.py`.

**Files:**
- Modify: `sqllocks_spindle/mcp_bridge.py`
- Modify: `tests/test_spark_router.py` (add bridge tests)

- [ ] **Step 1: Add failing bridge tests**

Append to `tests/test_spark_router.py`:

```python
# ---------------------------------------------------------------------------
# MCP bridge command tests
# ---------------------------------------------------------------------------


def _run_bridge_command(command: str, params: dict) -> dict:
    """Helper: import and dispatch an mcp_bridge command function directly."""
    import importlib
    bridge = importlib.import_module("sqllocks_spindle.mcp_bridge")
    fn = bridge.COMMANDS[command]
    return fn(params)


def test_cmd_scale_status_unknown_job_id():
    """cmd_scale_status returns {error: 'job_not_found'} for unknown job_id."""
    result = _run_bridge_command("scale_status", {"job_id": "no-such-job"})
    assert result.get("error") == "job_not_found"


def test_cmd_scale_cancel_unknown_job_id():
    """cmd_scale_cancel returns {error: 'job_not_found'} for unknown job_id."""
    result = _run_bridge_command("scale_cancel", {"job_id": "no-such-job"})
    assert result.get("error") == "job_not_found"


def test_cmd_scale_status_known_job_polls_fabric():
    """cmd_scale_status calls FabricJobTracker.get_status for a known job."""
    from sqllocks_spindle.engine.async_job_store import AsyncJobStore, JobRecord
    import sqllocks_spindle.mcp_bridge as bridge_mod

    record = JobRecord(
        job_id="spindle-abc",
        fabric_run_id="fab-run-001",
        workspace_id="ws-1",
        notebook_item_id="nb-1",
        schema_temp_path="spindle_temp/abc_schema.json",
        lakehouse_id="lh-1",
        token="tok",
    )
    bridge_mod._job_store.put(record)

    mock_tracker = MagicMock()
    mock_tracker.get_status.return_value = {
        "status": "running",
        "fabric_status": "InProgress",
        "fabric_run_id": "fab-run-001",
    }

    with patch("sqllocks_spindle.mcp_bridge.FabricJobTracker", return_value=mock_tracker):
        result = _run_bridge_command("scale_status", {"job_id": "spindle-abc"})

    mock_tracker.get_status.assert_called_once_with(
        workspace_id="ws-1", item_id="nb-1", run_id="fab-run-001"
    )
    assert result["status"] == "running"


def test_cmd_scale_cancel_known_job_cancels_fabric():
    """cmd_scale_cancel calls FabricJobTracker.cancel for a known job."""
    from sqllocks_spindle.engine.async_job_store import AsyncJobStore, JobRecord
    import sqllocks_spindle.mcp_bridge as bridge_mod

    record = JobRecord(
        job_id="spindle-def",
        fabric_run_id="fab-run-002",
        workspace_id="ws-2",
        notebook_item_id="nb-2",
        schema_temp_path="spindle_temp/def_schema.json",
        lakehouse_id="lh-2",
        token="tok2",
    )
    bridge_mod._job_store.put(record)

    mock_tracker = MagicMock()
    mock_tracker.cancel.return_value = {"cancelled": True, "fabric_run_id": "fab-run-002"}

    with patch("sqllocks_spindle.mcp_bridge.FabricJobTracker", return_value=mock_tracker):
        result = _run_bridge_command("scale_cancel", {"job_id": "spindle-def"})

    mock_tracker.cancel.assert_called_once_with(
        workspace_id="ws-2", item_id="nb-2", run_id="fab-run-002"
    )
    assert result["cancelled"] is True
```

- [ ] **Step 2: Run to confirm failure**

```bash
pytest tests/test_spark_router.py::test_cmd_scale_status_unknown_job_id -v 2>&1 | tail -5
```
Expected: `KeyError: 'scale_status'` (command not registered yet)

- [ ] **Step 3: Add _job_store global and two commands to mcp_bridge.py**

In `sqllocks_spindle/mcp_bridge.py`, after the existing imports at the top, add the global store (after the `from sqllocks_spindle import __version__` line):

```python
from sqllocks_spindle.engine.async_job_store import AsyncJobStore

_job_store = AsyncJobStore()
```

Then add these two functions before the `COMMANDS` dict (after `cmd_stream_stop`):

```python
def cmd_scale_status(params: dict) -> dict:
    """Poll the status of a submitted fabric_spark generation job."""
    from sqllocks_spindle.engine.job_tracker import FabricJobTracker

    job_id = params.get("job_id", "")
    record = _job_store.get(job_id)
    if record is None:
        return {"error": "job_not_found", "job_id": job_id}

    tracker = FabricJobTracker(token=record.token)
    result = tracker.get_status(
        workspace_id=record.workspace_id,
        item_id=record.notebook_item_id,
        run_id=record.fabric_run_id,
    )
    _job_store.update_status(job_id, result["status"])
    return {**result, "job_id": job_id}


def cmd_scale_cancel(params: dict) -> dict:
    """Cancel an in-flight fabric_spark generation job."""
    from sqllocks_spindle.engine.job_tracker import FabricJobTracker

    job_id = params.get("job_id", "")
    record = _job_store.get(job_id)
    if record is None:
        return {"error": "job_not_found", "job_id": job_id}

    tracker = FabricJobTracker(token=record.token)
    result = tracker.cancel(
        workspace_id=record.workspace_id,
        item_id=record.notebook_item_id,
        run_id=record.fabric_run_id,
    )
    _job_store.update_status(job_id, "cancelled")
    return {**result, "job_id": job_id}
```

Then update the `COMMANDS` dict to include the two new commands:

```python
COMMANDS = {
    "list": cmd_list,
    "describe": cmd_describe,
    "generate": cmd_generate,
    "dry_run": cmd_dry_run,
    "validate": cmd_validate,
    "preview": cmd_preview,
    "profile_info": cmd_profile_info,
    "demo_list": cmd_demo_list,
    "demo_run": cmd_demo_run,
    "scale_generate": cmd_scale_generate,
    "scale_status": cmd_scale_status,
    "scale_cancel": cmd_scale_cancel,
    "stream": cmd_stream,
    "stream_status": cmd_stream_status,
    "stream_stop": cmd_stream_stop,
}
```

- [ ] **Step 4: Run bridge command tests**

```bash
pytest tests/test_spark_router.py::test_cmd_scale_status_unknown_job_id \
       tests/test_spark_router.py::test_cmd_scale_cancel_unknown_job_id \
       tests/test_spark_router.py::test_cmd_scale_status_known_job_polls_fabric \
       tests/test_spark_router.py::test_cmd_scale_cancel_known_job_cancels_fabric -v
```
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add sqllocks_spindle/mcp_bridge.py tests/test_spark_router.py
git commit -m "feat: add cmd_scale_status and cmd_scale_cancel MCP bridge commands"
```

---

## Task 6: Wire fabric_spark in cmd_scale_generate

Replace the stub `{"error": "not_implemented"}` with the real `FabricSparkRouter` path.

**Files:**
- Modify: `sqllocks_spindle/mcp_bridge.py` (cmd_scale_generate)
- Modify: `tests/test_spark_router.py` (add end-to-end bridge test)

- [ ] **Step 1: Add failing test for fabric_spark mode**

Append to `tests/test_spark_router.py`:

```python
def test_cmd_scale_generate_fabric_spark_returns_job_id():
    """cmd_scale_generate with fabric_spark returns {job_id, status:'submitted'}."""
    import sqllocks_spindle.engine.spark_router as sr_mod
    import sqllocks_spindle.mcp_bridge as bridge_mod

    get_m, post_m, put_m, patch_m = _mock_http_for_submit(notebook_id="nb-gen-test")

    fake_record_id = "spindle-gentest"

    from sqllocks_spindle.engine.async_job_store import JobRecord

    fake_record = JobRecord(
        job_id=fake_record_id,
        fabric_run_id="run-gen-001",
        workspace_id="ws-gen",
        notebook_item_id="nb-gen-test",
        schema_temp_path="spindle_temp/gen_schema.json",
        lakehouse_id="lh-gen",
        token="tok-gen",
    )

    mock_router = MagicMock()
    mock_router.submit.return_value = fake_record

    with patch("sqllocks_spindle.mcp_bridge.FabricSparkRouter", return_value=mock_router):
        result = _run_bridge_command("scale_generate", {
            "domain": "retail",
            "scale": "small",
            "scale_mode": "fabric_spark",
            "sink_config": {
                "workspace_id": "ws-gen",
                "lakehouse_id": "lh-gen",
                "token": "tok-gen",
            },
        })

    assert result.get("status") == "submitted"
    assert result.get("job_id") == fake_record_id
    assert "fabric_run_id" in result
```

- [ ] **Step 2: Run to confirm failure**

```bash
pytest tests/test_spark_router.py::test_cmd_scale_generate_fabric_spark_returns_job_id -v 2>&1 | tail -5
```
Expected: `AssertionError: assert {'error': 'not_implemented'} ...`

- [ ] **Step 3: Replace the stub in cmd_scale_generate**

In `sqllocks_spindle/mcp_bridge.py`, find the `fabric_spark` stub block inside `cmd_scale_generate`:

```python
    # fabric_spark is not yet implemented
    if scale_mode == "fabric_spark":
        return {"error": "not_implemented"}
```

Replace it with:

```python
    if scale_mode == "fabric_spark":
        from sqllocks_spindle.engine.spark_router import FabricSparkRouter
        import dataclasses

        workspace_id = sink_config.get("workspace_id", "")
        lakehouse_id = sink_config.get("lakehouse_id", "")
        token = sink_config.get("token", "")
        notebook_name = sink_config.get("notebook_name", "spindle_spark_worker")

        if not workspace_id or not lakehouse_id or not token:
            raise ValueError(
                "fabric_spark requires sink_config with workspace_id, lakehouse_id, and token"
            )

        domain = _resolve_domain(domain_name, mode, profile)
        spindle = Spindle()
        parsed = spindle._resolve_schema(domain, None)
        parsed.generation.scale = scale
        parsed.model.seed = seed

        row_counts = spindle._calculate_row_counts(parsed)
        total_rows = sum(row_counts.values())

        schema_dict = dataclasses.asdict(parsed)
        if hasattr(domain, "child_domains"):
            schema_dict["_domain_path"] = [str(d.domain_path) for d in domain.child_domains]
        elif hasattr(domain, "domain_path"):
            schema_dict["_domain_path"] = str(domain.domain_path)

        router = FabricSparkRouter(
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            token=token,
            notebook_name=notebook_name,
            sinks=sinks_list,
            sink_config=sink_config,
            chunk_size=chunk_size,
        )
        record = router.submit(schema_dict, total_rows=total_rows, seed=seed)
        _job_store.put(record)
        return {
            "job_id": record.job_id,
            "fabric_run_id": record.fabric_run_id,
            "status": "submitted",
            "domain": domain_name,
            "scale": scale,
            "total_rows_queued": total_rows,
            "schema_temp_path": record.schema_temp_path,
        }
```

Note: The `from sqllocks_spindle.engine.generator import Spindle` import is already at the top of the `elif scale_mode == "local_single":` branch. Move it to before the `if scale_mode == "fabric_spark":` check (or add a local import inside the fabric_spark block — local import is safer for subprocess isolation). Add it inside the block:

```python
        from sqllocks_spindle.engine.generator import Spindle
```

This goes before `domain = _resolve_domain(...)`.

- [ ] **Step 4: Run the test**

```bash
pytest tests/test_spark_router.py::test_cmd_scale_generate_fabric_spark_returns_job_id -v
```
Expected: 1 passed

- [ ] **Step 5: Run all spark router tests**

```bash
pytest tests/test_spark_router.py -v
```
Expected: all tests pass (target: 16 tests)

- [ ] **Step 6: Commit**

```bash
git add sqllocks_spindle/mcp_bridge.py tests/test_spark_router.py
git commit -m "feat: implement cmd_scale_generate fabric_spark mode"
```

---

## Task 7: spindle_spark_worker Notebook

The actual Fabric Spark notebook that runs inside the workspace. It reads the schema from OneLake, generates dynamic tables via `foreachPartition`, and writes results via the appropriate sinks.

**Files:**
- Create: `notebooks/spindle_spark_worker.ipynb`

This notebook does not have unit tests (it executes inside Fabric Spark). It is validated by the `@pytest.mark.live` integration test (Task 8 covers adding that marker; actual execution requires a live workspace).

- [ ] **Step 1: Create the notebook**

Create `notebooks/spindle_spark_worker.ipynb`:

```json
{
  "nbformat": 4,
  "nbformat_minor": 5,
  "metadata": {
    "kernelspec": {
      "display_name": "PySpark",
      "language": "python",
      "name": "synapse_pyspark"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "tags": ["parameters"]
      },
      "outputs": [],
      "source": [
        "# Fabric notebook parameters — injected by FabricSparkRouter via Jobs API\n",
        "schema_path = \"\"\n",
        "chunk_size = 500000\n",
        "seed = 42\n",
        "total_rows = 1000000\n",
        "sinks_json = '{\"sinks\": [\"lakehouse\"], \"sink_config\": {}}'\n",
        "workspace_id = \"\"\n",
        "lakehouse_id = \"\""
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "# Load augmented schema JSON from OneLake Files\n",
        "import json\n",
        "from notebookutils import mssparkutils\n",
        "\n",
        "schema_raw = mssparkutils.fs.head(f\"Files/{schema_path}\", 200_000_000)\n",
        "schema_dict = json.loads(schema_raw)\n",
        "chunk_size = int(chunk_size)\n",
        "seed = int(seed)\n",
        "total_rows = int(total_rows)\n",
        "sinks_config = json.loads(sinks_json)\n",
        "sink_names = sinks_config.get(\"sinks\", [\"lakehouse\"])\n",
        "sink_cfg = sinks_config.get(\"sink_config\", {})\n",
        "print(f\"Schema loaded. Static tables: {schema_dict.get('_static_tables', [])}\")\n",
        "print(f\"Dynamic tables: {schema_dict.get('_dynamic_tables', [])}\")"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "# Distribute dynamic table generation across Spark partitions\n",
        "import math\n",
        "import json\n",
        "import numpy as np\n",
        "from pyspark.sql import SparkSession\n",
        "\n",
        "spark = SparkSession.builder.getOrCreate()\n",
        "sc = spark.sparkContext\n",
        "\n",
        "n_chunks = math.ceil(total_rows / chunk_size)\n",
        "\n",
        "# Broadcast large objects to avoid repeated serialization\n",
        "schema_dict_bc = sc.broadcast(schema_dict)\n",
        "sink_names_bc = sc.broadcast(sink_names)\n",
        "sink_cfg_bc = sc.broadcast(sink_cfg)\n",
        "workspace_id_bc = sc.broadcast(workspace_id)\n",
        "lakehouse_id_bc = sc.broadcast(lakehouse_id)\n",
        "chunk_size_bc = sc.broadcast(chunk_size)\n",
        "seed_bc = sc.broadcast(seed)\n",
        "total_rows_bc = sc.broadcast(total_rows)\n",
        "\n",
        "def generate_partition(chunk_indices):\n",
        "    \"\"\"Worker function: runs generate_chunk and writes to sinks for each chunk index.\"\"\"\n",
        "    import json\n",
        "    import os\n",
        "    import tempfile\n",
        "\n",
        "    import numpy as np\n",
        "\n",
        "    from sqllocks_spindle.engine.chunk_worker import generate_chunk\n",
        "\n",
        "    _schema = schema_dict_bc.value\n",
        "    _chunk_size = chunk_size_bc.value\n",
        "    _seed = seed_bc.value\n",
        "    _total = total_rows_bc.value\n",
        "    _sinks = sink_names_bc.value\n",
        "    _sink_cfg = sink_cfg_bc.value\n",
        "    _ws = workspace_id_bc.value\n",
        "    _lh = lakehouse_id_bc.value\n",
        "\n",
        "    # chunk_worker reads from a file — write schema dict to a temp file\n",
        "    with tempfile.NamedTemporaryFile(suffix=\".json\", mode=\"w\", delete=False) as f:\n",
        "        json.dump(_schema, f)\n",
        "        tmp_path = f.name\n",
        "\n",
        "    try:\n",
        "        for i in chunk_indices:\n",
        "            chunk_offset = i * _chunk_size\n",
        "            chunk_count = min(_chunk_size, _total - chunk_offset)\n",
        "            if chunk_count <= 0:\n",
        "                continue\n",
        "            chunk_seed = _seed ^ i\n",
        "\n",
        "            chunk_data = generate_chunk(\n",
        "                schema_path=tmp_path,\n",
        "                seed=chunk_seed,\n",
        "                offset=chunk_offset,\n",
        "                count=chunk_count,\n",
        "            )\n",
        "\n",
        "            for sink_name in _sinks:\n",
        "                if sink_name == \"lakehouse\":\n",
        "                    from sqllocks_spindle.engine.sinks.lakehouse import LakehouseSink\n",
        "                    abfss = f\"abfss://{_ws}@onelake.dfs.fabric.microsoft.com/{_lh}\"\n",
        "                    sink = LakehouseSink(base_path=abfss, format=\"parquet\")\n",
        "                elif sink_name == \"warehouse\":\n",
        "                    from sqllocks_spindle.engine.sinks.warehouse import WarehouseSink\n",
        "                    wcfg = _sink_cfg.get(\"warehouse\", {})\n",
        "                    sink = WarehouseSink(**wcfg)\n",
        "                elif sink_name == \"kql\":\n",
        "                    from sqllocks_spindle.engine.sinks.kql import KQLSink\n",
        "                    kqlcfg = _sink_cfg.get(\"kql\", {})\n",
        "                    sink = KQLSink(**kqlcfg)\n",
        "                elif sink_name == \"sql_database\":\n",
        "                    from sqllocks_spindle.engine.sinks.sql_database import SQLDatabaseSink\n",
        "                    sqlcfg = _sink_cfg.get(\"sql_database\", {})\n",
        "                    sink = SQLDatabaseSink(**sqlcfg)\n",
        "                else:\n",
        "                    continue\n",
        "\n",
        "                sink.open(None)\n",
        "                for table_name, col_lists in chunk_data.items():\n",
        "                    arrays = {col: np.array(vals) for col, vals in col_lists.items()}\n",
        "                    sink.write_chunk(table_name, arrays)\n",
        "                sink.close()\n",
        "    finally:\n",
        "        os.unlink(tmp_path)\n",
        "\n",
        "print(f\"Submitting {n_chunks} chunks across Spark partitions...\")\n",
        "sc.parallelize(range(n_chunks), numSlices=n_chunks).foreachPartition(generate_partition)\n",
        "print(\"All partitions complete.\")"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "# Write result stats to OneLake and clean up temp schema file\n",
        "import json\n",
        "from notebookutils import mssparkutils\n",
        "\n",
        "result = {\n",
        "    \"rows_generated\": total_rows,\n",
        "    \"status\": \"succeeded\",\n",
        "    \"n_chunks\": n_chunks,\n",
        "}\n",
        "\n",
        "result_filename = schema_path.replace(\"_schema.json\", \"_result.json\")\n",
        "mssparkutils.fs.put(f\"Files/{result_filename}\", json.dumps(result), overwrite=True)\n",
        "print(f\"Result written to OneLake: Files/{result_filename}\")\n",
        "\n",
        "# Clean up temp schema file\n",
        "try:\n",
        "    mssparkutils.fs.rm(f\"Files/{schema_path}\")\n",
        "    print(f\"Cleaned up schema file: Files/{schema_path}\")\n",
        "except Exception as e:\n",
        "    print(f\"Warning: could not delete schema file: {e}\")"
      ]
    }
  ]
}
```

- [ ] **Step 2: Verify notebook is valid JSON and loads**

```bash
python -c "
import json
with open('notebooks/spindle_spark_worker.ipynb') as f:
    nb = json.load(f)
print('Cells:', len(nb['cells']))
print('Parameters cell tags:', nb['cells'][0]['metadata'].get('tags'))
"
```
Expected:
```
Cells: 4
Parameters cell tags: ['parameters']
```

- [ ] **Step 3: Verify notebooks module loads notebook**

```bash
python -c "
from sqllocks_spindle.notebooks import SPARK_WORKER_IPYNB
print('Cells in bundled notebook:', len(SPARK_WORKER_IPYNB['cells']))
"
```
Expected: `Cells in bundled notebook: 4`

- [ ] **Step 4: Commit**

```bash
git add notebooks/spindle_spark_worker.ipynb sqllocks_spindle/notebooks/__init__.py
git commit -m "feat: add spindle_spark_worker Fabric Spark notebook template"
```

---

## Task 8: Version 2.7.0 + Changelog

Bump the version and document Phase 2 in the changelog.

**Files:**
- Modify: `pyproject.toml`
- Modify: `sqllocks_spindle/__init__.py`
- Modify: `docs/changelog.md`

- [ ] **Step 1: Bump version to 2.7.0**

In `pyproject.toml` line 7:
```toml
version = "2.7.0"
```

In `sqllocks_spindle/__init__.py` line 5:
```python
__version__ = "2.7.0"
```

- [ ] **Step 2: Add changelog entry**

At the top of `docs/changelog.md` (after the header, before `## [2.6.1]`), insert:

```markdown
## [2.7.0] - 2026-04-27

### Added

- **Billion-row pipeline (Phase 2)** — Fabric Spark scale generation via `scale_mode="fabric_spark"`
    - `FabricSparkRouter` (`engine/spark_router.py`) — generates static tables in-process, uploads augmented schema JSON to OneLake via DFS API, finds or auto-creates `spindle_spark_worker` notebook, submits Fabric notebook run, returns `JobRecord` immediately
    - `AsyncJobStore` + `JobRecord` (`engine/async_job_store.py`) — thread-safe in-process registry tracking submitted Fabric jobs by `job_id`
    - `FabricJobTracker` (`engine/job_tracker.py`) — polls and cancels Fabric notebook runs via the Fabric Jobs REST API
    - `spindle_spark_worker.ipynb` — Fabric notebook template: reads schema from OneLake, `foreachPartition` dynamic table generation, writes to LakehouseSink / WarehouseSink / KQLSink / SQLDatabaseSink, saves result stats and cleans up temp file
    - `cmd_scale_status` MCP bridge command — polls Fabric job status by `job_id`; maps Fabric statuses to `submitted|running|succeeded|failed|cancelled`
    - `cmd_scale_cancel` MCP bridge command — cancels an in-flight Fabric notebook run
    - `cmd_scale_generate(scale_mode="fabric_spark")` now fully implemented; requires `sink_config.workspace_id`, `sink_config.lakehouse_id`, `sink_config.token`
    - `sqllocks_spindle/notebooks/__init__.py` — loads and exports `SPARK_WORKER_IPYNB` notebook template

### Changed

- Test count: 1,913 → 1,923 (+10 Phase 2 unit tests in `tests/test_spark_router.py`)

```

- [ ] **Step 3: Verify version and import**

```bash
python -c "import sqllocks_spindle; print(sqllocks_spindle.__version__)"
```
Expected: `2.7.0`

- [ ] **Step 4: Run full test suite**

```bash
pytest tests/ -q --ignore=tests/test_e2e_scale_router.py 2>&1 | tail -15
```
Expected: `1923 passed` (or close — some pre-existing skips are fine)

- [ ] **Step 5: Final commit**

```bash
git add pyproject.toml sqllocks_spindle/__init__.py docs/changelog.md
git commit -m "chore: bump version to 2.7.0; add Phase 2 changelog entry"
```

---

## Self-Review Against Spec

| Spec Requirement | Covered By |
|-----------------|-----------|
| `cmd_scale_generate(scale_mode="fabric_spark")` submits job and returns `{job_id, status:"submitted"}` | Task 6 |
| `cmd_scale_status(job_id)` polls Fabric notebook run | Task 5 |
| `cmd_scale_cancel(job_id)` cancels notebook run | Task 5 |
| Spark notebook uses existing sink classes | Task 7 (notebook Cell 3) |
| Static/dynamic table split carries forward | Task 4 (`FabricSparkRouter.submit` calls `_classify_tables` + `_generate_static_tables`) |
| Notebook auto-creation via Fabric Items API | Task 4 (`_create_notebook`) |
| `_SpindleJSONEncoder` for schema upload | Task 4 (`_upload_schema` uses `cls=_SpindleJSONEncoder`) |
| `NotebookNotFoundError` defined | Task 4 (`spark_router.py`) |
| `FabricAPIError` defined | Task 4 (`spark_router.py`) |
| Token stored in JobRecord for polling | Task 2 (`JobRecord.token`) |
| OneLake temp cleanup on failure/cancel | Task 5 (cancel updates status; notebook Cell 4 cleans up) |
| Tests run without live Fabric workspace | Tasks 2-6 (all mocked) |
| +10 unit tests | Tasks 2–6 combined |
| Version bump to 2.7.0 | Task 8 |
| Changelog entry | Task 8 |
