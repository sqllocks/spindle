"""Unit tests for FabricSparkRouter, AsyncJobStore, and FabricJobTracker.

All tests mock HTTP calls — no live Fabric workspace required.
"""
from __future__ import annotations

import json
import threading
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
