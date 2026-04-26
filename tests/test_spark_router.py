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
    """Return (get_mock, post_mock, put_mock, patch_mock) for a happy-path submit."""
    if notebook_list_items is None:
        notebook_list_items = [{"id": notebook_id, "displayName": "spindle_spark_worker"}]

    get_mock = MagicMock()
    get_mock.return_value.status_code = 200
    get_mock.return_value.json.return_value = {"value": notebook_list_items}
    get_mock.return_value.raise_for_status = MagicMock()

    post_mock = MagicMock()
    post_mock.return_value.status_code = submit_status
    post_mock.return_value.headers = {
        "Location": f"https://api.fabric.microsoft.com/v1/runs/{notebook_id}/jobs/instances/run-001"
    }
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
    import sqllocks_spindle.engine.spark_router as sr_mod

    get_m, post_m, put_m, patch_m = _mock_http_for_submit()
    schema = _minimal_schema_dict_dynamic()

    with patch.object(sr_mod.requests, "get", get_m), \
         patch.object(sr_mod.requests, "post", post_m), \
         patch.object(sr_mod.requests, "put", put_m), \
         patch.object(sr_mod.requests, "patch", patch_m):
        router = _make_router()
        router.submit(schema, total_rows=1000, seed=42)

    assert put_m.call_count == 1
    assert patch_m.call_count == 2
    put_url = put_m.call_args[0][0]
    assert "onelake.dfs.fabric.microsoft.com" in put_url
    assert "ws-123" in put_url
    assert "lh-456" in put_url
    assert "spindle_temp/" in put_url


def test_submit_embeds_schema_counts_in_upload():
    """Uploaded schema JSON contains _schema_counts and _base_seed keys."""
    import sqllocks_spindle.engine.spark_router as sr_mod

    get_m, post_m, put_m, patch_m = _mock_http_for_submit()
    schema = _minimal_schema_dict_dynamic()
    captured_body = []

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
    import sqllocks_spindle.engine.spark_router as sr_mod

    get_m, post_m, put_m, patch_m = _mock_http_for_submit(notebook_list_items=[])
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
    first_call_url = post_m.call_args_list[0][0][0]
    assert "/items" in first_call_url


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
    import sqllocks_spindle.mcp_bridge as bridge_mod
    from sqllocks_spindle.engine.async_job_store import JobRecord

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
    import sqllocks_spindle.mcp_bridge as bridge_mod
    from sqllocks_spindle.engine.async_job_store import JobRecord

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
