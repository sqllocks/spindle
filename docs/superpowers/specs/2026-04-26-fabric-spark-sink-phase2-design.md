# Spindle Phase 2 — Fabric Spark Scale Generation

**Date:** 2026-04-26  
**Version target:** 2.7.0  
**Status:** Approved for implementation

---

## Problem

Phase 1 (v2.6.x) delivers multi-process chunk generation via `ProcessPoolExecutor` running
locally (the `local_mp` scale mode). For datasets beyond what a single developer machine can
generate in reasonable time — and for Fabric demo environments where the data should be generated
_inside_ the Fabric workspace rather than uploaded from outside — we need a Fabric-native scale mode.

The `fabric_spark` scale mode is already stubbed in `cmd_scale_generate` (returns `{"error": "not_implemented"}`).
Phase 2 implements it end-to-end.

---

## Goals

1. `cmd_scale_generate(scale_mode="fabric_spark", ...)` submits a generation job to a Fabric
   Spark notebook and returns immediately with `{job_id, status: "submitted"}`.
2. `cmd_scale_status(job_id)` polls the Fabric notebook run for completion/failure.
3. `cmd_scale_cancel(job_id)` cancels an in-flight notebook run.
4. The Spark notebook uses the existing sink classes (LakehouseSink, WarehouseSink, KQLSink,
   SQLDatabaseSink) — no new Spark-specific writers.
5. Static/dynamic table split (GAP 1 architecture) carries forward: static tables are generated
   once in the driver, broadcast to all partitions; dynamic tables use `foreachPartition`.
6. Tests exercise the full path using a `MemorySink`-compatible mock that doesn't require Spark.

---

## Non-Goals

- No new Spark-native Delta writer (existing LakehouseSink via `deltalake` covers this).
- No Spark DataFrame output to the caller — the sinks consume data directly inside the cluster.
- No PySpark dependency in the base Spindle package (deferred import pattern, same as KQLSink).

---

## Architecture

### Component Overview

```
MCP Bridge (cmd_scale_generate, scale_mode="fabric_spark")
    │
    ▼
FabricSparkRouter                        # new: engine/spark_router.py
    │
    ├─ Step 1: Generate static tables    # reuses _generate_static_tables from scale_router.py
    │          in main process
    │
    ├─ Step 2: Serialize schema + static # augmented schema JSON (same as ScaleRouter)
    │          PK data to OneLake temp   # uploaded via OneLake Files API
    │
    ├─ Step 3: Submit notebook run       # Fabric REST API: POST /jobs/instances
    │          (spindle_spark_worker)    # passes schema path + params as notebook parameters
    │
    └─ Returns {job_id, status: "submitted", schema_temp_path}

AsyncJobStore                            # new: engine/async_job_store.py
    └─ In-memory dict: job_id → JobRecord
       (job_id, submitted_at, fabric_run_id, schema_temp_path, status)

cmd_scale_status(job_id)
    └─ FabricJobTracker.get_status(fabric_run_id)
       → Fabric REST: GET /jobs/instances/{runId}
       → returns {status, progress_pct, rows_generated, error}

cmd_scale_cancel(job_id)
    └─ FabricJobTracker.cancel(fabric_run_id)
       → Fabric REST: POST /jobs/instances/{runId}/cancel
```

### Fabric Notebook: `spindle_spark_worker`

Pre-built notebook stored in the Fabric workspace (created once during setup, or auto-created
by FabricSparkRouter if missing). Receives parameters via Fabric notebook parameters API.

**Notebook parameters:**
```python
schema_path = ""       # OneLake path to augmented schema JSON
chunk_size  = 500_000
seed        = 42
total_rows  = 1_000_000
sinks_json  = "[]"     # JSON list of {type, config} dicts
```

**Notebook execution flow:**
```
Cell 1: pip install sqllocks-spindle[fabric] (if not pre-installed)
Cell 2: Load schema JSON from OneLake
Cell 3: Generate static tables (driver) — same logic as _generate_static_tables
Cell 4: Distribute dynamic generation across partitions via foreachPartition
Cell 5: Write rows_generated + stats to a result JSON in OneLake
Cell 6: Cleanup temp schema file
```

### foreachPartition Design

```python
n_chunks = ceil(total_rows / chunk_size)
partition_rdd = sc.parallelize(range(n_chunks), numSlices=n_chunks)

def generate_partition(chunk_indices):
    for i in chunk_indices:
        chunk_data = generate_chunk(
            schema_path, seed ^ i, i * chunk_size, min(chunk_size, total_rows - i*chunk_size)
        )
        for table_name, col_lists in chunk_data.items():
            sink.write_chunk(table_name, {col: np.array(v) for col, v in col_lists.items()})

partition_rdd.foreachPartition(generate_partition)
```

Each partition initializes its own sinks, generates one chunk, writes, and closes. The sink
classes (LakehouseSink, WarehouseSink, etc.) already handle concurrent writes correctly because
each is initialized fresh per partition (no shared state).

### Static Table Broadcast

Static tables are generated in the Spark driver cell before the `foreachPartition` call.
Their PK data is already embedded in the augmented schema JSON (same format as Phase 1).
The schema JSON is uploaded to OneLake before the notebook run and read by each partition worker.

---

## New Files

| File | Purpose |
|------|---------|
| `sqllocks_spindle/engine/spark_router.py` | `FabricSparkRouter` — submits job, returns job_id |
| `sqllocks_spindle/engine/async_job_store.py` | `AsyncJobStore`, `JobRecord` — in-process job registry |
| `sqllocks_spindle/engine/job_tracker.py` | `FabricJobTracker` — polls/cancels Fabric notebook runs |
| `notebooks/spindle_spark_worker.ipynb` | Pre-built Fabric notebook template |
| `tests/test_spark_router.py` | Unit tests (no Spark required — mock notebook submission) |

---

## MCP Bridge Changes

### Modified: `cmd_scale_generate`

```python
elif scale_mode == "fabric_spark":
    from sqllocks_spindle.engine.spark_router import FabricSparkRouter
    router = FabricSparkRouter(
        workspace_id=sink_config.get("workspace_id", ""),
        notebook_name=sink_config.get("notebook_name", "spindle_spark_worker"),
        token=sink_config.get("token", ""),
        sinks=sinks_list,
        sink_config=sink_config,
        chunk_size=chunk_size,
    )
    job = router.submit(schema_dict, total_rows=total_rows, seed=seed)
    _job_store.put(job)
    return {
        "job_id": job.job_id,
        "fabric_run_id": job.fabric_run_id,
        "status": "submitted",
        "schema_temp_path": job.schema_temp_path,
    }
```

### New: `cmd_scale_status`

```python
def cmd_scale_status(params):
    job_id = params["job_id"]
    job = _job_store.get(job_id)
    tracker = FabricJobTracker(token=job.token)
    return tracker.get_status(job.fabric_run_id)
    # → {status: "running|succeeded|failed|cancelled", progress_pct, rows_generated, error}
```

### New: `cmd_scale_cancel`

```python
def cmd_scale_cancel(params):
    job_id = params["job_id"]
    job = _job_store.get(job_id)
    tracker = FabricJobTracker(token=job.token)
    return tracker.cancel(job.fabric_run_id)
    # → {cancelled: true, fabric_run_id}
```

---

## Fabric REST API Calls

All via `requests` (already in dependencies). Entra token from `sink_config["token"]`
(same pattern as existing WarehouseSink/LakehouseSink).

| Operation | Endpoint |
|-----------|---------|
| Submit notebook run | `POST /v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances?jobType=RunNotebook` |
| Poll status | `GET /v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}` |
| Cancel | `POST /v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}/cancel` |
| Upload schema to OneLake | `PUT https://onelake.dfs.fabric.microsoft.com/{workspace}/{lakehouse}/Files/{path}` |

Reference: [Fabric REST Jobs API](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler)

---

## Error Handling

| Scenario | Behavior |
|----------|---------|
| Notebook not found in workspace | `FabricSparkRouter.submit` raises `NotebookNotFoundError` with setup instructions |
| OneLake upload fails | raises `SinkError` before submission |
| Fabric API returns 4xx | raises `FabricAPIError(status_code, message)` |
| `cmd_scale_status` called with unknown `job_id` | returns `{error: "job_not_found"}` |
| Notebook run fails inside Spark | `get_status` returns `{status: "failed", error: "<notebook error>"}` |
| Partial partition failures | Notebook captures per-partition errors; final status is `failed` if any partition failed |

---

## Testing Strategy

Tests run without a live Fabric workspace. `FabricJobTracker` and OneLake upload are mocked.

```
tests/test_spark_router.py
├── test_submit_returns_job_id          # mock HTTP, verify job_id returned
├── test_schema_uploaded_to_onelake     # verify augmented schema JSON written before submit
├── test_static_tables_in_schema        # verify static_pk_data embedded in uploaded schema
├── test_status_polls_fabric_api        # mock GET jobs/instances, verify status mapping
├── test_cancel_calls_fabric_cancel     # mock POST cancel, verify response
├── test_notebook_not_found_raises      # 404 on submit → NotebookNotFoundError
└── test_job_store_get_unknown_id       # returns None, bridge returns error dict
```

Integration test (marked `@pytest.mark.live` — skipped in CI):
```
tests/test_spark_e2e_live.py
└── test_spark_generate_retail_small    # real Fabric workspace, real notebook submission
```

---

## Test Count Impact

Expected: +10 unit tests → 1,923 total (from 1,913).

---

## Changelog Entry

```markdown
## [2.7.0] - 2026-04-27

### Added

- **Billion-row pipeline (Phase 2)** — Fabric Spark scale generation
    - `FabricSparkRouter` — submits generation job as Fabric notebook run; returns job_id immediately
    - `AsyncJobStore` + `JobRecord` — in-process registry of submitted Fabric jobs
    - `FabricJobTracker` — polls and cancels Fabric notebook runs via REST API
    - `spindle_spark_worker` notebook template — foreachPartition generation with all 4 sink types
    - `cmd_scale_status` MCP bridge command — poll Fabric job status by job_id
    - `cmd_scale_cancel` MCP bridge command — cancel in-flight Fabric job
    - `cmd_scale_generate(scale_mode="fabric_spark")` now fully implemented
```

---

## Open Questions / Risks

1. **Notebook pre-creation**: Does the user need to manually upload `spindle_spark_worker.ipynb`
   to their workspace, or does FabricSparkRouter auto-create it via the Fabric Items API?
   **Decision**: FabricSparkRouter checks first (GET item by name); auto-creates if missing.
   Auto-creation uses the Fabric REST Items API (`POST /v1/workspaces/{id}/notebooks`).

2. **Token lifetime**: Notebook runs can take 10+ minutes for billion-row jobs. Entra tokens
   expire in 1 hour by default. `FabricJobTracker` should refresh the token before each poll.
   **Decision**: Accept the caller's token for now; document 1-hour limit. Token refresh is a
   future enhancement.

3. **OneLake temp file cleanup**: Schema JSON is uploaded to OneLake before the notebook run.
   The notebook deletes it in Cell 6. If the notebook fails before Cell 6, the temp file leaks.
   **Decision**: `cmd_scale_cancel` and `cmd_scale_status(status="failed")` both attempt cleanup.

4. **Sink concurrency in Spark**: Multiple partitions write to the same Lakehouse simultaneously.
   Delta handles concurrent writes via transaction log. Warehouse COPY INTO is partition-keyed.
   KQL ingestion is append-safe. SQL Database uses per-connection transactions.
   **Decision**: No locking needed at the Spindle level — sink implementations are already safe.
