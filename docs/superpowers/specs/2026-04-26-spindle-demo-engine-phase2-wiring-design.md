# Spindle Demo Engine — Phase 2 Wiring + MCP Commands

**Date:** 2026-04-26
**Version target:** 2.7.1
**Status:** Approved for implementation

---

## Problem

The Spindle Demo Engine (built 2026-04-24) has a complete scaffold — `ScenarioCatalog`,
`DemoOrchestrator`, three demo modes, `CleanupEngine`, `DemoManifest`, and 8 CLI commands.
21 unit tests pass.

But `SeedingDemoMode.run()` has a stub for the actual Fabric writes: it generates data with
`Spindle().generate()` (single-process, no scale), records artifacts in the manifest, and
returns success without ever writing to a Fabric target. The Phase 2 `FabricSparkRouter`
is not wired in anywhere.

This spec completes the seeding mode by:
1. Wiring the Phase 2 `FabricSparkRouter` for large-scale generation
2. Performing real sink fan-out (lakehouse + warehouse + sql_db + eventhouse) via the
   existing `SinkRegistry`
3. Adding MCP bridge commands so a demo can be driven through Claude conversation
   (no CLI required during a client meeting)

---

## Goals

1. `SeedingDemoMode.run()` writes to every configured Fabric target via existing sink classes.
2. New `--scale-mode` flag (`auto` | `local` | `spark`) on `spindle demo run`. `auto`
   chooses Spark when a connection profile is set AND `rows >= 500_000`; otherwise local.
3. Three new MCP bridge commands: `cmd_demo_run`, `cmd_demo_status`, `cmd_demo_cleanup`.
4. `DemoManifest` records `fabric_run_id`, `scale_mode`, `workspace_id`, and
   `notebook_item_id` so async Spark runs can be polled and cleaned up later.
5. Tests run without a live Fabric workspace — all HTTP mocked.

---

## Non-Goals

- No new sink types (LakehouseSink/WarehouseSink/KQLSink/SQLDatabaseSink already exist).
- No new scenarios (retail/healthcare/enterprise/adventureworks already in catalog).
- No changes to `InferenceDemoMode` or `StreamingDemoMode`.
- No live Fabric end-to-end test in this phase (deferred to a separate validation pass).

---

## Architecture

### Component Overview

```
spindle demo run retail --scale-mode auto --rows 10000000 --connection prod
    │
    ▼
DemoOrchestrator
    └─ SeedingDemoMode.run()
        ├─ Resolve scale_mode (auto → local|spark based on connection + row count)
        ├─ _build_sinks(connection_profile) → list[Sink]
        │     ├─ lakehouse_id   → LakehouseSink
        │     ├─ warehouse_conn → WarehouseSink
        │     ├─ sql_db_conn    → SQLDatabaseSink
        │     └─ eventhouse_uri → KQLSink
        │
        ├─ scale_mode == "local":
        │     ScaleRouter(schema_path, sinks, chunk_size).run(total_rows, seed)
        │     ← blocks, returns stats {rows_generated, elapsed_seconds, ...}
        │
        └─ scale_mode == "spark":
              FabricSparkRouter(workspace_id, lakehouse_id, token, sinks_list,
                                sink_config, chunk_size).submit(schema_dict, total_rows, seed)
              ← async, returns JobRecord
              _job_store.put(job)
              manifest.fabric_run_id = job.fabric_run_id

cmd_demo_run(params)        → blocks for local | returns job_id for spark
cmd_demo_status(session_id) → reads manifest + (if spark) polls FabricJobTracker
cmd_demo_cleanup(session_id) → CleanupEngine.cleanup(manifest)
```

### Scale Mode Resolution

```python
def _resolve_scale_mode(requested: str, conn_profile, rows: int) -> str:
    if requested == "local":
        return "local"
    if requested == "spark":
        if conn_profile is None:
            raise ValueError("Spark mode requires a connection profile")
        if not conn_profile.lakehouse_id:
            raise ValueError("Spark mode requires lakehouse_id in connection profile")
        return "spark"
    # auto
    if conn_profile is not None and conn_profile.lakehouse_id and rows >= 500_000:
        return "spark"
    return "local"
```

### Sink Construction

```python
def _build_sinks(conn) -> tuple[list[Sink], dict]:
    """Build sink instances + sink_config dict from a ConnectionProfile."""
    sinks: list[Sink] = []
    sinks_list: list[dict] = []  # for FabricSparkRouter
    sink_config: dict = {
        "workspace_id": conn.workspace_id,
        "lakehouse_id": conn.lakehouse_id,
        "token": conn.token,
    }
    if conn.lakehouse_id:
        sinks.append(LakehouseSink(workspace_id=conn.workspace_id,
                                   lakehouse_id=conn.lakehouse_id,
                                   token=conn.token))
        sinks_list.append({"type": "lakehouse", "config": {...}})
    if conn.warehouse_conn_str:
        sinks.append(WarehouseSink(...))
        sinks_list.append({"type": "warehouse", "config": {...}})
    if conn.sql_db_conn_str:
        sinks.append(SQLDatabaseSink(...))
        sinks_list.append({"type": "sql_db", "config": {...}})
    if conn.eventhouse_uri:
        sinks.append(KQLSink(...))
        sinks_list.append({"type": "kql", "config": {...}})
    return sinks, sinks_list, sink_config
```

A sink whose construction fails (missing dependency, bad credentials) is logged as a
warning and skipped; the manifest records skipped sinks. Other sinks proceed.

---

## Components

### Modified: `sqllocks_spindle/demo/params.py`

Add field to `DemoParams`:
```python
scale_mode: Literal["auto", "local", "spark"] = "auto"
```

### Modified: `sqllocks_spindle/demo/manifest.py`

Add fields to `DemoManifest`:
```python
fabric_run_id: Optional[str] = None
scale_mode: Optional[str] = None
workspace_id: Optional[str] = None
notebook_item_id: Optional[str] = None
```

Update `_to_dict` / `from_dict` to round-trip the new fields.

### Modified: `sqllocks_spindle/demo/modes/seeding.py`

Rewrite the write step in `run()`:
- Resolve `scale_mode` via `_resolve_scale_mode`
- Build sinks via `_build_sinks` (gracefully skip failures)
- Branch on resolved mode:
  - `local`: serialize the schema dict to a temp file, instantiate `ScaleRouter`,
    call `.run(total_rows, seed)`, record per-table row counts in manifest, delete temp file
  - `spark`: instantiate `FabricSparkRouter`, call `.submit`, store `JobRecord` in
    the shared `_job_store`, record `fabric_run_id` + `notebook_item_id` + `workspace_id`
    in manifest

Manifest is saved either way. Local path returns `{success, session_id, stats}`.
Spark path returns `{success, session_id, fabric_run_id, status: "submitted"}`.

### Modified: `sqllocks_spindle/cli.py`

Add `--scale-mode` flag to `spindle demo run`:
```python
@demo.command("run")
@click.option("--scale-mode", type=click.Choice(["auto", "local", "spark"]), default="auto")
...
```

Pass the value into `DemoParams.scale_mode`.

### Modified: `sqllocks_spindle/mcp_bridge.py`

Three new commands:

```python
def cmd_demo_run(params: dict) -> dict:
    from sqllocks_spindle.demo.params import DemoParams
    from sqllocks_spindle.demo.orchestrator import DemoOrchestrator
    p = DemoParams(**{k: v for k, v in params.items() if k in DemoParams.__annotations__})
    return DemoOrchestrator().run(p)

def cmd_demo_status(params: dict) -> dict:
    from sqllocks_spindle.demo.manifest import DemoManifest
    session_id = params["session_id"]
    try:
        manifest = DemoManifest.load(session_id)
    except FileNotFoundError:
        return {"error": "session_not_found", "session_id": session_id}
    result = {"session_id": session_id, "manifest": manifest.to_dict()}
    if manifest.fabric_run_id:
        tracker = FabricJobTracker(token=params["token"])
        result["fabric"] = tracker.get_status(
            manifest.workspace_id, manifest.notebook_item_id, manifest.fabric_run_id,
        )
    return result

def cmd_demo_cleanup(params: dict) -> dict:
    from sqllocks_spindle.demo.manifest import DemoManifest
    from sqllocks_spindle.demo.cleanup import CleanupEngine
    session_id = params["session_id"]
    try:
        manifest = DemoManifest.load(session_id)
    except FileNotFoundError:
        return {"error": "session_not_found", "session_id": session_id}
    return CleanupEngine().cleanup(manifest, dry_run=params.get("dry_run", False))
```

Register in `COMMANDS` dict: `"demo_run"`, `"demo_status"`, `"demo_cleanup"`.

---

## Error Handling

| Scenario | Behavior |
|----------|---------|
| `scale_mode="spark"` with no connection profile | `ValueError("Spark mode requires a connection profile")` |
| `scale_mode="spark"` with no `lakehouse_id` (needed for OneLake schema upload) | `ValueError("Spark mode requires lakehouse_id in connection profile")` |
| Sink construction fails (missing creds, missing optional dep) | Log warning, skip that sink, manifest records skipped sinks; other sinks proceed |
| `cmd_demo_status` with unknown `session_id` | `{error: "session_not_found", session_id}` |
| Local `ScaleRouter` raises | `SeedingDemoMode` catches, manifest finished with `success=False` and error message, returns `{success: False, error}` |
| Spark submit raises (4xx, 5xx) | `SeedingDemoMode` catches, manifest never gets `fabric_run_id`, returns `{success: False, error}` |
| Cleanup called on still-running Spark job | Cleanup returns `{error: "job_still_running", fabric_run_id}`; user must call `cmd_scale_cancel` first |

---

## Testing

All tests use HTTP mocks; no live Fabric workspace required.

```
tests/test_demo_seeding_v2.py        (9 tests)
├── test_scale_mode_auto_picks_local_under_threshold
├── test_scale_mode_auto_picks_spark_with_connection_and_large_rows
├── test_scale_mode_auto_picks_local_when_no_connection
├── test_scale_mode_explicit_spark_without_connection_raises
├── test_scale_mode_explicit_spark_without_lakehouse_id_raises
├── test_local_path_invokes_scale_router_with_sinks
├── test_spark_path_invokes_fabric_spark_router
├── test_spark_path_records_fabric_run_id_in_manifest
└── test_build_sinks_skips_missing_targets

tests/test_demo_mcp_commands.py      (6 tests)
├── test_cmd_demo_run_local_returns_stats
├── test_cmd_demo_run_spark_returns_job_id
├── test_cmd_demo_status_local_reads_manifest
├── test_cmd_demo_status_spark_polls_fabric_tracker
├── test_cmd_demo_cleanup_invokes_cleanup_engine
└── test_cmd_demo_status_unknown_session_returns_error
```

---

## Test Count Impact

1,930 → 1,945 (+15 new tests).

---

## Files Changed

| File | Change |
|------|--------|
| `sqllocks_spindle/demo/params.py` | Add `scale_mode` field |
| `sqllocks_spindle/demo/manifest.py` | Add `fabric_run_id`, `scale_mode`, `workspace_id`, `notebook_item_id` |
| `sqllocks_spindle/demo/modes/seeding.py` | Rewrite write step — sink fan-out + scale routing |
| `sqllocks_spindle/cli.py` | Add `--scale-mode` flag to `spindle demo run` |
| `sqllocks_spindle/mcp_bridge.py` | Add `cmd_demo_run`, `cmd_demo_status`, `cmd_demo_cleanup`; register in `COMMANDS` |
| `tests/test_demo_seeding_v2.py` | NEW — 9 unit tests |
| `tests/test_demo_mcp_commands.py` | NEW — 6 unit tests |
| `docs/changelog.md` | v2.7.1 entry |
| `sqllocks_spindle/__init__.py` | 2.7.0 → 2.7.1 |
| `pyproject.toml` | 2.7.0 → 2.7.1 |

---

## Changelog Entry

```markdown
## [2.7.1] - 2026-04-27

### Changed

- **Demo Engine — Phase 2 wiring**: `SeedingDemoMode` now performs real Fabric sink
  writes via `SinkRegistry` fan-out, replacing the previous manifest-only stub.
- New `--scale-mode {auto,local,spark}` flag on `spindle demo run`. `auto` selects
  `spark` when a connection profile is configured and `rows >= 500_000`; otherwise `local`.
- `DemoManifest` now records `fabric_run_id`, `scale_mode`, `workspace_id`, and
  `notebook_item_id` so Spark runs can be polled and cleaned up by `session_id`.

### Added

- `cmd_demo_run` MCP bridge command — runs a demo through `DemoOrchestrator`; blocks
  for local mode, returns immediately with `fabric_run_id` for Spark mode
- `cmd_demo_status` MCP bridge command — reads the manifest by `session_id` and, when
  the run was a Spark submission, polls `FabricJobTracker` for live status
- `cmd_demo_cleanup` MCP bridge command — runs `CleanupEngine` against a saved manifest

### Test count

1,930 → 1,945 (+15 new tests in `tests/test_demo_seeding_v2.py` and
`tests/test_demo_mcp_commands.py`)
```

---

## Open Questions / Risks

1. **`ConnectionProfile.workspace_id` and `.token` fields**: the existing
   `ConnectionProfile` dataclass may not have these fields by name. Implementation must
   verify and add them if missing (deferred to plan).
   **Decision**: plan task 1 reads `connections.py` and adds any missing fields.

2. **Local-mode schema serialization**: `ScaleRouter` requires a JSON file path.
   `SeedingDemoMode` currently builds the schema in-memory from a `Domain` instance.
   The plan must serialize it to a temp file (same pattern as the existing
   `tests/test_e2e_scale_router.py`) and clean up in a finally block.
   **Decision**: use `tempfile.NamedTemporaryFile(suffix=".json", delete=False)`,
   `json.dump(schema_dict, ...)`, finally `os.unlink(path)`.

3. **Module-level `_job_store` shared between bridge and demo module**: The Phase 2
   code keeps `_job_store` as a module-level global in `mcp_bridge.py`. The demo
   module needs to write into it.
   **Decision**: import `_job_store` from `mcp_bridge` in `seeding.py`. If that creates
   an import cycle, move `_job_store` to `engine/async_job_store.py` as a module-level
   singleton and import it from there in both `mcp_bridge` and `seeding`.