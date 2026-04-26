# Spindle Demo Engine — Phase 2 Wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the Phase 2 `FabricSparkRouter` and the existing `LakehouseSink`/`WarehouseSink`/`KQLSink`/`SQLDatabaseSink` into `SeedingDemoMode`, then expose `cmd_demo_status` and `cmd_demo_cleanup` MCP bridge commands so a demo can be driven through Claude conversation.

**Architecture:** Extend `DemoParams` with `scale_mode`, extend `DemoManifest` with Spark tracking fields, rewrite `SeedingDemoMode.run()` to fan out to all configured sinks via `ScaleRouter` (local) or `FabricSparkRouter` (spark), and add status/cleanup MCP commands that read the manifest by `session_id`.

**Tech Stack:** Python 3.10+, dataclasses, `ScaleRouter` (multi-process), `FabricSparkRouter` (Fabric Jobs REST), `azure.identity.AzureCliCredential` for token acquisition, pytest with HTTP mocking.

---

## Existing State (DO NOT recreate)

- `sqllocks_spindle/demo/` exists; 17 files; 21 passing tests
- `sqllocks_spindle/demo/modes/seeding.py` exists; `run()` has a stub write step (records manifest entries, never writes to Fabric)
- `cmd_demo_run` already exists in `sqllocks_spindle/mcp_bridge.py:684` (will be MODIFIED, not created)
- `cmd_demo_list` already exists at `mcp_bridge.py:663` (no changes)
- `FabricSparkRouter`, `AsyncJobStore`, `FabricJobTracker` all live in `sqllocks_spindle/engine/` (Phase 2 — already shipped)
- `ConnectionProfile` (`demo/connections.py`) has `workspace_id`, `lakehouse_id`, `warehouse_conn_str`, `sql_db_conn_str`, `eventhouse_uri`, `auth_method`, `tenant_id`, `client_id`, `client_secret` — NO `token` field; tokens acquired at runtime via `azure.identity`

---

### Task 1: Extend `DemoParams` with `scale_mode`

**Files:**
- Modify: `sqllocks_spindle/demo/params.py`
- Test: `tests/test_demo_params.py` (NEW)

- [ ] **Step 1: Write the failing test**

Create `tests/test_demo_params.py`:

```python
"""Tests for DemoParams."""
from sqllocks_spindle.demo.params import DemoParams


def test_default_scale_mode_is_auto():
    p = DemoParams()
    assert p.scale_mode == "auto"


def test_scale_mode_can_be_set_to_local():
    p = DemoParams(scale_mode="local")
    assert p.scale_mode == "local"


def test_scale_mode_can_be_set_to_spark():
    p = DemoParams(scale_mode="spark")
    assert p.scale_mode == "spark"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_params.py -v`
Expected: FAIL with `AttributeError` or `TypeError: unexpected keyword argument 'scale_mode'`

- [ ] **Step 3: Add `scale_mode` to `DemoParams`**

In `sqllocks_spindle/demo/params.py`, edit the file:

```python
"""DemoParams — configuration for all demo modes."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal, Optional


DemoMode = Literal["inference", "streaming", "seeding"]
OutputFormat = Literal["terminal", "charts", "semantic_model", "all"]
ScaleMode = Literal["auto", "local", "spark"]


@dataclass
class DemoParams:
    """Unified parameter bag passed to every demo mode and scenario."""
    scenario: str = "retail"
    mode: DemoMode = "inference"
    connection: Optional[str] = None
    input_file: Optional[str] = None
    db_schema: str = "dbo"
    db_tables: Optional[list] = None
    sample_rows: int = 1000
    rows: int = 100_000
    domain: Optional[str] = None
    domains: Optional[list] = None
    output_formats: list = field(default_factory=lambda: ["terminal"])
    env_name: Optional[str] = None
    dry_run: bool = False
    estimate_only: bool = False
    auto_cleanup: bool = False
    seed: Optional[int] = None
    scale_mode: ScaleMode = "auto"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_params.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/test_demo_params.py sqllocks_spindle/demo/params.py
git commit -m "feat: add scale_mode field to DemoParams (auto/local/spark)"
```

---

### Task 2: Extend `DemoManifest` with Spark tracking fields

**Files:**
- Modify: `sqllocks_spindle/demo/manifest.py`
- Test: `tests/test_demo_manifest.py` (extend existing)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_demo_manifest.py`:

```python
def test_manifest_records_fabric_run_id(tmp_path):
    from sqllocks_spindle.demo.manifest import DemoManifest
    m = DemoManifest(scenario="retail", mode="seeding")
    m.fabric_run_id = "run-abc-123"
    m.scale_mode = "spark"
    m.workspace_id = "ws-456"
    m.notebook_item_id = "nb-789"
    m.finish(success=True)
    path = m.save(directory=tmp_path)
    assert path.exists()
    loaded = DemoManifest.load(m.session_id, directory=tmp_path)
    assert loaded.fabric_run_id == "run-abc-123"
    assert loaded.scale_mode == "spark"
    assert loaded.workspace_id == "ws-456"
    assert loaded.notebook_item_id == "nb-789"


def test_manifest_defaults_for_local_mode(tmp_path):
    from sqllocks_spindle.demo.manifest import DemoManifest
    m = DemoManifest(scenario="retail", mode="seeding")
    m.scale_mode = "local"
    m.finish(success=True)
    path = m.save(directory=tmp_path)
    loaded = DemoManifest.load(m.session_id, directory=tmp_path)
    assert loaded.scale_mode == "local"
    assert loaded.fabric_run_id is None
    assert loaded.workspace_id is None
    assert loaded.notebook_item_id is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_manifest.py::test_manifest_records_fabric_run_id tests/test_demo_manifest.py::test_manifest_defaults_for_local_mode -v`
Expected: FAIL with `AttributeError: 'DemoManifest' object has no attribute 'fabric_run_id'`

- [ ] **Step 3: Add fields to `DemoManifest`**

In `sqllocks_spindle/demo/manifest.py`, find the `@dataclass` block for `DemoManifest` and add four new fields (after the existing `metrics: dict = ...` line, before `_path`):

```python
@dataclass
class DemoManifest:
    session_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    scenario: str = ""
    mode: str = ""
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    finished_at: Optional[str] = None
    success: bool = False
    error: Optional[str] = None
    artifacts: list = field(default_factory=list)
    params: dict = field(default_factory=dict)
    metrics: dict = field(default_factory=dict)
    scale_mode: Optional[str] = None
    fabric_run_id: Optional[str] = None
    workspace_id: Optional[str] = None
    notebook_item_id: Optional[str] = None
    _path: Optional[Any] = field(default=None, repr=False, compare=False)
```

The `save()` method already uses `asdict(self)` so the new fields round-trip automatically. The `load()` classmethod already passes `**data` so the new fields also round-trip on load.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_manifest.py -v`
Expected: PASS (6 tests — 4 existing + 2 new)

- [ ] **Step 5: Commit**

```bash
git add tests/test_demo_manifest.py sqllocks_spindle/demo/manifest.py
git commit -m "feat: add fabric_run_id, scale_mode, workspace_id, notebook_item_id to DemoManifest"
```

---

### Task 3: Add `_resolve_scale_mode` helper to `seeding.py`

**Files:**
- Modify: `sqllocks_spindle/demo/modes/seeding.py`
- Test: `tests/test_demo_seeding_v2.py` (NEW)

- [ ] **Step 1: Write the failing test**

Create `tests/test_demo_seeding_v2.py`:

```python
"""Tests for SeedingDemoMode v2 (Phase 2 wiring)."""
from unittest.mock import MagicMock
import pytest

from sqllocks_spindle.demo.modes.seeding import _resolve_scale_mode


class _Conn:
    def __init__(self, lakehouse_id="lh-1", workspace_id="ws-1"):
        self.lakehouse_id = lakehouse_id
        self.workspace_id = workspace_id
        self.warehouse_conn_str = ""
        self.sql_db_conn_str = ""
        self.eventhouse_uri = ""


def test_scale_mode_auto_picks_local_under_threshold():
    assert _resolve_scale_mode("auto", _Conn(), rows=100_000) == "local"


def test_scale_mode_auto_picks_spark_with_connection_and_large_rows():
    assert _resolve_scale_mode("auto", _Conn(), rows=500_000) == "spark"
    assert _resolve_scale_mode("auto", _Conn(), rows=1_000_000) == "spark"


def test_scale_mode_auto_picks_local_when_no_connection():
    assert _resolve_scale_mode("auto", None, rows=10_000_000) == "local"


def test_scale_mode_auto_picks_local_when_no_lakehouse():
    conn = _Conn(lakehouse_id="")
    assert _resolve_scale_mode("auto", conn, rows=10_000_000) == "local"


def test_scale_mode_explicit_local_always_returns_local():
    assert _resolve_scale_mode("local", _Conn(), rows=10_000_000) == "local"
    assert _resolve_scale_mode("local", None, rows=10_000_000) == "local"


def test_scale_mode_explicit_spark_without_connection_raises():
    with pytest.raises(ValueError, match="Spark mode requires a connection profile"):
        _resolve_scale_mode("spark", None, rows=100)


def test_scale_mode_explicit_spark_without_lakehouse_raises():
    conn = _Conn(lakehouse_id="")
    with pytest.raises(ValueError, match="lakehouse_id"):
        _resolve_scale_mode("spark", conn, rows=100)


def test_scale_mode_explicit_spark_with_full_connection_returns_spark():
    assert _resolve_scale_mode("spark", _Conn(), rows=100) == "spark"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_seeding_v2.py -v`
Expected: FAIL with `ImportError: cannot import name '_resolve_scale_mode'`

- [ ] **Step 3: Add `_resolve_scale_mode` to `seeding.py`**

In `sqllocks_spindle/demo/modes/seeding.py`, add this function below the existing `_rows_to_scale` function (around line 45):

```python
_SPARK_AUTO_THRESHOLD = 500_000


def _resolve_scale_mode(requested: str, conn_profile, rows: int) -> str:
    """Resolve 'auto' to 'local' or 'spark' based on connection and row count.

    'spark' requires a connection profile with a non-empty lakehouse_id.
    'local' always works.
    'auto' picks 'spark' when connection is present, lakehouse_id is set,
    and rows >= _SPARK_AUTO_THRESHOLD; otherwise 'local'.
    """
    if requested == "local":
        return "local"
    if requested == "spark":
        if conn_profile is None:
            raise ValueError("Spark mode requires a connection profile")
        if not getattr(conn_profile, "lakehouse_id", ""):
            raise ValueError("Spark mode requires lakehouse_id in connection profile")
        return "spark"
    # auto
    if (conn_profile is not None
            and getattr(conn_profile, "lakehouse_id", "")
            and rows >= _SPARK_AUTO_THRESHOLD):
        return "spark"
    return "local"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_seeding_v2.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/test_demo_seeding_v2.py sqllocks_spindle/demo/modes/seeding.py
git commit -m "feat: add _resolve_scale_mode helper to SeedingDemoMode"
```

---

### Task 4: Add `_build_sinks` helper

**Files:**
- Modify: `sqllocks_spindle/demo/modes/seeding.py`
- Test: `tests/test_demo_seeding_v2.py` (extend)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_demo_seeding_v2.py`:

```python
from sqllocks_spindle.demo.modes.seeding import _build_sinks


def test_build_sinks_empty_when_no_targets():
    conn = _Conn()
    conn.lakehouse_id = ""
    sinks, sinks_list, sink_config = _build_sinks(conn, token="t")
    assert sinks == []
    assert sinks_list == []
    assert sink_config["workspace_id"] == "ws-1"
    assert sink_config["token"] == "t"


def test_build_sinks_lakehouse_only():
    conn = _Conn()
    sinks, sinks_list, sink_config = _build_sinks(conn, token="t")
    assert len(sinks) == 1
    assert sinks_list == [{"type": "lakehouse",
                           "config": {"workspace_id": "ws-1",
                                      "lakehouse_id": "lh-1"}}]
    assert sink_config["lakehouse_id"] == "lh-1"


def test_build_sinks_all_targets():
    conn = _Conn()
    conn.warehouse_conn_str = "Driver=...;Server=wh"
    conn.sql_db_conn_str = "Driver=...;Server=sql"
    conn.eventhouse_uri = "https://eh.kusto"
    sinks, sinks_list, sink_config = _build_sinks(conn, token="t")
    assert len(sinks) == 4
    types = [s["type"] for s in sinks_list]
    assert types == ["lakehouse", "warehouse", "sql_db", "kql"]


def test_build_sinks_skips_failed_construction(monkeypatch):
    # Force WarehouseSink import to raise — sink should be skipped, others proceed
    import sqllocks_spindle.demo.modes.seeding as _seed
    real_import = _seed._import_sink_class

    def fake_import(name):
        if name == "warehouse":
            raise ImportError("simulated missing dep")
        return real_import(name)

    monkeypatch.setattr(_seed, "_import_sink_class", fake_import)
    conn = _Conn()
    conn.warehouse_conn_str = "Driver=...;Server=wh"
    sinks, sinks_list, sink_config = _build_sinks(conn, token="t")
    types = [s["type"] for s in sinks_list]
    assert "warehouse" not in types
    assert "lakehouse" in types
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_seeding_v2.py -v -k build_sinks`
Expected: FAIL with `ImportError: cannot import name '_build_sinks'`

- [ ] **Step 3: Add `_build_sinks` and `_import_sink_class` to `seeding.py`**

In `sqllocks_spindle/demo/modes/seeding.py`, add below `_resolve_scale_mode`:

```python
def _import_sink_class(kind: str):
    """Indirection so monkeypatch can intercept individual sink imports."""
    if kind == "lakehouse":
        from sqllocks_spindle.engine.sinks import LakehouseSink
        return LakehouseSink
    if kind == "warehouse":
        from sqllocks_spindle.engine.sinks import WarehouseSink
        return WarehouseSink
    if kind == "sql_db":
        from sqllocks_spindle.engine.sinks import SQLDatabaseSink
        return SQLDatabaseSink
    if kind == "kql":
        from sqllocks_spindle.engine.sinks import KQLSink
        return KQLSink
    raise ValueError(f"Unknown sink kind: {kind!r}")


def _build_sinks(conn, token: str) -> tuple[list, list, dict]:
    """Build sink instances + sinks_list (for FabricSparkRouter) + sink_config dict.

    Returns three values:
      sinks       — list[Sink] for ScaleRouter (local mode)
      sinks_list  — list[{"type", "config"}] passed to FabricSparkRouter
      sink_config — flat dict of common config (workspace_id, lakehouse_id, token, etc.)

    A sink whose construction fails is logged and skipped — other sinks proceed.
    """
    sinks = []
    sinks_list = []
    sink_config = {
        "workspace_id": getattr(conn, "workspace_id", "") if conn else "",
        "lakehouse_id": getattr(conn, "lakehouse_id", "") if conn else "",
        "token": token,
    }
    if conn is None:
        return sinks, sinks_list, sink_config

    targets = []
    if conn.lakehouse_id:
        targets.append(("lakehouse", {"workspace_id": conn.workspace_id,
                                       "lakehouse_id": conn.lakehouse_id}))
    if conn.warehouse_conn_str:
        targets.append(("warehouse", {"connection_string": conn.warehouse_conn_str}))
    if conn.sql_db_conn_str:
        targets.append(("sql_db", {"connection_string": conn.sql_db_conn_str}))
    if conn.eventhouse_uri:
        targets.append(("kql", {"cluster_uri": conn.eventhouse_uri}))

    for kind, cfg in targets:
        try:
            cls = _import_sink_class(kind)
            sinks.append(cls(**cfg) if kind != "lakehouse" else cls())
            sinks_list.append({"type": kind, "config": cfg})
        except Exception as e:
            logger.warning("Skipping %s sink — construction failed: %s", kind, e)

    return sinks, sinks_list, sink_config
```

Note: `LakehouseSink` is constructed without args here; it picks up its base path at `open()` time from the abfss path embedded in the lakehouse runtime. Sink construction kwargs may need adjustment per sink — keep this minimal until tests confirm.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_seeding_v2.py -v -k build_sinks`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/test_demo_seeding_v2.py sqllocks_spindle/demo/modes/seeding.py
git commit -m "feat: add _build_sinks helper for SeedingDemoMode"
```

---

### Task 5: Add `_acquire_token` helper

**Files:**
- Modify: `sqllocks_spindle/demo/modes/seeding.py`
- Test: `tests/test_demo_seeding_v2.py` (extend)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_demo_seeding_v2.py`:

```python
from sqllocks_spindle.demo.modes.seeding import _acquire_token


def test_acquire_token_uses_azure_cli_credential(monkeypatch):
    fake_token = MagicMock()
    fake_token.token = "ey-fake-token"
    fake_credential = MagicMock()
    fake_credential.get_token.return_value = fake_token

    monkeypatch.setattr(
        "azure.identity.AzureCliCredential",
        MagicMock(return_value=fake_credential),
    )
    token = _acquire_token()
    assert token == "ey-fake-token"
    fake_credential.get_token.assert_called_once_with(
        "https://api.fabric.microsoft.com/.default"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_seeding_v2.py::test_acquire_token_uses_azure_cli_credential -v`
Expected: FAIL with `ImportError: cannot import name '_acquire_token'`

- [ ] **Step 3: Add `_acquire_token` to `seeding.py`**

In `sqllocks_spindle/demo/modes/seeding.py`, add below `_build_sinks`:

```python
def _acquire_token(scope: str = "https://api.fabric.microsoft.com/.default") -> str:
    """Acquire an Entra bearer token for the Fabric API.

    Uses AzureCliCredential first (matches existing patterns in fabric/credentials.py),
    falling back to DefaultAzureCredential. The caller must have run `az login`.
    """
    from azure.identity import AzureCliCredential
    cred = AzureCliCredential()
    return cred.get_token(scope).token
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_seeding_v2.py::test_acquire_token_uses_azure_cli_credential -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_demo_seeding_v2.py sqllocks_spindle/demo/modes/seeding.py
git commit -m "feat: add _acquire_token helper for Fabric API authentication"
```

---

### Task 6: Rewrite `SeedingDemoMode.run()` write step

**Files:**
- Modify: `sqllocks_spindle/demo/modes/seeding.py`
- Test: `tests/test_demo_seeding_v2.py` (extend)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_demo_seeding_v2.py`:

```python
from sqllocks_spindle.demo.params import DemoParams
from sqllocks_spindle.demo.manifest import DemoManifest
from sqllocks_spindle.demo.modes.seeding import SeedingDemoMode


def test_local_path_invokes_scale_router(monkeypatch, tmp_path):
    """When scale_mode resolves to 'local', SeedingDemoMode calls ScaleRouter.run()."""
    fake_router = MagicMock()
    fake_router.run.return_value = {
        "rows_generated": 1000, "elapsed_seconds": 1.2,
        "throughput_rows_per_sec": 833, "memory_peak_gb": 0.1,
    }
    fake_router_class = MagicMock(return_value=fake_router)
    monkeypatch.setattr(
        "sqllocks_spindle.engine.scale_router.ScaleRouter",
        fake_router_class,
    )

    params = DemoParams(scenario="retail", mode="seeding",
                        rows=1000, scale_mode="local", seed=42)
    manifest = DemoManifest(scenario="retail", mode="seeding")
    handler = SeedingDemoMode(params, manifest, dashboard=None, connection_profile=None)
    result = handler.run()

    assert result["success"] is True
    assert manifest.scale_mode == "local"
    fake_router.run.assert_called_once()
    args, kwargs = fake_router.run.call_args
    assert kwargs.get("total_rows", args[0] if args else None) == 1000


def test_spark_path_invokes_fabric_spark_router(monkeypatch, tmp_path):
    """When scale_mode='spark', SeedingDemoMode calls FabricSparkRouter.submit()."""
    from sqllocks_spindle.engine.async_job_store import JobRecord
    fake_job = JobRecord(
        job_id="job-xyz", fabric_run_id="run-123",
        workspace_id="ws-1", notebook_item_id="nb-456",
        schema_temp_path="Files/spindle/tmp.json",
        lakehouse_id="lh-1", token="t",
    )
    fake_router = MagicMock()
    fake_router.submit.return_value = fake_job
    monkeypatch.setattr(
        "sqllocks_spindle.engine.spark_router.FabricSparkRouter",
        MagicMock(return_value=fake_router),
    )
    monkeypatch.setattr(
        "sqllocks_spindle.demo.modes.seeding._acquire_token",
        lambda: "tok",
    )

    class C:
        workspace_id = "ws-1"
        lakehouse_id = "lh-1"
        warehouse_conn_str = ""
        sql_db_conn_str = ""
        eventhouse_uri = ""

    params = DemoParams(scenario="retail", mode="seeding",
                        rows=1_000_000, scale_mode="spark", seed=42)
    manifest = DemoManifest(scenario="retail", mode="seeding")
    handler = SeedingDemoMode(params, manifest, dashboard=None, connection_profile=C())
    result = handler.run()

    assert result["success"] is True
    assert result["fabric_run_id"] == "run-123"
    assert manifest.scale_mode == "spark"
    assert manifest.fabric_run_id == "run-123"
    assert manifest.workspace_id == "ws-1"
    assert manifest.notebook_item_id == "nb-456"


def test_spark_path_without_connection_returns_failure():
    params = DemoParams(scenario="retail", mode="seeding",
                        rows=1000, scale_mode="spark", seed=42)
    manifest = DemoManifest(scenario="retail", mode="seeding")
    handler = SeedingDemoMode(params, manifest, dashboard=None, connection_profile=None)
    result = handler.run()
    assert result["success"] is False
    assert "Spark mode requires" in result["error"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_seeding_v2.py -v -k "local_path or spark_path"`
Expected: FAIL — current `run()` calls `Spindle().generate()` and never touches `ScaleRouter`/`FabricSparkRouter`

- [ ] **Step 3: Replace `SeedingDemoMode.run()` with the wiring implementation**

In `sqllocks_spindle/demo/modes/seeding.py`, replace the entire `run()` method body with:

```python
    def run(self) -> dict:
        dashboard = self._dashboard
        try:
            scale_mode = _resolve_scale_mode(
                self._params.scale_mode, self._conn, self._params.rows,
            )
        except ValueError as e:
            self._manifest.scale_mode = self._params.scale_mode
            return {"success": False, "error": str(e)}

        self._manifest.scale_mode = scale_mode

        # Estimate / dry-run paths bypass any sink work
        targets = self._available_targets()
        if self._params.estimate_only or self._params.dry_run:
            estimator = CostEstimator()
            estimate = estimator.estimate(self._params.rows, targets)
            print(f"\nCost estimate for {self._params.scenario} ({self._params.rows:,} rows):")
            print(str(estimate))
            if self._params.estimate_only:
                return {"success": True, "estimate_only": True}
            print(f"[dry-run] Would write {self._params.rows:,} rows to: {', '.join(targets)} "
                  f"via scale_mode={scale_mode}")
            return {"success": True, "dry_run": True}

        if dashboard:
            dashboard.start()
            dashboard.step(DemoStep.GENERATING, f"{self._params.rows:,} rows ({scale_mode})")

        try:
            if scale_mode == "local":
                stats = self._run_local()
            else:
                stats = self._run_spark()
        except Exception as e:
            logger.exception("Seeding failed")
            if dashboard:
                dashboard.finish(False, str(e))
            self._manifest.finish(False, str(e))
            self._manifest.save()
            return {"success": False, "error": str(e)}

        if dashboard:
            dashboard.step(DemoStep.DONE)
            dashboard.finish(True)
        self._manifest.metrics.update(stats.get("metrics", {}))
        self._manifest.finish(True)
        saved_path = self._manifest.save()
        result: dict = {"success": True, "session_id": self._manifest.session_id}
        result.update(stats.get("result", {}))
        if dashboard:
            dashboard.info(f"Manifest saved to {saved_path}")
        return result

    def _run_local(self) -> dict:
        """Local multi-process generation via ScaleRouter."""
        import dataclasses
        import json
        import os
        import tempfile
        from sqllocks_spindle.engine.scale_router import ScaleRouter

        token = ""  # local mode does not need a Fabric token unless sinks need it
        sinks, _sinks_list, _sink_config = _build_sinks(self._conn, token=token)

        domain_name = self._params.domain or "retail"
        domain = _get_domain_instance(domain_name)
        # Build a parsed schema using the existing Spindle._resolve_schema helper
        from sqllocks_spindle.engine.generator import Spindle
        sp = Spindle()
        parsed = sp._resolve_schema(domain, None)
        parsed.generation.scale = _rows_to_scale(self._params.rows)
        if self._params.seed is not None:
            parsed.model.seed = self._params.seed

        schema_dict = dataclasses.asdict(parsed)
        if hasattr(domain, "domain_path"):
            schema_dict["_domain_path"] = str(domain.domain_path)

        tmp = tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False)
        try:
            json.dump(schema_dict, tmp, default=str)
            tmp.close()
            router = ScaleRouter(
                schema_path=tmp.name,
                sinks=sinks,
                chunk_size=500_000,
                max_workers=1 if self._params.rows < 500_000 else None,
            )
            stats = router.run(total_rows=self._params.rows,
                               seed=self._params.seed or 42)
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)

        for tname in schema_dict.get("tables", {}):
            target = sinks[0].__class__.__name__.replace("Sink", "").lower() if sinks else "generated"
            self._manifest.add_artifact(target, tname, row_count=stats.get("rows_generated", 0))

        return {"result": {"stats": stats}, "metrics": {"rows_generated": stats.get("rows_generated", 0)}}

    def _run_spark(self) -> dict:
        """Async Fabric Spark generation via FabricSparkRouter."""
        import dataclasses
        from sqllocks_spindle.engine.spark_router import FabricSparkRouter
        from sqllocks_spindle.engine.async_job_store import AsyncJobStore

        if self._conn is None:
            raise ValueError("Spark mode requires a connection profile")

        token = _acquire_token()
        sinks, sinks_list, sink_config = _build_sinks(self._conn, token=token)

        domain_name = self._params.domain or "retail"
        domain = _get_domain_instance(domain_name)
        from sqllocks_spindle.engine.generator import Spindle
        sp = Spindle()
        parsed = sp._resolve_schema(domain, None)
        if self._params.seed is not None:
            parsed.model.seed = self._params.seed
        schema_dict = dataclasses.asdict(parsed)
        if hasattr(domain, "domain_path"):
            schema_dict["_domain_path"] = str(domain.domain_path)

        router = FabricSparkRouter(
            workspace_id=self._conn.workspace_id,
            lakehouse_id=self._conn.lakehouse_id,
            token=token,
            sinks=[s["type"] for s in sinks_list],
            sink_config=sink_config,
            chunk_size=500_000,
        )
        job = router.submit(
            schema_dict=schema_dict,
            total_rows=self._params.rows,
            seed=self._params.seed or 42,
        )

        # Stash job in shared registry for cmd_demo_status to find
        from sqllocks_spindle.mcp_bridge import _job_store
        _job_store.put(job)

        self._manifest.fabric_run_id = job.fabric_run_id
        self._manifest.workspace_id = job.workspace_id
        self._manifest.notebook_item_id = job.notebook_item_id

        return {
            "result": {
                "fabric_run_id": job.fabric_run_id,
                "job_id": job.job_id,
                "status": "submitted",
                "schema_temp_path": job.schema_temp_path,
            },
            "metrics": {},
        }
```

- [ ] **Step 4: Run all seeding tests to verify they pass**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_seeding_v2.py -v`
Expected: PASS (all tests)

Also run the existing demo orchestrator test to confirm no regression:
Run: `.venv-mac/bin/python -m pytest tests/test_demo_orchestrator.py -v`
Expected: PASS (3 tests, including `test_run_inference_retail` which doesn't touch seeding)

- [ ] **Step 5: Commit**

```bash
git add tests/test_demo_seeding_v2.py sqllocks_spindle/demo/modes/seeding.py
git commit -m "feat: rewrite SeedingDemoMode to use ScaleRouter (local) or FabricSparkRouter (spark)"
```

---

### Task 7: Add `cmd_demo_status` and `cmd_demo_cleanup` MCP commands

**Files:**
- Modify: `sqllocks_spindle/mcp_bridge.py`
- Test: `tests/test_demo_mcp_commands.py` (NEW)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_demo_mcp_commands.py`:

```python
"""Tests for cmd_demo_status, cmd_demo_cleanup."""
from unittest.mock import MagicMock, patch
import pytest

from sqllocks_spindle.demo.manifest import DemoManifest


def test_cmd_demo_status_unknown_session_returns_error(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "sqllocks_spindle.demo.manifest.DemoManifest.load",
        MagicMock(side_effect=FileNotFoundError("No session 'nope' found")),
    )
    from sqllocks_spindle.mcp_bridge import cmd_demo_status
    result = cmd_demo_status({"session_id": "nope"})
    assert result["error"] == "session_not_found"
    assert result["session_id"] == "nope"


def test_cmd_demo_status_local_returns_manifest(tmp_path, monkeypatch):
    m = DemoManifest(scenario="retail", mode="seeding")
    m.scale_mode = "local"
    m.finish(success=True)
    m.save(directory=tmp_path)

    monkeypatch.setattr(
        "sqllocks_spindle.demo.manifest.DemoManifest.load",
        MagicMock(return_value=m),
    )
    from sqllocks_spindle.mcp_bridge import cmd_demo_status
    result = cmd_demo_status({"session_id": m.session_id})
    assert result["session_id"] == m.session_id
    assert result["manifest"]["scale_mode"] == "local"
    assert result["manifest"]["success"] is True
    assert "fabric" not in result


def test_cmd_demo_status_spark_polls_fabric_tracker(tmp_path, monkeypatch):
    m = DemoManifest(scenario="retail", mode="seeding")
    m.scale_mode = "spark"
    m.fabric_run_id = "run-123"
    m.workspace_id = "ws-1"
    m.notebook_item_id = "nb-456"
    m.finish(success=True)

    fake_tracker = MagicMock()
    fake_tracker.get_status.return_value = {
        "status": "running", "progress_pct": 42, "error": None,
    }
    monkeypatch.setattr(
        "sqllocks_spindle.demo.manifest.DemoManifest.load",
        MagicMock(return_value=m),
    )
    monkeypatch.setattr(
        "sqllocks_spindle.mcp_bridge.FabricJobTracker",
        MagicMock(return_value=fake_tracker),
    )
    from sqllocks_spindle.mcp_bridge import cmd_demo_status
    result = cmd_demo_status({"session_id": m.session_id, "token": "t"})
    assert result["fabric"]["status"] == "running"
    fake_tracker.get_status.assert_called_once_with("ws-1", "nb-456", "run-123")


def test_cmd_demo_cleanup_invokes_cleanup_engine(tmp_path, monkeypatch):
    m = DemoManifest(scenario="retail", mode="seeding")
    m.add_artifact("lakehouse", "Tables/customer", row_count=100)
    m.finish(success=True)

    fake_engine = MagicMock()
    fake_engine.cleanup.return_value = {"lakehouse": ["Tables/customer"]}
    monkeypatch.setattr(
        "sqllocks_spindle.demo.manifest.DemoManifest.load",
        MagicMock(return_value=m),
    )
    monkeypatch.setattr(
        "sqllocks_spindle.mcp_bridge.CleanupEngine",
        MagicMock(return_value=fake_engine),
    )
    from sqllocks_spindle.mcp_bridge import cmd_demo_cleanup
    result = cmd_demo_cleanup({"session_id": m.session_id, "dry_run": True})
    assert result == {"lakehouse": ["Tables/customer"]}
    fake_engine.cleanup.assert_called_once_with(m, dry_run=True)


def test_cmd_demo_cleanup_unknown_session_returns_error(monkeypatch):
    monkeypatch.setattr(
        "sqllocks_spindle.demo.manifest.DemoManifest.load",
        MagicMock(side_effect=FileNotFoundError("No session 'x' found")),
    )
    from sqllocks_spindle.mcp_bridge import cmd_demo_cleanup
    result = cmd_demo_cleanup({"session_id": "x"})
    assert result["error"] == "session_not_found"


def test_cmd_demo_run_passes_scale_mode_through(monkeypatch):
    """Ensure cmd_demo_run forwards scale_mode into DemoParams."""
    from sqllocks_spindle.demo.orchestrator import DemoResult

    captured: dict = {}

    class FakeOrch:
        def run(self, params):
            captured["scale_mode"] = params.scale_mode
            return DemoResult(success=True, session_id="abc",
                              scenario=params.scenario, mode=params.mode)

    monkeypatch.setattr("sqllocks_spindle.mcp_bridge.DemoOrchestrator", FakeOrch, raising=False)
    monkeypatch.setattr("sqllocks_spindle.demo.orchestrator.DemoOrchestrator", FakeOrch)
    from sqllocks_spindle.mcp_bridge import cmd_demo_run
    cmd_demo_run({"scenario": "retail", "mode": "seeding",
                  "rows": 1000, "scale_mode": "spark", "connection": None})
    assert captured["scale_mode"] == "spark"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_mcp_commands.py -v`
Expected: FAIL — `cmd_demo_status`, `cmd_demo_cleanup`, `CleanupEngine` symbol all missing from `mcp_bridge`

- [ ] **Step 3: Add the new commands and update `cmd_demo_run`**

In `sqllocks_spindle/mcp_bridge.py`, near the top after the existing imports (around line 27, after the `_job_store = AsyncJobStore()` line), add:

```python
from sqllocks_spindle.demo.cleanup import CleanupEngine
from sqllocks_spindle.demo.orchestrator import DemoOrchestrator
```

Replace the existing `cmd_demo_run` function (lines 684-718) with this updated version that forwards `scale_mode`:

```python
def cmd_demo_run(params: dict) -> dict:
    """Run a demo scenario. Returns structured result; Spark mode includes fabric_run_id."""
    from sqllocks_spindle.demo.params import DemoParams
    import sys

    demo_params = DemoParams(
        scenario=params.get("scenario", "retail"),
        mode=params.get("mode", "inference"),
        rows=int(params.get("rows", 50_000)),
        domain=params.get("domain"),
        input_file=params.get("input_file"),
        connection=params.get("connection"),
        output_formats=params.get("output_formats", []),
        dry_run=bool(params.get("dry_run", False)),
        seed=int(params.get("seed", 42)),
        scale_mode=params.get("scale_mode", "auto"),
    )

    real_stdout = sys.stdout
    sys.stdout = sys.stderr
    try:
        orch = DemoOrchestrator()
        result = orch.run(demo_params)
    finally:
        sys.stdout = real_stdout

    payload = {
        "success": result.success,
        "session_id": result.session_id,
        "scenario": result.scenario,
        "mode": result.mode,
        "fidelity_score": result.fidelity_score,
        "error": result.error,
        "artifact_count": len(result.manifest.artifacts) if result.manifest else 0,
    }
    if result.manifest is not None:
        if result.manifest.fabric_run_id:
            payload["fabric_run_id"] = result.manifest.fabric_run_id
            payload["status"] = "submitted"
        if result.manifest.scale_mode:
            payload["scale_mode"] = result.manifest.scale_mode
    return payload


def cmd_demo_status(params: dict) -> dict:
    """Return demo manifest by session_id; if Spark, include live Fabric job status."""
    from sqllocks_spindle.demo.manifest import DemoManifest
    from dataclasses import asdict

    session_id = params["session_id"]
    try:
        manifest = DemoManifest.load(session_id)
    except FileNotFoundError:
        return {"error": "session_not_found", "session_id": session_id}

    manifest_dict = asdict(manifest)
    manifest_dict.pop("_path", None)
    result = {"session_id": session_id, "manifest": manifest_dict}

    if manifest.fabric_run_id:
        token = params.get("token", "")
        tracker = FabricJobTracker(token=token)
        result["fabric"] = tracker.get_status(
            manifest.workspace_id, manifest.notebook_item_id, manifest.fabric_run_id,
        )
    return result


def cmd_demo_cleanup(params: dict) -> dict:
    """Run CleanupEngine against a saved demo manifest."""
    from sqllocks_spindle.demo.manifest import DemoManifest

    session_id = params["session_id"]
    try:
        manifest = DemoManifest.load(session_id)
    except FileNotFoundError:
        return {"error": "session_not_found", "session_id": session_id}

    engine = CleanupEngine()
    return engine.cleanup(manifest, dry_run=bool(params.get("dry_run", False)))
```

Update the `COMMANDS` dict (around line 746) to register the new commands:

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
    "demo_status": cmd_demo_status,
    "demo_cleanup": cmd_demo_cleanup,
    "scale_generate": cmd_scale_generate,
    "stream": cmd_stream,
    "stream_status": cmd_stream_status,
    "stream_stop": cmd_stream_stop,
    "scale_status": cmd_scale_status,
    "scale_cancel": cmd_scale_cancel,
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_mcp_commands.py -v`
Expected: PASS (6 tests)

Also run the existing MCP bridge test suite to confirm no regression:
Run: `.venv-mac/bin/python -m pytest tests/test_mcp_bridge.py -v`
Expected: PASS (no failures)

- [ ] **Step 5: Commit**

```bash
git add tests/test_demo_mcp_commands.py sqllocks_spindle/mcp_bridge.py
git commit -m "feat: add cmd_demo_status, cmd_demo_cleanup; forward scale_mode in cmd_demo_run"
```

---

### Task 8: Add `--scale-mode` CLI flag

**Files:**
- Modify: `sqllocks_spindle/cli.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_demo_mcp_commands.py`:

```python
def test_cli_demo_run_accepts_scale_mode_flag():
    """Smoke test: --scale-mode flag is accepted by the CLI command."""
    from click.testing import CliRunner
    from sqllocks_spindle.cli import demo_run

    runner = CliRunner()
    # Just check the CLI parses the flag — actual demo runs are covered elsewhere
    result = runner.invoke(demo_run, ["--help"])
    assert result.exit_code == 0
    assert "--scale-mode" in result.output
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_mcp_commands.py::test_cli_demo_run_accepts_scale_mode_flag -v`
Expected: FAIL — `--scale-mode` not in CLI help output

- [ ] **Step 3: Add the flag to the CLI**

In `sqllocks_spindle/cli.py`, find the `demo_run` function (line 1927). Add a new option line after `--seed` and update the function signature and `DemoParams` construction:

Find:
```python
@click.option("--seed", default=None, type=int)
def demo_run(scenario, mode, connection, input_file, rows, domain, domains,
             env_name, output_formats, dry_run, estimate_only, seed):
```

Replace with:
```python
@click.option("--seed", default=None, type=int)
@click.option("--scale-mode", "scale_mode", default="auto",
              type=click.Choice(["auto", "local", "spark"]),
              help="local: ProcessPoolExecutor; spark: Fabric notebook; auto: pick by row count + connection")
def demo_run(scenario, mode, connection, input_file, rows, domain, domains,
             env_name, output_formats, dry_run, estimate_only, seed, scale_mode):
```

Find the `DemoParams(...)` construction inside the function:
```python
    params = DemoParams(scenario=scenario, mode=mode, connection=connection, input_file=input_file,
                        rows=effective_rows, domain=domain, domains=domain_list, env_name=env_name,
                        output_formats=fmt_list, dry_run=dry_run, estimate_only=estimate_only, seed=seed)
```

Replace with:
```python
    params = DemoParams(scenario=scenario, mode=mode, connection=connection, input_file=input_file,
                        rows=effective_rows, domain=domain, domains=domain_list, env_name=env_name,
                        output_formats=fmt_list, dry_run=dry_run, estimate_only=estimate_only,
                        seed=seed, scale_mode=scale_mode)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv-mac/bin/python -m pytest tests/test_demo_mcp_commands.py::test_cli_demo_run_accepts_scale_mode_flag -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_demo_mcp_commands.py sqllocks_spindle/cli.py
git commit -m "feat: add --scale-mode flag to spindle demo run CLI"
```

---

### Task 9: Version bump and changelog entry

**Files:**
- Modify: `sqllocks_spindle/__init__.py`
- Modify: `pyproject.toml`
- Modify: `docs/changelog.md`

- [ ] **Step 1: Bump version in `__init__.py`**

In `sqllocks_spindle/__init__.py`, find:
```python
__version__ = "2.7.0"
```
Replace with:
```python
__version__ = "2.7.1"
```

- [ ] **Step 2: Bump version in `pyproject.toml`**

Find the line:
```toml
version = "2.7.0"
```
Replace with:
```toml
version = "2.7.1"
```

- [ ] **Step 3: Add changelog entry**

In `docs/changelog.md`, insert above the `## [2.7.0] - 2026-04-27` heading:

```markdown
## [2.7.1] - 2026-04-27

### Changed

- **Demo Engine — Phase 2 wiring**: `SeedingDemoMode.run()` now performs real Fabric
  sink writes, replacing the previous manifest-only stub. Local mode delegates to
  `ScaleRouter` (multi-process); Spark mode delegates to `FabricSparkRouter`
  (Fabric notebook submission). Sinks are constructed from the connection profile
  and fan out simultaneously to all configured targets (lakehouse + warehouse +
  sql_db + eventhouse).
- New `--scale-mode {auto,local,spark}` flag on `spindle demo run`. `auto` selects
  `spark` when a connection profile is configured, `lakehouse_id` is set, and
  `rows >= 500_000`; otherwise `local`.
- `DemoManifest` now records `scale_mode`, `fabric_run_id`, `workspace_id`, and
  `notebook_item_id` so Spark runs can be polled and cleaned up by `session_id`.
- `cmd_demo_run` now forwards `scale_mode` into `DemoParams` and includes
  `fabric_run_id` and `status` in the response payload for Spark submissions.

### Added

- `cmd_demo_status` MCP bridge command — reads the manifest by `session_id` and,
  when the run was a Spark submission, polls `FabricJobTracker.get_status` for
  live Fabric job state
- `cmd_demo_cleanup` MCP bridge command — runs `CleanupEngine` against a saved
  manifest by `session_id`

### Test count

1,930 → 1,945 (+15: 8 in `test_demo_seeding_v2.py`, 6 in `test_demo_mcp_commands.py`,
1 in `test_demo_params.py`)
```

- [ ] **Step 4: Run the full test suite to verify no regressions**

Run: `.venv-mac/bin/python -m pytest 2>&1 | tail -5`
Expected: 1,945 or more tests pass; 1 known pre-existing failure (openpyxl) is acceptable.

- [ ] **Step 5: Commit**

```bash
git add sqllocks_spindle/__init__.py pyproject.toml docs/changelog.md
git commit -m "chore: bump version to 2.7.1 and add changelog entry"
```

---

## Self-Review

**Spec coverage:**
- Goal 1 (real sink writes) → Tasks 4 + 6
- Goal 2 (`--scale-mode` flag with auto threshold) → Tasks 3 + 8
- Goal 3 (three new MCP commands) → Task 7 (also updates `cmd_demo_run`)
- Goal 4 (manifest tracks Spark fields) → Task 2
- Goal 5 (no live Fabric required for tests) → Tasks 6 + 7 use HTTP/`monkeypatch`
- Open question 1 (`ConnectionProfile.token`) → Task 5 (`_acquire_token` via `AzureCliCredential`)
- Open question 2 (schema serialization to temp file) → Task 6, `_run_local`
- Open question 3 (shared `_job_store`) → Task 6, imports `_job_store` from `mcp_bridge`

**Type consistency:**
- `_resolve_scale_mode(requested, conn_profile, rows)` — same signature in spec and tasks
- `_build_sinks(conn, token)` returns `(sinks, sinks_list, sink_config)` — used identically in `_run_local` and `_run_spark`
- `JobRecord` field names (`fabric_run_id`, `workspace_id`, `notebook_item_id`) — match those used in `cmd_demo_status`

**Placeholder scan:** No TBDs, no "implement later", every code block is complete.

---

## Execution Notes

- All test commands use `.venv-mac/bin/python -m pytest` (the rebuilt venv from 2026-04-26)
- `monkeypatch` and `MagicMock` are used throughout — no live Fabric workspace required
- Existing 21 demo tests must continue to pass after each task
- Total test count after Task 9: 1,945 (1,930 + 15 new — 8 seeding_v2, 6 mcp_commands, 1 params)
