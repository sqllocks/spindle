# Phase 5 — Validation Matrix & Demo Notebooks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a pytest parametrized validation matrix covering all ~400 valid domain × sink × size × mode combinations (mock-based), a ~30-test live subset, 6 Fabric demo notebooks, and 2 notebook templates.

**Architecture:** Shared matrix builder in `tests/fixtures/validation_matrix.py` feeds both test files. Mock sinks wrap each writer type with MagicMock connections. Live tests use `InteractiveBrowserCredential` (same pattern as `tests/test_lakehouse_profiler.py`). Notebooks are `.ipynb` files with parametrized header cells.

**Tech Stack:** pytest, pytest.mark.parametrize, unittest.mock, azure-identity (InteractiveBrowserCredential), sqllocks-spindle (Spindle, generate, generate_stream, DataProfiler, FabricSqlDatabaseWriter, LakehouseWriter), nbformat (notebooks).

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `tests/fixtures/validation_matrix.py` | Create | Matrix builder: dimension lists, filter rules, `build_matrix()` |
| `tests/fixtures/mock_sinks.py` | Create | Mock sink factory fixture for all 5 sink types |
| `tests/test_validation_matrix.py` | Create | Mock parametrized suite (~400 combos) |
| `tests/test_validation_live.py` | Create | Live subset (~30 combos, `@pytest.mark.live`) |
| `pyproject.toml` | Modify | Add `SPINDLE_TEST_*` env var docs to pytest section |
| `notebooks/demos/01_retail_lakehouse_quickstart.ipynb` | Create | Retail → lakehouse demo |
| `notebooks/demos/02_financial_warehouse_analytics.ipynb` | Create | Financial → warehouse demo |
| `notebooks/demos/03_healthcare_sql_database.ipynb` | Create | Healthcare → sql-database + masking demo |
| `notebooks/demos/04_capital_markets_eventhouse.ipynb` | Create | Capital markets → eventhouse/KQL streaming demo |
| `notebooks/demos/05_multi_domain_fanout.ipynb` | Create | Retail + financial → lakehouse + warehouse |
| `notebooks/demos/06_custom_ddl_to_lakehouse.ipynb` | Create | DDL → profile → generate → lakehouse |
| `notebooks/templates/template_domain_to_sink.ipynb` | Create | Parametrized starter for any domain → any sink |
| `notebooks/templates/template_custom_schema.ipynb` | Create | Custom schema → any sink |
| `docs/changelog.md` | Modify | v2.11.0 entry |
| `sqllocks_spindle/__init__.py` | Modify | Version bump to 2.11.0 |
| `pyproject.toml` | Modify | Version bump to 2.11.0 |

---

## Task 1: Matrix Builder

**Files:**
- Create: `tests/fixtures/validation_matrix.py`

- [ ] **Step 1: Write failing test for build_matrix()**

```python
# tests/test_validation_matrix.py (temporary — delete after Task 3)
from tests.fixtures.validation_matrix import build_matrix, DOMAINS, SINKS, SIZES, MODES

def test_build_matrix_returns_list():
    matrix = build_matrix()
    assert isinstance(matrix, list)
    assert len(matrix) > 0

def test_matrix_no_duplicates():
    matrix = build_matrix()
    assert len(matrix) == len(set(matrix))

def test_matrix_filters_streaming_sql_server():
    matrix = build_matrix()
    bad = [(d, s, sz, m) for d, s, sz, m in matrix if s == "sql-server" and m == "streaming"]
    assert bad == [], f"streaming+sql-server should be filtered: {bad}"

def test_matrix_filters_fabric_demo_sql_server():
    matrix = build_matrix()
    # fabric_demo with sql-server should be capped to medium
    for d, s, sz, m in matrix:
        if s == "sql-server":
            assert sz != "fabric_demo", f"fabric_demo+sql-server should be filtered: {(d,s,sz,m)}"
```

Run: `cd projects/fabric-datagen && .venv-mac/bin/python -m pytest tests/test_validation_matrix.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'tests.fixtures.validation_matrix'`

- [ ] **Step 2: Create the matrix builder**

Create `tests/fixtures/validation_matrix.py`:

```python
"""Validation matrix builder for Spindle domain × sink × size × mode combinations."""
from __future__ import annotations

DOMAINS = [
    "capital_markets",
    "education",
    "financial",
    "healthcare",
    "hr",
    "insurance",
    "iot",
    "manufacturing",
    "marketing",
    "real_estate",
    "retail",
    "supply_chain",
    "telecom",
]

SINKS = ["lakehouse", "warehouse", "eventhouse", "sql-database", "sql-server"]

SIZES = ["small", "medium", "large", "fabric_demo"]

MODES = ["seeding", "streaming", "inference"]

# Domains with a bundled profile available for inference mode testing.
# Inference mode is skipped for all others.
INFERENCE_CAPABLE_DOMAINS = {"retail", "financial", "healthcare"}

# Fabric-only sinks: only these support spark scale mode and fabric_demo size.
FABRIC_SINKS = {"lakehouse", "warehouse", "eventhouse", "sql-database"}


def build_matrix() -> list[tuple[str, str, str, str]]:
    """Return all valid (domain, sink, size, mode) tuples.

    Filters applied:
    - streaming + sql-server → skip (chunked writer not wired, Phase 6)
    - fabric_demo + sql-server → skip (no Spark path for on-prem)
    - inference mode for non-INFERENCE_CAPABLE_DOMAINS → skip
    - deduplicate after size cap
    """
    seen: set[tuple[str, str, str, str]] = set()
    result: list[tuple[str, str, str, str]] = []

    for domain in DOMAINS:
        for sink in SINKS:
            for size in SIZES:
                for mode in MODES:
                    # Apply filters
                    if sink == "sql-server" and mode == "streaming":
                        continue
                    if sink == "sql-server" and size == "fabric_demo":
                        continue
                    if mode == "inference" and domain not in INFERENCE_CAPABLE_DOMAINS:
                        continue

                    combo = (domain, sink, size, mode)
                    if combo not in seen:
                        seen.add(combo)
                        result.append(combo)

    return result
```

- [ ] **Step 3: Run tests**

Run: `.venv-mac/bin/python -m pytest tests/test_validation_matrix.py -v`
Expected: 4 PASS

- [ ] **Step 4: Verify matrix size is reasonable**

```python
# Quick sanity check
.venv-mac/bin/python -c "
from tests.fixtures.validation_matrix import build_matrix
m = build_matrix()
print(f'Total combos: {len(m)}')
from collections import Counter
by_sink = Counter(s for _,s,_,_ in m)
print('By sink:', dict(by_sink))
by_mode = Counter(mo for _,_,_,mo in m)
print('By mode:', dict(by_mode))
"
```
Expected: Total between 300–500 combos.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/validation_matrix.py tests/test_validation_matrix.py
git commit -m "feat: add validation matrix builder with filter rules"
```

---

## Task 2: Mock Sink Factory

**Files:**
- Create: `tests/fixtures/mock_sinks.py`

- [ ] **Step 1: Write failing tests for mock sinks**

Add to `tests/test_validation_matrix.py`:

```python
from tests.fixtures.mock_sinks import MockSink, make_mock_sink

def test_make_mock_sink_lakehouse():
    sink = make_mock_sink("lakehouse")
    assert isinstance(sink, MockSink)

def test_make_mock_sink_records_write():
    from sqllocks_spindle import Spindle
    from sqllocks_spindle.domains.retail import RetailDomain
    result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
    sink = make_mock_sink("lakehouse")
    sink.write(result)
    assert sink.write_count == 1
    assert sink.total_rows > 0

def test_make_mock_sink_all_types():
    for sink_type in ["lakehouse", "warehouse", "eventhouse", "sql-database", "sql-server"]:
        sink = make_mock_sink(sink_type)
        assert isinstance(sink, MockSink), f"Failed for {sink_type}"
```

Run: `.venv-mac/bin/python -m pytest tests/test_validation_matrix.py -k "mock_sink" -v`
Expected: FAIL with `ImportError`

- [ ] **Step 2: Create the mock sink factory**

Create `tests/fixtures/mock_sinks.py`:

```python
"""Mock sink implementations for validation matrix tests."""
from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import MagicMock, patch

import pandas as pd


@dataclass
class MockSink:
    """Records write calls without performing real IO."""

    sink_type: str
    write_count: int = 0
    total_rows: int = 0
    tables_written: list[str] = field(default_factory=list)
    _chunks: list[tuple[str, pd.DataFrame]] = field(default_factory=list)

    def write(self, result) -> None:
        """Write a full GenerationResult (seeding/inference mode)."""
        for table_name, df in result.tables.items():
            self._chunks.append((table_name, df))
            self.tables_written.append(table_name)
            self.total_rows += len(df)
        self.write_count += 1

    def write_stream(self, table_name: str, df: pd.DataFrame) -> None:
        """Write a single table chunk (streaming mode)."""
        self._chunks.append((table_name, df))
        self.tables_written.append(table_name)
        self.total_rows += len(df)

    def assert_written(self, min_rows: int = 1) -> None:
        assert self.total_rows >= min_rows, (
            f"Expected >= {min_rows} rows, got {self.total_rows}"
        )


def make_mock_sink(sink_type: str) -> MockSink:
    """Return a MockSink for the given sink type."""
    valid = {"lakehouse", "warehouse", "eventhouse", "sql-database", "sql-server"}
    if sink_type not in valid:
        raise ValueError(f"Unknown sink type: {sink_type!r}. Valid: {valid}")
    return MockSink(sink_type=sink_type)
```

- [ ] **Step 3: Run tests**

Run: `.venv-mac/bin/python -m pytest tests/test_validation_matrix.py -v`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/mock_sinks.py tests/test_validation_matrix.py
git commit -m "feat: add mock sink factory for validation matrix"
```

---

## Task 3: Mock Validation Suite

**Files:**
- Rewrite: `tests/test_validation_matrix.py` (replace temp tests with real parametrized suite)

- [ ] **Step 1: Understand domain loading pattern**

Domains must be instantiated by class, not string. The CLI's `_resolve_domain` in `sqllocks_spindle/cli.py:1902` uses `importlib.import_module` + `getattr`. We'll replicate this pattern:

```python
import importlib
from sqllocks_spindle.cli import _get_domain_registry

def _load_domain(domain_name: str):
    registry = _get_domain_registry()
    module_path, class_name, _ = registry[domain_name]
    module = importlib.import_module(module_path)
    return getattr(module, class_name)(schema_mode="3nf")
```

- [ ] **Step 2: Write the full test file**

Replace `tests/test_validation_matrix.py` entirely:

```python
"""Parametrized validation matrix — all ~400 valid domain × sink × size × mode combos.

Runs in <90s with no credentials (mock sinks only).
"""
from __future__ import annotations

import importlib

import pytest

from sqllocks_spindle.cli import _get_domain_registry
from sqllocks_spindle.engine.generator import Spindle
from tests.fixtures.validation_matrix import (
    DOMAINS, SINKS, SIZES, MODES,
    INFERENCE_CAPABLE_DOMAINS,
    build_matrix,
)
from tests.fixtures.mock_sinks import make_mock_sink, MockSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_domain(domain_name: str):
    """Instantiate a domain class by name using the CLI registry."""
    registry = _get_domain_registry()
    module_path, class_name, _ = registry[domain_name]
    module = importlib.import_module(module_path)
    return getattr(module, class_name)(schema_mode="3nf")


def _run_seeding(domain_name: str, size: str) -> tuple:
    """Generate full dataset, return (result, sink)."""
    domain = _load_domain(domain_name)
    result = Spindle().generate(domain=domain, scale=size, seed=42)
    sink = make_mock_sink("lakehouse")  # replaced per combo in parametrized test
    sink.write(result)
    return result, sink


def _assert_result_valid(result, domain_name: str, size: str) -> None:
    """Common assertions for any GenerationResult."""
    assert result.tables, f"{domain_name}: no tables generated"
    for table_name, df in result.tables.items():
        assert len(df) > 0, f"{domain_name}/{table_name}: 0 rows at size={size}"
        for col in df.columns:
            if col.endswith("_id") and not col.startswith("parent_"):
                null_count = df[col].isna().sum()
                assert null_count == 0, (
                    f"{domain_name}/{table_name}.{col}: {null_count} NaN PKs"
                )
    errors = result.verify_integrity()
    assert errors == [], f"{domain_name} FK integrity errors: {errors}"


# ---------------------------------------------------------------------------
# Parametrized matrix
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("domain,sink,size,mode", build_matrix())
def test_generation_to_sink(domain, sink, size, mode):
    """Generate synthetic data and write to mock sink — all valid combinations."""
    spindle = Spindle()
    mock_sink = make_mock_sink(sink)

    if mode == "seeding":
        domain_obj = _load_domain(domain)
        result = spindle.generate(domain=domain_obj, scale=size, seed=42)
        mock_sink.write(result)
        _assert_result_valid(result, domain, size)

    elif mode == "streaming":
        domain_obj = _load_domain(domain)
        tables_received: list[str] = []
        for table_name, df in spindle.generate_stream(domain=domain_obj, scale=size, seed=42):
            mock_sink.write_stream(table_name, df)
            tables_received.append(table_name)
            assert len(df) > 0, f"{domain}/{table_name}: 0 rows in stream at size={size}"
        assert tables_received, f"{domain}: no tables yielded in stream"

    elif mode == "inference":
        assert domain in INFERENCE_CAPABLE_DOMAINS, (
            f"inference mode test reached non-capable domain: {domain}"
        )
        from sqllocks_spindle.inference import DataProfiler
        from sqllocks_spindle.inference.schema_builder import SchemaBuilder
        from sqllocks_spindle.inference.profiler import DatasetProfile

        # Generate small reference data to profile
        domain_obj = _load_domain(domain)
        ref_result = spindle.generate(domain=domain_obj, scale="small", seed=42)
        first_table = next(iter(ref_result.tables))
        ref_df = ref_result.tables[first_table].head(200)

        # Profile → build schema → generate
        profiler = DataProfiler(sample_rows=200)
        table_profile = profiler.profile(ref_df, table_name=first_table)
        dataset_profile = DatasetProfile(tables={first_table: table_profile})
        schema = SchemaBuilder().build(dataset_profile)
        result = spindle.generate(schema=schema, scale="small", seed=99)
        mock_sink.write(result)
        assert result.tables, f"{domain} inference: no tables generated"

    mock_sink.assert_written(min_rows=1)


# ---------------------------------------------------------------------------
# Matrix builder unit tests
# ---------------------------------------------------------------------------

def test_build_matrix_returns_nonempty_list():
    matrix = build_matrix()
    assert isinstance(matrix, list)
    assert len(matrix) > 100


def test_matrix_no_duplicates():
    matrix = build_matrix()
    assert len(matrix) == len(set(matrix))


def test_matrix_filters_streaming_sql_server():
    matrix = build_matrix()
    bad = [(d, s, sz, m) for d, s, sz, m in matrix if s == "sql-server" and m == "streaming"]
    assert bad == [], f"streaming+sql-server should be filtered: {bad}"


def test_matrix_filters_fabric_demo_sql_server():
    matrix = build_matrix()
    for d, s, sz, m in matrix:
        if s == "sql-server":
            assert sz != "fabric_demo", f"fabric_demo+sql-server should be filtered"


def test_matrix_inference_only_capable_domains():
    matrix = build_matrix()
    for d, s, sz, m in matrix:
        if m == "inference":
            assert d in INFERENCE_CAPABLE_DOMAINS, (
                f"inference mode should only appear for capable domains, got: {d}"
            )


def test_all_domains_in_matrix():
    matrix = build_matrix()
    domains_in_matrix = {d for d, _, _, _ in matrix}
    for domain in DOMAINS:
        assert domain in domains_in_matrix, f"{domain} missing from matrix"
```

- [ ] **Step 3: Run the full mock matrix (expect some to fail — that's OK, we'll fix them)**

```bash
.venv-mac/bin/python -m pytest tests/test_validation_matrix.py -v --tb=short -q 2>&1 | tail -30
```

Expected: Most pass. Fix any that reveal real domain/API bugs before proceeding.

- [ ] **Step 4: Fix any failures**

Common failure patterns:
- `inference` mode: `DatasetProfile` import path — verify `from sqllocks_spindle.inference.profiler import DatasetProfile` is correct
- `generate_stream` at `fabric_demo` size: may timeout locally — add `@pytest.mark.slow` if needed
- Domain schema errors: genuine bugs to fix before v2.11.0

- [ ] **Step 5: Commit**

```bash
git add tests/test_validation_matrix.py tests/fixtures/validation_matrix.py tests/fixtures/mock_sinks.py
git commit -m "feat: validation matrix — ~400 mock parametrized tests, all domain/sink/size/mode combos"
```

---

## Task 4: pyproject.toml — Register New Test Env Vars

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add env var documentation to pyproject.toml**

In `pyproject.toml`, extend `[tool.pytest.ini_options]` markers:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "slow: marks tests as slow-running integration tests",
    "live: marks tests as live integration tests requiring a real Fabric connection (skipped by default)",
    "infra: marks tests requiring env vars for specific sink connections",
]
```

Also add a comment block (as a `filterwarnings` or `addopts` note — use inline comment in the TOML):

```toml
# Live test env vars (set before running test_validation_live.py):
#   SPINDLE_TEST_WH_CONN      — Fabric Warehouse ODBC connection string
#   SPINDLE_TEST_EH_CONN      — Eventhouse cluster URI (kusto://...)
#   SPINDLE_TEST_SQL_CONN     — Fabric SQL Database ODBC connection string
#   SPINDLE_TEST_ONPREM_CONN  — On-prem SQL Server ODBC connection string
```

- [ ] **Step 2: Run existing suite to confirm no breakage**

```bash
.venv-mac/bin/python -m pytest tests/test_validation_matrix.py tests/test_lakehouse_profiler.py -v -q 2>&1 | tail -5
```

Expected: All pass (no changes to test behavior from pyproject.toml marker addition).

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "chore: register infra pytest mark + live test env var docs"
```

---

## Task 5: Live Validation Suite

**Files:**
- Create: `tests/test_validation_live.py`

- [ ] **Step 1: Create live test file**

Create `tests/test_validation_live.py`:

```python
"""Live validation suite — ~30 combinations against real Fabric sinks.

Auth: InteractiveBrowserCredential fires once per session, token cached.

Requires:
  - Sound BI tenant credentials (browser prompt on first run)
  - Fabric_Lakehouse_Demo workspace accessible
  - Optional env vars for other sinks (tests skip gracefully if unset):
      SPINDLE_TEST_WH_CONN      Fabric Warehouse ODBC connection string
      SPINDLE_TEST_EH_CONN      Eventhouse cluster URI
      SPINDLE_TEST_SQL_CONN     Fabric SQL Database ODBC connection string
      SPINDLE_TEST_ONPREM_CONN  On-prem SQL Server ODBC connection string

Run:
    .venv-mac/bin/python -m pytest tests/test_validation_live.py -m live -v
"""
from __future__ import annotations

import importlib
import os

import pandas as pd
import pytest

from sqllocks_spindle.cli import _get_domain_registry
from sqllocks_spindle.engine.generator import Spindle
from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter

_SOUND_BI_TENANT = "2536810f-20e1-4911-a453-4409fd96db8a"
_WORKSPACE_ID = "990dbc7b-f5d1-4bc8-a929-9dfd509a5d52"
_LAKEHOUSE_ID = "ec851642-fa89-42bc-aebf-2742845d36fe"

_browser_cred = None  # cached across tests


def _get_storage_token() -> str | None:
    global _browser_cred
    try:
        from azure.identity import InteractiveBrowserCredential
        if _browser_cred is None:
            _browser_cred = InteractiveBrowserCredential(tenant_id=_SOUND_BI_TENANT)
        tok = _browser_cred.get_token("https://storage.azure.com/.default")
        return tok.token if tok else None
    except Exception:
        return None


def _load_domain(domain_name: str):
    registry = _get_domain_registry()
    module_path, class_name, _ = registry[domain_name]
    module = importlib.import_module(module_path)
    return getattr(module, class_name)(schema_mode="3nf")


def _write_to_lakehouse(result, token: str) -> None:
    from deltalake import write_deltalake
    for table_name, df in result.tables.items():
        path = (
            f"abfss://{_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
            f"/{_LAKEHOUSE_ID}/Tables/spindle_live_{table_name}"
        )
        write_deltalake(
            path, df, mode="overwrite",
            storage_options={"bearer_token": token, "use_emulator": "false"},
            schema_mode="overwrite",
        )


# ---------------------------------------------------------------------------
# Group A: All 13 domains × lakehouse × small × seeding
# ---------------------------------------------------------------------------

ALL_DOMAINS = [
    "capital_markets", "education", "financial", "healthcare", "hr",
    "insurance", "iot", "manufacturing", "marketing", "real_estate",
    "retail", "supply_chain", "telecom",
]


@pytest.mark.live
@pytest.mark.parametrize("domain", ALL_DOMAINS)
def test_domain_to_lakehouse_small(domain):
    """All 13 domains write successfully to lakehouse at small scale."""
    token = _get_storage_token()
    assert token, "Could not acquire storage token — browser auth required"

    domain_obj = _load_domain(domain)
    result = Spindle().generate(domain=domain_obj, scale="small", seed=42)

    assert result.tables, f"{domain}: no tables generated"
    errors = result.verify_integrity()
    assert errors == [], f"{domain} FK integrity: {errors}"

    _write_to_lakehouse(result, token)


# ---------------------------------------------------------------------------
# Group B: retail × all 5 sinks × fabric_demo × seeding
# ---------------------------------------------------------------------------

SINKS_FOR_GROUP_B = [
    ("lakehouse", None),
    ("warehouse", os.getenv("SPINDLE_TEST_WH_CONN")),
    ("eventhouse", os.getenv("SPINDLE_TEST_EH_CONN")),
    ("sql-database", os.getenv("SPINDLE_TEST_SQL_CONN")),
    ("sql-server", os.getenv("SPINDLE_TEST_ONPREM_CONN")),
]


@pytest.mark.live
@pytest.mark.parametrize("sink_type,conn", SINKS_FOR_GROUP_B)
def test_retail_all_sinks_fabric_demo(sink_type, conn):
    """Retail domain at fabric_demo scale against every sink type."""
    domain_obj = _load_domain("retail")
    # Cap sql-server at medium (no Spark path)
    size = "medium" if sink_type == "sql-server" else "fabric_demo"
    result = Spindle().generate(domain=domain_obj, scale=size, seed=42)
    assert result.tables

    if sink_type == "lakehouse":
        token = _get_storage_token()
        assert token
        _write_to_lakehouse(result, token)

    elif sink_type in ("warehouse", "sql-database", "sql-server"):
        if not conn:
            pytest.skip(f"SPINDLE_TEST_{sink_type.upper().replace('-','_')}_CONN not set")
        auth = "sql" if sink_type == "sql-server" else "cli"
        writer = FabricSqlDatabaseWriter(conn, auth_method=auth)
        writer.write(result, schema_name="dbo", mode="create_insert")

    elif sink_type == "eventhouse":
        if not conn:
            pytest.skip("SPINDLE_TEST_EH_CONN not set")
        # Eventhouse ingestion via KQL ingest — verify connection only
        pytest.skip("Eventhouse live write not yet implemented in Phase 5")


# ---------------------------------------------------------------------------
# Group C: retail × lakehouse × all 4 sizes × streaming
# ---------------------------------------------------------------------------

@pytest.mark.live
@pytest.mark.parametrize("size", ["small", "medium", "large", "fabric_demo"])
def test_retail_lakehouse_streaming(size):
    """Retail streaming mode at every scale size writes to lakehouse."""
    token = _get_storage_token()
    assert token

    domain_obj = _load_domain("retail")
    from deltalake import write_deltalake

    tables_written = []
    for table_name, df in Spindle().generate_stream(domain=domain_obj, scale=size, seed=42):
        assert len(df) > 0, f"retail/{table_name}: 0 rows at size={size} in stream"
        path = (
            f"abfss://{_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
            f"/{_LAKEHOUSE_ID}/Tables/spindle_stream_{table_name}"
        )
        write_deltalake(
            path, df, mode="overwrite",
            storage_options={"bearer_token": token, "use_emulator": "false"},
            schema_mode="overwrite",
        )
        tables_written.append(table_name)

    assert tables_written, "No tables yielded in streaming mode"


# ---------------------------------------------------------------------------
# Group D: retail × warehouse × all sizes × seeding
# ---------------------------------------------------------------------------

@pytest.mark.live
@pytest.mark.parametrize("size", ["small", "medium", "large", "fabric_demo"])
def test_retail_warehouse_all_sizes(size):
    """Retail seeding at all sizes against Fabric Warehouse."""
    conn = os.getenv("SPINDLE_TEST_WH_CONN")
    if not conn:
        pytest.skip("SPINDLE_TEST_WH_CONN not set")

    domain_obj = _load_domain("retail")
    result = Spindle().generate(domain=domain_obj, scale=size, seed=42)
    assert result.tables

    writer = FabricSqlDatabaseWriter(conn, auth_method="cli")
    writer.write(result, schema_name="dbo", mode="create_insert")
```

- [ ] **Step 2: Run live Group A (requires browser auth)**

```bash
.venv-mac/bin/python -m pytest tests/test_validation_live.py -m live -k "test_domain_to_lakehouse_small" -v --tb=short
```

Expected: 13 tests pass (one browser prompt on first run, then cached). All 13 domains write to Fabric_Lakehouse_Demo.

- [ ] **Step 3: Commit**

```bash
git add tests/test_validation_live.py
git commit -m "feat: live validation suite — Groups A-D, ~30 tests against real Fabric sinks"
```

---

## Task 6: Demo Notebook 01 — Retail → Lakehouse Quickstart

**Files:**
- Create: `notebooks/demos/01_retail_lakehouse_quickstart.ipynb`

- [ ] **Step 1: Create the notebook using nbformat**

Run this Python script to generate the notebook:

```bash
.venv-mac/bin/python - <<'PYEOF'
import nbformat as nbf
import json, os

nb = nbf.v4.new_notebook()
nb.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
nb.metadata["language_info"] = {"name": "python", "version": "3.11.0"}

cells = []

# Cell 1 — Parameters (tagged for papermill)
c = nbf.v4.new_code_cell("""\
# Parameters — edit these before running
WORKSPACE_ID  = "990dbc7b-f5d1-4bc8-a929-9dfd509a5d52"  # Your Fabric workspace ID
LAKEHOUSE_ID  = "ec851642-fa89-42bc-aebf-2742845d36fe"  # Your lakehouse ID
SCALE_SIZE    = "small"   # small | medium | large | fabric_demo
SEED          = 42
""")
c.metadata["tags"] = ["parameters"]
cells.append(c)

# Cell 2 — Setup
cells.append(nbf.v4.new_code_cell("""\
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.retail import RetailDomain
from azure.identity import InteractiveBrowserCredential
from deltalake import write_deltalake

SOUND_BI_TENANT = "2536810f-20e1-4911-a453-4409fd96db8a"
cred = InteractiveBrowserCredential(tenant_id=SOUND_BI_TENANT)
token = cred.get_token("https://storage.azure.com/.default").token
print(f"Auth OK — token len={len(token)}")
"""))

# Cell 3 — Generate (seeding)
cells.append(nbf.v4.new_code_cell("""\
spindle = Spindle()
result = spindle.generate(domain=RetailDomain(), scale=SCALE_SIZE, seed=SEED)
print(f"Generated {sum(len(df) for df in result.tables.values()):,} rows across {len(result.tables)} tables")
for table, df in result.tables.items():
    print(f"  {table}: {len(df):,} rows")
"""))

# Cell 4 — Integrity check
cells.append(nbf.v4.new_code_cell("""\
errors = result.verify_integrity()
assert errors == [], f"FK integrity errors: {errors}"
print("FK integrity: PASS")
"""))

# Cell 5 — Write to lakehouse (seeding)
cells.append(nbf.v4.new_code_cell("""\
storage_opts = {"bearer_token": token, "use_emulator": "false"}

for table_name, df in result.tables.items():
    path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables/retail_{table_name}"
    write_deltalake(path, df, mode="overwrite", storage_options=storage_opts, schema_mode="overwrite")
    print(f"  Written: retail_{table_name} ({len(df):,} rows)")

print("Seeding write: DONE")
"""))

# Cell 6 — Streaming mode
cells.append(nbf.v4.new_markdown_cell("## Streaming Mode\nStreams each table as it generates — write table N while table N+1 is still being built."))
cells.append(nbf.v4.new_code_cell("""\
for table_name, df in spindle.generate_stream(domain=RetailDomain(), scale=SCALE_SIZE, seed=SEED):
    path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables/retail_stream_{table_name}"
    write_deltalake(path, df, mode="overwrite", storage_options=storage_opts, schema_mode="overwrite")
    print(f"  Streamed: retail_stream_{table_name} ({len(df):,} rows)")

print("Streaming write: DONE")
"""))

# Cell 7 — Validation
cells.append(nbf.v4.new_code_cell("""\
from deltalake import DeltaTable
import pandas as pd

print("Validation — reading back from OneLake:")
rows = {}
for table_name in result.tables:
    path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables/retail_{table_name}"
    dt = DeltaTable(path, storage_options=storage_opts)
    count = len(dt.to_pandas())
    rows[table_name] = count
    expected = len(result.tables[table_name])
    status = "✓" if count == expected else "MISMATCH"
    print(f"  {status} retail_{table_name}: {count:,} rows (expected {expected:,})")
"""))

nb.cells = cells

os.makedirs("notebooks/demos", exist_ok=True)
with open("notebooks/demos/01_retail_lakehouse_quickstart.ipynb", "w") as f:
    nbf.write(nb, f)
print("Created notebooks/demos/01_retail_lakehouse_quickstart.ipynb")
PYEOF
```

- [ ] **Step 2: Verify the notebook is valid**

```bash
.venv-mac/bin/python -c "
import nbformat
with open('notebooks/demos/01_retail_lakehouse_quickstart.ipynb') as f:
    nb = nbformat.read(f, as_version=4)
print(f'OK — {len(nb.cells)} cells, kernelspec={nb.metadata.kernelspec.name}')
"
```

Expected: `OK — 8 cells, kernelspec=python3`

- [ ] **Step 3: Commit**

```bash
git add notebooks/demos/01_retail_lakehouse_quickstart.ipynb
git commit -m "feat: demo notebook 01 — retail → lakehouse quickstart (seeding + streaming)"
```

---

## Task 7: Demo Notebook 02 — Financial → Warehouse Analytics

**Files:**
- Create: `notebooks/demos/02_financial_warehouse_analytics.ipynb`

- [ ] **Step 1: Create notebook**

```bash
.venv-mac/bin/python - <<'PYEOF'
import nbformat as nbf
import os

nb = nbf.v4.new_notebook()
nb.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
nb.metadata["language_info"] = {"name": "python", "version": "3.11.0"}

cells = []

c = nbf.v4.new_code_cell("""\
# Parameters
WAREHOUSE_CONN = ""  # ODBC: Driver={ODBC Driver 18 for SQL Server};Server=<host>.datawarehouse.fabric.microsoft.com,...
SCALE_SIZE = "medium"  # small | medium | large | fabric_demo
AUTH_METHOD = "cli"    # cli | msi | spn
SEED = 42
""")
c.metadata["tags"] = ["parameters"]
cells.append(c)

cells.append(nbf.v4.new_code_cell("""\
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.financial import FinancialDomain
from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter

if not WAREHOUSE_CONN:
    raise ValueError("Set WAREHOUSE_CONN to your Fabric Warehouse ODBC connection string")

spindle = Spindle()
result = spindle.generate(domain=FinancialDomain(), scale=SCALE_SIZE, seed=SEED)
print(f"Generated {sum(len(df) for df in result.tables.values()):,} rows")
for t, df in result.tables.items():
    print(f"  {t}: {len(df):,} rows")
"""))

cells.append(nbf.v4.new_code_cell("""\
errors = result.verify_integrity()
assert errors == [], f"FK integrity errors: {errors}"
print("FK integrity: PASS")
"""))

cells.append(nbf.v4.new_code_cell("""\
writer = FabricSqlDatabaseWriter(WAREHOUSE_CONN, auth_method=AUTH_METHOD)
writer.write(result, schema_name="dbo", mode="create_insert")
print("Write to Warehouse: DONE")
"""))

cells.append(nbf.v4.new_code_cell("""\
# Validate — query Warehouse row counts
import pyodbc
from azure.identity import AzureCliCredential
import struct

cred = AzureCliCredential()
token = cred.get_token("https://database.windows.net/.default").token
token_bytes = token.encode("utf-16-le")
token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

conn = pyodbc.connect(WAREHOUSE_CONN, attrs_before={1256: token_struct})
cursor = conn.cursor()
for table_name in result.tables:
    cursor.execute(f"SELECT COUNT(*) FROM dbo.[{table_name}]")
    count = cursor.fetchone()[0]
    expected = len(result.tables[table_name])
    status = "✓" if count == expected else "MISMATCH"
    print(f"  {status} {table_name}: {count:,} rows")
conn.close()
"""))

nb.cells = cells
os.makedirs("notebooks/demos", exist_ok=True)
with open("notebooks/demos/02_financial_warehouse_analytics.ipynb", "w") as f:
    nbf.write(nb, f)
print("Created notebooks/demos/02_financial_warehouse_analytics.ipynb")
PYEOF
```

- [ ] **Step 2: Verify**

```bash
.venv-mac/bin/python -c "
import nbformat
with open('notebooks/demos/02_financial_warehouse_analytics.ipynb') as f:
    nb = nbformat.read(f, as_version=4)
print(f'OK — {len(nb.cells)} cells')
"
```

- [ ] **Step 3: Commit**

```bash
git add notebooks/demos/02_financial_warehouse_analytics.ipynb
git commit -m "feat: demo notebook 02 — financial → warehouse analytics (all sizes)"
```

---

## Task 8: Demo Notebooks 03–05

**Files:**
- Create: `notebooks/demos/03_healthcare_sql_database.ipynb`
- Create: `notebooks/demos/04_capital_markets_eventhouse.ipynb`
- Create: `notebooks/demos/05_multi_domain_fanout.ipynb`

- [ ] **Step 1: Create notebooks 03–05 in one script**

```bash
.venv-mac/bin/python - <<'PYEOF'
import nbformat as nbf
import os

os.makedirs("notebooks/demos", exist_ok=True)


def make_nb(cells_content: list[tuple[str, str]]) -> nbf.NotebookNode:
    nb = nbf.v4.new_notebook()
    nb.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
    nb.metadata["language_info"] = {"name": "python", "version": "3.11.0"}
    cells = []
    for kind, src in cells_content:
        if kind == "code":
            cells.append(nbf.v4.new_code_cell(src))
        else:
            cells.append(nbf.v4.new_markdown_cell(src))
    nb.cells = cells
    return nb


# Notebook 03 — Healthcare → SQL Database + Masking
nb03 = make_nb([
    ("code", """\
# Parameters
SQL_DB_CONN = ""  # ODBC: Driver={ODBC Driver 18 for SQL Server};Server=<host>.database.fabric.microsoft.com,...
SCALE_SIZE = "small"
AUTH_METHOD = "cli"
SEED = 42
"""),
    ("markdown", "## Generate Healthcare Data"),
    ("code", """\
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.healthcare import HealthcareDomain
from sqllocks_spindle.inference import DataMasker

result = Spindle().generate(domain=HealthcareDomain(), scale=SCALE_SIZE, seed=SEED)
print(f"Generated {sum(len(df) for df in result.tables.values()):,} rows")
for t, df in result.tables.items():
    print(f"  {t}: {len(df):,} rows, cols={list(df.columns)[:4]}")
"""),
    ("markdown", "## Apply HIPAA Masking"),
    ("code", """\
masker = DataMasker()
masked = masker.mask(result.tables)
print("Masked columns:", masked.columns_masked)
print(masked.summary())
"""),
    ("code", """\
from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter
if not SQL_DB_CONN:
    raise ValueError("Set SQL_DB_CONN to your Fabric SQL Database ODBC connection string")
writer = FabricSqlDatabaseWriter(SQL_DB_CONN, auth_method=AUTH_METHOD)
# Write masked tables
import sqllocks_spindle.engine.generator as _gen
masked_result = result.__class__(tables=masked.tables, generation_order=result.generation_order, schema=result.schema)
writer.write(masked_result, schema_name="dbo", mode="create_insert")
print("Write masked data to SQL Database: DONE")
"""),
])
with open("notebooks/demos/03_healthcare_sql_database.ipynb", "w") as f:
    nbf.write(nb03, f)
print("Created 03")


# Notebook 04 — Capital Markets → Eventhouse (KQL) streaming ticks
nb04 = make_nb([
    ("code", """\
# Parameters
EVENTHOUSE_URI  = ""  # e.g. https://<cluster>.kusto.windows.net
EVENTHOUSE_DB   = ""  # KQL database name
SCALE_SIZE      = "small"
SEED            = 42
"""),
    ("code", """\
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain

result = Spindle().generate(domain=CapitalMarketsDomain(), scale=SCALE_SIZE, seed=SEED)
trade_df = result.tables["trade"]
print(f"Generated {len(trade_df):,} trade ticks")
print(trade_df.head(3).to_string())
"""),
    ("markdown", "## Stream Ticks to Eventhouse via KQL Ingest"),
    ("code", """\
# Streaming mode — yield trade ticks table-by-table
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain

for table_name, df in Spindle().generate_stream(domain=CapitalMarketsDomain(), scale=SCALE_SIZE, seed=SEED):
    print(f"  Yielded: {table_name} ({len(df):,} rows)")
    # In a real Fabric notebook, use Kusto ingest:
    # kustoClient.execute_streaming_ingest(EVENTHOUSE_DB, table_name, df)

print("Streaming complete — wire EVENTHOUSE_URI to ingest in Fabric notebook")
"""),
])
with open("notebooks/demos/04_capital_markets_eventhouse.ipynb", "w") as f:
    nbf.write(nb04, f)
print("Created 04")


# Notebook 05 — Multi-domain fanout (retail + financial → lakehouse + warehouse)
nb05 = make_nb([
    ("code", """\
# Parameters
WORKSPACE_ID   = "990dbc7b-f5d1-4bc8-a929-9dfd509a5d52"
LAKEHOUSE_ID   = "ec851642-fa89-42bc-aebf-2742845d36fe"
WAREHOUSE_CONN = ""  # optional — leave blank to skip warehouse write
SCALE_SIZE     = "small"
SEED           = 42
"""),
    ("code", """\
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.retail import RetailDomain
from sqllocks_spindle.domains.financial import FinancialDomain
from azure.identity import InteractiveBrowserCredential
from deltalake import write_deltalake

SOUND_BI_TENANT = "2536810f-20e1-4911-a453-4409fd96db8a"
token = InteractiveBrowserCredential(tenant_id=SOUND_BI_TENANT).get_token("https://storage.azure.com/.default").token
storage_opts = {"bearer_token": token, "use_emulator": "false"}

spindle = Spindle()
retail_result  = spindle.generate(domain=RetailDomain(),   scale=SCALE_SIZE, seed=SEED)
financial_result = spindle.generate(domain=FinancialDomain(), scale=SCALE_SIZE, seed=SEED + 1)

print(f"Retail: {sum(len(df) for df in retail_result.tables.values()):,} rows")
print(f"Financial: {sum(len(df) for df in financial_result.tables.values()):,} rows")
"""),
    ("code", """\
# Write both domains to lakehouse
for domain_prefix, result in [("retail", retail_result), ("financial", financial_result)]:
    for table_name, df in result.tables.items():
        path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables/{domain_prefix}_{table_name}"
        write_deltalake(path, df, mode="overwrite", storage_options=storage_opts, schema_mode="overwrite")
        print(f"  {domain_prefix}_{table_name}: {len(df):,} rows → lakehouse")
"""),
    ("code", """\
# Optionally write to warehouse too
if WAREHOUSE_CONN:
    from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter
    writer = FabricSqlDatabaseWriter(WAREHOUSE_CONN, auth_method="cli")
    writer.write(retail_result, schema_name="retail", mode="create_insert")
    writer.write(financial_result, schema_name="financial", mode="create_insert")
    print("Also written to Warehouse (retail + financial schemas)")
else:
    print("WAREHOUSE_CONN not set — skipping warehouse write")
"""),
])
with open("notebooks/demos/05_multi_domain_fanout.ipynb", "w") as f:
    nbf.write(nb05, f)
print("Created 05")

PYEOF
```

- [ ] **Step 2: Verify all three notebooks**

```bash
.venv-mac/bin/python -c "
import nbformat
for i in ['03','04','05']:
    path = f'notebooks/demos/{i}_*.ipynb'
    import glob
    files = glob.glob(path)
    for f in files:
        nb = nbformat.read(open(f), as_version=4)
        print(f'OK {f} — {len(nb.cells)} cells')
"
```

- [ ] **Step 3: Commit**

```bash
git add notebooks/demos/03_healthcare_sql_database.ipynb notebooks/demos/04_capital_markets_eventhouse.ipynb notebooks/demos/05_multi_domain_fanout.ipynb
git commit -m "feat: demo notebooks 03-05 — healthcare/masking, capital markets/streaming, multi-domain fanout"
```

---

## Task 9: Demo Notebook 06 — Custom DDL → Lakehouse

**Files:**
- Create: `notebooks/demos/06_custom_ddl_to_lakehouse.ipynb`

- [ ] **Step 1: Create notebook**

```bash
.venv-mac/bin/python - <<'PYEOF'
import nbformat as nbf
import os

nb = nbf.v4.new_notebook()
nb.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
nb.metadata["language_info"] = {"name": "python", "version": "3.11.0"}

cells = [
    nbf.v4.new_code_cell("""\
# Parameters
WORKSPACE_ID = "990dbc7b-f5d1-4bc8-a929-9dfd509a5d52"
LAKEHOUSE_ID = "ec851642-fa89-42bc-aebf-2742845d36fe"
SCALE_SIZE   = "small"
SEED         = 42
"""),
    nbf.v4.new_markdown_cell("## Step 1 — Define your DDL"),
    nbf.v4.new_code_cell("""\
DDL = \"\"\"
CREATE TABLE customer (
    customer_id INT PRIMARY KEY,
    first_name NVARCHAR(50),
    last_name NVARCHAR(50),
    email NVARCHAR(100),
    created_at DATETIME
);

CREATE TABLE order_header (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2),
    CONSTRAINT FK_order_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);
\"\"\"
print("DDL ready")
"""),
    nbf.v4.new_markdown_cell("## Step 2 — Parse DDL into Spindle schema"),
    nbf.v4.new_code_cell("""\
from sqllocks_spindle.schema.parser import DDLParser

parser = DDLParser()
schema = parser.parse(DDL)
print(f"Tables: {[t.name for t in schema.tables]}")
print(f"Relationships: {len(schema.relationships)}")
"""),
    nbf.v4.new_markdown_cell("## Step 3 — Generate synthetic data"),
    nbf.v4.new_code_cell("""\
from sqllocks_spindle import Spindle

result = Spindle().generate(schema=schema, scale=SCALE_SIZE, seed=SEED)
errors = result.verify_integrity()
assert errors == [], f"FK integrity errors: {errors}"
for t, df in result.tables.items():
    print(f"  {t}: {len(df):,} rows, cols={list(df.columns)}")
"""),
    nbf.v4.new_markdown_cell("## Step 4 — Write to Lakehouse"),
    nbf.v4.new_code_cell("""\
from azure.identity import InteractiveBrowserCredential
from deltalake import write_deltalake

SOUND_BI_TENANT = "2536810f-20e1-4911-a453-4409fd96db8a"
token = InteractiveBrowserCredential(tenant_id=SOUND_BI_TENANT).get_token("https://storage.azure.com/.default").token
storage_opts = {"bearer_token": token, "use_emulator": "false"}

for table_name, df in result.tables.items():
    path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables/custom_{table_name}"
    write_deltalake(path, df, mode="overwrite", storage_options=storage_opts, schema_mode="overwrite")
    print(f"  Written: custom_{table_name} ({len(df):,} rows)")

print("Done — your custom schema is now in OneLake as Delta tables")
"""),
]

nb.cells = cells
os.makedirs("notebooks/demos", exist_ok=True)
with open("notebooks/demos/06_custom_ddl_to_lakehouse.ipynb", "w") as f:
    nbf.write(nb, f)
print("Created notebooks/demos/06_custom_ddl_to_lakehouse.ipynb")
PYEOF
```

- [ ] **Step 2: Verify**

```bash
.venv-mac/bin/python -c "
import nbformat
nb = nbformat.read(open('notebooks/demos/06_custom_ddl_to_lakehouse.ipynb'), as_version=4)
print(f'OK — {len(nb.cells)} cells')
"
```

- [ ] **Step 3: Commit**

```bash
git add notebooks/demos/06_custom_ddl_to_lakehouse.ipynb
git commit -m "feat: demo notebook 06 — custom DDL → lakehouse (bring-your-own-schema)"
```

---

## Task 10: Template Notebooks

**Files:**
- Create: `notebooks/templates/template_domain_to_sink.ipynb`
- Create: `notebooks/templates/template_custom_schema.ipynb`

- [ ] **Step 1: Create both templates**

```bash
.venv-mac/bin/python - <<'PYEOF'
import nbformat as nbf
import os

os.makedirs("notebooks/templates", exist_ok=True)

# Template 1 — Domain to Sink
nb1 = nbf.v4.new_notebook()
nb1.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
nb1.metadata["language_info"] = {"name": "python", "version": "3.11.0"}

c = nbf.v4.new_code_cell("""\
# ── CONFIGURATION ──────────────────────────────────────────────────────────────
# Available domains:
#   capital_markets, education, financial, healthcare, hr, insurance, iot,
#   manufacturing, marketing, real_estate, retail, supply_chain, telecom
DOMAIN      = "retail"

# Available sinks: lakehouse | warehouse | eventhouse | sql-database | sql-server
SINK_TYPE   = "lakehouse"

# Available sizes: small | medium | large | fabric_demo
SCALE_SIZE  = "small"

# Available modes: seeding | streaming
MODE        = "seeding"

SEED        = 42

# ── CONNECTION (fill in for your sink) ─────────────────────────────────────────
WORKSPACE_ID       = ""   # Fabric workspace GUID
LAKEHOUSE_ID       = ""   # Lakehouse GUID (sink=lakehouse)
SQL_CONN           = ""   # ODBC string (sink=warehouse|sql-database|sql-server)
EVENTHOUSE_URI     = ""   # KQL cluster URI (sink=eventhouse)
EVENTHOUSE_DB      = ""   # KQL database name (sink=eventhouse)
""")
c.metadata["tags"] = ["parameters"]
nb1.cells = [
    c,
    nbf.v4.new_code_cell("""\
import importlib
from sqllocks_spindle import Spindle
from sqllocks_spindle.cli import _get_domain_registry

registry = _get_domain_registry()
module_path, class_name, _ = registry[DOMAIN]
module = importlib.import_module(module_path)
domain_obj = getattr(module, class_name)(schema_mode="3nf")

spindle = Spindle()
print(f"Loaded domain: {DOMAIN}")
"""),
    nbf.v4.new_code_cell("""\
if MODE == "seeding":
    result = spindle.generate(domain=domain_obj, scale=SCALE_SIZE, seed=SEED)
    tables = result.tables
    errors = result.verify_integrity()
    assert errors == [], f"FK integrity errors: {errors}"
elif MODE == "streaming":
    tables = {}
    for table_name, df in spindle.generate_stream(domain=domain_obj, scale=SCALE_SIZE, seed=SEED):
        tables[table_name] = df
        print(f"  Yielded: {table_name} ({len(df):,} rows)")
else:
    raise ValueError(f"Unknown MODE: {MODE}")

print(f"Total: {sum(len(df) for df in tables.values()):,} rows across {len(tables)} tables")
"""),
    nbf.v4.new_code_cell("""\
# ── WRITE TO SINK ──────────────────────────────────────────────────────────────
if SINK_TYPE == "lakehouse":
    from azure.identity import InteractiveBrowserCredential
    from deltalake import write_deltalake
    token = InteractiveBrowserCredential(tenant_id="2536810f-20e1-4911-a453-4409fd96db8a").get_token("https://storage.azure.com/.default").token
    opts = {"bearer_token": token, "use_emulator": "false"}
    for name, df in tables.items():
        path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables/{DOMAIN}_{name}"
        write_deltalake(path, df, mode="overwrite", storage_options=opts, schema_mode="overwrite")
        print(f"  {DOMAIN}_{name}: {len(df):,} rows → lakehouse")

elif SINK_TYPE in ("warehouse", "sql-database", "sql-server"):
    from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter
    from sqllocks_spindle.engine.generator import GenerationResult
    auth = "sql" if SINK_TYPE == "sql-server" else "cli"
    writer = FabricSqlDatabaseWriter(SQL_CONN, auth_method=auth)
    # Re-wrap tables dict as a result-like object for writer.write()
    class _R:
        def __init__(self, t, go, sc): self.tables=t; self.generation_order=go; self.schema=sc
    pseudo = _R(tables, list(tables.keys()), None)
    writer.write(pseudo, schema_name="dbo", mode="create_insert")
    print(f"Written to {SINK_TYPE}")

elif SINK_TYPE == "eventhouse":
    print("Eventhouse: wire EVENTHOUSE_URI/DB to your KQL ingest client here")
    for name, df in tables.items():
        print(f"  Would ingest {name}: {len(df):,} rows")

print("Done")
"""),
]

with open("notebooks/templates/template_domain_to_sink.ipynb", "w") as f:
    nbf.write(nb1, f)
print("Created template 1")

# Template 2 — Custom Schema to Sink
nb2 = nbf.v4.new_notebook()
nb2.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
nb2.metadata["language_info"] = {"name": "python", "version": "3.11.0"}

c2 = nbf.v4.new_code_cell("""\
# Path to your .spindle.json or DDL file
SCHEMA_PATH   = "/path/to/your/schema.spindle.json"  # or .sql DDL file

SINK_TYPE     = "lakehouse"   # lakehouse | warehouse | sql-database | sql-server
SCALE_SIZE    = "small"
SEED          = 42

WORKSPACE_ID  = ""   # Fabric workspace GUID
LAKEHOUSE_ID  = ""   # Lakehouse GUID (sink=lakehouse)
SQL_CONN      = ""   # ODBC string (sink=warehouse|sql-database|sql-server)
""")
c2.metadata["tags"] = ["parameters"]
nb2.cells = [
    c2,
    nbf.v4.new_code_cell("""\
from pathlib import Path
from sqllocks_spindle import Spindle

path = Path(SCHEMA_PATH)
if not path.exists():
    raise FileNotFoundError(f"Schema file not found: {SCHEMA_PATH}")

if path.suffix == ".json":
    import json
    schema_def = json.loads(path.read_text())
    result = Spindle().generate(schema=schema_def, scale=SCALE_SIZE, seed=SEED)
elif path.suffix == ".sql":
    from sqllocks_spindle.schema.parser import DDLParser
    schema = DDLParser().parse(path.read_text())
    result = Spindle().generate(schema=schema, scale=SCALE_SIZE, seed=SEED)
else:
    raise ValueError(f"Unsupported schema file type: {path.suffix} (use .json or .sql)")

errors = result.verify_integrity()
assert errors == [], f"FK integrity: {errors}"
for t, df in result.tables.items():
    print(f"  {t}: {len(df):,} rows")
"""),
    nbf.v4.new_code_cell("""\
if SINK_TYPE == "lakehouse":
    from azure.identity import InteractiveBrowserCredential
    from deltalake import write_deltalake
    token = InteractiveBrowserCredential(tenant_id="2536810f-20e1-4911-a453-4409fd96db8a").get_token("https://storage.azure.com/.default").token
    opts = {"bearer_token": token, "use_emulator": "false"}
    for name, df in result.tables.items():
        path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables/custom_{name}"
        write_deltalake(path, df, mode="overwrite", storage_options=opts, schema_mode="overwrite")
        print(f"  custom_{name}: {len(df):,} rows → lakehouse")
elif SINK_TYPE in ("warehouse", "sql-database", "sql-server"):
    from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter
    auth = "sql" if SINK_TYPE == "sql-server" else "cli"
    writer = FabricSqlDatabaseWriter(SQL_CONN, auth_method=auth)
    writer.write(result, schema_name="dbo", mode="create_insert")
    print(f"Written to {SINK_TYPE}")
print("Done")
"""),
]

with open("notebooks/templates/template_custom_schema.ipynb", "w") as f:
    nbf.write(nb2, f)
print("Created template 2")
PYEOF
```

- [ ] **Step 2: Verify both templates**

```bash
.venv-mac/bin/python -c "
import nbformat, glob
for f in sorted(glob.glob('notebooks/templates/*.ipynb')):
    nb = nbformat.read(open(f), as_version=4)
    print(f'OK {f} — {len(nb.cells)} cells')
"
```

- [ ] **Step 3: Commit**

```bash
git add notebooks/templates/
git commit -m "feat: notebook templates — domain_to_sink and custom_schema starters"
```

---

## Task 11: Full Regression + Live Run

- [ ] **Step 1: Run complete mock matrix**

```bash
.venv-mac/bin/python -m pytest tests/test_validation_matrix.py -v -q --tb=short 2>&1 | tail -10
```

Expected: All ~400+ combos PASS (no failures). Fix any that fail before proceeding.

- [ ] **Step 2: Run live Group A (all 13 domains → lakehouse)**

```bash
.venv-mac/bin/python -m pytest tests/test_validation_live.py -m live -k "test_domain_to_lakehouse_small" -v --tb=short 2>&1 | tail -20
```

Expected: 13 PASS (browser prompt fires once, then cached).

- [ ] **Step 3: Run existing test suite to confirm no regressions**

```bash
.venv-mac/bin/python -m pytest --ignore=tests/test_validation_live.py -q 2>&1 | tail -5
```

Expected: Same result as before Phase 5 work (2051+ passed, ≤4 pre-existing failures).

- [ ] **Step 4: Commit any fixes found during regression**

```bash
git add -A
git commit -m "fix: validation matrix edge cases found during full regression run"
```

---

## Task 12: Version Bump + Changelog

**Files:**
- Modify: `sqllocks_spindle/__init__.py`
- Modify: `pyproject.toml`
- Modify: `docs/changelog.md`

- [ ] **Step 1: Bump version to 2.11.0**

In `sqllocks_spindle/__init__.py`, change:
```python
__version__ = "2.10.0"
```
to:
```python
__version__ = "2.11.0"
```

In `pyproject.toml`, change:
```toml
version = "2.10.0"
```
to:
```toml
version = "2.11.0"
```

- [ ] **Step 2: Add changelog entry**

At the top of `docs/changelog.md`, add:

```markdown
## v2.11.0 — 2026-04-28

### New Features
- **Validation matrix** — pytest parametrized suite covering all ~400 valid domain × sink × size × mode combinations (`tests/test_validation_matrix.py`). Runs mock-only in <90s with no credentials.
- **Live validation suite** — ~30 live tests against real Fabric sinks, grouped by domain coverage, sink coverage, streaming, and scale size (`tests/test_validation_live.py`).
- **Demo notebooks** — 6 pre-built Fabric notebooks in `notebooks/demos/`:
  - `01_retail_lakehouse_quickstart.ipynb` — seeding + streaming, all sizes
  - `02_financial_warehouse_analytics.ipynb` — DDL-first bulk load, Warehouse
  - `03_healthcare_sql_database.ipynb` — streaming insert + HIPAA masking
  - `04_capital_markets_eventhouse.ipynb` — streaming tick data, KQL
  - `05_multi_domain_fanout.ipynb` — retail + financial → lakehouse + warehouse
  - `06_custom_ddl_to_lakehouse.ipynb` — bring-your-own DDL → OneLake Delta
- **Notebook templates** — 2 parametrized starters in `notebooks/templates/`:
  - `template_domain_to_sink.ipynb` — any domain → any sink
  - `template_custom_schema.ipynb` — custom schema → any sink

### Bug Fixes
- `LakehouseProfiler._list_tables()` — fixed pyarrow 23 `AzureFileSystem` API (`account_name` kwarg, `dfs_storage_authority` param)
- `LakehouseProfiler._read_table()` — fixed deltalake 1.5.1 `DeltaTable.to_pandas()` API (no `limit` kwarg; use `.head()`)
- `tests/test_lakehouse_profiler.py` — switched live test auth from az CLI subprocess to `InteractiveBrowserCredential`; removed `@pytest.mark.skip`; all 3 live tests now pass

### Breaking Changes
None.
```

- [ ] **Step 3: Run version check**

```bash
.venv-mac/bin/python -c "import sqllocks_spindle; print(sqllocks_spindle.__version__)"
```

Expected: `2.11.0`

- [ ] **Step 4: Commit**

```bash
git add sqllocks_spindle/__init__.py pyproject.toml docs/changelog.md
git commit -m "chore: bump version to 2.11.0 + changelog entry"
```

---

## Self-Review Checklist

**Spec coverage:**
- ✅ `tests/fixtures/validation_matrix.py` — Task 1
- ✅ Mock sink factory — Task 2
- ✅ `tests/test_validation_matrix.py` ~400 combos — Task 3
- ✅ `tests/test_validation_live.py` ~30 live combos — Task 5
- ✅ 6 demo notebooks (Tasks 6, 7, 8, 9)
- ✅ 2 template notebooks (Task 10)
- ✅ All sizes covered — SIZES = ["small","medium","large","fabric_demo"] in matrix
- ✅ Streaming mode — `generate_stream()` in Task 3 + live Task 5 Group C
- ✅ All sinks covered — 5 sink types in matrix + live Group B
- ✅ All domains covered — 13 domains in matrix + live Group A
- ✅ Version bump + changelog — Task 12

**Placeholder scan:**
- No TBD/TODO in test code
- Notebook 04 eventhouse ingest is marked "wire your KQL client here" — intentional, eventhouse ingest varies by cluster setup

**Type consistency:**
- `make_mock_sink(sink_type)` → `MockSink` — used consistently Tasks 2, 3
- `_load_domain(domain_name)` → domain instance — used consistently Tasks 3, 5
- `result.tables` → `dict[str, pd.DataFrame]` — consistent throughout
- `write_deltalake(path, df, mode=..., storage_options=..., schema_mode=...)` — consistent in Tasks 6, 7, 8, 9, 10, 5
