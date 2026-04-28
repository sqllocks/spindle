# Spindle Phase 5 — Validation Matrix & Demo Notebooks

> **Date:** 2026-04-28
> **Author:** Jonathan Stewart
> **Status:** Approved
> **Version target:** v2.11.0

---

## Overview

Phase 5 validates every supported combination of domain × sink × size × mode and packages the results into a set of client-facing demo notebooks and reusable templates. No new sink code is required — `FabricSqlDatabaseWriter` already covers all SQL Server targets (on-prem, Azure SQL DB, Azure SQL MI) and was confirmed ship-ready in Phase 4.

### Deliverables

| # | Artifact | Location | Purpose |
|---|---|---|---|
| 1 | `test_validation_matrix.py` | `tests/` | Mock-based full matrix, ~400 combos, <90s |
| 2 | `test_validation_live.py` | `tests/` | Live subset, ~30 combos, `@pytest.mark.live` |
| 3 | `validation_matrix.py` | `tests/fixtures/` | Shared matrix builder + filters |
| 4 | Demo notebooks (6) | `notebooks/demos/` | Pre-built Fabric notebooks for client demos |
| 5 | Template notebooks (2) | `notebooks/templates/` | Parametrized starters for users |

---

## Section 1 — Matrix Builder (`tests/fixtures/validation_matrix.py`)

### Dimensions

```python
DOMAINS = [
    "capital_markets", "education", "financial", "healthcare",
    "hr", "insurance", "iot", "manufacturing", "marketing",
    "real_estate", "retail", "supply_chain", "telecom",
]

SINKS = ["lakehouse", "warehouse", "eventhouse", "sql-database", "sql-server"]

SIZES = ["small", "medium", "large", "fabric_demo"]

MODES = ["seeding", "streaming", "inference"]
```

### Filter Rules

Applied by `build_matrix()` to reduce ~3,120 raw combos to ~400 valid:

| Rule | Condition | Action |
|---|---|---|
| Spark-only | `sink in ("sql-server",)` and `scale_mode == "spark"` | Skip |
| Size cap | `sink == "sql-server"` and `size == "fabric_demo"` | Cap at `medium` (local scale mode only) |
| Inference | Mode is `inference` and domain has no bundled `.spindle.json` profile | Skip |
| Streaming + SQL Server | `mode == "streaming"` and `sink == "sql-server"` | Skip — streaming chunked write to SQL Server deferred to Phase 6 |
| Duplicate after cap | Same effective combo after size cap applied | Deduplicate |

### `build_matrix()` return type

```python
def build_matrix() -> list[tuple[str, str, str, str]]:
    """Returns list of (domain, sink, size, mode) tuples — all valid combinations."""
```

---

## Section 2 — Mock Validation Suite (`tests/test_validation_matrix.py`)

### Structure

```python
@pytest.mark.parametrize("domain,sink,size,mode", build_matrix())
def test_generation_to_sink(domain, sink, size, mode, mock_sink_factory):
    result = run_generation(domain=domain, size=size, mode=mode)
    sink_writer = mock_sink_factory(sink)
    sink_writer.write(result)
    assert_result(result, domain, size)
```

### Assertions (`assert_result`)

1. No exception during generation or write
2. Every declared table has `row_count > 0`
3. No NaN primary keys in any table
4. FK integrity passes — all child FK values resolve to a parent PK
5. Row counts within 10% of the size spec for that domain
6. All declared columns present in output DataFrame

### Mock Sink Factory

Each sink type gets a mock that records write calls and returns success:

```python
class MockSink:
    def __init__(self, sink_type: str): ...
    def write(self, result: GenerationResult) -> None: ...
    def assert_written(self, min_rows: int = 1) -> None: ...
```

Sinks mapped:
- `lakehouse` → `MockLakehouseSink` (records ABFSS path + payload)
- `warehouse` → `MockWarehouseSink` (records DDL + INSERT batches)
- `eventhouse` → `MockKQLSink` (records ingest payloads)
- `sql-database` → `MockSqlDatabaseSink` (wraps `FabricSqlDatabaseWriter` with mock connection)
- `sql-server` → `MockSqlServerSink` (wraps `FabricSqlDatabaseWriter`, `auth_method="sql"`, mock connection)

### Runtime target

Full mock matrix: **<90 seconds** on local dev machine (M-series Mac).

---

## Section 3 — Live Validation Suite (`tests/test_validation_live.py`)

All tests tagged `@pytest.mark.live`. Auth via `InteractiveBrowserCredential(tenant_id=SOUND_BI_TENANT)` — browser fires once per session, token cached.

### Live Subset (~30 tests)

**Group A — Domain coverage (all 13 domains × lakehouse × small × seeding)**
Validates every domain writes successfully to at least one real sink.

**Group B — Sink coverage (retail × all 5 sinks × fabric_demo × seeding)**
Validates every sink type at production scale.

**Group C — Streaming (retail × lakehouse × all 4 sizes × streaming)**
Validates streaming mode end-to-end at every scale.

**Group D — Size scaling (retail × warehouse × all 4 sizes × seeding)**
Validates that scale sizes don't break at the large/fabric_demo boundary.

### Live Test Assertions

Same as mock suite, plus:
- Actual row count in sink matches generated count (read-back verification)
- No auth errors (401/403)
- Write completes within timeout (size-dependent: small=30s, medium=120s, large=300s, fabric_demo=600s)

### Fabric Sink Config

```python
LIVE_CONFIG = {
    "workspace_id": "990dbc7b-f5d1-4bc8-a929-9dfd509a5d52",        # Sound BI demo workspace
    "lakehouse_id": "ec851642-fa89-42bc-aebf-2742845d36fe",         # Fabric_Lakehouse_Demo
    "warehouse_connection": os.getenv("SPINDLE_TEST_WH_CONN"),      # set locally before live run
    "eventhouse_connection": os.getenv("SPINDLE_TEST_EH_CONN"),     # set locally before live run
    "sql_database_connection": os.getenv("SPINDLE_TEST_SQL_CONN"),  # set locally before live run
    "sql_server_connection": os.getenv("SPINDLE_TEST_ONPREM_CONN"), # set locally before live run
}
```

Missing env vars cause the relevant live test group to be skipped with a clear message — no hard failure.

---

## Section 4 — Demo Notebooks (`notebooks/demos/`)

Six pre-built Fabric notebooks. Each has:
- **Parametrized header cell** — workspace_id, lakehouse_id/target_id, scale_size (user edits this cell)
- **Generation cells** — run Spindle generation for the scenario
- **Write cells** — write to the target sink
- **Validation cell** — row count assertions, FK integrity check, summary table
- **Teardown cell** (optional) — cleanup generated tables

### Notebook Inventory

| File | Domain → Sink | Key features |
|---|---|---|
| `01_retail_lakehouse_quickstart.ipynb` | retail → lakehouse | All 4 sizes, seeding + streaming, Delta table verification |
| `02_financial_warehouse_analytics.ipynb` | financial → warehouse | Bulk DDL-first load, all sizes, Warehouse query validation |
| `03_healthcare_sql_database.ipynb` | healthcare → sql-database | Streaming insert, DataMasker HIPAA masking demo |
| `04_capital_markets_eventhouse.ipynb` | capital_markets → eventhouse/KQL | Streaming tick data, KQL query validation |
| `05_multi_domain_fanout.ipynb` | retail + financial → lakehouse + warehouse | Parallel writes to 2 sinks simultaneously |
| `06_custom_ddl_to_lakehouse.ipynb` | DDL → profile → generate → lakehouse | Bring-your-own-schema: `from-ddl` → `generate` → lakehouse |

### Notebook Cell Structure (per notebook)

```
[1] # Parameters (tag: parameters)
    WORKSPACE_ID = "..."
    TARGET_ID = "..."
    SCALE_SIZE = "small"  # small | medium | large | fabric_demo

[2] # Setup & Auth
    from sqllocks_spindle import Spindle
    from azure.identity import InteractiveBrowserCredential
    ...

[3] # Generate
    result = spindle.generate(domain=DOMAIN, scale=SCALE_SIZE, seed=42)

[4] # Write to sink
    writer = <SinkWriter>(...)
    writer.write(result)

[5] # Validate
    assert_row_counts(result)
    print_summary_table(result)
```

---

## Section 5 — Templates (`notebooks/templates/`)

### `template_domain_to_sink.ipynb`

Fully parametrized starter. User fills in:
- `DOMAIN` — one of the 13 built-in domains (list provided as comment)
- `SINK_TYPE` — lakehouse | warehouse | eventhouse | sql-database | sql-server
- `SCALE_SIZE` — small | medium | large | fabric_demo
- `MODE` — seeding | streaming
- Connection params for the chosen sink

Notebook generates, writes, and validates. Drop-in for any standard use case.

### `template_custom_schema.ipynb`

For bring-your-own schemas:
- `SCHEMA_PATH` — path to a `.spindle.json` file (or use `from-ddl` output)
- `SINK_TYPE`, `SCALE_SIZE`, connection params — same as above

Notebook loads schema, generates, writes, and validates.

---

## Section 6 — Test Run Commands

```bash
# Mock matrix — all ~400 combos, no credentials needed
cd projects/fabric-datagen
.venv-mac/bin/python -m pytest tests/test_validation_matrix.py -v --tb=short -q

# Live subset — ~30 combos, browser auth fires once
.venv-mac/bin/python -m pytest tests/test_validation_live.py -m live -v

# Single domain smoke
.venv-mac/bin/python -m pytest tests/test_validation_matrix.py -k "retail" -v

# Single sink smoke
.venv-mac/bin/python -m pytest tests/test_validation_matrix.py -k "lakehouse" -v

# Full matrix + live in one run
.venv-mac/bin/python -m pytest tests/test_validation_matrix.py tests/test_validation_live.py -v
```

---

## Section 7 — Implementation Order

1. `tests/fixtures/validation_matrix.py` — matrix builder + filters (no test infra, just data)
2. `tests/test_validation_matrix.py` — mock suite + mock sink factory
3. Run mock suite, fix any domain/sink edge cases found
4. `tests/test_validation_live.py` — live suite (Group A first, then B, C, D)
5. Run live suite against Sound BI Fabric_Lakehouse_Demo
6. Demo notebooks (01 → 06 in order)
7. Templates (after demos, since templates are simplified demos)
8. Version bump to v2.11.0, changelog entry

---

## Out of Scope for Phase 5

- Docs site (mkdocs-material) — Phase 5C
- Public release blog post / LinkedIn posts — Phase 5D
- New sink code — `FabricSqlDatabaseWriter` already covers all SQL Server targets
- CI pipeline for live tests — deferred; requires secret management setup
- Azure SQL MI specific test — deferred; requires SQLMI instance provisioning
