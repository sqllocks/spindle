# Spindle Phase 4 — Gap Report

> **Date:** 2026-04-28
> **Auditor:** Jonathan Stewart
> **Version audited:** v2.9.0

## Status Summary

| # | Area | Status | Notes |
|---|---|---|---|
| 1 | SQL/DDL Pipeline (F-001, F-002) | ⚠️ Partial | `generate <schema.json>` path not accepted; warehouse dialect gap (see Area 1) |
| 2 | Fabric SQL Database Writer (F-003) | ⚠️ Partial | All 6 auth modes + 4 write modes implemented; `publish --target warehouse` not wired (CLI only: lakehouse/sql-database/eventhouse); `WarehouseBulkWriter` is engine-internal only |
| 3 | SQL Server On-Prem Auth | — | |
| 4 | Phase 3B Live Test | — | |
| 5 | Capital Markets Domain (F-012) | — | |
| 6 | Incremental Engine (F-007) | — | |
| 7 | SCD2 Strategy + Data Masker (F-009, F-011) | — | |
| 8 | Package Hygiene (F-014) | — | |

Legend: ✅ Ship-ready | ⚠️ Partial | ❌ Broken/stub

---

## Area 1 — SQL/DDL Pipeline

**Status:** ⚠️ Partial

### Test results

**29 passed, 0 failed** (1.51s) — `tests/test_ddl_parser.py` (21 tests) + `tests/test_e2e_ddl_pipeline.py` (8 tests).

All coverage categories green: table detection, column parsing, PK/FK detection, strategy inference (sequence/faker/temporal/distribution/weighted-enum), scale generation, and end-to-end data generation from parsed DDL.

### CLI smoke test

**`spindle from-ddl` — PASS**

Input: 2-table SQL Server DDL (`customer`, `order`) with an explicit `CONSTRAINT FK_order_customer FOREIGN KEY` clause.

```
Tables:        2 (customer, order)
Relationships: 1 (fk_order_customer_id)  ← FK detected: YES
Business rules: 1
Inferences:    8 (strategy assignments)
```

Output written to `/tmp/smoke_schema.spindle.json`. CLI hint correctly suggests `spindle generate custom --schema <file>`.

---

**`spindle generate retail --scale fabric_demo --format sql` — PASS**

Output: 9 `.sql` files, 4,670 total rows, referential integrity PASS.

File structure is DDL-first: each file opens with `IF OBJECT_ID ... DROP TABLE`, then `CREATE TABLE` with typed columns and a `CONSTRAINT PK_...` line, followed by batched `INSERT INTO ... VALUES` blocks. `GO` batch separators present throughout.

No `IDENTITY` keyword in any generated column (columns use explicit integer values) — correct behavior for INSERT-based generation.

---

**`spindle generate /tmp/smoke_schema.spindle.json --format sql` — FAIL (gap confirmed)**

Error: `Unknown domain: '/tmp/smoke_schema.spindle.json'`

The `generate` command's first positional argument is treated as a domain name, not a schema path. There is no code path to pass a `.spindle.json` file directly to `generate`. The workaround `spindle generate custom --schema <file>` is suggested by `from-ddl` output but is a separate sub-command path, not the advertised `generate <schema.json>` UX.

**Gap:** F-002 round-trip (`from-ddl` → `generate`) cannot be driven by a bare schema path via `spindle generate`. Requires `--schema` flag on `custom` sub-domain, which is not discoverable from the main `generate` help.

---

**`spindle generate retail --scale fabric_demo --format sql --sql-dialect tsql-fabric-warehouse` — PARTIAL**

`GO` batch separator: **YES** — present between DDL blocks and after `CREATE TABLE`.

`IDENTITY` keyword: **NO** — correctly absent. Fabric Warehouse does not support `IDENTITY`; columns use plain `INT NOT NULL`.

`PRIMARY KEY` constraint: replaced with comment `-- NOTE: Fabric Warehouse does not enforce PRIMARY KEY constraints.` — correct.

**Gap:** The `--sql-dialect` flag is silently accepted and produces subtly different DDL (PK comment, no IDENTITY) but the `IF OBJECT_ID ... DROP TABLE` preamble and `GO` separators are identical to the standard T-SQL dialect. There is no `CREATE TABLE` syntax difference for column definitions (e.g. no `DISTRIBUTION =`, no `CLUSTERED COLUMNSTORE INDEX`) that Fabric Warehouse DDL would normally include for production use. Output is functional for basic testing but not production-ready Warehouse DDL.

### Findings

| # | Finding | Severity | Gap ref |
|---|---------|----------|---------|
| 1 | `spindle generate <schema.json>` not accepted — "Unknown domain" error | Medium | F-002 |
| 2 | `tsql-fabric-warehouse` dialect omits `DISTRIBUTION` / `CLUSTERED COLUMNSTORE INDEX` — minimal diff from standard T-SQL | Low | F-001 |
| 3 | `--sql-dialect` not listed in `spindle generate --help` output (undiscoverable) | Low | F-001 |

---

## Area 2 — Fabric SQL Database Writer

**Status:** ⚠️ Partial

### Test results

**22 passed, 0 failed** (1.93s) — `tests/test_sql_database_writer.py` (10 tests) + `tests/test_publish_cli.py` (12 tests).

Coverage: DDL generation, INSERT SQL building, write-to-mock-connection, publish CLI help/validation, lakehouse publish, dry-run, SQL missing connection string, eventhouse missing params.

### Auth mode review

All 6 declared auth modes are **fully implemented** in `fabric/sql_database_writer.py` — no `NotImplementedError` stubs found.

| Auth method | Implementation | Notes |
|---|---|---|
| `cli` | ✅ | `AzureCliCredential` (azure.identity) |
| `msi` | ✅ | Tries mssparkutils first, falls back to `ManagedIdentityCredential` |
| `spn` | ✅ | `ClientSecretCredential` — requires client_id, client_secret, tenant_id |
| `sql` | ✅ | Direct pyodbc with connection string, no token injection |
| `device-code` | ✅ | `DeviceCodeCredential` with prompt callback |
| `fabric` | ✅ | mssparkutils only, raises `RuntimeError` if not in Fabric Notebook |

**Minor gap:** `publish --auth` CLI option (`cli.py:1348`) exposes only 5 choices: `cli, msi, spn, sql, device-code`. The `fabric` auth method is not exposed via `publish` (intended for Notebook use only, but not documented as such).

### Write mode coverage

All 4 write modes are implemented in `FabricSqlDatabaseWriter.write()` (`sql_database_writer.py:150-174`) and exposed via `generate --write-mode` (`cli.py:78`):

| Mode | Behavior |
|---|---|
| `create_insert` | DROP + CREATE + INSERT (full reset) |
| `insert_only` | INSERT only (no DDL) |
| `truncate_insert` | TRUNCATE + INSERT (keep schema, reset data) |
| `append` | INSERT without truncating (Day 2 loads) |

`publish --target sql-database` hardcodes `mode="create_insert"` (`cli.py:1548`). The `--write-mode` flag is only wired into `generate`, not `publish`. Minor usability gap — `publish` cannot do append or truncate modes.

### Warehouse target gap

`publish --target` (`cli.py:1337`) accepts only `["lakehouse", "eventhouse", "sql-database"]`. **`warehouse` is not a valid publish target.**

`WarehouseBulkWriter` (`fabric/warehouse_bulk_writer.py`, 610 lines) is **not** orphaned — it is wired into the engine sinks layer:
- `engine/sinks/warehouse.py` instantiates it for chunked generation
- `fabric/multi_writer.py` uses it in multi-target writes
- `output/multi_store_writer.py` wraps it as a composable writer
- `fabric/__init__.py` exports it as a public API symbol

However, `publish --target warehouse` is not exposed, so users cannot trigger a warehouse bulk load via the CLI `publish` command. The gap is specifically in `cli.py:1337` — one additional `Choice` value and a handler branch (similar to the `sql-database` branch at `cli.py:1528`).

### Findings

| # | Finding | Severity | Gap ref |
|---|---------|----------|---------|
| 1 | `publish --target warehouse` not wired — CLI only exposes lakehouse/sql-database/eventhouse | High | F-003 |
| 2 | `publish --target sql-database` hardcodes `mode="create_insert"` — no way to do append/truncate via `publish` | Low | F-003 |
| 3 | `fabric` auth method not exposed in `publish --auth` choices — undocumented restriction | Low | F-003 |

---

## Area 3 — SQL Server On-Prem Auth

**Status:** —

### Findings
_fill in_

---

## Area 4 — Phase 3B Live Test

**Status:** —

### LakehouseProfiler
_fill in_

### GaussianCopula
_fill in_

### FidelityReport
_fill in_

### Findings
_fill in_

---

## Area 5 — Capital Markets Domain

**Status:** —

### Test results
_fill in_

### CLI smoke test
_fill in_

### Findings
_fill in_

---

## Area 6 — Incremental Engine

**Status:** —

### Test results
_fill in_

### CLI smoke test
_fill in_

### Findings
_fill in_

---

## Area 7 — SCD2 Strategy + Data Masker

**Status:** —

### SCD2 test results
_fill in_

### Masker test results
_fill in_

### Findings
_fill in_

---

## Area 8 — Package Hygiene

**Status:** —

### Conflict files
_fill in_

### Dependency audit
_fill in_

### Findings
_fill in_

---

## Trivial Fixes Applied

_List any fixes applied inline during the sprint with file + line reference._

---

## Phase 5 Candidate Scope

### Must-fix before PyPI publish
_fill in_

### Should-fix in Phase 5
_fill in_

### Defer
_fill in_