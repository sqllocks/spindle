# Spindle Phase 4 — Gap Report

> **Date:** 2026-04-28
> **Auditor:** Jonathan Stewart
> **Version audited:** v2.9.0

## Status Summary

| # | Area | Status | Notes |
|---|---|---|---|
| 1 | SQL/DDL Pipeline (F-001, F-002) | ⚠️ Partial | `generate <schema.json>` path not accepted; warehouse dialect gap (see Area 1) |
| 2 | Fabric SQL Database Writer (F-003) | — | |
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

**Status:** —

### Auth mode review
_fill in_

### Warehouse target gap
_fill in_

### Test results
_fill in_

### Findings
_fill in_

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