# Spindle Phase 4 — Gap Report

> **Date:** 2026-04-28
> **Auditor:** Jonathan Stewart
> **Version audited:** v2.9.0

## Status Summary

| # | Area | Status | Notes |
|---|---|---|---|
| 1 | SQL/DDL Pipeline (F-001, F-002) | ⚠️ Partial | `generate <schema.json>` path not accepted; warehouse dialect gap (see Area 1) |
| 2 | Fabric SQL Database Writer (F-003) | ⚠️ Partial | All 6 auth modes + 4 write modes implemented; `publish --target warehouse` not wired (CLI only: lakehouse/sql-database/eventhouse); `WarehouseBulkWriter` is engine-internal only |
| 3 | SQL Server On-Prem Auth | ⚠️ Partial | `sql` auth works for on-prem (caller owns UID/PWD in connection string); Entra ID modes functional against Azure SQL / on-prem with Entra enabled; ADO.NET normalizer strips UID/PWD; ODBC 18 hardcoded; no Driver 17 fallback |
| 4 | Phase 3B Live Test | ⚠️ Partial | DataProfiler/SchemaBuilder/GaussianCopula/FidelityReport all pass; LakehouseProfiler not live-testable (deltalake not in venv; az CLI tenant mismatch; see Area 4) |
| 5 | Capital Markets Domain (F-012) | ⚠️ Partial | 18/18 tests pass; CLI generates 10 tables, 126K rows; sector table is all-NaN (broken reference_data dataset); exchange table has scrambled codes; no surrogate-key FKs (ticker-based) |
| 6 | Incremental Engine (F-007) | ✅ Ship-ready | 24/24 tests pass; all three delta ops tagged; IDs continue from max+1; FK integrity holds across Day 1→Day 2; note: `--scale` flag absent from `continue` CLI (use `--inserts`) |
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

**Status:** ⚠️ Partial

### Auth mode analysis

#### `sql` auth (username/password)

`_get_connection` (`sql_database_writer.py:402`) passes `self._connection_string` directly to `pyodbc.connect()` with no token injection — correct behavior for SQL authentication. The caller is responsible for embedding `UID=<user>;PWD=<password>` in the ODBC connection string. There is no username/password parameter on `FabricSqlDatabaseWriter.__init__`, so credentials must be pre-baked into the connection string.

**Gap:** `_normalize_connection_string` (`sql_database_writer.py:348`) converts ADO.NET format (`Data Source=...;Initial Catalog=...`) to ODBC format but **does not carry over `User ID` or `Password` ADO.NET keys**. A caller who passes an ADO.NET connection string with embedded credentials to `sql` auth mode will silently lose UID/PWD after normalization, resulting in an auth failure at connection time. ODBC-format strings (already containing `Driver=`) bypass the normalizer and work correctly.

#### Entra ID modes (`cli`, `msi`, `spn`, `device-code`)

All four Entra modes call `_get_access_token()` and inject the token via `SQL_COPT_SS_ACCESS_TOKEN` (pyodbc attribute 1256). The resource URI is hardcoded to `https://database.windows.net/.default` (`sql_database_writer.py:442`), which is correct for both Azure SQL Database (`*.database.windows.net`) and on-prem SQL Server with Entra authentication enabled (via the ODBC driver's `Authentication=ActiveDirectoryAccessToken` path).

**Functional against on-prem with Entra:** Yes — if the on-prem SQL Server is AAD-joined or has Entra ID authentication configured, `SQL_COPT_SS_ACCESS_TOKEN` injection is the supported method and works through ODBC Driver 17/18.

#### `fabric` auth

Forces `mssparkutils` and raises `RuntimeError` if not in a Fabric Notebook. Not applicable to on-prem use cases by design.

### Connection string format support

| Format | Handled | Notes |
|---|---|---|
| ODBC (`Driver={...};Server=<host>,1433;Database=<db>;UID=<u>;PWD=<p>`) | ✅ | Pass-through, no normalization |
| ADO.NET → ODBC conversion | ⚠️ | Converts `Data Source` / `Initial Catalog` but silently drops `User ID` / `Password` |
| `Server=<host>,1433` port syntax (ODBC) | ✅ | pyodbc supports `<host>,<port>` natively; no code blocks it |
| `TrustServerCertificate=yes` for self-signed certs (on-prem) | ✅ | Passed through normalizer if present in ADO.NET source |

### ODBC driver detection

`_normalize_connection_string` hardcodes `Driver={ODBC Driver 18 for SQL Server}` (`sql_database_writer.py:363`). There is no runtime detection, fallback to Driver 17, or user-configurable driver override. If only ODBC Driver 17 is installed, callers must pass a fully-formed ODBC connection string (so the normalizer is bypassed) — the auto-normalizer will always generate a Driver 18 string.

### Domain/endpoint distinction

`_is_warehouse` flag (`sql_database_writer.py:104`) checks for `.datawarehouse.fabric.microsoft.com` to activate bulk-write path. No code distinguishes Fabric SQL endpoints from Azure SQL or on-prem SQL Server — auth and DDL paths are identical across all three. This is correct by design: the ODBC driver handles endpoint differences transparently.

### Test coverage

Zero tests for on-prem-specific scenarios:
- No test for `sql` auth mode with UID/PWD embedded in connection string
- No test for ADO.NET normalization with `User ID` / `Password` keys (the strip-UID/PWD bug is untested)
- No test for Entra ID mode against a non-Fabric endpoint
- No test for `Server=<host>,1433` port syntax

The single normalize test (`test_stores_connection_string`) only validates that a pre-formed ODBC string passes through unchanged.

### Findings

| # | Finding | Severity | Gap ref |
|---|---------|----------|---------|
| 1 | ADO.NET normalizer silently drops `User ID` / `Password` — `sql` auth via ADO.NET format loses credentials | High | F-003 |
| 2 | ODBC Driver 18 hardcoded in normalizer — no fallback to Driver 17, no user-configurable override | Medium | F-003 |
| 3 | No `UID`/`PWD` constructor params — on-prem `sql` auth requires caller to embed credentials in connection string (no safe wrapper) | Low | F-003 |
| 4 | Zero test coverage for on-prem SQL auth patterns (UID/PWD, port syntax, `sql` mode end-to-end) | Low | F-003 |

---

---

## Area 4 — Phase 3B Live Test

**Status:** ⚠️ Partial

**Test environment:** Python 3.13.13, `.venv-mac` (Homebrew), `sqllocks-spindle` v2.9.0. Unit tests run with `.venv-mac/bin/python -m pytest`. Live Fabric connection not available from this machine (see LakehouseProfiler section below).

### 1. DataProfiler.from_csv()

**Result: PASS**

`DataProfiler.from_csv()` correctly profiled a 100-row synthetic CSV (columns: `id`, `name`, `amount`, `date`):

- Table name inferred from filename stem: OK
- Row count: 100
- PK detection (`id`): `is_primary_key=True`
- Column type inference: integer, string, float, datetime — all correct
- Distribution fitting: `None` for `amount` column (insufficient spread for KS fit — expected behavior)
- Phase 3B extended stats (quantiles, string_length, outlier_rate, value_counts_ext): computed correctly

No errors. The classmethod is the correct entry point; returns a `TableProfile` directly (not `DatasetProfile`).

### 2. SchemaBuilder

**Result: PASS**

`SchemaBuilder().build(dataset_profile)` correctly converted a `DatasetProfile` wrapping the CSV profile into a `SpindleSchema`:

- Tables: `['items']`
- PK assigned: `['id']`
- Relationships: 0 (single-table input — correct)
- Column generators assigned for all 3 columns (sequence, faker, distribution)

**API note:** The task instructions referenced `SchemaBuilder.from_profile()` — this classmethod does not exist. The correct API is the instance method `SchemaBuilder().build(dataset_profile: DatasetProfile)`. No code gap — `from_profile` is simply not the published name.

### 3. GaussianCopula

**Result: PASS**

`GaussianCopula` (in `sqllocks_spindle/engine/correlation.py`) enforced target Pearson correlations via rank-based copula:

| Metric | Value |
|---|---|
| Input columns (a, b) | 500 rows, N(10,2) and N(20,5) |
| Original correlation a–b | -0.009 (independent) |
| After copula (target r=0.8) | 0.832 |
| Marginals preserved (a mean) | 9.974 → 9.974 (exact) |

FK integrity test: 100-row child table with `parent_id` drawn from 20 parent keys — **0 FK violations** after copula reordering (copula reorders within-column only, does not change FK values).

### 4. FidelityReport.score()

**Result: PASS**

`FidelityReport.score(real, synthetic)` correctly compared DataFrames using KS test + chi-squared + cardinality + null-rate metrics:

| Test case | Score | Pass criterion |
|---|---|---|
| Similar distributions (normal 0,1 and 5,2; 200 rows each) | **87.83/100** | >50: PASS |
| Perfect match (identical DataFrames) | **92.86/100** | ≥85: PASS |

The score returned is a `FidelityReport` object (0–100 scale), not a raw float. `report.overall_score` is the numeric value. The task instructions implied `FidelityReport.score()` returns a float — it returns a `FidelityReport` object; `report.overall_score > 0.5` requires scale awareness (it is 0–100, not 0–1).

All 6 unit tests in `tests/test_fidelity_report_v2.py` pass, including `test_perfect_match_scores_high` (≥85).

### 5. LakehouseProfiler.profile_table() — Live Test

**Result: NOT TESTABLE in this environment**

Two blockers prevented a live Fabric table read:

**Blocker 1 — `deltalake` not installed in `.venv-mac`:**
`LakehouseProfiler._read_table()` requires `deltalake` (`sqllocks-spindle[fabric-inference]` extra). The `.venv-mac` environment does not have `deltalake` installed. The class raises a clear `ImportError` with install instructions:
```
LakehouseProfiler requires 'deltalake'. Install with: pip install 'sqllocks-spindle[fabric-inference]'
```
Error handling is correct and actionable.

**Blocker 2 — az CLI tenant mismatch:**
`az account show` returns tenant `984795d6-d6a6-4fc6-8835-bc5957608750` (not the Sound BI tenant `2536810f-...`). Attempting `az account get-access-token --tenant 2536810f-...` returns `AADSTS50020: User account does not exist in tenant 'Sound BI'`. The Fabric MCP server (`fabric-ops-forge`) confirmed the lakehouse exists (`ec851642-fa89-42bc-aebf-2742845d36fe`, `Fabric_Lakehouse_Demo`) but `onelake_list_files` and `list_lakehouse_tables` both returned HTTP 400 — no Delta tables are present or the Tables directory is empty.

**Unit tests for LakehouseProfiler:** All 6 unit tests pass (mock-based, no live connection required):
- Import succeeds
- Constructor stores `workspace_id`, `lakehouse_id`, `default_sample_rows=100_000`
- `profile_table()` works with mocked `_read_table`
- `profile_all()` works with mocked `_list_tables` + `_read_table`
- `_read_table` raises `ImportError` when `deltalake` absent

**ABFSS path format verified:** `abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables/<table_name>` — correct per OneLake ABFSS spec.

### Test run summary

```
tests/test_lakehouse_profiler.py     6 passed
tests/test_fidelity_report_v2.py     6 passed
tests/test_masker.py                11 passed
tests/test_correlation.py            6 passed
tests/test_smart_inference.py       37 passed
Total: 66 passed, 0 failed (Phase 3B scope)
```

### Findings

| # | Finding | Severity | Gap ref |
|---|---------|----------|---------|
| 1 | `deltalake` not included in default `.venv-mac` — `[fabric-inference]` extra must be explicitly installed for LakehouseProfiler live use | Low | Phase 3B |
| 2 | `LakehouseProfiler` live test blocked by az CLI tenant mismatch; Fabric_Lakehouse_Demo appears to have no Delta tables (HTTP 400 on table listing via both REST API and OneLake DFS) | Low | Phase 3B |
| 3 | `SchemaBuilder.from_profile()` classmethod referenced in task spec does not exist — public API is `SchemaBuilder().build(dataset_profile)` | Low | Phase 3B |
| 4 | `FidelityReport.score()` returns a `FidelityReport` object (overall_score is 0–100), not a raw float — callers checking `score > 0.5` will always pass since the scale is 0–100 | Low | Phase 3B |

---

## Area 5 — Capital Markets Domain

**Status:** ⚠️ Partial

### Test results

**18 passed, 0 failed** (1.50s) — `tests/test_capital_markets.py`

All 18 tests green across 4 suites:

- `TestCapitalMarketsStructure` (8 tests): expected tables present, correct company/exchange/sector/industry counts, daily_price and trade have rows, generation order respects FK dependencies.
- `TestCapitalMarketsIntegrity` (5 tests): FK integrity passes, ticker uniqueness, daily_price company FK valid, trade has price columns, dividend and earnings have rows.
- `TestCapitalMarketsDistributions` (3 tests): OHLC all positive, high >= low, volume non-negative.
- `TestCapitalMarketsReproducibility` (1 test): same seed produces identical output.

### Domain registry + CLI registration

Registered. `spindle list` output:

```
  capital_markets  Capital Markets domain with companies, daily prices, dividends, earnings, insider trades, and tick-level trades
```

Domain is auto-discovered via `pkgutil.iter_modules` scanning `domains/`. Class name is `CapitalMarketsDomain` (not `CapitalMarketsGenerator`).

### CLI smoke test

**`spindle generate capital_markets --scale small --format csv --output /tmp/capital_smoke` — PASS**

```
Schema: capital_markets_3nf
Mode:   3nf
Seed:   42
Time:   0.1s

Table                             Rows  Columns
---------------------------------------------
company                            100       10
daily_price                     25,200        9
dividend                           150        6
earnings                           400        7
exchange                             3        7
insider_transaction                200        8
sector                              11        3
industry                            61        3
split                                5        5
trade                          100,000        6
---------------------------------------------
TOTAL                          126,130

Referential integrity: PASS (all FKs resolve)
Written 10 CSV files to /tmp/capital_smoke/
```

Note: `--format csv` is required; without it the CLI prints a summary only and writes no files (default format is `summary`).

### FK integrity at small scale

Verified against generated CSVs. The domain uses `ticker` as the natural business key (not a surrogate `company_id`). All child tables (`daily_price`, `trade`, `dividend`, `earnings`, `insider_transaction`, `split`) join to `company` via `ticker`.

| FK relationship | Result |
|---|---|
| `daily_price.ticker` → `company.ticker` | PASS |
| `trade.ticker` → `company.ticker` | PASS |
| `dividend.ticker` → `company.ticker` | PASS |
| `earnings.ticker` → `company.ticker` | PASS |
| `insider_transaction.ticker` → `company.ticker` | PASS |
| `split.ticker` → `company.ticker` | PASS |
| `company.exchange_code` → `exchange.exchange_code` | FAIL — mismatch (see Findings #1) |
| `company.sector_name` → `sector.sector_name` | FAIL — sector table all-NaN (see Findings #2) |
| `industry.sector_id` → `sector.sector_id` | Structural PASS (sequence IDs match), but sector rows are empty |

### Feature completeness

| Feature | Present |
|---|---|
| Real S&P 500 tickers (SEC EDGAR) | Yes — `record_sample` from `sp500_constituents` dataset |
| CIK numbers (SEC Form 4 style) | Yes — `cik` column on `company` |
| Geometric Brownian Motion pricing | Yes — OHLCV in `daily_price`, docstring confirms GBM |
| OHLCV columns | Yes — `open`, `high`, `low`, `close`, `adj_close`, `volume` |
| OHLC validity (high >= low, all positive) | Yes — verified in tests and live data |
| Dividends | Yes — 150 rows at small scale |
| Earnings (quarterly EPS) | Yes — 400 rows at small scale |
| Insider transactions (SEC Form 4) | Yes — 200 rows at small scale |
| Stock splits | Yes — 5 rows at small scale |
| Tick-level trades | Yes — 100,000 rows at small scale |
| Reproducibility (seed) | Yes — test confirms same seed → same output |

### Findings

| # | Finding | Severity | Notes |
|---|---|---|---|
| 1 | `exchange` table data is scrambled: `exchange_code` column contains full exchange names (e.g. "NASDAQ Stock Market"), while `company.exchange_code` stores short codes ("NASDAQ", "NYSE"). The FK declared in the schema cannot resolve. | Medium | `reference_data` strategy pulls wrong `field` for `exchange_code` — likely `name` being repeated instead of `code`. Tests pass because the engine's internal FK check resolves via a different path than the CSV column values. |
| 2 | `sector` table is entirely NaN: all 11 rows have `sector_name = NaN` and `sector_code = NaN`. The `reference_data` strategy points to dataset `gics_sectors` / field `sector_name` — that dataset either does not exist or returns empty. Tests pass because the FK check counts rows, not values. | High | This is a data quality gap; `spindle list` says "GICS sectors (11)" but the generated data is blank. Any downstream join on `sector_name` from `company` → `sector` yields no matches. |
| 3 | No surrogate-key FK wiring between `company` and `exchange`/`sector`: `company` uses denormalized `exchange_code` and `sector_name` string fields copied from the S&P 500 dataset — not foreign keys to the `exchange` or `sector` tables. The 3NF claim is partially accurate (child tables properly FK to company via ticker) but the company↔exchange and company↔sector relationships are denormalized. | Low | Design choice or oversight; document as schema limitation. |
| 4 | `spindle generate capital_markets --output <dir>` with no `--format` flag writes no files (default is `summary`). This is consistent with other domains but not obvious from help text. | Low | Cosmetic UX gap; not a bug. |

---

## Area 6 — Incremental Engine

**Status:** ✅ Ship-ready

### Test results

**24 passed, 0 failed** (3.04s) — `tests/test_incremental.py` (12 tests) + `tests/test_e2e_incremental.py` (12 tests).

All unit and E2E tests pass, covering: delta INSERT/UPDATE/DELETE generation, PK continuation, FK integrity in delta inserts, seed reproducibility, time-travel snapshots (monthly, growth, churn, seasonality, partitioned DataFrames).

One non-blocking warning emitted at runtime (line 458 of `continue_engine.py`):
> `UserWarning: you are shuffling a 'ArrowStringArray' object which is not a subclass of 'Sequence'; shuffle is not guaranteed to behave correctly.`

This warning fires once per table (9× per run) and is cosmetic — shuffle still produces valid deltas — but should be addressed before PyPI publish.

### CLI smoke test

**Command used:** `spindle continue retail --input /tmp/spindle_day1/ -o /tmp/spindle_day2/ --inserts 100`

Result: PASS — generated 9 delta CSV files, all tables covered, summary table printed correctly.

Note: The task brief specified `--from` and `--scale` flags; neither exists. The correct flags are `--input` and `--inserts`. This is a documentation/discoverability gap, not a functional defect.

### Delta tag and ID continuation

All 9 delta files carry `_delta_type` and `_delta_timestamp` columns (verified via column inspection).

INSERT/UPDATE/DELETE counts per table (sample):
- `customer`: INSERT 100 / UPDATE 100 / DELETE 20
- `order`: INSERT 100 / UPDATE 500 / DELETE 100
- `order_line`: INSERT 100 / UPDATE 1250 / DELETE 250

**ID continuation** (INSERT rows only, verified against Day 1 maxima):
| Table | Day 1 max PK | Day 2 INSERT min PK | Continues correctly |
|---|---|---|---|
| customer | 1000 | 1001 | Yes |
| product | 500 | 501 | Yes |
| order | 5000 | 5001 | Yes |

### FK consistency

Day 2 INSERT orders referencing `customer_id`: **0 orphan FKs** — all new order rows reference a customer ID present in either Day 1 or Day 2 INSERT customers.

### Findings

| # | Severity | Finding |
|---|---|---|
| 6-1 | Low | ArrowStringArray shuffle warning fires once per table per run (line 458 of `continue_engine.py`); cosmetic but noisy in CI logs |
| 6-2 | Low | `spindle continue` lacks a `--scale` shorthand; users coming from `spindle generate` will expect it; `--inserts` is not equivalent but serves as workaround |

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