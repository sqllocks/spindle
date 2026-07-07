# Changelog

All notable changes to Spindle will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.0.1] - 2026-07-06

### Fixed
- `schema/ddl_parser.py`: `DdlParser` had zero awareness of SQL comments.
  `_extract_paren_body` and `_split_columns` count raw parens and commas
  directly on the input text, so a `/* */` block comment containing a comma
  (a normal authoring style, e.g. a column note listing several values)
  fragmented the column splitter mid-comment. One fragment's leftover text
  could contain the substring `PRIMARY KEY`, which then hijacked primary-key
  detection (only the first PK-flagged column is kept) and silently dropped
  the real next column with no error raised. Added
  `DdlParser._strip_comments()`, called at the top of `parse_string()`,
  which strips `/* */` and `--` comments in a single pass while tracking
  single-quoted string literals so a value like `'10--20'` is never mistaken
  for a comment. 5 new regression tests added to `test_ddl_parser.py`
  covering the fragmentation, the PK hijack, line comments, and string
  literal preservation.

## [3.0.0] - 2026-06-14

Output-changing audit remediation. Resolves the Tier-1/Tier-2/Tier-3 findings
from the multi-pass deep audit (memory id=4788) that were intentionally held
back from 2.14.5 because their corrections change the data a given seed
produces. 2.14.5 remains the recommended-stable line for callers that pin
2.x output.

### Fixed
- `engine/strategies/temporal.py`: top-level `start`/`end` are now read as a
  lowest-priority fallback. `range_ref` still wins, then nested `date_range`
  / `range`, then top-level `start`/`end`, then the legacy 2022-2025 default.
  Closes the largest single finding: about 60 columns across 10 domains were
  silently generating all dates in 2022-2025 because the domains wrote
  `start`/`end` top-level while the strategy only read nested keys.
- `engine/strategies/temporal.py`: seasonal pattern picks up top-level
  `month_weights` / `day_of_week_weights` when the nested `profiles` dict
  lacks the corresponding key.
- `engine/strategies/derived.py`: accepts `operation` as alias for `rule`;
  expands a top-level `days: N` to `add_days` uniform `[N, N]`. Repairs the
  HIGH-severity finding where 9 derived date columns (card expiry, claim
  dates, policy/billing/delivery offsets) were silently `rule="copy"` and
  produced exact copies of their source.
- `engine/strategies/distribution.py`: `normal` accepts `sigma`/`std` as
  aliases for `std_dev`; `log_normal` accepts `std` as alias for `sigma`.
  Repairs columns that collapsed to std=1 because the strategy ignored the
  domain's spelling (credit_score with sigma=80 collapsed to sigma=1, etc.).
- `engine/strategies/self_referencing.py`: accepts `max_depth` as alias for
  `levels` (used in financial and HR hierarchies).
- `engine/strategies/foreign_key.py`: top-level `alpha`/`max_per_parent` are
  now routed into the `params` dict id_manager sees. Nested `params` still
  wins. Repairs about 12 columns where the pareto distribution
  parameters were silently defaulted.
- `engine/strategies/correlated.py`: accepts `operation` as alias for `rule`.
- `schema/ddl_parser.py`: `TYPE_MAP` now emits the canonical `std_dev`
  (normal) and `sigma` (log_normal) keys instead of the previously-ignored
  `std` key. All decimal / numeric / money columns imported from DDL now
  honor the declared spread.
- `schema/parser.py`: relationship parser accepts scalar `parent_key` /
  `child_key` as aliases for `parent_columns` / `child_columns`. Previously
  10 of 14 built-in domains had silently empty relationship lists and
  `verify_integrity()` returned `[]` regardless of actual orphans.
- `engine/generator.py`: `GaussianCopula` now receives the model seed, so
  the correlation post-pass is deterministic across runs (previously used
  system entropy).
- `engine/chunk_worker.py` + `engine/scale_router.py`: per-table child RNGs
  now derive from `zlib.crc32(table_name)` instead of Python builtin
  `hash(table_name)`. The builtin hash is per-process-randomized via
  `PYTHONHASHSEED`, so chunked / scale-routed generation drifted across
  subprocesses on the same seed.
- `engine/chunk_worker.py`: per-table chunk row count is now proportional
  to the declared natural row count via `_schema_counts`. Previously every
  dynamic table got the largest dynamic table's chunk size, over-replicating
  small dim tables.
- `engine/table_generator.py` + `engine/chunked_generator.py`: chunk 0
  registers the table; chunks 1..N append PKs to the existing pool via
  `IDManager.append_pks()`. Pre-3.0.0 each chunk replaced the prior pool,
  so child FKs in later chunks only saw the last parent chunk's PKs.
- `engine/correlation.py`: `GaussianCopula.apply` now skips columns whose
  name looks like a key (`_id` / `_pk` / `_fk` / `id` / `pk`). The copula
  reorders each column independently and would otherwise break PK/FK row
  alignment.
- `transform/star_schema.py`: dim tables are deduped on natural key BEFORE
  surrogate-key assignment, so duplicate NKs no longer arbitrarily keep the
  last SK. Fact build warn-logs orphan FKs left after SK substitution.
  `_build_date_dim` caps the date dimension span at 60 years and warns when
  source dates would have produced a multi-century table.
- `incremental/continue_engine.py` + `incremental/time_travel.py`: persisted
  per-(table, pk) high-water-mark on the engine instance so repeated
  `continue_from` / snapshot iterations issue strictly disjoint integer PKs
  instead of restarting from `max(existing) + 1`.
- `demo/modes/seeding.py`: when `params.seed` is None, the seeding path now
  computes one `effective_seed` and uses it for both `parsed.model.seed`
  (which the chunk workers derive children from) and `router.run(seed=...)`,
  closing the divergence that broke determinism on default-seed runs.
- `demo/estimator.py`: write-to-N-targets duration is now the SUM of
  per-target costs, not the max. Targets run sequentially.
- `demo/cleanup.py`: SQL `DROP TABLE` always schema-qualifies (`dbo.` when
  unqualified) so cleanup hits the spindle artifact rather than the caller's
  default schema.
- `demo/modes/streaming.py`: `KeyboardInterrupt` with `auto_cleanup=True`
  now actually invokes `CleanupEngine` (previously logged intent and
  no-op'd).
- `domains/healthcare`: `fact_claim.date_cols` now `filing_date`. The
  previous `service_date` reference lived on `encounter` and was never
  joined into the claim fact.
- `domains/iot`: `dim_device.enrich.left_on` corrected from `type_id` to
  `device_type_id` (the actual FK column on `device`).
- `domains/marketing`: `dim_campaign.enrich.left_on` corrected from
  `type_id` to `campaign_type_id`.
- `domains/education`: `financial_aid` now defines an `award_date` column
  that `fact_financial_aid.date_cols` already referenced.
- `domains/composite._ensure_bridge_columns`: bridge FK columns inherit
  the parent PK's declared type instead of hard-coding integer (used to
  silently mismatch string PKs like ticker symbols).
- `domains/capital_markets`: tightened `daily_price.close` to `[0.94, 1.04]`
  of `open` so the business-rules envelope pass has less corrective work.
  Added a declared exchange-to-company relationship via `exchange_code` so
  `verify_integrity()` covers the link.

### Behavior changes (seed output differs from 2.x)
- Any column whose strategy was previously falling back to default values
  because of a config-key spelling mismatch will now read the domain's
  intended config. For most generated data, this means dates land in the
  domain's declared range, log_normal/normal columns have the declared
  spread, derived offsets actually offset, and pareto FKs use the declared
  alpha. Concretely: a 3.0.0 run with seed=42 will NOT match a 2.x run with
  the same seed for any domain that relied on the affected strategies.
- Multi-chunk and scale-routed runs are now deterministic across subprocesses
  with different `PYTHONHASHSEED`. Previously these paths could drift.
- Chunked generation respects per-table natural row counts; dim tables no
  longer over-replicate to the fact-table chunk size.
- Repeated `continue_from` / time-travel snapshots on the same engine
  instance now produce disjoint integer PKs instead of restarting from
  `max(existing) + 1`.
- Star-schema dim tables dedupe on natural key, so duplicate NK input rows
  produce one dim row, not several.

### Migration
- Regenerate any pinned synthetic datasets and the 35 example notebook
  outputs you depended on; output for a given seed changes.
- 2.14.5 remains the recommended-stable release for 2.x output compatibility.

## [3.0.0] - 2026-06-14

Output-changing audit remediation release. Closes the Tier-1 to Tier-3
correctness gaps surfaced by the deep audit (memory id=4788, four-iteration
spiral) that 2.14.5 deliberately deferred. **Seed output changes from 2.x**;
any pinned, regenerated, or notebook-frozen dataset built against 2.14.5 or
earlier must be regenerated against 3.0.0.

### Fixed

Config-key tolerance (Tier-1, the largest single class of silent defaults):
- `engine/strategies/temporal.py`: accepts top-level `start` / `end` as a
  lowest-priority fallback, after `range_ref` and nested `date_range`.
  Domains that wrote dates at the top level were silently falling back to the
  engine default 2022-2025 window for ~60 columns across 10 of 14 domains
  (all DOB fields landed in 2022-2025 instead of the declared birth range).
  Seasonal pattern also accepts top-level `month_weights` and
  `day_of_week_weights`.
- `engine/strategies/derived.py`: `operation` accepted as alias for `rule`;
  top-level `days: N` expands to `add_days` uniform [N, N]. Closes ~9 columns
  where derived dates collapsed to copy-of-source (card expiry == issue date,
  delivery == ship, claim == policy).
- `engine/strategies/distribution.py`: `normal` accepts `sigma` / `std` as
  aliases for `std_dev`; `log_normal` accepts `std` as alias for `sigma`.
  Previously these distributions silently collapsed to std=1 across credit
  score, GPA, IoT sensor, and capital-markets numeric columns.
- `engine/strategies/self_referencing.py`: `max_depth` accepted as alias for
  `levels`. Financial and HR org-chart hierarchies now respect declared depth.
- `engine/strategies/foreign_key.py`: top-level `alpha` and `max_per_parent`
  routed into the id_manager params dict (nested `params` still wins).
- `engine/strategies/correlated.py`: `operation` accepted as alias for `rule`.
- `schema/ddl_parser.py`: TYPE_MAP emits canonical `std_dev` (normal) /
  `sigma` (log_normal) keys instead of the previously ignored `std`.
- `schema/parser.py`: relationship parser accepts scalar
  `parent_key` / `child_key` (and `parent_table` / `child_table`) as aliases
  for `parent_columns` / `child_columns` / `parent` / `child`. Without this
  10 of 14 domains were dropping declared relationships on the floor, so
  `verify_integrity()` had no checks to run and returned `[]` regardless of
  whether the data was consistent.

Cross-subprocess determinism:
- `engine/generator.py`: `GaussianCopula` now seeded from
  `parsed.model.seed` so the correlation post-pass is reproducible.
- `engine/chunk_worker.py` + `engine/scale_router.py`: per-table child RNG
  seed derived via `zlib.crc32(table_name.encode())` instead of
  `hash(table_name)`. Built-in `hash` is per-process-randomized by
  `PYTHONHASHSEED` in Python 3.3+, so cross-subprocess runs produced
  different output. CRC32 is stable across processes and machines.
- `demo/modes/seeding.py`: when `params.seed` is `None`, a single
  `effective_seed` is computed and used for BOTH `model.seed` (consumed by
  chunk workers) AND `router.run(seed=...)`. Previously these diverged.

Referential integrity across non-default paths:
- `engine/table_generator.py`: `TableGenerator.generate(register=True)`. When
  `register=False`, PKs are appended to the IDManager pool via
  `append_pks()` instead of replacing the prior pool.
- `engine/chunked_generator.py`: chunk 0 registers, chunks 1..N pass
  `register=False`. Removes orphan FKs across chunks (child FKs in late
  chunks previously saw only the last chunk's parent slice).
- `engine/chunk_worker.py`: per-table per-chunk row count scaled by
  `schema_counts` ratio to the largest dynamic table, so smaller dynamic
  tables (dim_*) are no longer over-replicated to the full chunk size.
- `engine/scale_router.py`: embeds `_total_rows` in the schema_dict so chunk
  workers can do the proportional scaling above.
- `engine/correlation.py`: `GaussianCopula` skips columns whose names look
  like keys (id, pk, `*_id`, `*_pk`, `*_fk`). The copula reorders each
  participating column independently, which breaks row-aligned key
  relationships; PKs and FKs must not be copula-reordered.
- `transform/star_schema.py`: dim rows deduped on the natural key BEFORE
  surrogate-key assignment (previously duplicate NKs silently kept the last
  SK and corrupted fact joins). `_build_date_dim` caps the date range at 60
  years and warns when the source dates would have produced a multi-century
  table. Orphan fact rows now warn-log instead of silently propagating
  null FKs.
- `incremental/continue_engine.py`: persisted per-engine HWM on integer PKs
  so repeated `continue_from()` calls on the same snapshot keep issuing
  strictly disjoint PK ranges.
- `incremental/time_travel.py`: same persisted HWM so cross-snapshot PKs
  do not collide.

Domain-specific factual fixes (B4 omitted findings):
- `domains/healthcare/healthcare.py`: `fact_claim.date_cols` points at
  `filing_date` (which the claim_line + claim join exposes); previously
  pointed at `service_date` which was never joined into the fact.
- `domains/education/education.py`: `financial_aid` table declares the
  `award_date` column referenced by `fact_financial_aid.date_cols`.
- `domains/iot/iot.py`: `dim_device` enrich `left_on` uses `device_type_id`
  (the real FK column) instead of `type_id` (which did not exist).
- `domains/marketing/marketing.py`: same fix, `campaign_type_id` instead of
  `type_id`.
- `domains/capital_markets/capital_markets.py`: `close` factor narrowed
  from 0.93..1.07 to 0.94..1.04 so the OHLC business-rule repair pass has
  less to enforce after the initial draw.
- `domains/composite.py`: bridge FK columns injected by the shared-entity
  registry now inherit the parent PK ColumnDef type instead of always being
  `integer`. String-keyed parents (ticker, etc.) get a string bridge.
- `demo/estimator.py`: `CostEstimator.estimate()` SUMs per-target seconds
  instead of taking the max. Targets are written sequentially, not in
  parallel, so total duration accumulates.
- `demo/cleanup.py`: `_cleanup_sql_table` always schema-qualifies the
  `DROP TABLE` (defaults to `dbo` when unqualified) so the cleanup hits
  the spindle artifact rather than the caller's default schema.
- `demo/modes/streaming.py`: streaming demo with `auto_cleanup` on
  KeyboardInterrupt now actually invokes `CleanupEngine` instead of only
  logging the intent.

### Behavior changes (seed output differs from 2.x)

- Generated dates land in the domain-declared range (e.g. healthcare DOBs
  in 1940-2005, financial transactions in the declared trade window) rather
  than the previous default 2022-2025.
- Derived date offsets actually offset instead of copying the source.
- Normal / log-normal numeric distributions honor the declared spread
  instead of collapsing to std=1.
- Self-referencing hierarchies build the declared number of levels.
- Multi-chunk runs have referentially-correct FKs across all chunks (was:
  orphans in chunks 2..N).
- Correlation post-pass is deterministic for a given seed (was: system
  entropy when correlations were enforced).
- Star-schema dimensions deduplicated on NK (was: duplicate NKs silently
  collapsed to the last SK).
- For `chunked_generator` + `scale_router` runs, smaller dynamic tables
  scale proportionally to their natural cardinality (was: over-replicated
  to the primary chunk size).

### Migration

- Regenerate any pinned or cached datasets built against Spindle 2.x. The
  output for a given seed is INTENTIONALLY different; the new output is
  the correct one.
- The 35 example notebooks (`SpindleAW_*`, `Spindle_*`) regenerate on
  first run; the bundled output artifacts under `notebooks/` will differ
  from 2.x snapshots.
- Pre-3.0.0 `verify_integrity()` returned `[]` on 10 of 14 domains because
  parsed relationships were empty. 3.0.0 actually checks; if your custom
  schema relied on the previous no-op behavior, declare the relationship
  with `optional: True` or remove it.
- 2.14.5 remains the recommended-stable 2.x line for pinned 2.x output
  consumers.
## [2.14.5] - 2026-06-14

Safe bugfix release from the deep audit: corrects broken/corrupt output and
unsafe behavior **without changing valid generated data for a given seed**.
(Output-changing correctness fixes ship separately in 3.0.0.)

### Fixed
- `engine/generator.py`: when a `fidelity_profile` is passed, the exception path
  now returns the documented `(result, None)` 2-tuple instead of a bare `result`,
  so callers unpacking the tuple no longer hit a `ValueError` that masks the real
  error.
- `fabric/sql_database_writer.py` `_to_bool`: unrecognized boolean strings (e.g.
  `"N"`, `"null"`, `"maybe"`) were silently coerced to `True`, corrupting BIT
  columns. They now map to `NULL` with a warning; only an explicit true/false set
  is honored.
- `fabric/sql_database_writer.py`: the `fast_executemany` buffer-sizing hack
  mutated `params[0]` with other rows' values, silently corrupting the first row
  of every batch. It no longer mutates any row; batches whose first row is not the
  widest fall back to per-row sizing for that batch (no corruption, no silent
  truncation).
- `fabric/warehouse_bulk_writer.py` `copy_into`: reported `SELECT COUNT(*)` of the
  whole table as "rows loaded", over-reporting on append and masking a COPY that
  loaded zero rows. Now uses the COPY statement's own row count.
- `inference/tier3_research.py` `_chi2_test`: drift test failed **open** (returned
  "no drift" on any exception). Now fails closed (`method="error"`, drifted flag).
- `inference/masker.py`: tables are now masked parent-before-child (topological
  order) so PK→FK remaps propagate; previously a child masked before its parent
  kept stale FK values (orphans).
- `inference/schema_builder.py`: profiler-fitted distributions were emitted with
  raw scipy names/params, so `lognormal` raised and `uniform` silently became
  `uniform(0,1)`. Now routed through the shared `_translate_distribution`.
- `inference/comparator.py`: the Chi² p-value column was computed but never
  rendered in the HTML fidelity report; the column is now shown.
- `inference/profile_store.py`: saved profiles emitted bare `NaN`/`Infinity`
  (invalid JSON for non-Python consumers). Non-finite floats now serialize to
  `null` and the writer forbids `NaN`/`Inf`.
- `inference/lakehouse_profiler.py`: validate Delta table names against
  `[A-Za-z0-9_]+` (prevents path traversal into other OneLake locations), and
  sample rows randomly instead of `head()` (head biased every inferred statistic).
- `demo/notebook_gen.py`: generated cells shallow-copied a shared template,
  aliasing mutable `metadata`/`outputs` across all cells; now deep-copied.
- `demo/cleanup.py`: `_cleanup_file` would `shutil.rmtree` any directory; it now
  refuses to recursively delete top-level or non-spindle directories.
- `demo/connections.py`: connection profiles holding a `client_secret` are now
  written `chmod 600` (where supported) with a warning about secret-at-rest.

### Notes
- DB-path fixes (`copy_into` row count, `fast_executemany` sizing, lakehouse read)
  are correct by construction but require a live Fabric SQL/lakehouse to exercise
  end to end.

## [2.14.4] - 2026-06-13

### Fixed
- `engine/generator.py` `GenerationResult`: the class was not iterable. It defined
  `__getitem__(table_name)` but no `__iter__`, so `for table_name, df in result:`
  (the exact pattern shipped in the Fabric quickstart) fell back to Python's
  integer-index sequence protocol and raised `KeyError: 0`. Added `__iter__`
  yielding `(table_name, DataFrame)` pairs in generation order, so the documented
  quickstart loop now works as written.

### Added
- `GenerationResult.items()`, `.keys()`, `.values()` convenience accessors
  (delegate to the underlying `tables` dict). `dict(result)` now reconstructs
  `{table_name: DataFrame}`. Indexing and membership remain name-keyed
  (`result["order"]`, `"order" in result`); `len(result)` still returns total
  row count, not table count (use `len(result.tables)` for the table count).

## [2.14.2] - 2026-06-08

### Fixed
- `incremental/continue_engine.py` `_fk_map`: `ContinueEngine.continue_from()` now
  preserves foreign-key integrity on schemas WITHOUT explicit `strategy: foreign_key`
  generators (i.e. schemas produced by `SchemaBuilder`/`DataProfiler` over real data).
  Previously such FK columns fell through to `_perturb_columns` and were corrupted into
  orphan keys (FK valid rate dropped to ~0.0). FKs are now resolved from three sources in
  priority order: explicit FK generators, declared `schema.relationships`
  (child_columns → parent), and unambiguous name-based PK-name matching. This is the bug
  the Contoso "Day 2 / continue_from" demo had to work around with manual FK resampling.

### Notes
- Version was previously inconsistent across `pyproject.toml` (2.14.1) and
  `sqllocks_spindle/__init__.py` (2.14.0); both are now 2.14.2.

## [2.2.3] - 2026-03-17

### Fixed
- `packs/runner.py`: WindowsPath objects passed where strings expected, causing
  PackLoader/PackRunner failures on Windows — all path operations now use `str()`
- 12 YAML scenario pack files: unquoted colons causing parse errors on strict YAML parsers
- Removed hardcoded Event Hub shared access key from config/sweep scripts

### Changed
- `.gitignore`: updated patterns to catch generated notebook output in subdirectories

## [2.2.2] - 2026-03-17

### Fixed
- `DataProfiler._detect_distribution()`: KS test was using friendly names (`"normal"`,
  `"exponential"`) instead of scipy names (`"norm"`, `"expon"`), causing all distribution
  fits to silently fail — now uses `dist.name` for correct scipy lookup
- Sink import-error tests: fixed flaky `sys.modules` removal approach that failed to
  block re-import — now uses `unittest.mock.patch("builtins.__import__")` to properly
  simulate missing `azure-eventhub` and `kafka-python` packages
- Test count: 1715 passed + 3 failed → **1718 passed, 0 failed**

## [2.2.1] - 2026-03-17

### Fixed
- `WarehouseBulkWriter.write_tables()`: replaced Unicode arrow (`→`) in log messages
  with ASCII `->` to prevent `UnicodeEncodeError` on Windows cp1252 consoles
- `WarehouseBulkWriter.write_tables()`: reduced `max_workers` from 30 to 4 — concurrent
  COPY INTO operations were overwhelming Fabric Warehouse, causing socket timeouts on
  queued tables
- `WarehouseBulkWriter.copy_into()`: added `conn.timeout = 600` (10 min) to prevent
  premature connection drops during long-running COPY INTO loads

### Verified
- 23/23 integration test groups PASS on two consecutive seeds (42, 7) against live
  Fabric Warehouse at `large` scale (~37.7M rows across Retail/Financial/CapitalMarkets)

## [2.2.0] - 2026-03-17

### Fixed
- `FabricSqlDatabaseWriter`: boolean string columns (`"true"`/`"false"`) now correctly
  converted to Python `bool` before INSERT, preventing HY000 right-truncation on BIT
  columns with pyodbc `fast_executemany` (affected enterprise composite domain writes)

### Performance
- `FabricSqlDatabaseWriter`: `fast_executemany=True` + vectorized `_coerce_df_for_insert()`
  reduces SQL Database write time from 34 min → ~24s for 100K rows
- `FabricSqlDatabaseWriter`: COPY INTO path for Fabric Warehouse via `WarehouseBulkWriter`
  with parallel multi-file staging and concurrent table loading (per MS Learn performance guidelines)
- Cover-row algorithm ensures pyodbc VARCHAR buffer is sized from max-length row,
  eliminating right-truncation for variable-length string columns

### Added
- `--seed` CLI arg for `fabric_integration_sweep.py` — enables multi-seed regression testing
- `xxl` scale tier: ~1B orders; `xxxl` scale tier: ~1T rows total across all tables
- Warehouse load test upgraded to `scale="xxxl"` to validate COPY INTO at extreme volume
- `WarehouseBulkWriter`: parallel multi-file staging (all chunks first) + concurrent table COPY INTO

## [2.0.0] - 2026-03-14

### Added
- All 18 Blueprint items (E1-E18): CredentialResolver, RunManifest enhancements, observability, IoT/financial/clickstream/operational log simulation, state machines, SCD2 file drops, `spindle publish` CLI, acceptance tests, EventhouseWriter, Fabric provisioning guide
- Tier 3 features: `spindle learn` (schema inference), `spindle continue` (incremental generation), `spindle compare` (fidelity), `spindle time-travel` (snapshots), `spindle mask` (PII masking), composite presets, profile sharing
- 34/35 notebooks pre-executed with saved output
- 7 Fabric notebooks fixed (F01, F04, F05, F07, F08, F09, F10)

### Changed
- Version: 1.3.0 → 2.0.0 (major bump reflects complete feature set — all tiers, Blueprint, and Fabric integration)
- Test count: 989 → 1,250

## [1.3.0] - 2026-03-13

### Added
- **Chaos engine** — `ChaosEngine`, `ChaosConfig`, `ChaosCategory`, `ChaosOverride`
  - Six chaos categories: `schema`, `value`, `file`, `referential`, `temporal`, `volume`
  - Four intensity levels: `calm` (0.25x), `moderate` (1.0x), `stormy` (2.5x), `hurricane` (5.0x)
- **Simulation layer** — `FileDropSimulator`, `StreamEmitter`, `HybridSimulator`
- **Scenario Packs** — `PackLoader`, `PackRunner`, `PackValidator` (44 built-in packs)
- **GSL spec parser** — `GSLParser`, `GenerationSpec` (declarative YAML)
- **Validation gates + quarantine** — 8 gates + `QuarantineManager`
- **CompositeDomain + SharedEntityRegistry** — cross-domain FK enforcement
- **EventEnvelope + EnvelopeFactory** — CloudEvents-style wrapper
- **Fabric integration** — `OneLakePaths`, `LakehouseFilesWriter`, `EventstreamClient`
- **MCP bridge** — `python -m sqllocks_spindle.mcp_bridge` (7 commands)
- **SQL DDL import** — `DdlParser` for 4 SQL dialects, `spindle from-ddl` CLI
- **CREATE TABLE DDL in SQL output** — DDL generation in `to_sql_inserts()`
- **Fabric SQL Database Writer** — `FabricSqlDatabaseWriter` (4 auth methods, 4 write modes)
- **Semantic Model Writer** — `SemanticModelExporter` (.bim TOM JSON export)
- **Fabric Stream Writer** — `FabricStreamWriter` convenience wrapper
- **Capital Markets domain** (13th domain) — 10 tables, real S&P 500 tickers
- **Star schema + CDM maps for all 13 domains**
- 7 new Fabric guide doc pages, 12 new notebooks, 10 new example scripts

### Changed
- Version: 1.2.0 → 1.3.0
- Test count: 549 → 989

## [1.2.0] - 2026-03-12

### Added
- **Phase 6: Star schema transform** — `StarSchemaTransform`, `StarSchemaMap`, `DimSpec`, `FactSpec`, `StarSchemaResult`
  - Converts 3NF `GenerationResult` tables into dimension + fact layout
  - Auto-generates `dim_date` (YYYYMMDD surrogate key, 14 columns including fiscal year/quarter)
  - Dimension enrichment via left joins (`enrich` spec with prefix support)
  - Surrogate key assignment + natural key preservation (`sk_*` / `nk_*` columns)
  - `RetailDomain.star_schema_map()` — dim_customer, dim_product (enriched with category), dim_store, dim_promotion, fact_sale, fact_return
  - `HealthcareDomain.star_schema_map()` — dim_patient, dim_provider, dim_facility, fact_encounter, fact_claim
- **Phase 6: CDM folder export** — `CdmMapper`, `CdmEntityMap`
  - Writes Microsoft CDM folder structure (model.json + entity data files)
  - Compatible with Fabric CDM connectors, Dataverse, Power Platform, Azure Data Lake Storage CDM folders
  - `model.json` with entity definitions, attribute types, and partition metadata
  - CSV (default) and Parquet output formats
  - `RetailDomain.cdm_map()` — maps to CDM standard entities (Contact, Product, SalesOrder, etc.)
  - `HealthcareDomain.cdm_map()` — maps to healthcare CDM entities (Patient, Practitioner, Appointment, etc.)
- **Phase 6: Scale presets** — `fabric_demo` and `warehouse` added to all 13 domains
  - `fabric_demo`: ~10% of small scale — fast, ideal for conference demos and Fabric notebooks
  - `warehouse`: practical Fabric Data Warehouse scale — millions of rows in fact tables
- **Phase 6: CLI commands** — `spindle to-star <domain> --output ./star/` and `spindle to-cdm <domain> --output ./cdm/`
- **Phase 2: Streaming engine** — `SpindleStreamer`, `StreamConfig`, `BurstWindow`, `TimePattern`
  - Poisson inter-arrival times for statistically realistic event pacing
  - Token-bucket rate limiter with configurable burst windows
  - Out-of-order event injection
  - Sinks: `ConsoleSink`, `FileSink` (no extra deps); `EventHubSink` (`[streaming]` extra), `KafkaSink` (`[streaming]` extra)
- **Phase 2: Anomaly injection** — `AnomalyRegistry`, `PointAnomaly`, `ContextualAnomaly`, `CollectiveAnomaly`
  - All injected rows tagged with `_spindle_is_anomaly` / `_spindle_anomaly_type` columns
- **Phase 2: CLI command** — `spindle stream <domain> --table <t> --rate N --realtime --burst START:DUR:MULT`
- All new symbols exported from top-level `sqllocks_spindle` package

### Changed
- Version: 1.0.0 → 1.2.0

## [1.0.0] - 2026-03-11

Initial public release.

### Added
- Core generation engine with 21 column-level strategies
- Schema definition format (`.spindle.json`) with parser, validator, and topological sort
- **12 industry domains** — Retail (9 tables), Healthcare (9 tables), Financial (10), Supply Chain (10), IoT (8), HR (9), Insurance (9), Marketing (10), Education (9), Real Estate (9), Manufacturing (9), Telecom (9)
- Configurable distribution profiles (`profiles/default.json`) with `_dist()` and `_ratio()` API
- Profile overrides at runtime via `overrides={}` dict
- Real-world calibrated distributions from 40+ authoritative sources (NRF, Census, CMS, CDC, KFF, AAMC, BLS, HCUP)
- `METHODOLOGY.md` — full citation trail for every distribution weight
- Real US address data (40,977 ZIP codes from GeoNames CC-BY-4.0) with lat/lng for Power BI maps
- ID Manager with Pareto, Zipf, and uniform FK distributions
- Business rules engine for cross-table constraint enforcement
- CLI commands: `generate`, `describe`, `validate`, `list`
- `--dry-run` mode for generate command
- Output formats: CSV, TSV, JSON Lines, Parquet, Excel, SQL INSERT
- 103 tests (33 retail + 35 healthcare + 35 strategy)
- `py.typed` marker for PEP 561 compliance
