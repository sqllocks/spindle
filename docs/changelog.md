# Changelog

All notable changes to Spindle will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.11.0] - 2026-04-29

### Added — Phase 5: Validation Matrix & Demo Notebooks

#### Validation Matrix
- **`tests/fixtures/validation_matrix.py`** — Matrix builder with filter rules. `build_matrix()` returns 512 valid `(domain, sink, size, mode)` tuples covering 13 domains × 5 sinks × 4 sizes × 3 modes after filters (streaming + sql-server, fabric_demo + sql-server, inference + non-capable domains).
- **`tests/fixtures/mock_sinks.py`** — `MockSink` dataclass + `make_mock_sink(sink_type)` factory for all 5 sink types. Records write calls without performing real IO.
- **`tests/test_validation_matrix.py`** — Parametrized mock suite, 518 tests (512 combos + 6 matrix-builder unit tests). All passing.
- **`tests/test_validation_live.py`** — Live suite with 26 tests across 4 groups: A (13 domains × lakehouse × small × seeding), B (retail × all 5 sinks × fabric_demo × seeding), C (retail × lakehouse × all 4 sizes × streaming), D (retail × warehouse × all sizes × seeding). Auth via `InteractiveBrowserCredential` (browser fires once, token cached).
- **`pyproject.toml`** — Registered `infra` pytest marker; documented `SPINDLE_TEST_*_CONN` env vars for live tests.

#### Demo Notebooks (`notebooks/demos/`)
- `01_retail_lakehouse_quickstart.ipynb` — retail → lakehouse, seeding + streaming, all sizes, Delta read-back validation.
- `02_financial_warehouse_analytics.ipynb` — financial → Fabric Warehouse, all sizes, ODBC row-count validation.
- `03_healthcare_sql_database.ipynb` — healthcare → Fabric SQL Database, optional DataMasker HIPAA masking.
- `04_capital_markets_eventhouse.ipynb` — capital markets → Eventhouse/KQL, streaming tick data.
- `05_multi_domain_fanout.ipynb` — retail + financial → lakehouse + optional warehouse.
- `06_custom_ddl_to_lakehouse.ipynb` — bring-your-own DDL → DDLParser → generate → lakehouse.

#### Notebook Templates (`notebooks/templates/`)
- `template_domain_to_sink.ipynb` — parametrized starter for any domain → any sink.
- `template_custom_schema.ipynb` — custom `.spindle.json` or `.sql` schema → any sink.

### Notes
- No new sink code required — `FabricSqlDatabaseWriter` covers SQL Server (on-prem), Azure SQL Database, Azure SQL Managed Instance, Fabric Warehouse, and Fabric SQL Database via `auth_method` parameter.
- Mock matrix runtime: ~12 minutes locally (heavy at fabric_demo size). All 518 tests pass.

## [2.9.0] - 2026-04-28

### Added — Phase 3B: Inference Depth

Spindle generated data now statistically matches real source data across all fidelity dimensions: distribution shape, cardinality, null rates, temporal patterns, string formats, outlier rates, and column correlations.

#### New Classes

- **`EmpiricalStrategy`** (`engine/strategies/empirical.py`) — Quantile-fingerprint interpolation for numeric columns when parametric distribution fit is poor. Requires a `quantiles` dict (keys `p1`–`p99`). Supports `"linear"` (default, NumPy) and `"cubic"` (scipy, optional) interpolation.
- **`GaussianCopula`** (`engine/correlation.py`) — Post-generation correlation enforcement. Reorders column values to achieve target Pearson correlations without changing any column's marginal distribution. Algorithm: Cholesky decompose → draw correlated normals → re-rank values. Pure NumPy, no scipy.
- **`LakehouseProfiler`** (`inference/lakehouse_profiler.py`) — Fabric-native Delta table profiler. Reads tables over ABFSS via `deltalake`. Returns the same `DatasetProfile`/`TableProfile` as the other entry points. Requires `[fabric-inference]` extra.
- **`FidelityReport`** — Extended with `.score()` classmethod, `.failing_columns()`, `.to_dict()`, `.to_dataframe()`. Enables inline fidelity measurement during generation via new `fidelity_profile=` kwarg on `Spindle.generate()`.

#### Enhanced Classes

- **`DataProfiler`** — New constructor kwargs: `fit_threshold`, `top_n_values`, `outlier_iqr_factor`, `sample_rows`. New `profile()` alias (same as `profile_dataset()`). New `from_csv()` classmethod. Extended string pattern detection: `ssn`, `ip_address` (IPv4 + IPv6), `mac_address`, `iban`, `currency_code`, `language_code`, `postal_code`.
- **`ColumnProfile`** — New optional fields: `quantiles` (dict), `hour_histogram`, `dow_histogram`, `string_length`, `outlier_rate`, `value_counts_ext`, `fit_score`.
- **`TableProfile`** — New `correlation_matrix: dict[str, dict[str, float]] | None` field.
- **`SchemaBuilder.build()`** — New kwargs: `fit_threshold`, `correlation_threshold`, `include_anomaly_registry`. Returns `(SpindleSchema, AnomalyRegistry)` tuple when `include_anomaly_registry=True`. Extended priority tree (13 levels) with empirical fallback when KS fit < `fit_threshold`, temporal histogram routing, and correlation detection.
- **`Spindle.generate()`** — New kwargs: `enforce_correlations=True` (auto-applies `GaussianCopula` when schema contains `correlated_columns`) and `fidelity_profile=None` (returns `(GenerationResult, FidelityReport)` tuple when supplied).

#### New Extras

```bash
pip install sqllocks-spindle[inference]          # scipy for FidelityReport + empirical strategies
pip install sqllocks-spindle[fabric-inference]   # scipy + deltalake + pyarrow for LakehouseProfiler
```

#### New String Patterns in Engine

`ssn`, `ip_address` (IPv4 + IPv6), `mac_address`, `iban`, `currency_code`, `language_code`, `postal_code`

### Changed

- Test count: 1,946 → 1,973 (+27 Phase 3B tests across `test_empirical_strategy.py`, `test_correlation.py`, `test_fidelity_report_v2.py`, `test_lakehouse_profiler.py`, and additions to `test_inference.py` and `test_e2e_generation.py`)

---

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
- `ConnectionProfile` extended with `warehouse_staging_path` and
  `eventhouse_database` fields (required by `WarehouseSink` and `KQLSink`).

### Added

- `cmd_demo_status` MCP bridge command — reads the manifest by `session_id` and,
  when the run was a Spark submission, polls `FabricJobTracker.get_status` for
  live Fabric job state
- `cmd_demo_cleanup` MCP bridge command — runs `CleanupEngine` against a saved
  manifest by `session_id`

### Test count

1,930 → 1,946 (+16 new tests)

## [2.7.0] - 2026-04-27

### Added

- **Billion-row pipeline (Phase 2)** — Fabric Spark scale generation via `scale_mode="fabric_spark"`
    - `FabricSparkRouter` (`engine/spark_router.py`) — generates static tables in-process, uploads augmented schema JSON to OneLake via DFS API, finds or auto-creates `spindle_spark_worker` notebook, submits Fabric notebook run, returns `JobRecord` immediately
    - `AsyncJobStore` + `JobRecord` (`engine/async_job_store.py`) — thread-safe in-process registry tracking submitted Fabric jobs by `job_id`
    - `FabricJobTracker` (`engine/job_tracker.py`) — polls and cancels Fabric notebook runs via the Fabric Jobs REST API
    - `spindle_spark_worker.ipynb` — Fabric notebook template: reads schema from OneLake, `foreachPartition` dynamic table generation, writes to LakehouseSink / WarehouseSink / KQLSink / SQLDatabaseSink, saves result stats and cleans up temp file
    - `cmd_scale_status` MCP bridge command — polls Fabric job status by `job_id`; maps Fabric statuses to `submitted|running|succeeded|failed|cancelled`
    - `cmd_scale_cancel` MCP bridge command — cancels an in-flight Fabric notebook run
    - `cmd_scale_generate(scale_mode="fabric_spark")` now fully implemented; requires `sink_config.workspace_id`, `sink_config.lakehouse_id`, `sink_config.token`
    - `sqllocks_spindle/notebooks/__init__.py` — loads and exports `SPARK_WORKER_IPYNB` notebook template

### Changed

- Test count: 1,913 → 1,930 (+17 Phase 2 unit tests in `tests/test_spark_router.py`)

## [2.6.1] - 2026-04-26

### Fixed

- **GAP 1 — Reference table chunk replication**: `ScaleRouter` now classifies tables as *static* (schema count < `chunk_size`) or *dynamic* (schema count ≥ `chunk_size`). Static tables are generated once with their natural cardinality and broadcast as pre-loaded PK pools into every chunk worker via the augmented schema JSON. Dynamic tables are generated `chunk_size` rows per chunk. Added `_classify_tables`, `_generate_static_tables`, and `_SpindleJSONEncoder` (handles `pd.Timestamp`, numpy scalars) to `scale_router.py`.
- **GAP 2 — Composite FK reference impossible**: New `composite_foreign_key` strategy (`engine/strategies/composite_foreign_key.py`) — takes `ref_table` + `ref_columns: [list]`, samples rows from the parent table, returns a dict of per-column arrays. New `composite_fk_field` strategy reads one component from the stashed dict. Both strategies registered in `Spindle`, `ChunkWorker`, `ScaleRouter._generate_static_tables`.
- **GAP 3 — Composite PK FK lookup returns 2D array**: `TableGenerator.generate()` now detects `dict` returns from strategies (multi-column path) and unpacks each key into `ctx.current_table`. `_cfo_` prefix cache keys are filtered from the public DataFrame alongside `_rs_` and `_sr_`.
- **GAP 4 — Computed columns not applied in `ChunkWorker`**: Extracted `_compute_phase` into module-level `apply_compute_phase(tables, schema)` in `generator.py`; `chunk_worker.generate_chunk` now calls it after generating all tables.
- **GAP 5 — Business rules not applied in `ChunkWorker`**: `generate_chunk` calls `BusinessRulesEngine.fix_violations()` after `apply_compute_phase` when the schema defines business rules.
- **GAP 6 — PK-free tables rejected as errors**: Downgraded `"Table has no primary key defined"` from `error` to `warning` in `SchemaValidator`. `IDManager.register_table()` now gracefully skips pool registration for empty `pk_columns` lists (registers data-only for constrained FK lookups).
- **GAP 7 — Self-referencing hierarchies shatter across chunks**: Resolved by GAP 1 fix — tables using `self_referencing` strategy are typically small reference tables (count < `chunk_size`) and are now generated once, preserving a single unified hierarchy.
- **GAP 8 — `get_filtered_fks` reads first column, not PK**: Replaced `df.loc[mask, df.columns[0]]` with `pool[np.where(mask.values)[0]]` — uses the PK pool (aligned with df rows) regardless of column order.
- **GAP 9 — `generate_stream()` missing compute phase and business rules**: `Spindle.generate_stream()` now buffers all generated tables internally before yielding, then applies `_compute_phase` and `fix_violations` in the same pass as `Spindle.generate()`.
- **GAP 10 — Wrong exception type in `DependencyResolver`**: Added `MissingTableError(ValueError)` to `schema/dependency.py`; the resolver now raises it (not `CircularDependencyError`) when a table depends on a non-existent table.

### Changed

- Test count: 1,912 → 1,913 (+1 revised E2E test asserting correct static/dynamic cardinalities)
- `test_e2e_scale_router.py`: Assertions updated to validate static table natural cardinality (e.g., `product_category` = 50 rows) and dynamic table chunk multiplication, replacing the incorrect "all tables = TOTAL_ROWS" assertion.

## [2.6.0] - 2026-04-25

### Added

- **Billion-row pipeline (Phase 1)** — multi-process scale generation for datasets up to 1B+ rows
    - `SinkRegistry` — fan-out coordinator; writes to all sinks in parallel via `ThreadPoolExecutor`; raises `SinkError` with per-sink failures on partial errors
    - `ChunkWorker` (`generate_chunk`) — subprocess-safe pure function; deferred imports; returns plain Python lists (pickle-safe); applies `sequence_offset` for PK continuity across chunks
    - `ScaleRouter` — `ProcessPoolExecutor`-based orchestrator; psutil RAM guard caps workers at 80% available RAM; `as_completed()` fan-out with configurable `max_workers` and `chunk_size`
    - `StreamManager` — singleton per process; daemon threads; `stop_event.wait()` for interruptible sleep; thread-safe `counter_lock` on `StreamState`; `stop()` returns `bool | None` (None=unknown, True=clean, False=timeout)
    - `LakehouseSink` — writes Parquet via `LakehouseFilesWriter`; supports local path mode for testing
    - `WarehouseSink` — stages Parquet and loads via COPY INTO using `WarehouseBulkWriter`
    - `KQLSink` — ingests into Fabric Eventhouse via `EventhouseWriter`; deferred import with clear pip-install error
    - `SQLDatabaseSink` — bulk-inserts into Fabric SQL Database / Azure SQL via `FabricSqlDatabaseWriter`
    - `cmd_scale_generate` MCP bridge command — local single-process and multi-process (subprocess workers) modes; temp file cleanup in finally; seed propagated in return dict
    - `cmd_stream` / `cmd_stream_status` / `cmd_stream_stop` MCP bridge commands — background streaming with configurable `interval_seconds`, `max_chunks`, sink fan-out

### Fixed

- `reference_data.py` — `_load_dataset` now wraps domain path strings with `Path()` before `/` operator; was raising `TypeError` when `_domain_path` was injected as a plain string from JSON
- `19_scenario_packs.py` — updated to use dict-access (`p['domain']`, `p['pack_id']`) after `PackLoader.list_builtin()` API change

### Changed

- Test count: 1,867 → 1,912 (+45 Phase 1 tests including e2e integration test)

## [2.0.0] - 2026-03-14

### Added

- All 18 Blueprint items (E1-E18): CredentialResolver, RunManifest enhancements, observability, IoT/financial/clickstream/operational log simulation, state machines, SCD2 file drops, `spindle publish` CLI, acceptance tests, EventhouseWriter, Fabric provisioning guide
- Tier 3 features: `spindle learn`, `spindle continue`, `spindle compare`, `spindle time-travel`, `spindle mask`, composite presets, profile sharing
- 34/35 notebooks pre-executed with saved output

### Changed

- Version: 1.3.0 -> 2.0.0 (major bump reflects complete feature set)
- Test count: 989 -> 1,250

## [1.3.0] - 2026-03-13

### Added

- **Chaos engine** -- `ChaosEngine`, `ChaosConfig`, `ChaosCategory`, `ChaosOverride`
    - Six chaos categories: `schema`, `value`, `file`, `referential`, `temporal`, `volume`
    - Four intensity levels: `calm` (0.25x), `moderate` (1.0x), `stormy` (2.5x), `hurricane` (5.0x)
    - Escalation modes: `gradual`, `random`, `front-loaded`
    - Methods: `corrupt_dataframe()`, `drift_schema()`, `corrupt_file()`, `inject_referential_chaos()`, `inject_temporal_chaos()`, `inject_volume_chaos()`, `apply_all()`

- **Simulation layer** -- three modes for realistic pipeline testing
    - `FileDropSimulator` -- daily/hourly/15-min cadence, Parquet/CSV/JSONL, manifests, done flags, lateness, duplicates, backfill
    - `StreamEmitter` -- CloudEvents envelopes, rate + jitter, out-of-order, replay windows, multi-topic
    - `HybridSimulator` -- concurrent batch + stream, correlation ID linking

- **Scenario Packs** -- `PackLoader`, `PackRunner`, `PackValidator`, `ScenarioPack`
    - 44 built-in packs: 11 verticals x 4 simulation types
    - `list_builtin()`, `load_builtin()`, `PackRunner.run()`

- **GSL spec parser** -- `GSLParser`, `GenerationSpec`
    - Declarative YAML tying schema, scenario pack, chaos, outputs, and validation gates

- **Validation gates + quarantine** -- `ReferentialIntegrityGate`, `SchemaConformanceGate`, `NullConstraintGate`, `UniqueConstraintGate`, `RangeConstraintGate`, `TemporalConsistencyGate`, `FileFormatGate`, `SchemaDriftGate`
    - `QuarantineManager` -- `quarantine_file()`, `quarantine_dataframe()`, `list_quarantined()`

- **CompositeDomain + SharedEntityRegistry**
    - Multi-domain generation with cross-domain FK enforcement
    - `SharedConcept` enum: `PERSON`, `LOCATION`, `ORGANIZATION`, `CALENDAR`

- **EventEnvelope + EnvelopeFactory** -- CloudEvents-style wrapper

- **Fabric integration** -- `OneLakePaths`, `LakehouseFilesWriter`, `EventstreamClient`

- **MCP bridge** -- `python -m sqllocks_spindle.mcp_bridge` (7 commands)

- **10 new example scripts** (13-22) and **3 new notebooks** (06-08)

- **SQL DDL import** -- `DdlParser` for 4 SQL dialects (F-001)
    - `spindle from-ddl` CLI command
    - 30+ type-to-strategy mappings, 25+ column name heuristics
    - FK detection from explicit constraints and naming conventions

- **CREATE TABLE DDL in SQL output** -- `to_sql_inserts()` with DDL generation (F-002)
    - 3 dialect type maps (T-SQL, PostgreSQL, MySQL)
    - Fabric Warehouse compatibility (no PK constraints, no IDENTITY)
    - CLI: `--sql-ddl`, `--sql-drop`, `--sql-go`, `--sql-dialect`, `--schema-name`

- **Fabric SQL Database Writer** -- `FabricSqlDatabaseWriter` (F-003)
    - 4 auth methods: `cli` (Entra/az login), `msi`, `spn`, `sql`
    - 4 write modes: `create_insert`, `insert_only`, `truncate_insert`, `append`
    - Parameterized `executemany`, dependency-ordered writes/drops
    - CLI: `--format sql-database`, `--connection-string`, `--auth`, `--write-mode`
    - New `[fabric-sql]` extra: `pyodbc>=5.0`, `azure-identity>=1.15`

- **Semantic Model Writer** -- `SemanticModelExporter` (F-004)
    - .bim TOM JSON export at compatibilityLevel 1604
    - Auto DAX measures (COUNTROWS + SUM/AVERAGE for numerics)
    - M expressions for lakehouse, warehouse, and sql_database source types
    - CLI: `spindle export-model`

- **Fabric Stream Writer** -- `FabricStreamWriter` convenience wrapper (F-005)
    - Single `stream()` call with sensible defaults for Fabric Notebooks

- **Capital Markets domain** (13th domain) -- 10 tables (F-012)
    - Real S&P 500 tickers (110 companies), GICS sectors/industries
    - Daily OHLCV pricing, dividends, splits, earnings with EPS surprise
    - Insider transactions, tick-level trades for streaming
    - Star schema map (4 dims, 4 facts) and CDM mapping

- **Star schema + CDM maps for all 13 domains**
    - Every domain now provides `star_schema_map()` and `cdm_map()` methods

- **7 new Fabric guide doc pages** -- Lakehouse, Warehouse, SQL Database, Notebooks, Star Schema, CDM Export, 60-Second Overview

- **12 new notebooks** -- T05-T09 tutorials + F01-F07 Fabric scenarios

### Changed

- Version: 1.2.0 -> 1.3.0
- Test count: 549 -> 989

## [1.2.0] - 2026-03-12

### Added

- **Star schema transform** -- `StarSchemaTransform`, `StarSchemaMap`, `DimSpec`, `FactSpec`, `StarSchemaResult`
    - Auto-generates `dim_date` (YYYYMMDD surrogate key, 14 columns)
    - `RetailDomain.star_schema_map()` and `HealthcareDomain.star_schema_map()`

- **CDM folder export** -- `CdmMapper`, `CdmEntityMap`
    - Microsoft CDM folder structure (model.json + entity data files)
    - `RetailDomain.cdm_map()` and `HealthcareDomain.cdm_map()`

- **Scale presets** -- `fabric_demo` and `warehouse` added to all 13 domains

- **CLI commands** -- `spindle to-star` and `spindle to-cdm`

- **Streaming engine** -- `SpindleStreamer`, `StreamConfig`, `BurstWindow`, `TimePattern`
    - Poisson inter-arrivals, token-bucket rate limiting, burst windows
    - Sinks: `ConsoleSink`, `FileSink`, `EventHubSink`, `KafkaSink`

- **Anomaly injection** -- `AnomalyRegistry`, `PointAnomaly`, `ContextualAnomaly`, `CollectiveAnomaly`

- **CLI** -- `spindle stream` command

### Changed

- Version: 1.0.0 -> 1.2.0

## [1.0.0] - 2026-03-11

### Added

- Core generation engine with 21 column-level strategies
- Schema definition format (`.spindle.json`) with parser, validator, and topological sort
- **Retail domain** -- 9 tables, 3NF normalized
- **Healthcare domain** -- 9 tables, 3NF normalized
- 10 additional domains: Financial, Supply Chain, IoT, HR, Insurance, Marketing, Education, Real Estate, Manufacturing, Telecom
- Distribution profiles with `_dist()` and `_ratio()` API, runtime overrides
- Real-world calibrations from 40+ authoritative sources (NRF, Census, CMS, CDC, KFF, AAMC, BLS)
- Real US address data (40,977 ZIP codes from GeoNames CC-BY-4.0) with lat/lng
- ID Manager with Pareto, Zipf, and uniform FK distributions
- Business rules engine for cross-table constraint enforcement
- CLI: `generate`, `describe`, `validate`, `list`, `--dry-run`
- Output formats: CSV, TSV, JSON Lines, Parquet, Excel, SQL INSERT, Delta
- Fabric Lakehouse writer (`DeltaWriter` via delta-rs)
- 103 tests
