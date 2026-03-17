# Changelog

All notable changes to Spindle will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
