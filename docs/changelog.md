# Changelog

All notable changes to Spindle will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

- **Scale presets** -- `fabric_demo` and `warehouse` added to all 12 domains

- **CLI commands** -- `spindle to-star` and `spindle to-cdm`

- **Streaming engine** -- `SpindleStreamer`, `StreamConfig`, `BurstWindow`, `TimePattern`
    - Poisson inter-arrivals, token-bucket rate limiting, burst windows
    - Sinks: `ConsoleSink`, `FileSink`, `EventHubSink`, `KafkaSink`

- **Anomaly injection** -- `AnomalyRegistry`, `PointAnomaly`, `ContextualAnomaly`, `CollectiveAnomaly`

- **CLI** -- `spindle stream` command

### Changed

- Version: 1.0.0 -> 1.2.0

## [0.1.0] - 2026-03-11

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
