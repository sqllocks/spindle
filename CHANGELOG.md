# Changelog

All notable changes to Spindle will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- **Phase 6: Scale presets** — `fabric_demo` and `warehouse` added to all 12 domains
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

## [0.1.0] - 2026-03-11

### Added
- Core generation engine with 21 column-level strategies
- Schema definition format (`.spindle.json`) with parser, validator, and topological sort
- **Retail domain** — 9 tables (customer, address, product_category, product, store, promotion, order, order_line, return)
- **Healthcare domain** — 9 tables (provider, facility, patient, encounter, diagnosis, procedure, medication, claim, claim_line)
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
