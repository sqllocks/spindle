# Spindle Notebooks

Interactive Jupyter notebooks organized by skill level. Open in Jupyter, VS Code, or Microsoft Fabric.

For step-by-step tutorials that explain the concepts, see the [Tutorials](../../docs/tutorials/) on the docs site. These notebooks are the executable companion — run them alongside the tutorials or use them standalone.

## Prerequisites

- Python 3.10+ with `sqllocks-spindle` installed
- Jupyter, VS Code, or Microsoft Fabric notebook environment
- For Fabric scenarios (F01-F10): access to a Fabric workspace with a Lakehouse

## Tracks

### Quickstart (`quickstart/`)

**Audience:** New to Spindle. Covers installation through basic generation and export.

| # | Notebook | Time | What you'll learn |
|---|----------|------|-------------------|
| T01 | [Hello Spindle](quickstart/T01_hello_spindle.ipynb) | 5 min | Install, generate, verify FK integrity |
| T02 | [Explore All Domains](quickstart/T02_explore_all_domains.ipynb) | 10 min | Survey 13 domains, compare schemas |
| T03 | [Custom Schema](quickstart/T03_custom_schema.ipynb) | 15 min | Build .spindle.json from scratch |
| T04 | [Healthcare Deep Dive](quickstart/T04_healthcare_deep_dive.ipynb) | 15 min | Explore calibrated distributions, ICD-10/CPT |
| T05 | [Distribution Overrides](quickstart/T05_distribution_overrides.ipynb) | 10 min | Override defaults, reproducibility |
| T06 | [Star Schema Export](quickstart/T06_star_schema_export.ipynb) | 15 min | Dimensional modeling, CDM export |
| T07 | [Domain Quickstarts](quickstart/T07_domain_quickstarts.ipynb) | 10 min | Quick start snippet for each domain |

### Intermediate (`intermediate/`)

**Audience:** Know the basics. Ready for Fabric integration, streaming, chaos, and advanced patterns.

| # | Notebook | Time | What you'll learn |
|---|----------|------|-------------------|
| T08 | [Fabric Lakehouse](intermediate/T08_fabric_lakehouse.ipynb) | 15 min | Write Delta tables, Spark SQL |
| T09 | [Fabric Quickstart](intermediate/T09_fabric_quickstart.ipynb) | 10 min | Fast path to Fabric integration |
| T10 | [Fabric SQL Database](intermediate/T10_fabric_sql_database.ipynb) | 15 min | FabricSqlDatabaseWriter, DDL, Entra auth |
| T11 | [Capital Markets](intermediate/T11_capital_markets.ipynb) | 15 min | GBM pricing, dividends, earnings |
| T12 | [Streaming Events](intermediate/T12_streaming_events.ipynb) | 15 min | SpindleStreamer, anomalies, EventHub |
| T13 | [File Drop Simulation](intermediate/T13_file_drop_simulation.ipynb) | 15 min | FileDropSimulator, manifests, late arrivals |
| T14 | [Chaos Engineering](intermediate/T14_chaos_engineering.ipynb) | 15 min | ChaosEngine, intensity presets |
| T15 | [Validation Gates](intermediate/T15_validation_gates.ipynb) | 15 min | 8 gates, GateRunner, quarantine |
| T16 | [Composite Domains](intermediate/T16_composite_domains.ipynb) | 15 min | Multi-domain, shared entity registry |
| T17 | [Day 2 Incremental](intermediate/T17_day2_incremental.ipynb) | 15 min | ContinueEngine, time-travel, snapshots |

### Fabric Scenarios (`fabric-scenarios/`)

**Audience:** Building real Fabric solutions. Each notebook solves a specific architecture problem end-to-end.

| # | Notebook | Architecture Pattern |
|---|----------|---------------------|
| F01 | [Medallion Architecture](fabric-scenarios/F01_medallion_architecture.ipynb) | Bronze/Silver/Gold with chaos + validation |
| F02 | [Warehouse Dimensional](fabric-scenarios/F02_warehouse_dimensional.ipynb) | SQL DW dimensional load with DDL |
| F03 | [SQL Database OLTP](fabric-scenarios/F03_sql_database_oltp.ipynb) | Financial OLTP with incremental append |
| F04 | [Real-Time Streaming](fabric-scenarios/F04_realtime_streaming.ipynb) | Eventstream with anomalies + burst windows |
| F05 | [Chaos Pipeline](fabric-scenarios/F05_chaos_pipeline.ipynb) | IoT pipeline resilience testing |
| F06 | [Semantic Model](fabric-scenarios/F06_semantic_model.ipynb) | .bim TOM export with DAX measures |
| F07 | [Healthcare RCM](fabric-scenarios/F07_healthcare_rcm.ipynb) | Revenue cycle: claims, denials, star schema |
| F08 | [File-Drop Ingestion](fabric-scenarios/F08_filedrop_ingestion.ipynb) | Landing zone testing with late arrivals |
| F09 | [Cross-Domain Enterprise](fabric-scenarios/F09_cross_domain_enterprise.ipynb) | Retail + HR + Insurance composite |
| F10 | [Month-End Close](fabric-scenarios/F10_month_end_close.ipynb) | Financial snapshots with seasonal growth |

### Showcase (`showcase/`)

**Audience:** Anyone. Standalone feature demonstrations — one notebook per capability.

| # | Notebook | Feature |
|---|----------|---------|
| 01 | [Quickstart](showcase/01_quickstart.ipynb) | Core generation + distribution overrides |
| 02 | [Domain Showcase](showcase/02_domain_showcase.ipynb) | All 13 domains benchmarked |
| 03 | [Fabric Lakehouse](showcase/03_fabric_lakehouse.ipynb) | 3NF + star schema to Delta |
| 04 | [Star Schema](showcase/04_star_schema.ipynb) | Full dimensional transform + CDM |
| 05 | [Streaming](showcase/05_streaming.ipynb) | Streaming + anomaly injection |
| 06 | [Chaos & Simulation](showcase/06_chaos_and_simulation.ipynb) | All 4 simulation modes + chaos |
| 07 | [Composite Domain](showcase/07_composite_domain.ipynb) | Multi-domain with cross-domain FK |
| 08 | [Scenario Packs](showcase/08_scenario_packs.ipynb) | 44 built-in YAML packs |
