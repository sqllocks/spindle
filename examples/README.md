# Spindle Examples

Complete, runnable examples for every Spindle capability. For narrative tutorials that explain concepts step by step, see the [Tutorials](../docs/tutorials/) on the docs site.

## Quick Install

```bash
pip install sqllocks-spindle          # core
pip install sqllocks-spindle[streaming]  # + Event Hubs + Kafka
pip install pyarrow openpyxl          # + Parquet + Excel output
```

## Where to Start

| I want to... | Start with |
|-------------|-----------|
| See Spindle work in 30 seconds | [`01_hello_world.py`](scenarios/01_hello_world.py) |
| Generate data in a Fabric notebook | [`notebooks/quickstart/T01_hello_spindle.ipynb`](notebooks/quickstart/T01_hello_spindle.ipynb) |
| Use the CLI without writing Python | [`12_cli_usage.py`](scenarios/12_cli_usage.py) |
| Test my pipeline with bad data | [`13_chaos_injection.py`](scenarios/13_chaos_injection.py) |
| Stream events to Event Hub | [`11_streaming_eventhub_kafka.py`](scenarios/11_streaming_eventhub_kafka.py) |
| Build a medallion architecture | [`notebooks/fabric-scenarios/F01_medallion_architecture.ipynb`](notebooks/fabric-scenarios/F01_medallion_architecture.ipynb) |

---

## Scenario Scripts (`scenarios/`)

Standalone Python scripts. Run any scenario directly:

```bash
python examples/scenarios/01_hello_world.py
```

### Core (01-12)

| # | File | What it covers |
|---|------|----------------|
| 01 | [`01_hello_world.py`](scenarios/01_hello_world.py) | Install, generate, inspect — minimum viable example |
| 02 | [`02_exploring_results.py`](scenarios/02_exploring_results.py) | GenerationResult API: summary, row counts, generation order, integrity check |
| 03 | [`03_scale_presets.py`](scenarios/03_scale_presets.py) | All named scale presets; custom scale overrides; programmatic scale inspection |
| 04 | [`04_output_formats.py`](scenarios/04_output_formats.py) | CSV, TSV, JSON Lines, Parquet, Excel, SQL INSERT |
| 05 | [`05_distribution_overrides.py`](scenarios/05_distribution_overrides.py) | Runtime distribution overrides; scenario simulation; reproducibility |
| 06 | [`06_star_schema.py`](scenarios/06_star_schema.py) | StarSchemaTransform; dim tables; fact tables; dim_date; SK/NK columns; custom fiscal year |
| 07 | [`07_cdm_export.py`](scenarios/07_cdm_export.py) | CDM folder export; standard entity names; Parquet CDM; in-memory model.json; custom entity maps |
| 08 | [`08_streaming_basics.py`](scenarios/08_streaming_basics.py) | ConsoleSink; FileSink (JSONL); streaming from pre-generated tables; multiple tables |
| 09 | [`09_streaming_realtime.py`](scenarios/09_streaming_realtime.py) | Rate limiting; Poisson inter-arrivals; burst windows; time-of-day patterns; out-of-order events |
| 10 | [`10_streaming_anomalies.py`](scenarios/10_streaming_anomalies.py) | PointAnomaly; ContextualAnomaly; CollectiveAnomaly; combined registries; labeled ground truth |
| 11 | [`11_streaming_eventhub_kafka.py`](scenarios/11_streaming_eventhub_kafka.py) | EventHubSink (Fabric Eventstream); KafkaSink; multi-table streaming |
| 12 | [`12_cli_usage.py`](scenarios/12_cli_usage.py) | All CLI commands: generate, describe, validate, list, stream, to-star, to-cdm |

### Advanced (13-22)

| # | File | What it covers |
|---|------|----------------|
| 13 | [`13_chaos_injection.py`](scenarios/13_chaos_injection.py) | ChaosEngine: intensity presets (calm/moderate/stormy/hurricane), day-by-day corruption, value/schema/temporal/volume chaos, force overrides |
| 14 | [`14_file_drop_simulation.py`](scenarios/14_file_drop_simulation.py) | FileDropSimulator: daily/hourly cadence, multi-format (Parquet/CSV/JSONL), partitioning, manifests, done flags, late arrivals, duplicate injection |
| 15 | [`15_stream_emitter.py`](scenarios/15_stream_emitter.py) | StreamEmitter: CloudEvents envelopes, jitter, out-of-order, replay windows (at-least-once delivery), multi-table topic mapping |
| 16 | [`16_hybrid_simulation.py`](scenarios/16_hybrid_simulation.py) | HybridSimulator: concurrent batch + stream with shared correlation_id, split table routing, link strategies |
| 17 | [`17_composite_domain.py`](scenarios/17_composite_domain.py) | CompositeDomain: multi-domain generation, SharedEntityRegistry (PERSON, LOCATION, ORGANIZATION, CALENDAR), cross-domain FK enforcement |
| 18 | [`18_validation_gates.py`](scenarios/18_validation_gates.py) | ReferentialIntegrityGate, SchemaConformanceGate, GateResult inspection, QuarantineManager |
| 19 | [`19_scenario_packs.py`](scenarios/19_scenario_packs.py) | PackLoader, PackValidator, PackRunner: 44 built-in YAML packs (11 verticals x 4 types), custom pack loading |
| 20 | [`20_gsl_spec.py`](scenarios/20_gsl_spec.py) | GSLParser: declarative YAML specs tying schema, chaos, outputs, and validation gates |
| 21 | [`21_event_envelope.py`](scenarios/21_event_envelope.py) | EnvelopeFactory: CloudEvents wrappers, batch wrapping, custom event types, metadata, tenant overrides |
| 22 | [`22_fabric_integration.py`](scenarios/22_fabric_integration.py) | OneLakePaths, LakehouseFilesWriter (partition writes, manifests, done flags), EventstreamClient blueprint |

---

## Notebooks (`notebooks/`)

Jupyter notebooks organized by skill level. Open in Jupyter, VS Code, or Microsoft Fabric.

### Quickstart Track (`notebooks/quickstart/`)

For beginners. Zero to generating data.

| # | Notebook | What it covers |
|---|----------|----------------|
| T01 | [`T01_hello_spindle.ipynb`](notebooks/quickstart/T01_hello_spindle.ipynb) | Install, generate retail at fabric_demo scale, verify FK integrity, export CSV |
| T02 | [`T02_explore_all_domains.ipynb`](notebooks/quickstart/T02_explore_all_domains.ipynb) | Survey all 13 domains, compare table counts, deep-dive retail and healthcare schemas |
| T03 | [`T03_custom_schema.ipynb`](notebooks/quickstart/T03_custom_schema.ipynb) | Build a .spindle.json schema from scratch, 2 tables, 3 strategies |
| T04 | [`T04_healthcare_deep_dive.ipynb`](notebooks/quickstart/T04_healthcare_deep_dive.ipynb) | Healthcare domain: patient demographics, encounter distributions, ICD-10/CPT codes |
| T05 | [`T05_distribution_overrides.ipynb`](notebooks/quickstart/T05_distribution_overrides.ipynb) | Override distributions at runtime, compare default vs custom, reproducibility with seeds |
| T06 | [`T06_star_schema_export.ipynb`](notebooks/quickstart/T06_star_schema_export.ipynb) | StarSchemaTransform, dim/fact exploration, SK integrity, CDM export |
| T07 | [`T07_domain_quickstarts.ipynb`](notebooks/quickstart/T07_domain_quickstarts.ipynb) | Quick start snippets for each of the 13 domains |

### Intermediate Track (`notebooks/intermediate/`)

Real-world patterns and Fabric integration.

| # | Notebook | What it covers |
|---|----------|----------------|
| T08 | [`T08_fabric_lakehouse.ipynb`](notebooks/intermediate/T08_fabric_lakehouse.ipynb) | Write to Fabric Lakehouse as Delta tables, Spark SQL queries |
| T09 | [`T09_fabric_quickstart.ipynb`](notebooks/intermediate/T09_fabric_quickstart.ipynb) | Fast path to Fabric integration with sensible defaults |
| T10 | [`T10_fabric_sql_database.ipynb`](notebooks/intermediate/T10_fabric_sql_database.ipynb) | FabricSqlDatabaseWriter with Entra auth, DDL generation, write modes |
| T11 | [`T11_capital_markets.ipynb`](notebooks/intermediate/T11_capital_markets.ipynb) | Capital Markets domain deep dive: GBM pricing, dividends, earnings, insider transactions |
| T12 | [`T12_streaming_events.ipynb`](notebooks/intermediate/T12_streaming_events.ipynb) | SpindleStreamer, anomaly injection, burst windows, EventHub integration |
| T13 | [`T13_file_drop_simulation.ipynb`](notebooks/intermediate/T13_file_drop_simulation.ipynb) | FileDropSimulator with late arrivals, manifests, chaos injection |
| T14 | [`T14_chaos_engineering.ipynb`](notebooks/intermediate/T14_chaos_engineering.ipynb) | ChaosEngine: intensity presets, escalation, category-specific corruption |
| T15 | [`T15_validation_gates.ipynb`](notebooks/intermediate/T15_validation_gates.ipynb) | 8 validation gates, GateRunner, QuarantineManager |
| T16 | [`T16_composite_domains.ipynb`](notebooks/intermediate/T16_composite_domains.ipynb) | CompositeDomain with SharedEntityRegistry, cross-domain FK enforcement |
| T17 | [`T17_day2_incremental.ipynb`](notebooks/intermediate/T17_day2_incremental.ipynb) | ContinueEngine, time-travel snapshots, incremental generation |

### Fabric Scenarios (`notebooks/fabric-scenarios/`)

End-to-end Fabric workflows. Each solves a real architecture problem.

| # | Notebook | What it covers |
|---|----------|----------------|
| F01 | [`F01_medallion_architecture.ipynb`](notebooks/fabric-scenarios/F01_medallion_architecture.ipynb) | Bronze/Silver/Gold pipeline with chaos injection and validation gates |
| F02 | [`F02_warehouse_dimensional.ipynb`](notebooks/fabric-scenarios/F02_warehouse_dimensional.ipynb) | FabricSqlDatabaseWriter: DDL generation, SQL INSERT, write modes |
| F03 | [`F03_sql_database_oltp.ipynb`](notebooks/fabric-scenarios/F03_sql_database_oltp.ipynb) | Financial domain OLTP simulation with Day 2 incremental append |
| F04 | [`F04_realtime_streaming.ipynb`](notebooks/fabric-scenarios/F04_realtime_streaming.ipynb) | Streaming to console/file/Eventstream with burst windows and anomalies |
| F05 | [`F05_chaos_pipeline.ipynb`](notebooks/fabric-scenarios/F05_chaos_pipeline.ipynb) | IoT chaos pipeline: corruption, schema drift, validation, quarantine |
| F06 | [`F06_semantic_model.ipynb`](notebooks/fabric-scenarios/F06_semantic_model.ipynb) | SemanticModelExporter: .bim TOM JSON, DAX measures, M expressions |
| F07 | [`F07_healthcare_rcm.ipynb`](notebooks/fabric-scenarios/F07_healthcare_rcm.ipynb) | Healthcare RCM: claim processing, denial rates, star schema for analytics |
| F08 | [`F08_filedrop_ingestion.ipynb`](notebooks/fabric-scenarios/F08_filedrop_ingestion.ipynb) | File-drop ingestion testing: daily cadence, late arrivals, chaos, validation |
| F09 | [`F09_cross_domain_enterprise.ipynb`](notebooks/fabric-scenarios/F09_cross_domain_enterprise.ipynb) | Retail + HR + Insurance composite, per-domain star schemas, partitioned Parquet |
| F10 | [`F10_month_end_close.ipynb`](notebooks/fabric-scenarios/F10_month_end_close.ipynb) | Financial month-end close: time-travel snapshots, seasonal growth, adjusting entries |

### Showcase (`notebooks/showcase/`)

Feature demonstrations. Standalone notebooks showing one capability each.

| # | Notebook | What it covers |
|---|----------|----------------|
| 01 | [`01_quickstart.ipynb`](notebooks/showcase/01_quickstart.ipynb) | Generate retail, explore DataFrames, verify integrity, override distributions |
| 02 | [`02_domain_showcase.ipynb`](notebooks/showcase/02_domain_showcase.ipynb) | All 13 domains at fabric_demo scale with benchmarks and sample queries |
| 03 | [`03_fabric_lakehouse.ipynb`](notebooks/showcase/03_fabric_lakehouse.ipynb) | Write 3NF + star schema to Lakehouse as Delta; Spark SQL queries |
| 04 | [`04_star_schema.ipynb`](notebooks/showcase/04_star_schema.ipynb) | StarSchemaTransform, dim/fact exploration, SK integrity, CDM export |
| 05 | [`05_streaming.ipynb`](notebooks/showcase/05_streaming.ipynb) | SpindleStreamer, anomaly injection, burst windows, EventHubSink overview |
| 06 | [`06_chaos_and_simulation.ipynb`](notebooks/showcase/06_chaos_and_simulation.ipynb) | ChaosEngine + FileDropSimulator + StreamEmitter + HybridSimulator |
| 07 | [`07_composite_domain.ipynb`](notebooks/showcase/07_composite_domain.ipynb) | CompositeDomain for HR + Retail + Financial with cross-domain FK |
| 08 | [`08_scenario_packs.ipynb`](notebooks/showcase/08_scenario_packs.ipynb) | PackLoader: browse, validate, run all 4 pack types, custom YAML packs |

---

## Key Concepts

### Scale Presets

| Preset | Retail customers | Total rows (retail) | Use case |
|--------|-----------------|---------------------|----------|
| `fabric_demo` | ~200 | ~3,000 | Notebooks, demos, CI |
| `small` | ~1,000 | ~21,000 | Local dev, unit tests |
| `medium` | ~50,000 | ~1,000,000 | Integration testing |
| `large` | ~500,000 | ~10,000,000 | Staging environments |
| `warehouse` | ~1,000,000 | ~20,000,000 | Fabric Data Warehouse loads |
| `xlarge` | ~5,000,000 | ~100,000,000 | Extreme scale (use Spark) |

### All 13 Domains

`retail` . `healthcare` . `financial` . `supply_chain` . `iot` . `hr` .
`insurance` . `marketing` . `education` . `real_estate` . `manufacturing` . `telecom` . `capital_markets`

---

## CLI Quick Reference

```bash
spindle list                                          # list all domains
spindle describe retail                               # schema details
spindle validate retail                               # validate schema
spindle generate retail --scale fabric_demo           # CSV output
spindle generate retail --scale small --format parquet --output ./out
spindle to-star retail --scale fabric_demo --output ./star
spindle to-cdm  retail --scale fabric_demo --output ./cdm
spindle stream  retail --table order --max-events 100
```
