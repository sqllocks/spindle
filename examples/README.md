# Spindle Examples

Complete examples for every Spindle capability, organized by scenario.

## Quick install

```bash
pip install sqllocks-spindle          # core
pip install sqllocks-spindle[streaming]  # + Event Hubs + Kafka
pip install pyarrow openpyxl          # + Parquet + Excel output
```

---

## Scenario scripts (`examples/scenarios/`)

Run any scenario directly:
```bash
python examples/scenarios/01_hello_world.py
```

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

---

## Fabric notebooks (`examples/notebooks/`)

Jupyter notebooks ready to run in Microsoft Fabric. Each uses `spark` for Lakehouse writes.

| # | Notebook | What it covers |
|---|----------|----------------|
| 01 | [`01_quickstart.ipynb`](notebooks/01_quickstart.ipynb) | Generate retail data, explore DataFrames, integrity check, distribution overrides |
| 02 | [`02_domain_showcase.ipynb`](notebooks/02_domain_showcase.ipynb) | All 12 domains at `fabric_demo` scale with benchmarks and sample queries |
| 03 | [`03_fabric_lakehouse.ipynb`](notebooks/03_fabric_lakehouse.ipynb) | Write 3NF tables + star schema to Lakehouse as Delta; Spark SQL queries |
| 04 | [`04_star_schema.ipynb`](notebooks/04_star_schema.ipynb) | StarSchemaTransform, dim/fact exploration, SK integrity, CDM export |
| 05 | [`05_streaming.ipynb`](notebooks/05_streaming.ipynb) | SpindleStreamer, anomaly injection, burst windows, EventHubSink setup |

---

## Key concepts

### Scale presets

| Preset | Retail customers | Total rows (retail) | Use case |
|--------|-----------------|---------------------|----------|
| `fabric_demo` | ~200 | ~3,000 | Notebooks, demos, CI |
| `small` | ~1,000 | ~21,000 | Local dev, unit tests |
| `medium` | ~50,000 | ~1,000,000 | Integration testing |
| `large` | ~500,000 | ~10,000,000 | Staging environments |
| `warehouse` | ~1,000,000 | ~20,000,000 | Fabric Data Warehouse loads |
| `xlarge` | ~5,000,000 | ~100,000,000 | Extreme scale (use Spark) |

### Output formats

| Format | Method | Extra dependency |
|--------|--------|-----------------|
| CSV | `PandasWriter().to_csv()` | — |
| TSV | `PandasWriter().to_tsv()` | — |
| JSON Lines | `PandasWriter().to_jsonl()` | — |
| Parquet | `PandasWriter().to_parquet()` | `pyarrow` |
| Excel | `PandasWriter().to_excel()` | `openpyxl` |
| SQL INSERT | `PandasWriter().to_sql_inserts()` | — |
| Delta (Fabric) | `spark.createDataFrame(df).write.format("delta")` | Fabric notebook |
| CDM folder | `CdmMapper().write_cdm_folder()` | — |

### Streaming sinks

| Sink | Import | Extra dependency |
|------|--------|-----------------|
| `ConsoleSink` | `sqllocks_spindle.streaming` | — |
| `FileSink` | `sqllocks_spindle.streaming` | — |
| `EventHubSink` | `sqllocks_spindle.streaming` | `sqllocks-spindle[streaming]` |
| `KafkaSink` | `sqllocks_spindle.streaming` | `sqllocks-spindle[streaming]` |

### All 12 domains

`retail` · `healthcare` · `financial` · `supply_chain` · `iot` · `hr` ·
`insurance` · `marketing` · `education` · `real_estate` · `manufacturing` · `telecom`

---

## CLI quick reference

```bash
spindle list                                          # list all domains
spindle describe retail                               # schema details
spindle validate retail                               # validate schema
spindle generate retail --scale fabric_demo           # CSV output
spindle generate retail --scale small --format parquet --output ./out
spindle generate retail --scale fabric_demo --dry-run
spindle to-star retail --scale fabric_demo --output ./star
spindle to-cdm  retail --scale fabric_demo --output ./cdm
spindle stream  retail --table order --max-events 100
spindle stream  retail --table order --max-events 1000 --sink file --output orders.jsonl
spindle stream  retail --table order --rate 50 --realtime
```
