# CLI Cheatsheet

All commands are available after installing `sqllocks-spindle`.

## Data Generation

### `spindle generate`

Generate synthetic data for a domain.

```bash
spindle generate retail --scale small --seed 42 --format csv --output ./output/
spindle generate healthcare --scale medium --format parquet --output ./data/
spindle generate retail --scale medium --dry-run   # preview without generating
```

| Option | Default | Description |
| --- | --- | --- |
| `--scale, -s` | `small` | Scale preset: `fabric_demo`, `small`, `medium`, `large`, `warehouse`, `xlarge` |
| `--seed` | `42` | Random seed for reproducibility |
| `--output, -o` | — | Output directory (required for file formats) |
| `--format` | `summary` | Output format: `summary`, `csv`, `tsv`, `jsonl`, `parquet`, `excel`, `sql`, `delta` |
| `--mode, -m` | `3nf` | Schema mode: `3nf`, `star` |
| `--dry-run` | — | Show planned row counts without generating |

### `spindle to-star`

Generate and export as a star schema (dim_* + fact_* tables).

```bash
spindle to-star retail --scale small --output ./star/ --format parquet
```

| Option | Default | Description |
| --- | --- | --- |
| `--scale, -s` | `small` | Scale preset |
| `--seed` | `42` | Random seed |
| `--output, -o` | — | Output directory (required) |
| `--format` | `csv` | `csv` or `parquet` |

### `spindle to-cdm`

Generate and export as a Microsoft CDM folder (model.json + data files).

```bash
spindle to-cdm retail --scale small --output ./cdm/
```

| Option | Default | Description |
| --- | --- | --- |
| `--scale, -s` | `small` | Scale preset |
| `--seed` | `42` | Random seed |
| `--output, -o` | — | Output directory (required) |
| `--format` | `csv` | `csv` or `parquet` |
| `--model-name` | — | CDM model name (defaults to `Spindle<Domain>`) |

---

## Schema & Domain Info

### `spindle list`

List all available domains and their profiles.

```bash
spindle list
```

### `spindle describe`

Show a domain's schema, tables, relationships, and generation order.

```bash
spindle describe retail
spindle describe healthcare --mode star
```

| Option | Default | Description |
| --- | --- | --- |
| `--mode, -m` | `3nf` | Schema mode |

### `spindle validate`

Validate a `.spindle.json` schema file.

```bash
spindle validate my_schema.spindle.json
```

---

## Streaming

### `spindle stream`

Stream synthetic events row-by-row.

```bash
# Stream to console
spindle stream retail --table order --max-events 1000

# Stream to file with rate limiting
spindle stream retail --table order --rate 100 --realtime --max-events 5000 \
  --sink file --output events.jsonl

# With burst window: at 30s, 10x rate for 60s
spindle stream retail --table order --rate 50 --realtime --burst 30:60:10

# With anomaly injection (5% of rows)
spindle stream retail --table order --max-events 1000 --anomaly-fraction 0.05
```

| Option | Default | Description |
| --- | --- | --- |
| `--table, -t` | — | Table to stream (required) |
| `--scale, -s` | `small` | Scale preset |
| `--seed` | `42` | Random seed |
| `--rate` | `10.0` | Target events/sec (realtime mode) |
| `--max-events` | — | Stop after N events |
| `--duration` | — | Stop after N seconds (realtime mode) |
| `--out-of-order` | `0.0` | Fraction of events to reorder (0.0-1.0) |
| `--sink` | `console` | `console` or `file` |
| `--output, -o` | — | Output file path (file sink) |
| `--realtime/--no-realtime` | off | Rate-limit to `--rate` events/sec |
| `--burst` | — | Burst spec `START:DURATION:MULTIPLIER` (repeatable) |
| `--anomaly-fraction` | `0.0` | Fraction of rows injected as point anomalies |

---

## Schema Import & Inference

### `spindle from-ddl`

Import SQL DDL (CREATE TABLE) into a `.spindle.json` schema.

```bash
spindle from-ddl my_tables.sql --output my_schema.spindle.json
spindle from-ddl warehouse_ddl.sql --output schema.json --domain custom
```

| Option | Default | Description |
| --- | --- | --- |
| `INPUT_FILE` | — | Path to SQL DDL file (required) |
| `--output, -o` | — | Output `.spindle.json` path |
| `--domain` | `custom` | Domain name for the generated schema |
| `--scale` | — | Scale overrides: `small:table1=1000,table2=5000` |

### `spindle learn`

Infer a `.spindle.json` schema from existing data files.

```bash
spindle learn ./real_data/ --format csv --output inferred.spindle.json
spindle learn ./exports/ --format parquet --output schema.json --domain my_retail
```

| Option | Default | Description |
| --- | --- | --- |
| `INPUT_PATH` | — | Directory or single file (required) |
| `--output, -o` | — | Output `.spindle.json` path |
| `--format` | `csv` | Input format: `csv`, `parquet`, `jsonl` |
| `--domain` | `inferred` | Domain name for the schema |

### `spindle compare`

Compare real vs synthetic data and generate a fidelity report.

```bash
spindle compare ./real_data/ ./synthetic/ --format csv --output report.md
```

| Option | Default | Description |
| --- | --- | --- |
| `REAL_PATH` | — | Path to real data (required) |
| `SYNTH_PATH` | — | Path to synthetic data (required) |
| `--format` | `csv` | Input format: `csv`, `parquet` |
| `--output, -o` | — | Save markdown report to file |

### `spindle mask`

Mask PII columns in real datasets.

```bash
spindle mask ./real_data/ --output ./masked/ --format csv --seed 42
spindle mask ./exports/ --output ./masked/ --exclude customer_id --exclude order_id
```

| Option | Default | Description |
| --- | --- | --- |
| `INPUT_PATH` | — | Directory or file to mask (required) |
| `--output, -o` | — | Output directory (required) |
| `--format` | `csv` | Input format: `csv`, `parquet`, `jsonl` |
| `--seed` | `42` | Random seed |
| `--exclude` | — | Columns to skip (repeatable) |

---

## Incremental & Day 2

### `spindle continue`

Generate incremental deltas (inserts, updates, deletes) from existing data.

```bash
spindle continue retail --input ./day1/ --output ./deltas/ --inserts 100
spindle continue healthcare --input ./baseline/ --output ./day2/ --update-fraction 0.15
```

| Option | Default | Description |
| --- | --- | --- |
| `DOMAIN_NAME` | — | Domain to continue (required) |
| `--input` | — | Directory with existing data (required) |
| `--output, -o` | — | Output directory for deltas |
| `--format` | `csv` | Output format: `csv`, `parquet`, `jsonl` |
| `--inserts` | `100` | New rows per anchor table |
| `--update-fraction` | `0.1` | Fraction of rows to update |
| `--delete-fraction` | `0.02` | Fraction of rows to delete |
| `--seed` | `42` | Random seed |

### `spindle time-travel`

Generate monthly point-in-time snapshots showing data evolution.

```bash
spindle time-travel retail --months 12 --output ./snapshots/ --format parquet
spindle time-travel financial --months 6 --growth-rate 0.08 --churn-rate 0.03 --output ./snaps/
```

| Option | Default | Description |
| --- | --- | --- |
| `DOMAIN_NAME` | — | Domain to snapshot (required) |
| `--months` | `12` | Number of monthly snapshots |
| `--scale, -s` | `small` | Scale preset |
| `--output, -o` | — | Output directory (required) |
| `--format` | `parquet` | `csv` or `parquet` |
| `--growth-rate` | `0.05` | Monthly growth rate |
| `--churn-rate` | `0.02` | Monthly churn rate |
| `--seed` | `42` | Random seed |

---

## Semantic Model & Publishing

### `spindle export-model`

Export a domain schema as a Power BI / Fabric semantic model (.bim).

```bash
spindle export-model retail --output retail.bim --source-type lakehouse
spindle export-model financial --output fin.bim --source-type warehouse --source-name FinDW
```

| Option | Default | Description |
| --- | --- | --- |
| `DOMAIN_NAME` | — | Domain to export (required) |
| `--output, -o` | `model.bim` | Output file path |
| `--source-type` | `lakehouse` | `lakehouse`, `warehouse`, or `sql_database` |
| `--source-name` | — | Data source name in M expression |
| `--include-measures` | `True` | Generate starter DAX measures |
| `--schema-name` | `dbo` | SQL schema name |

### `spindle publish`

Generate and publish data directly to a Fabric workspace.

```bash
spindle publish retail --target lakehouse --base-path "abfss://..." --scale small
spindle publish retail --target sql-database --connection-string "env://SPINDLE_SQL_CONNECTION"
spindle publish retail --target eventhouse --connection-string "https://..." --database mydb
```

| Option | Default | Description |
| --- | --- | --- |
| `DOMAIN_NAME` | — | Domain to publish (required) |
| `--target` | — | `lakehouse`, `sql-database`, or `eventhouse` |
| `--scale, -s` | `small` | Scale preset |
| `--seed` | `42` | Random seed |
| `--base-path` | — | OneLake path (lakehouse target) |
| `--connection-string` | — | Connection string or `env://VAR` reference |
| `--database` | — | Database name (eventhouse target) |
| `--dry-run` | — | Preview without writing |

---

## Multi-Domain

### `spindle composite`

Generate data from a composite preset or ad-hoc domain combination.

```bash
spindle composite enterprise --scale small --output ./data/ --format parquet
spindle composite retail+hr+financial --scale medium --output ./enterprise/
```

| Option | Default | Description |
| --- | --- | --- |
| `PRESET_OR_DOMAINS` | — | Preset name or `domain+domain` (required) |
| `--scale, -s` | `small` | Scale preset |
| `--seed` | `42` | Random seed |
| `--output, -o` | — | Output directory |
| `--format` | `summary` | `summary`, `csv`, `parquet`, `jsonl` |

### `spindle presets`

List all available composite presets.

```bash
spindle presets
```

### `spindle profile`

Manage domain distribution profiles.

```bash
spindle profile list retail
spindle profile export retail --output retail_profile.json
spindle profile import custom_profile.json retail --save-as custom
```

---

## Simulation

### `spindle simulate file-drop`

Simulate file drops into a landing zone.

```bash
spindle simulate file-drop --domain retail --scale small --seed 42 \
  --start-date 2025-01-01 --end-date 2025-01-31 --output ./landing/
```

### `spindle simulate stream`

Simulate event streaming with envelopes.

```bash
spindle simulate stream --domain retail --scale small --seed 42 \
  --max-events 5000 --output ./events/
```

### `spindle simulate hybrid`

Run stream + file-drop simultaneously.

```bash
spindle simulate hybrid --domain retail --scale small --seed 42 \
  --output ./output/
```

---

## Orchestration

### `spindle run`

Execute a GSL (Generation Spec Language) YAML spec end-to-end.

```bash
spindle run --spec specs/retail_demo_estate.yaml
```

### `spindle validate-outputs`

Run validation gates against generated output artifacts.

```bash
spindle validate-outputs ./output/ --gates all --quarantine ./quarantine/
```
