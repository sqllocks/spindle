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
