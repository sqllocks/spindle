# GSL Specs

The Generation Spec Language (GSL) is a declarative YAML format that ties together schema, scenario pack, chaos configuration, output targets, and validation gates into a single executable specification.

## Quick Start

```yaml
# specs/retail_demo_estate.yaml
version: 1
name: retail_demo_estate
schema:
  type: domain
  domain: retail
scenario:
  pack: fd_daily_batch
  scale: medium
  seed: 42
  date_range:
    start: "2025-01-01"
    end: "2025-12-31"
outputs:
  lakehouse:
    mode: tables_and_files
    tables: [customer, order, order_line, product, store]
    landing_zone:
      root: Files/landing/retail
validation:
  gates:
    - referential_integrity
    - schema_conformance
  drift_policy: quarantine_on_breaking_change
```

## Running a Spec

```bash
spindle run --spec specs/retail_demo_estate.yaml
```

```python
from sqllocks_spindle.specs import GSLParser

spec = GSLParser().parse("specs/retail_demo_estate.yaml")
print(spec.name)       # "retail_demo_estate"
print(spec.schema)     # SchemaRef(type='domain', domain='retail')
print(spec.scenario)   # ScenarioRef(pack='fd_daily_batch', scale='medium', ...)
```

## GSL Sections

### `schema`

Defines where the data schema comes from.

```yaml
# From a built-in domain
schema:
  type: domain
  domain: retail

# From a .spindle.json file
schema:
  type: spindle_json
  path: schemas/my_custom.spindle.json
```

| Field | Type | Description |
| --- | --- | --- |
| `type` | str | `domain` or `spindle_json` |
| `domain` | str | Domain name (if type=domain) |
| `path` | str | Path to .spindle.json (if type=spindle_json) |

### `scenario`

References a scenario pack and sets generation parameters.

```yaml
scenario:
  pack: fd_daily_batch
  scale: medium
  seed: 42
  date_range:
    start: "2025-01-01"
    end: "2025-12-31"
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `pack` | str | — | Pack ID (e.g., `fd_daily_batch`, `st_realtime_events`) |
| `scale` | str | `small` | Scale preset |
| `seed` | int | `42` | Random seed |
| `date_range` | dict | — | `{start, end}` date strings |

### `chaos`

Optional chaos injection configuration.

```yaml
chaos:
  enabled: true
  intensity: moderate
  config:
    warmup_days: 7
    breaking_change_day: 20
```

### `outputs`

Where generated data is delivered.

```yaml
outputs:
  lakehouse:
    mode: tables_and_files
    tables: [customer, order, order_line]
    landing_zone:
      root: Files/landing/retail
  eventstream:
    enabled: true
    endpoint_secret_ref: kv://workspace/retail_eventstream_conn
    topics:
      - name: orders
        event_type: order_created
```

### `validation`

Which quality gates to run and how to handle failures.

```yaml
validation:
  gates:
    - referential_integrity
    - schema_conformance
    - null_constraint
  drift_policy: quarantine_on_breaking_change
```

| Field | Type | Description |
| --- | --- | --- |
| `gates` | list | Gate names to run |
| `drift_policy` | str | What to do on schema drift (e.g., `quarantine_on_breaking_change`) |

## Parsing API

```python
from sqllocks_spindle.specs import GSLParser, GenerationSpec

parser = GSLParser()

# From file
spec = parser.parse("specs/my_spec.yaml")

# From dict
spec = parser.parse_dict(raw_dict, base_dir=".")

# Resolve relative paths
schema_path = spec.resolve_path("schemas/retail.spindle.json")
```

## Built-In Scenario Packs

GSL specs reference scenario packs by ID. Spindle ships 44 built-in packs:

| Pack ID | Kind | Description |
| --- | --- | --- |
| `fd_daily_batch` | file_drop | Daily partitioned file drop |
| `fd_schema_drift` | file_drop | Daily batch with schema drift injection |
| `st_realtime_events` | stream | Real-time event stream |
| `hy_stream_plus_microbatch` | hybrid | Stream + periodic micro-batch files |

Each of the 11 domains has all 4 pack types. List them:

```python
from sqllocks_spindle.packs import PackLoader

for p in PackLoader().list_builtin():
    print(f"{p['domain']:15} {p['pack_id']}")
```

---

## See Also

- **Tutorial:** [15: GSL Specs](../tutorials/advanced/15-gsl-specs.md) — step-by-step walkthrough
- **Tutorial:** [17: CI Integration](../tutorials/advanced/17-ci-integration.md) — step-by-step walkthrough
- **Example script:** [`20_gsl_spec.py`](https://github.com/sqllocks/spindle/blob/main/examples/scenarios/20_gsl_spec.py)
