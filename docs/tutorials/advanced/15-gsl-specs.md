# Tutorial 15: GSL Specs

Define reproducible data generation pipelines in a single YAML file using GSL (Generation Spec Language) -- Spindle's declarative spec that ties together schema, chaos, outputs, and validation.

---

## Prerequisites

- Python 3.10 or later
- `pip install sqllocks-spindle`
- Completed [Tutorial 14: Scenario Packs](14-scenario-packs.md)
- Familiarity with YAML syntax

## What You'll Learn

- What a GSL spec is and why it exists
- How to write a basic GSL spec with schema, scenario, outputs, and validation
- How to add chaos injection with escalation and breaking-change configuration
- How to configure hybrid outputs (Lakehouse + Eventstream) in a single spec
- How to parse and inspect GSL specs with `GSLParser`
- How to run a GSL spec end-to-end from the CLI with `spindle run`

---

## What Is a GSL Spec?

Think of a GSL spec as a "data generation Dockerfile." It is a single YAML file that declares everything needed to produce a reproducible dataset:

- **Schema** -- which domain to generate
- **Scenario** -- which pack to use, scale, seed, and date range
- **Chaos** -- optional data quality degradation
- **Outputs** -- where to write (Lakehouse files, Delta tables, Eventstream)
- **Validation** -- which gates to run and how to handle drift

GSL specs are designed to be checked into source control alongside your Fabric workspace, giving you version-controlled, repeatable data generation pipelines.

## Step 1: Write a Basic GSL Spec

A minimal GSL spec defines the schema, scenario pack, output target, and validation gates:

```yaml
# retail_basic.gsl.yaml
version: 1
name: retail_daily_demo

schema:
  type: domain
  domain: retail

scenario:
  pack: packs/retail/fd_daily_batch.yaml
  scale: fabric_demo
  seed: 42
  date_range:
    start: "2025-01-01"
    end: "2025-01-31"

outputs:
  lakehouse:
    mode: files_only
    landing_zone:
      root: Files/landing/retail
    formats: [parquet]

validation:
  gates: [schema_conformance, referential_integrity]
```

Key sections:

- **`schema.domain`** selects the built-in domain (retail, healthcare, financial, etc.)
- **`scenario.pack`** references a scenario pack YAML that controls the simulation pattern
- **`scenario.seed`** ensures reproducibility -- the same seed always produces the same data
- **`outputs.lakehouse`** configures the Fabric Lakehouse target
- **`validation.gates`** lists which quality checks to run after generation

## Step 2: Add Chaos Configuration

A chaos-enabled spec injects data quality issues to test your pipeline's resilience:

```yaml
# retail_chaos.gsl.yaml
version: 1
name: retail_chaos_pipeline

schema:
  type: domain
  domain: retail

scenario:
  pack: packs/retail/fd_schema_drift.yaml
  scale: small
  seed: 99

chaos:
  enabled: true
  intensity: stormy
  config:
    warmup_days: 7
    escalation: gradual
    breaking_change_day: 20

outputs:
  lakehouse:
    mode: tables_and_files
    tables: [customer, order]
    landing_zone:
      root: Files/landing/retail
  eventstream:
    enabled: false

validation:
  gates: [schema_conformance, referential_integrity]
  drift_policy: quarantine_on_breaking_change
```

The chaos section controls:

- **`intensity`** -- `calm`, `moderate`, `stormy`, or `hurricane`
- **`warmup_days`** -- how many days of clean data before chaos begins
- **`escalation`** -- `gradual` ramps up over time; `immediate` starts at full intensity
- **`breaking_change_day`** -- the day a schema-breaking change is injected
- **`drift_policy`** -- what to do when drift is detected (`quarantine_on_breaking_change` isolates bad records)

## Step 3: Configure Hybrid Outputs

A hybrid spec sends batch files to Lakehouse and streaming events to Eventstream simultaneously:

```yaml
# retail_hybrid.gsl.yaml
version: 1
name: retail_hybrid_ingest

schema:
  type: domain
  domain: retail

scenario:
  pack: packs/retail/hy_stream_plus_microbatch.yaml
  scale: fabric_demo
  seed: 42

outputs:
  lakehouse:
    mode: files_only
    landing_zone:
      root: Files/landing/retail
  eventstream:
    enabled: true
    endpoint_secret_ref: kv://my-workspace/eventstream_conn
    topic_prefix: retail

validation:
  gates: [schema_conformance]
```

The `endpoint_secret_ref` uses a key-vault reference pattern (`kv://workspace/secret-name`) so you never embed connection strings in the spec file itself.

## Step 4: Parse and Inspect Specs with GSLParser

Use `GSLParser` to load, validate, and inspect GSL specs programmatically:

```python
from sqllocks_spindle.specs.gsl_parser import GSLParser

parser = GSLParser()
spec = parser.parse("retail_basic.gsl.yaml")

print(f"name:    {spec.name}")
print(f"version: {spec.version}")

if spec.schema:
    print(f"schema.domain: {spec.schema.domain}")
if spec.scenario:
    print(f"scenario.pack:  {spec.scenario.pack}")
    print(f"scenario.scale: {spec.scenario.scale}")
    print(f"scenario.seed:  {spec.scenario.seed}")
if spec.outputs:
    lh = spec.outputs.lakehouse
    print(f"outputs.lakehouse.root: {lh.landing_zone.root if lh else 'N/A'}")
if spec.validation:
    print(f"validation.gates: {spec.validation.gates}")
```

Parse the chaos spec to inspect chaos configuration:

```python
spec_chaos = parser.parse("retail_chaos.gsl.yaml")
print(f"name:           {spec_chaos.name}")
if spec_chaos.chaos:
    print(f"chaos.enabled:  {spec_chaos.chaos.enabled}")
    print(f"chaos.intensity:{spec_chaos.chaos.intensity}")
    cfg = spec_chaos.chaos.config
    print(f"warmup_days:    {cfg.get('warmup_days')}")
    print(f"escalation:     {cfg.get('escalation')}")
if spec_chaos.validation:
    print(f"drift_policy:   {spec_chaos.validation.drift_policy}")
```

Parse the hybrid spec to inspect Eventstream configuration:

```python
spec_hybrid = parser.parse("retail_hybrid.gsl.yaml")
if spec_hybrid.outputs and spec_hybrid.outputs.eventstream:
    es = spec_hybrid.outputs.eventstream
    print(f"eventstream.enabled:      {es.enabled}")
    print(f"eventstream.secret_ref:   {es.endpoint_secret_ref}")
    print(f"eventstream.topic_prefix: {es.topic_prefix}")
```

## Step 5: Resolve Relative Paths

GSL specs support relative path resolution from the spec file's location:

```python
resolved = spec.resolve_path("outputs/my_file.parquet")
print(f"Relative path resolved to: {resolved}")
```

This is useful when specs reference scenario packs or output directories relative to the repository root.

## Step 6: Run a GSL Spec from the CLI

The fastest way to execute a GSL spec is from the command line:

```bash
spindle run --spec specs/retail_basic.gsl.yaml
```

This runs the full pipeline end-to-end: parse the spec, generate data, apply chaos (if configured), run validation gates, and write outputs to the specified targets.

## Step 7: Round-Trip Parse Summary

Verify all specs in a directory parse correctly:

```python
from pathlib import Path

spec_dir = Path("./demo_gsl_specs")
for path in sorted(spec_dir.glob("*.yaml")):
    s = parser.parse(path)
    chaos_on = s.chaos.enabled if s.chaos else False
    es_on = (s.outputs.eventstream.enabled
             if s.outputs and s.outputs.eventstream else False)
    print(f"  {path.name:<35} scale={getattr(s.scenario, 'scale', '?'):<12} "
          f"chaos={chaos_on}  eventstream={es_on}")
```

---

> **Run It Yourself**
>
> - Script: [`20_gsl_spec.py`](../../../examples/scenarios/20_gsl_spec.py)

---

## Related

- [GSL Specs guide](../../guides/gsl-specs.md) -- the full reference for all GSL spec sections and options

---

## Next Step

[Tutorial 16: Day 2 Operations](16-day2-operations.md) -- generate incremental CDC data, time-travel snapshots, and apply PII masking.
