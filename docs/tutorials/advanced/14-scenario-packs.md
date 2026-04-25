# Tutorial 14: Scenario Packs

Run pre-built YAML-defined end-to-end data generation workflows that bundle domain, scale, simulation mode, chaos, validation, and Fabric targets into a single declarative file.

---

## Prerequisites

- Python 3.10 or later
- `pip install sqllocks-spindle`
- Completed [Tutorial 13: Medallion Architecture](../fabric/13-medallion.md)
- Familiarity with YAML syntax

## What You'll Learn

- How to browse and inspect Spindle's 44 built-in scenario packs
- How to validate a pack against a domain before running it
- How to run file-drop, streaming, and hybrid packs with `PackRunner`
- How to run a schema-drift pack with chaos injection
- How to write custom packs from inline YAML
- How to batch-run all packs for a domain (useful for CI)

---

## What Are Scenario Packs?

A scenario pack is a YAML file that bundles an entire data generation workflow:

- **Domain + scale** -- which data to generate and how much
- **Simulation mode** -- `file_drop`, `stream`, or `hybrid`
- **Chaos configuration** -- optional schema drift, orphan FKs, etc.
- **Validation gates** -- which quality checks to run
- **Fabric target paths** -- where to write output

Spindle ships **44 built-in packs** covering 11 industry verticals and 4 simulation types:

| Type | Description |
|------|-------------|
| `fd_daily_batch` | Daily file-drop with partitioning, manifest, done flag |
| `fd_schema_drift` | File-drop with chaos-injected schema drift |
| `st_realtime_events` | Pure streaming via EventEnvelope |
| `hy_stream_plus_microbatch` | Hybrid: batch files + stream events |

**Verticals:** retail, healthcare, financial, supply_chain, iot, hr, insurance, marketing, education, real_estate, manufacturing

## Step 1: Browse Built-in Packs

Use `list_builtin()` to see every pack that ships with Spindle:

```python
import pandas as pd
from sqllocks_spindle.packs.loader import PackLoader, list_builtin, _BUILTIN_PACKS_ROOT

packs = list_builtin()
print(f"Total built-in packs: {len(packs)}")

pack_index = pd.DataFrame([
    {"domain": p.domain, "kind": p.kind, "id": p.id, "description": p.description}
    for p in packs
]).sort_values(["domain", "kind"])
pack_index
```

This returns a DataFrame with all 44 packs, sorted by domain and simulation type.

## Step 2: Inspect a Pack

Load a specific pack from disk and examine its configuration:

```python
from pathlib import Path

retail_pack_path = Path(_BUILTIN_PACKS_ROOT) / "retail" / "fd_daily_batch.yaml"
pack = PackLoader().load(retail_pack_path)

print(f"ID:          {pack.id}")
print(f"Kind:        {pack.kind}")
print(f"Domain:      {pack.domain}")
print(f"Description: {pack.description}")
print(f"Version:     {pack.pack_version}")

if pack.file_drop:
    print(f"\nFile-drop config:")
    print(f"  cadence:  {pack.file_drop.cadence}")
    print(f"  formats:  {pack.file_drop.formats}")

if pack.validation:
    print(f"\nValidation gates: {pack.validation.required_gates}")
```

## Step 3: Validate a Pack Against a Domain

Before running, verify that a pack's configuration is compatible with the target domain. This catches issues like referencing tables that do not exist in the domain schema.

```python
from sqllocks_spindle.packs.validator import PackValidator
from sqllocks_spindle.domains.retail.retail import RetailDomain

vr = PackValidator().validate(pack, RetailDomain())
print(f"Valid:    {vr.is_valid}")
print(f"Errors:   {vr.errors}")
print(f"Warnings: {vr.warnings}")
```

## Step 4: Run a File-Drop Pack

The `PackRunner` orchestrates the full workflow: generate data, simulate the delivery pattern, validate, and write output.

```python
from sqllocks_spindle.packs.runner import PackRunner

runner = PackRunner()

run_result = runner.run(
    pack=pack,
    domain=RetailDomain(),
    scale="fabric_demo",
    seed=42,
    base_path="/lakehouse/default/Files",
)

print(run_result.summary())
print(f"\nSuccess:       {run_result.is_success}")
print(f"Files written: {len(run_result.files_written)}")
print(f"Events:        {run_result.events_emitted:,}")
print(f"Elapsed:       {run_result.elapsed_time:.2f}s")
```

## Step 5: Run a Schema-Drift Pack (Chaos Enabled)

The `fd_schema_drift` pack includes chaos injection. When you run it, the runner injects schema drift into the generated data and then runs validation gates to detect the issues.

```python
drift_pack = PackLoader().load(
    Path(_BUILTIN_PACKS_ROOT) / "retail" / "fd_schema_drift.yaml"
)
print(f"Pack: {drift_pack.id}")
if drift_pack.failure_injection:
    print(f"Failure injection enabled: {drift_pack.failure_injection.enabled}")

drift_result = runner.run(
    pack=drift_pack,
    domain=RetailDomain(),
    scale="fabric_demo",
    seed=99,
    base_path="/lakehouse/default/Files",
)

print(f"\nSuccess:          {drift_result.is_success}")
print(f"Validation gates: {drift_result.validation_results}")
print(f"Errors:           {drift_result.errors}")
```

## Step 6: Run a Streaming Pack

Streaming packs generate events instead of files:

```python
stream_pack = PackLoader().load(
    Path(_BUILTIN_PACKS_ROOT) / "retail" / "st_realtime_events.yaml"
)

stream_result = runner.run(
    pack=stream_pack,
    domain=RetailDomain(),
    scale="fabric_demo",
    seed=42,
    base_path="/lakehouse/default/Files",
)

print(f"Success:        {stream_result.is_success}")
print(f"Events emitted: {stream_result.events_emitted:,}")
```

## Step 7: Run a Hybrid Pack

Hybrid packs produce both batch files and streaming events simultaneously:

```python
hybrid_pack = PackLoader().load(
    Path(_BUILTIN_PACKS_ROOT) / "retail" / "hy_stream_plus_microbatch.yaml"
)

hybrid_result = runner.run(
    pack=hybrid_pack,
    domain=RetailDomain(),
    scale="fabric_demo",
    seed=42,
    base_path="/lakehouse/default/Files",
)

print(f"Success:        {hybrid_result.is_success}")
print(f"Files written:  {len(hybrid_result.files_written)}")
print(f"Events emitted: {hybrid_result.events_emitted:,}")
```

## Step 8: Batch-Run All Packs for a Domain

A useful pattern for CI: loop all 4 pack types for a given domain and report which pass.

```python
domain = RetailDomain()
domain_name = "retail"
pack_types = [
    "fd_daily_batch", "fd_schema_drift",
    "st_realtime_events", "hy_stream_plus_microbatch",
]

report_rows = []
for pack_type in pack_types:
    p = PackLoader().load(
        Path(_BUILTIN_PACKS_ROOT) / domain_name / f"{pack_type}.yaml"
    )
    r = runner.run(
        pack=p, domain=domain, scale="fabric_demo", seed=42,
        base_path="/lakehouse/default/Files",
    )
    report_rows.append({
        "pack":    pack_type,
        "success": r.is_success,
        "files":   len(r.files_written),
        "events":  r.events_emitted,
        "elapsed": round(r.elapsed_time, 2),
        "errors":  r.errors,
    })

report_df = pd.DataFrame(report_rows)
report_df
```

## Step 9: Custom Pack from Inline YAML

Write your own pack spec for one-off testing scenarios:

```python
import tempfile, textwrap

CUSTOM_PACK_YAML = textwrap.dedent("""\
    pack_version: 1
    id: my_custom_pack
    kind: file_drop
    domain: retail
    description: Custom daily batch for demo

    fabric_targets:
      lakehouse_files_root: Files/landing/retail

    file_drop:
      cadence: daily
      partitioning: dt=YYYY-MM-DD
      formats: [parquet]
      entities: [customer, order]
      manifest:
        enabled: true
      done_flag:
        enabled: true
      lateness:
        enabled: false
      duplicates:
        enabled: false

    validation:
      required_gates: [schema_conformance]
""")

with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
    f.write(CUSTOM_PACK_YAML)
    tmp_path = f.name

custom_pack = PackLoader().load(tmp_path)
custom_result = runner.run(
    pack=custom_pack,
    domain=RetailDomain(),
    scale="fabric_demo",
    seed=42,
    base_path="/lakehouse/default/Files",
)

print(f"Pack ID:       {custom_pack.id}")
print(f"Success:       {custom_result.is_success}")
print(f"Files written: {len(custom_result.files_written)}")
```

For production use, save your custom pack as a `.yaml` file in your repository and load it with `PackLoader().load("path/to/my_pack.yaml")`.

---

> **Run It Yourself**
>
> - Notebook: [`08_scenario_packs.ipynb`](../../../examples/notebooks/showcase/08_scenario_packs.ipynb)
> - Script: [`19_scenario_packs.py`](../../../examples/scenarios/19_scenario_packs.py)

---

## Related

- [Simulation guide](../../guides/simulation.md) -- the condensed reference for simulation modes, file-drop cadences, and streaming configuration

---

## Next Step

[Tutorial 15: GSL Specs](15-gsl-specs.md) -- define reproducible generation pipelines in a single YAML file that ties together schema, chaos, outputs, and validation.
