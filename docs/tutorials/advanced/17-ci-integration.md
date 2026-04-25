# Tutorial 17: CI Integration

Use the Spindle CLI, GSL specs, and scenario packs to integrate synthetic data generation into your CI/CD pipeline for automated regression testing and environment provisioning.

---

## Prerequisites

- Python 3.10 or later
- `pip install sqllocks-spindle`
- Completed [Tutorial 15: GSL Specs](15-gsl-specs.md) and [Tutorial 14: Scenario Packs](14-scenario-packs.md)
- A CI/CD platform (GitHub Actions, Azure DevOps, GitLab CI, etc.)

## What You'll Learn

- How to use the `spindle` CLI for batch data generation, streaming, and star schema exports
- How to run GSL specs declaratively with `spindle run`
- How to use scenario packs for regression testing across multiple domains
- How to handle exit codes and validation gate failures in CI pipelines
- How to structure a CI workflow that generates, validates, and promotes test data

---

## The Spindle CLI

Every operation available in the Python API has a CLI equivalent. After installing `sqllocks-spindle`, the `spindle` command is available in your terminal:

```bash
spindle --help
```

The CLI is designed for CI/CD: it produces structured output, returns meaningful exit codes, and supports `--dry-run` for planning.

## Step 1: Generate Data from the Command Line

The `spindle generate` command produces synthetic data in any format:

```bash
# Generate retail data as CSV
spindle generate retail --scale fabric_demo --format csv --output ./output/retail_csv

# Generate as Parquet
spindle generate retail --scale fabric_demo --format parquet --output ./output/retail_parquet

# Generate as JSON Lines
spindle generate retail --scale fabric_demo --format jsonlines --output ./output/retail_jsonl

# Dry run -- preview row counts without writing files
spindle generate retail --scale fabric_demo --dry-run

# Reproducible generation with a fixed seed
spindle generate retail --scale fabric_demo --seed 42 --output ./output/seeded
```

Key options:

| Option | Default | Description |
|--------|---------|-------------|
| `--scale, -s` | `small` | Scale preset: `fabric_demo`, `small`, `medium`, `large`, `warehouse`, `xlarge` |
| `--seed` | `42` | Random seed for reproducibility |
| `--output, -o` | -- | Output directory |
| `--format` | `summary` | Output format: `summary`, `csv`, `tsv`, `jsonl`, `parquet`, `excel`, `sql`, `delta` |
| `--mode, -m` | `3nf` | Schema mode: `3nf` or `star` |
| `--dry-run` | -- | Show planned row counts without generating |

## Step 2: Star Schema and CDM Exports

The CLI provides specialized commands for star schema and CDM (Common Data Model) exports:

```bash
# Generate and export as a star schema (dim_* + fact_* tables)
spindle to-star retail --scale fabric_demo --output ./star/ --format parquet

# Generate and export as a CDM folder (model.json + data files)
spindle to-cdm retail --scale fabric_demo --output ./cdm/
```

## Step 3: Stream Events from the CLI

Use `spindle stream` for streaming scenarios in CI:

```bash
# Stream 10 events to console (quick smoke test)
spindle stream retail --table order --max-events 10

# Stream to a file with rate limiting
spindle stream retail --table order --rate 100 --realtime --max-events 5000 \
  --sink file --output events.jsonl

# With burst window: at 30s, 10x rate for 60s
spindle stream retail --table order --rate 50 --realtime --burst 30:60:10

# With anomaly injection (5% of rows)
spindle stream retail --table order --max-events 1000 --anomaly-fraction 0.05
```

## Step 4: Run GSL Specs Declaratively

GSL specs are the recommended approach for CI pipelines. A single `spindle run` command executes the entire generation pipeline defined in a YAML spec:

```bash
spindle run --spec specs/retail_demo_estate.yaml
```

This runs: parse spec, generate data, apply chaos (if configured), run validation gates, and write outputs to the specified targets. The exit code reflects the validation gate results.

Here is a typical GSL spec for CI:

```yaml
# specs/retail_ci.gsl.yaml
version: 1
name: retail_ci_validation

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

## Step 5: Validate Generated Outputs

After generation, run validation gates against the output artifacts:

```bash
spindle validate-outputs ./output/ --gates all --quarantine ./quarantine/
```

This checks that all generated files pass schema conformance, referential integrity, null constraints, and unique constraints. Failed records are written to the quarantine directory with full metadata.

## Step 6: Scenario Packs for Regression Testing

Scenario packs provide a structured way to regression-test your data pipelines across multiple domains and simulation types. This Python script runs all 4 pack types for a domain and reports results:

```python
from pathlib import Path
from sqllocks_spindle.domains.retail.retail import RetailDomain
from sqllocks_spindle.packs.loader import PackLoader, _BUILTIN_PACKS_ROOT
from sqllocks_spindle.packs.runner import PackRunner

runner = PackRunner()
domain = RetailDomain()
domain_name = "retail"
pack_types = [
    "fd_daily_batch", "fd_schema_drift",
    "st_realtime_events", "hy_stream_plus_microbatch",
]

all_passed = True
for pack_type in pack_types:
    p = PackLoader().load(
        Path(_BUILTIN_PACKS_ROOT) / domain_name / f"{pack_type}.yaml"
    )
    r = runner.run(
        pack=p, domain=domain, scale="fabric_demo", seed=42,
        base_path="./ci_output",
    )
    status = "PASS" if r.is_success else "FAIL"
    print(f"  [{status}] {pack_type}: {len(r.files_written)} files, "
          f"{r.events_emitted:,} events, {r.elapsed_time:.2f}s")
    if not r.is_success:
        all_passed = False
        print(f"         Errors: {r.errors}")

# Exit with non-zero code if any pack failed
import sys
sys.exit(0 if all_passed else 1)
```

## Step 7: Example GitHub Actions Workflow

Here is a complete GitHub Actions workflow that generates data, validates it, and fails the build if any gate fails:

```yaml
# .github/workflows/data-regression.yml
name: Data Regression Tests

on:
  pull_request:
    paths:
      - "specs/**"
      - "pipelines/**"

jobs:
  data-regression:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install Spindle
        run: pip install sqllocks-spindle

      - name: Generate and validate (GSL spec)
        run: spindle run --spec specs/retail_ci.gsl.yaml

      - name: Smoke test -- streaming
        run: spindle stream retail --table order --max-events 100

      - name: Run scenario packs
        run: python tests/run_scenario_packs.py
```

## Step 8: Example Azure DevOps Pipeline

```yaml
# azure-pipelines.yml
trigger:
  paths:
    include:
      - specs/*
      - pipelines/*

pool:
  vmImage: ubuntu-latest

steps:
  - task: UsePythonVersion@0
    inputs:
      versionSpec: "3.11"

  - script: pip install sqllocks-spindle
    displayName: Install Spindle

  - script: spindle run --spec specs/retail_ci.gsl.yaml
    displayName: Generate and validate

  - script: spindle stream retail --table order --max-events 100
    displayName: Streaming smoke test

  - script: python tests/run_scenario_packs.py
    displayName: Run scenario packs
```

## Exit Codes

The Spindle CLI uses standard exit codes that CI platforms interpret correctly:

| Exit Code | Meaning |
|-----------|---------|
| `0` | Success -- all generation and validation passed |
| `1` | Failure -- validation gates failed or generation error |
| `2` | Configuration error -- invalid spec, unknown domain, etc. |

Use these in your CI pipeline to gate promotions: if `spindle run` exits with a non-zero code, the build fails and the data quality issue must be investigated.

---

> **Run It Yourself**
>
> - Script: [`12_cli_usage.py`](../../../examples/scenarios/12_cli_usage.py)
> - Script: [`20_gsl_spec.py`](../../../examples/scenarios/20_gsl_spec.py)
> - Reference: [CLI Cheatsheet](../../getting-started/cli-cheatsheet.md)

---

## Related

- [GSL Specs guide](../../guides/gsl-specs.md) -- the full reference for all GSL spec sections and options

---

## Next Step

You have completed the tutorial series. From here, explore the [guides](../../guides/) for deep dives into specific topics, or browse the [API reference](../../api/) for detailed class and method documentation.
