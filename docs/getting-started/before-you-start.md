# Before You Start

New to synthetic data generation? This page covers what you need to know before diving into Spindle.

## Prerequisites

- **Python 3.10+** installed ([python.org](https://www.python.org/downloads/) or via your package manager)
- **pip** for installing packages (included with Python)
- Basic familiarity with **pandas DataFrames** (Spindle outputs DataFrames)
- For Fabric workflows: access to a **Microsoft Fabric workspace** with a Lakehouse

No prior experience with synthetic data tools is required.

## What is Synthetic Data?

Synthetic data is artificially generated data that mimics the statistical properties and structure of real-world datasets — without containing any actual sensitive information. It's used for:

- **Development and testing** — populate dashboards, test pipelines, train ML models
- **Demos and presentations** — realistic data that tells a compelling story
- **Performance benchmarking** — generate datasets at any scale (thousands to hundreds of millions of rows)
- **Data quality testing** — intentionally inject chaos (nulls, duplicates, schema drift) to stress-test your pipeline

## How Spindle is Different

Most synthetic data tools either generate random noise (Faker) or train ML models on existing data (SDV, MOSTLY AI). Spindle takes a third approach:

- **Rule-based and transparent** — every generation rule is a human-readable `.spindle.json` schema you can inspect and tweak
- **Calibrated from real sources** — all 13 domains sourced from published data (BLS, NAIC, NCES, NAR, FDIC, Federal Reserve, SEC, and 40+ more)
- **Schema-aware** — generates tables in dependency order, respects FK integrity, handles composite keys
- **Fabric-native** — targets every Microsoft Fabric data store (Lakehouse, Warehouse, SQL Database, Eventhouse, Semantic Models)

## Glossary

| Term | Definition |
|------|-----------|
| **Domain** | A pre-built industry schema (e.g., Retail, Healthcare). Spindle ships 13 domains. |
| **Strategy** | A column-level generation rule (e.g., `faker_name`, `weighted_enum`, `pareto_fk`). Spindle has 21 strategies. |
| **Scale preset** | A named size configuration (`fabric_demo`, `small`, `medium`, `large`, `warehouse`, `xlarge`). |
| **Schema** | A `.spindle.json` file defining tables, columns, strategies, relationships, and constraints. |
| **Chaos Engine** | A module that intentionally corrupts generated data to test pipeline resilience. |
| **Validation Gate** | A quality check (referential integrity, null constraints, schema conformance) with automatic quarantine. |
| **Star Schema** | A dimensional model (dimension + fact tables) transformed from Spindle's normalized 3NF output. |
| **CDM** | Common Data Model — a Microsoft standard folder structure for data interchange. |
| **GSL** | Generation Spec Language — a declarative YAML format that ties together schema, chaos, and output settings. |
| **Composite Domain** | Multi-domain generation with shared entities and cross-domain FK relationships. |

## Learning Paths

### Path 1: "I just need test data fast"

1. [60-Second Overview](60-seconds.md) — see it work in one command
2. [Quickstart](quickstart.md) — generate, inspect, export
3. [CLI Cheatsheet](cli-cheatsheet.md) — all 12 commands at a glance

### Path 2: "I'm building a Fabric project"

1. [Installation](installation.md) — install with Fabric extras
2. [Quickstart](quickstart.md) — generate your first dataset
3. [Fabric Lakehouse Guide](../guides/fabric-lakehouse.md) — write to Delta tables
4. [Fabric Notebooks Guide](../guides/fabric-notebook.md) — run in Fabric notebooks
5. [Star Schema Export](../guides/star-schema.md) — build dimensional models

### Path 3: "I want to understand everything"

1. [Installation](installation.md) — full setup with all extras
2. [Quickstart](quickstart.md) — first dataset
3. [Generation Strategies](../guides/strategies.md) — all 21 column-level strategies
4. [Custom Schemas](../guides/custom-schemas.md) — build your own domain
5. [Chaos Engine](../guides/chaos.md) — inject data quality issues
6. [Validation Gates](../guides/validation.md) — quality checks and quarantine
7. [Streaming](../guides/streaming.md) — real-time event emission
8. [Methodology](../methodology/calibration.md) — how distributions are calibrated

## Next Step

Ready? Start with the [60-Second Overview](60-seconds.md) or jump straight to the [Quickstart](quickstart.md).
