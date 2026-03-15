"""
Scenario 20 -- GSL Spec Parser (Generation Spec Language)
===========================================================
Parse a GSL YAML file — Spindle's high-level declarative spec that ties
together schema, scenario pack, chaos, outputs, and validation gates.

GSL is useful for defining reproducible generation pipelines that can be
checked into source control alongside your Fabric workspaces. Think of it
as a "data generation Dockerfile."

Run:
    python examples/scenarios/20_gsl_spec.py
"""

import textwrap
from pathlib import Path

from sqllocks_spindle.specs.gsl_parser import GSLParser

# ── Sample GSL specs (written to temp files for this example) ─────────────────
GSL_BASIC = textwrap.dedent("""\
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
""")

GSL_CHAOS = textwrap.dedent("""\
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
""")

GSL_HYBRID = textwrap.dedent("""\
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
""")

SPEC_DIR = Path("./demo_gsl_specs")
SPEC_DIR.mkdir(exist_ok=True)

(SPEC_DIR / "retail_basic.gsl.yaml").write_text(GSL_BASIC)
(SPEC_DIR / "retail_chaos.gsl.yaml").write_text(GSL_CHAOS)
(SPEC_DIR / "retail_hybrid.gsl.yaml").write_text(GSL_HYBRID)

print(f"Wrote sample GSL specs to {SPEC_DIR.resolve()}")

# ── 1. Parse basic spec ───────────────────────────────────────────────────────
print("\n── 1. Parse basic GSL spec ──")

parser = GSLParser()
spec = parser.parse(SPEC_DIR / "retail_basic.gsl.yaml")

print(f"  name:    {spec.name}")
print(f"  version: {spec.version}")
if spec.schema:
    print(f"  schema.domain: {spec.schema.domain}")
if spec.scenario:
    print(f"  scenario.pack:  {spec.scenario.pack}")
    print(f"  scenario.scale: {spec.scenario.scale}")
    print(f"  scenario.seed:  {spec.scenario.seed}")
if spec.outputs:
    print(f"  outputs.lakehouse.root: {spec.outputs.lakehouse.landing_zone.root if spec.outputs.lakehouse else 'N/A'}")
if spec.validation:
    print(f"  validation.gates: {spec.validation.gates}")

# ── 2. Parse chaos spec ───────────────────────────────────────────────────────
print("\n── 2. Parse chaos GSL spec ──")

spec_chaos = parser.parse(SPEC_DIR / "retail_chaos.gsl.yaml")
print(f"  name:           {spec_chaos.name}")
if spec_chaos.chaos:
    print(f"  chaos.enabled:  {spec_chaos.chaos.enabled}")
    print(f"  chaos.intensity:{spec_chaos.chaos.intensity}")
    if spec_chaos.chaos.config:
        cfg = spec_chaos.chaos.config
        print(f"  warmup_days:    {cfg.get('warmup_days')}")
        print(f"  escalation:     {cfg.get('escalation')}")
if spec_chaos.validation:
    print(f"  drift_policy:   {spec_chaos.validation.drift_policy}")

# ── 3. Parse hybrid spec ──────────────────────────────────────────────────────
print("\n── 3. Parse hybrid GSL spec ──")

spec_hybrid = parser.parse(SPEC_DIR / "retail_hybrid.gsl.yaml")
print(f"  name: {spec_hybrid.name}")
if spec_hybrid.outputs and spec_hybrid.outputs.eventstream:
    es = spec_hybrid.outputs.eventstream
    print(f"  eventstream.enabled:      {es.enabled}")
    print(f"  eventstream.secret_ref:   {es.endpoint_secret_ref}")
    print(f"  eventstream.topics:       {es.topics}")

# ── 4. resolve_path ───────────────────────────────────────────────────────────
print("\n── 4. spec.resolve_path() — relative path resolution ──")
resolved = spec.resolve_path("outputs/my_file.parquet")
print(f"  Relative path resolved to: {resolved}")

# ── 5. Round-trip: parse all three ───────────────────────────────────────────
print("\n── 5. Round-trip parse summary ──")
for path in sorted(SPEC_DIR.glob("*.yaml")):
    s = parser.parse(path)
    chaos_on = s.chaos.enabled if s.chaos else False
    es_on    = (s.outputs.eventstream.enabled if s.outputs and s.outputs.eventstream else False)
    print(f"  {path.name:<35} scale={getattr(s.scenario,'scale','?'):<12} chaos={chaos_on}  eventstream={es_on}")

print("\nDone. GSL specs in ./demo_gsl_specs/")
