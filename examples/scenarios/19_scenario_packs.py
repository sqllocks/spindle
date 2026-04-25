"""
Scenario 19 -- Scenario Packs
==============================
Load and run pre-built scenario packs (YAML-defined end-to-end workflows).
A scenario pack bundles domain, scale, simulation mode, chaos config,
validation gates, and Fabric targets into a single declarative file.

Spindle ships 44 built-in packs (11 verticals × 4 simulation types):
  fd_daily_batch       — file-drop, daily partition
  fd_schema_drift      — file-drop with chaos schema drift
  st_realtime_events   — pure streaming
  hy_stream_plus_microbatch — hybrid (batch + stream)

Run:
    python examples/scenarios/19_scenario_packs.py
"""

from pathlib import Path

from sqllocks_spindle.domains.retail.retail import RetailDomain
from sqllocks_spindle.domains.healthcare.healthcare import HealthcareDomain
from sqllocks_spindle.packs.loader import PackLoader
from sqllocks_spindle.packs.runner import PackRunner
from sqllocks_spindle.packs.validator import PackValidator

# ── 1. List built-in packs ────────────────────────────────────────────────────
print("── 1. List built-in packs ──")

from sqllocks_spindle.packs.loader import PackLoader

packs = PackLoader().list_builtin()
print(f"  Total built-in packs: {len(packs)}")
# Group by domain
domains = {}
for p in packs:
    domains.setdefault(p['domain'], []).append(p['pack_id'])
for domain, kinds in sorted(domains.items()):
    print(f"  {domain:<20} {kinds}")

# ── 2. Load a pack from disk ──────────────────────────────────────────────────
print("\n── 2. Load a built-in retail pack ──")

# Built-in packs are in scenario_packs_extracted/packs/<domain>/
from sqllocks_spindle.packs.loader import _BUILTIN_PACKS_ROOT
retail_pack_path = Path(_BUILTIN_PACKS_ROOT) / "retail" / "fd_daily_batch.yaml"

pack = PackLoader().load(retail_pack_path)
print(f"  ID:          {pack.id}")
print(f"  Kind:        {pack.kind}")
print(f"  Domain:      {pack.domain}")
print(f"  Description: {pack.description}")
print(f"  pack_version: {pack.pack_version}")

if pack.file_drop:
    print(f"  Cadence:     {pack.file_drop.cadence}")
    print(f"  Formats:     {pack.file_drop.formats}")

# ── 3. Validate a pack ────────────────────────────────────────────────────────
print("\n── 3. Validate pack against domain ──")

validator = PackValidator()
vr = validator.validate(pack, RetailDomain())
print(f"  Valid:    {vr.is_valid}")
print(f"  Errors:   {vr.errors}")
print(f"  Warnings: {vr.warnings}")

# ── 4. Run a pack ─────────────────────────────────────────────────────────────
print("\n── 4. Run retail fd_daily_batch pack ──")

runner = PackRunner()
run_result = runner.run(
    pack=pack,
    domain=RetailDomain(),
    scale="fabric_demo",
    seed=42,
    base_path="./pack_output",
)

print(run_result.summary())
print(f"  Success:         {run_result.is_success}")
print(f"  Files written:   {len(run_result.files_written)}")
print(f"  Events emitted:  {run_result.events_emitted:,}")
print(f"  Elapsed:         {run_result.elapsed_time:.2f}s")

# ── 5. Run a schema-drift pack (chaos included) ────────────────────────────────
print("\n── 5. Run retail fd_schema_drift pack ──")

drift_pack_path = Path(_BUILTIN_PACKS_ROOT) / "retail" / "fd_schema_drift.yaml"
drift_pack = PackLoader().load(drift_pack_path)

drift_result = runner.run(
    pack=drift_pack,
    domain=RetailDomain(),
    scale="fabric_demo",
    seed=99,
    base_path="./pack_output_drift",
)

print(f"  Success:  {drift_result.is_success}")
print(f"  Errors:   {drift_result.errors}")
if drift_result.validation_results:
    print(f"  Gate results: {drift_result.validation_results}")

# ── 6. Run a streaming pack ───────────────────────────────────────────────────
print("\n── 6. Run retail st_realtime_events pack ──")

stream_pack_path = Path(_BUILTIN_PACKS_ROOT) / "retail" / "st_realtime_events.yaml"
stream_pack = PackLoader().load(stream_pack_path)

stream_result = runner.run(
    pack=stream_pack,
    domain=RetailDomain(),
    scale="fabric_demo",
    seed=42,
    base_path="./pack_output_stream",
)

print(f"  Success:         {stream_result.is_success}")
print(f"  Events emitted:  {stream_result.events_emitted:,}")

# ── 7. Run a healthcare pack ──────────────────────────────────────────────────
print("\n── 7. Run healthcare fd_daily_batch pack ──")

hc_pack_path = Path(_BUILTIN_PACKS_ROOT) / "healthcare" / "fd_daily_batch.yaml"
hc_pack = PackLoader().load(hc_pack_path)

hc_result = runner.run(
    pack=hc_pack,
    domain=HealthcareDomain(),
    scale="fabric_demo",
    seed=42,
    base_path="./pack_output_hc",
)

print(f"  Success:       {hc_result.is_success}")
print(f"  Files written: {len(hc_result.files_written)}")

print("\nDone. Check ./pack_output* for generated files.")
