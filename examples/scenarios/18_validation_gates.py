"""
Scenario 18 -- Validation Gates & Quarantine
=============================================
Run built-in validation gates against generated data, inspect failures,
and quarantine bad artifacts to a dedicated folder.

Gates are the checkpoint layer between raw generation and downstream
consumption. Use them to catch referential integrity breaks, schema
drift, and type mismatches before they corrupt your Lakehouse tables.

Run:
    python examples/scenarios/18_validation_gates.py
"""

import shutil
from pathlib import Path

from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.chaos.config import ChaosConfig
from sqllocks_spindle.chaos.engine import ChaosEngine
from sqllocks_spindle.validation.gates import (
    GateResult,
    ReferentialIntegrityGate,
    SchemaConformanceGate,
    ValidationContext,
)
from sqllocks_spindle.validation.quarantine import QuarantineManager

QUARANTINE_DIR = Path("./demo_quarantine")
RUN_ID = "run_20250115_001"

# ── 1. Clean data — gates should pass ────────────────────────────────────────
print("── 1. Gates on clean generated data ──")

result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)
schema = RetailDomain().get_schema()

context_clean = ValidationContext(
    tables=result.tables,
    schema=schema,
)

ri_gate = ReferentialIntegrityGate()
sc_gate = SchemaConformanceGate()

ri_clean = ri_gate.check(context_clean)
sc_clean = sc_gate.check(context_clean)

print(f"  Referential integrity: {'PASS' if ri_clean.passed else 'FAIL'}")
print(f"  Schema conformance:    {'PASS' if sc_clean.passed else 'FAIL'}")

# ── 2. Corrupt data — inject chaos then re-run gates ─────────────────────────
print("\n── 2. Gates on chaos-corrupted data ──")

chaos_cfg = ChaosConfig(
    enabled=True,
    intensity="hurricane",
    seed=99,
    warmup_days=0,
    chaos_start_day=1,
)
engine = ChaosEngine(chaos_cfg)

corrupted_tables = dict(result.tables)
corrupted_tables["order"] = engine.corrupt_dataframe(result["order"].copy(), day=5)
corrupted_tables["order"] = engine.drift_schema(corrupted_tables["order"], day=25)

context_bad = ValidationContext(
    tables=corrupted_tables,
    schema=schema,
)

ri_bad = ri_gate.check(context_bad)
sc_bad = sc_gate.check(context_bad)

def print_gate(name: str, gate_result: GateResult) -> None:
    status = "PASS" if gate_result.passed else "FAIL"
    print(f"  {name}: {status}")
    if not gate_result.passed:
        for err in gate_result.errors[:3]:
            print(f"    - {err}")
        if gate_result.details:
            print(f"    Details: {gate_result.details}")

print_gate("Referential integrity", ri_bad)
print_gate("Schema conformance",    sc_bad)

# ── 3. Quarantine failed tables ────────────────────────────────────────────────
print("\n── 3. Quarantine on gate failure ──")

qm = QuarantineManager(domain="retail")

if not ri_bad.passed:
    path = qm.quarantine_dataframe(
        df=corrupted_tables["order"],
        quarantine_root=QUARANTINE_DIR,
        run_id=RUN_ID,
        table_name="order",
        reason="Referential integrity violations detected",
        gate_name="referential_integrity",
        fmt="csv",
    )
    print(f"  Quarantined order table -> {path}")

if not sc_bad.passed:
    path = qm.quarantine_dataframe(
        df=corrupted_tables["order"],
        quarantine_root=QUARANTINE_DIR,
        run_id=RUN_ID,
        table_name="order",
        reason="Schema drift — unexpected / missing columns",
        gate_name="schema_conformance",
        fmt="csv",
    )
    print(f"  Quarantined drifted table -> {path}")

# ── 4. Inspect quarantine inventory ──────────────────────────────────────────
print("\n── 4. Quarantine inventory ──")
inventory = qm.list_quarantined(QUARANTINE_DIR)
print(f"  Items in quarantine: {len(inventory)}")
for item in inventory[:5]:
    print(f"  - {item}")

# ── 5. GateResult properties ──────────────────────────────────────────────────
print("\n── 5. GateResult breakdown ──")
print(f"  gate_name:  {ri_bad.gate_name}")
print(f"  passed:     {ri_bad.passed}")
print(f"  errors:     {ri_bad.errors[:2]}")
print(f"  warnings:   {ri_bad.warnings[:2]}")
print(f"  details:    {dict(list(ri_bad.details.items())[:3]) if ri_bad.details else '{}'}")

# ── Cleanup ────────────────────────────────────────────────────────────────────
print(f"\nQuarantine folder: {QUARANTINE_DIR.resolve()}")
print("To clean up:  shutil.rmtree('./demo_quarantine')")
