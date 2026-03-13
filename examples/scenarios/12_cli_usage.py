"""
Scenario 12 -- CLI Usage
========================
Spindle ships a full CLI (spindle) alongside the Python API. Every
operation available in Python has a CLI equivalent.

This script documents all CLI commands with runnable examples using
subprocess so you can see the output inline.

Run:
    python examples/scenarios/12_cli_usage.py

Or run any command directly in your terminal:
    spindle --help
    spindle generate retail --scale fabric_demo
"""

import subprocess
import sys
import tempfile
from pathlib import Path


def run(cmd: str, capture: bool = True) -> str:
    """Run a CLI command and return/print its output."""
    print(f"\n$ {cmd}")
    result = subprocess.run(
        cmd, shell=True, capture_output=capture,
        text=True, encoding="utf-8", errors="replace"
    )
    output = ((result.stdout or "") + (result.stderr or "")).strip()
    if output:
        # Indent output for readability
        for line in output.splitlines()[:30]:  # cap at 30 lines
            safe = line.encode("ascii", errors="replace").decode("ascii")
            print(f"  {safe}")
        if len(output.splitlines()) > 30:
            print("  ... (truncated)")
    return output


# ------------------------------------------------------------------
# 1. spindle --help
# ------------------------------------------------------------------
print("=" * 60)
print("SPINDLE CLI REFERENCE")
print("=" * 60)

run("spindle --help")

# ------------------------------------------------------------------
# 2. spindle list -- show all available domains
# ------------------------------------------------------------------
print("\n--- spindle list ---")
run("spindle list")

# ------------------------------------------------------------------
# 3. spindle describe <domain> -- schema details
# ------------------------------------------------------------------
print("\n--- spindle describe retail ---")
run("spindle describe retail")

# ------------------------------------------------------------------
# 4. spindle validate <domain> -- validate schema definition
# ------------------------------------------------------------------
print("\n--- spindle validate retail ---")
run("spindle validate retail")

# ------------------------------------------------------------------
# 5. spindle generate -- batch data generation
# ------------------------------------------------------------------
print("\n--- spindle generate (CSV, fabric_demo scale) ---")
with tempfile.TemporaryDirectory() as tmp:
    run(f"spindle generate retail --scale fabric_demo --format csv --output {tmp}/retail_csv")
    csv_files = list(Path(tmp).glob("retail_csv/*.csv"))
    print(f"  -> {len(csv_files)} CSV files written")

print("\n--- spindle generate (Parquet) ---")
with tempfile.TemporaryDirectory() as tmp:
    run(f"spindle generate retail --scale fabric_demo --format parquet --output {tmp}/retail_parquet")
    pq_files = list(Path(tmp).glob("retail_parquet/*.parquet"))
    print(f"  -> {len(pq_files)} Parquet files written")

print("\n--- spindle generate (JSON Lines) ---")
with tempfile.TemporaryDirectory() as tmp:
    run(f"spindle generate retail --scale fabric_demo --format jsonlines --output {tmp}/retail_jsonl")

print("\n--- spindle generate --dry-run (no files written) ---")
run("spindle generate retail --scale fabric_demo --dry-run")

print("\n--- spindle generate --seed for reproducibility ---")
with tempfile.TemporaryDirectory() as tmp:
    run(f"spindle generate retail --scale fabric_demo --seed 42 --output {tmp}/seeded")

# ------------------------------------------------------------------
# 6. spindle to-star -- generate + star schema transform
# ------------------------------------------------------------------
print("\n--- spindle to-star ---")
with tempfile.TemporaryDirectory() as tmp:
    run(f"spindle to-star retail --scale fabric_demo --output {tmp}/star")
    star_files = list(Path(tmp).glob("star/**/*.csv"))
    print(f"  -> {len(star_files)} star schema files written")

# ------------------------------------------------------------------
# 7. spindle to-cdm -- generate + CDM folder export
# ------------------------------------------------------------------
print("\n--- spindle to-cdm ---")
with tempfile.TemporaryDirectory() as tmp:
    run(f"spindle to-cdm retail --scale fabric_demo --output {tmp}/cdm")
    cdm_files = list(Path(tmp).glob("cdm/**/*"))
    model_exists = (Path(tmp) / "cdm" / "model.json").exists()
    print(f"  -> model.json: {model_exists}  total files: {len(cdm_files)}")

# ------------------------------------------------------------------
# 8. spindle stream -- stream events to console or file
# ------------------------------------------------------------------
print("\n--- spindle stream (console, 10 events) ---")
run("spindle stream retail --table order --max-events 10")

print("\n--- spindle stream (file sink) ---")
with tempfile.TemporaryDirectory() as tmp:
    outfile = Path(tmp) / "orders.jsonl"
    run(f"spindle stream retail --table order --max-events 50 --sink file --output {outfile}")
    if outfile.exists():
        lines = outfile.read_text().splitlines()
        print(f"  -> {len(lines)} events written to {outfile.name}")

# ------------------------------------------------------------------
# 9. Healthcare domain examples
# ------------------------------------------------------------------
print("\n--- Healthcare domain ---")
run("spindle describe healthcare")

with tempfile.TemporaryDirectory() as tmp:
    run(f"spindle generate healthcare --scale fabric_demo --format csv --output {tmp}/hc")
    hc_files = list(Path(tmp).glob("hc/*.csv"))
    print(f"  -> {len(hc_files)} healthcare CSV files")

print("\n\nAll CLI commands completed.")
