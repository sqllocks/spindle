"""
Spindle Notebook Validation Runner
Executes all 41 notebooks via nbclient, captures output in-place,
and generates a comprehensive validation report.
"""

import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError

# ── Configuration ────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
NB_BASE = PROJECT_ROOT / "examples" / "notebooks"
SWEEP_DIR = PROJECT_ROOT / "sweep_results"
KERNEL_NAME = "spindle-venv"
DEFAULT_TIMEOUT = 300  # seconds per cell
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

# ── Notebook Manifest ────────────────────────────────────────────────────────
# (relative_path, track, phase, fabric_required)
NOTEBOOKS = [
    # Phase 1 — Local Only (31 notebooks)
    ("quickstart/T01_hello_spindle.ipynb", "quickstart", 1, False),
    ("quickstart/T02_explore_all_domains.ipynb", "quickstart", 1, False),
    ("quickstart/T03_custom_schema.ipynb", "quickstart", 1, False),
    ("quickstart/T04_healthcare_deep_dive.ipynb", "quickstart", 1, False),
    ("quickstart/T05_distribution_overrides.ipynb", "quickstart", 1, False),
    ("quickstart/T06_star_schema_export.ipynb", "quickstart", 1, False),
    ("quickstart/T07_domain_quickstarts.ipynb", "quickstart", 1, False),
    ("intermediate/T11_capital_markets.ipynb", "intermediate", 1, False),
    ("intermediate/T12_streaming_events.ipynb", "intermediate", 1, False),
    ("intermediate/T13_file_drop_simulation.ipynb", "intermediate", 1, False),
    ("intermediate/T14_chaos_engineering.ipynb", "intermediate", 1, False),
    ("intermediate/T15_ddl_import.ipynb", "intermediate", 1, False),
    ("intermediate/T15_validation_gates.ipynb", "intermediate", 1, False),
    ("intermediate/T16_composite_domains.ipynb", "intermediate", 1, False),
    ("intermediate/T16_learn_from_data.ipynb", "intermediate", 1, False),
    ("intermediate/T17_day2_incremental.ipynb", "intermediate", 1, False),
    ("intermediate/T17_fidelity_and_masking.ipynb", "intermediate", 1, False),
    ("intermediate/T19_time_travel_snapshots.ipynb", "intermediate", 1, False),
    ("intermediate/T20_simulation_patterns.ipynb", "intermediate", 1, False),
    ("intermediate/T21_composite_presets.ipynb", "intermediate", 1, False),
    ("fabric-scenarios/F01_medallion_architecture.ipynb", "fabric-scenarios", 1, False),
    ("fabric-scenarios/F05_chaos_pipeline.ipynb", "fabric-scenarios", 1, False),
    ("fabric-scenarios/F07_healthcare_rcm.ipynb", "fabric-scenarios", 1, False),
    ("fabric-scenarios/F09_cross_domain_enterprise.ipynb", "fabric-scenarios", 1, False),
    ("showcase/01_quickstart.ipynb", "showcase", 1, False),
    ("showcase/02_domain_showcase.ipynb", "showcase", 1, False),
    ("showcase/04_star_schema.ipynb", "showcase", 1, False),
    ("showcase/05_streaming.ipynb", "showcase", 1, False),
    ("showcase/06_chaos_and_simulation.ipynb", "showcase", 1, False),
    ("showcase/07_composite_domain.ipynb", "showcase", 1, False),
    ("showcase/08_scenario_packs.ipynb", "showcase", 1, False),
    # Phase 2 — Fabric SQL (5 notebooks)
    ("intermediate/T10_fabric_sql_database.ipynb", "intermediate", 2, True),
    ("fabric-scenarios/F02_warehouse_dimensional.ipynb", "fabric-scenarios", 2, True),
    ("fabric-scenarios/F03_sql_database_oltp.ipynb", "fabric-scenarios", 2, True),
    ("fabric-scenarios/F06_semantic_model.ipynb", "fabric-scenarios", 2, True),
    ("fabric-scenarios/F10_month_end_close.ipynb", "fabric-scenarios", 2, True),
    # Phase 3 — Fabric Spark (5 notebooks)
    ("intermediate/T08_fabric_lakehouse.ipynb", "intermediate", 3, True),
    ("intermediate/T09_fabric_quickstart.ipynb", "intermediate", 3, True),
    ("fabric-scenarios/F04_realtime_streaming.ipynb", "fabric-scenarios", 3, True),
    ("fabric-scenarios/F08_filedrop_ingestion.ipynb", "fabric-scenarios", 3, True),
    ("showcase/03_fabric_lakehouse.ipynb", "showcase", 3, True),
]


def count_outputs(nb):
    """Count code cells and how many have output."""
    code_cells = [c for c in nb.cells if c.cell_type == "code"]
    with_output = sum(1 for c in code_cells if c.outputs)
    return len(code_cells), with_output


def execute_notebook(nb_path: Path, kernel: str, timeout: int):
    """Execute a notebook and return detailed results."""
    result = {
        "notebook": nb_path.name,
        "path": str(nb_path.relative_to(PROJECT_ROOT)),
        "status": "PENDING",
        "pre_size_bytes": nb_path.stat().st_size,
        "post_size_bytes": 0,
        "total_cells": 0,
        "code_cells": 0,
        "cells_with_output": 0,
        "cells_without_output": 0,
        "execution_time_seconds": 0,
        "cell_timings": [],
        "errors": [],
    }

    nb = nbformat.read(str(nb_path), as_version=4)
    result["total_cells"] = len(nb.cells)
    code_cells_pre, outputs_pre = count_outputs(nb)
    result["code_cells"] = code_cells_pre

    # Set up the client — execute in the notebook's own directory
    client = NotebookClient(
        nb,
        timeout=timeout,
        kernel_name=kernel,
        resources={"metadata": {"path": str(nb_path.parent)}},
    )

    start = time.time()
    try:
        client.execute()
        elapsed = time.time() - start
        result["execution_time_seconds"] = round(elapsed, 2)
        result["status"] = "PASS"
    except CellExecutionError as e:
        elapsed = time.time() - start
        result["execution_time_seconds"] = round(elapsed, 2)
        result["status"] = "FAIL"
        result["errors"].append({
            "cell_index": getattr(e, "cell_index", None),
            "ename": getattr(e, "ename", ""),
            "evalue": getattr(e, "evalue", ""),
            "traceback_short": str(e)[:2000],
        })
    except Exception as e:
        elapsed = time.time() - start
        result["execution_time_seconds"] = round(elapsed, 2)
        result["status"] = "FAIL"
        result["errors"].append({
            "cell_index": None,
            "ename": type(e).__name__,
            "evalue": str(e)[:500],
            "traceback_short": traceback.format_exc()[:2000],
        })

    # Save executed notebook (with output) back to disk
    nbformat.write(nb, str(nb_path))
    result["post_size_bytes"] = nb_path.stat().st_size

    # Recount outputs
    code_cells_post, outputs_post = count_outputs(nb)
    result["cells_with_output"] = outputs_post
    result["cells_without_output"] = code_cells_post - outputs_post

    # Gather per-cell timings for code cells
    for i, cell in enumerate(nb.cells):
        if cell.cell_type == "code":
            exec_count = cell.get("execution_count")
            has_output = bool(cell.outputs)
            cell_status = "ok"
            if any(o.get("output_type") == "error" for o in cell.outputs):
                cell_status = "error"
            result["cell_timings"].append({
                "cell_index": i,
                "execution_count": exec_count,
                "has_output": has_output,
                "status": cell_status,
            })

    return result


def run_phase(phase_num: int, phase_name: str, notebooks: list, all_results: list):
    """Run all notebooks in a given phase."""
    phase_nbs = [(p, t, ph, fr) for p, t, ph, fr in notebooks if ph == phase_num]
    print(f"\n{'='*70}")
    print(f"  PHASE {phase_num}: {phase_name} ({len(phase_nbs)} notebooks)")
    print(f"{'='*70}\n")

    for idx, (rel_path, track, phase, fabric_required) in enumerate(phase_nbs, 1):
        nb_path = NB_BASE / rel_path
        nb_name = nb_path.name
        print(f"  [{idx}/{len(phase_nbs)}] {nb_name} (track={track})")
        print(f"         Path: {rel_path}")
        print(f"         Pre-size: {nb_path.stat().st_size:,} bytes")

        result = execute_notebook(nb_path, KERNEL_NAME, DEFAULT_TIMEOUT)
        result["track"] = track
        result["phase"] = phase
        result["fabric_required"] = fabric_required

        status_icon = "PASS" if result["status"] == "PASS" else "FAIL"
        print(f"         Status: {status_icon}")
        print(f"         Time: {result['execution_time_seconds']}s")
        print(f"         Code cells: {result['code_cells']}, "
              f"with output: {result['cells_with_output']}, "
              f"without: {result['cells_without_output']}")
        print(f"         Post-size: {result['post_size_bytes']:,} bytes "
              f"(delta: +{result['post_size_bytes'] - result['pre_size_bytes']:,})")

        if result["errors"]:
            for err in result["errors"]:
                print(f"         ERROR cell {err.get('cell_index')}: "
                      f"{err.get('ename')}: {err.get('evalue', '')[:200]}")

        all_results.append(result)
        print()


def generate_report(results: list):
    """Generate JSON and markdown reports."""
    SWEEP_DIR.mkdir(exist_ok=True)
    json_path = SWEEP_DIR / f"notebook_validation_{TIMESTAMP}.json"
    md_path = SWEEP_DIR / f"notebook_validation_{TIMESTAMP}.md"

    # JSON
    report = {
        "timestamp": TIMESTAMP,
        "total_notebooks": len(results),
        "passed": sum(1 for r in results if r["status"] == "PASS"),
        "failed": sum(1 for r in results if r["status"] == "FAIL"),
        "total_execution_time": round(sum(r["execution_time_seconds"] for r in results), 2),
        "results": results,
    }
    json_path.write_text(json.dumps(report, indent=2))
    print(f"\nJSON report: {json_path}")

    # Markdown
    lines = [
        f"# Spindle Notebook Validation Report",
        f"",
        f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**Total notebooks:** {len(results)}",
        f"**Passed:** {report['passed']} | **Failed:** {report['failed']}",
        f"**Total execution time:** {report['total_execution_time']}s",
        f"",
        f"## Results",
        f"",
        f"| # | Notebook | Track | Phase | Status | Code Cells | With Output | Time (s) | Size Delta |",
        f"|---|----------|-------|-------|--------|-----------|-------------|----------|------------|",
    ]
    for i, r in enumerate(results, 1):
        delta = r["post_size_bytes"] - r["pre_size_bytes"]
        lines.append(
            f"| {i} | {r['notebook']} | {r.get('track','')} | {r.get('phase','')} | "
            f"{r['status']} | {r['code_cells']} | {r['cells_with_output']} | "
            f"{r['execution_time_seconds']} | +{delta:,} |"
        )

    # Failures section
    failures = [r for r in results if r["status"] == "FAIL"]
    if failures:
        lines.extend(["", "## Failures", ""])
        for r in failures:
            lines.append(f"### {r['notebook']}")
            for err in r["errors"]:
                lines.append(f"- **Cell {err.get('cell_index')}**: {err.get('ename')}: {err.get('evalue', '')[:300]}")
            lines.append("")

    md_path.write_text("\n".join(lines))
    print(f"Markdown report: {md_path}")

    return json_path, md_path


def main():
    print(f"Spindle Notebook Validation Runner")
    print(f"Timestamp: {TIMESTAMP}")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Notebook base: {NB_BASE}")
    print(f"Kernel: {KERNEL_NAME}")
    print(f"Total notebooks: {len(NOTEBOOKS)}")

    # Filter by phase if argument provided
    phase_filter = None
    if len(sys.argv) > 1:
        phase_filter = int(sys.argv[1])
        print(f"Running phase {phase_filter} only")

    all_results = []

    if phase_filter is None or phase_filter == 1:
        run_phase(1, "Local Only", NOTEBOOKS, all_results)
    if phase_filter is None or phase_filter == 2:
        run_phase(2, "Fabric SQL", NOTEBOOKS, all_results)
    if phase_filter is None or phase_filter == 3:
        run_phase(3, "Fabric Spark", NOTEBOOKS, all_results)

    # Generate reports
    json_path, md_path = generate_report(all_results)

    # Summary
    passed = sum(1 for r in all_results if r["status"] == "PASS")
    failed = sum(1 for r in all_results if r["status"] == "FAIL")
    print(f"\n{'='*70}")
    print(f"  SUMMARY: {passed} passed, {failed} failed out of {len(all_results)}")
    print(f"{'='*70}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
