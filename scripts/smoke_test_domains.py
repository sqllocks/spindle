#!/usr/bin/env python3
"""Smoke-test every domain through the Phase 2 Fabric Spark path.

For each of the 13 domains: clean OneLake spindle_* tables, submit a demo run,
poll livySessions until terminal state, record the result. Continue on failure
per user instruction.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import requests

WORKSPACE_ID = "990dbc7b-f5d1-4bc8-a929-9dfd509a5d52"
LAKEHOUSE_ID = "ec851642-fa89-42bc-aebf-2742845d36fe"
CONNECTION = "fabric-demo"
ROWS = 50_000  # maps to "medium" via _rows_to_scale; ~30s generation per domain
SEED = 42

DOMAINS = [
    "retail",
    "healthcare",
    "financial",
    "supply_chain",
    "iot",
    "hr",
    "insurance",
    "marketing",
    "education",
    "real_estate",
    "manufacturing",
    "telecom",
    "capital_markets",
]

POLL_INTERVAL_SEC = 10
POLL_TIMEOUT_SEC = 600  # 10 min per domain


def get_token(resource: str) -> str:
    out = subprocess.check_output(
        ["az", "account", "get-access-token", "--resource", resource,
         "--query", "accessToken", "-o", "tsv"],
        text=True,
    ).strip()
    return out


def list_spindle_tables(storage_token: str) -> list[str]:
    url = (
        f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}"
        f"?resource=filesystem&recursive=false"
        f"&directory={LAKEHOUSE_ID}/Tables&maxResults=500"
    )
    r = requests.get(url, headers={"Authorization": f"Bearer {storage_token}"}, timeout=30)
    r.raise_for_status()
    paths = r.json().get("paths", [])
    return [p["name"].split("/")[-1] for p in paths if "spindle_" in p.get("name", "")]


def delete_spindle_tables(storage_token: str) -> int:
    tables = list_spindle_tables(storage_token)
    deleted = 0
    for tbl in tables:
        url = (
            f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{LAKEHOUSE_ID}"
            f"/Tables/{tbl}?recursive=true"
        )
        r = requests.delete(url, headers={"Authorization": f"Bearer {storage_token}"}, timeout=30)
        if r.status_code in (200, 202, 204):
            deleted += 1
    return deleted


def get_table_sizes(storage_token: str) -> dict[str, int]:
    url = (
        f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}"
        f"?resource=filesystem&recursive=true"
        f"&directory={LAKEHOUSE_ID}/Tables&maxResults=10000"
    )
    r = requests.get(url, headers={"Authorization": f"Bearer {storage_token}"}, timeout=30)
    r.raise_for_status()
    sizes: dict[str, int] = {}
    for p in r.json().get("paths", []):
        name = p.get("name", "")
        parts = name.split("/")
        if len(parts) >= 3 and parts[2].startswith("spindle_") and p.get("isDirectory") != "true":
            tbl = parts[2]
            sizes[tbl] = sizes.get(tbl, 0) + int(p.get("contentLength", 0))
    return sizes


def submit_demo_run(domain: str) -> dict:
    """Run the demo CLI and return the parsed manifest."""
    cmd = [
        ".venv-mac/bin/python", "-m", "sqllocks_spindle.cli", "demo", "run", "retail",
        "--mode", "seeding",
        "--scale-mode", "spark",
        "--rows", str(ROWS),
        "--connection", CONNECTION,
        "--seed", str(SEED),
        "--domain", domain,
    ]
    cwd = Path(__file__).parent.parent
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        return {
            "submit_failed": True,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
    # Find the latest session manifest
    sessions_dir = Path.home() / ".spindle" / "sessions"
    latest = max(sessions_dir.glob("demo-*.json"), key=lambda p: p.stat().st_mtime)
    manifest = json.loads(latest.read_text())
    return {
        "submit_failed": False,
        "session_id": manifest["session_id"],
        "fabric_run_id": manifest.get("fabric_run_id"),
        "notebook_item_id": manifest.get("notebook_item_id"),
    }


def poll_until_done(api_token: str, notebook_item_id: str, fabric_run_id: str) -> dict:
    """Poll livySessions until the run reaches a terminal state."""
    url = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"
        f"/notebooks/{notebook_item_id}/livySessions"
    )
    headers = {"Authorization": f"Bearer {api_token}"}
    start = time.time()
    while time.time() - start < POLL_TIMEOUT_SEC:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        for s in r.json().get("value", []):
            if s.get("jobInstanceId") == fabric_run_id:
                state = s.get("state")
                if state in ("Succeeded", "Failed", "Cancelled", "Dead", "Error"):
                    return {
                        "state": state,
                        "running_sec": s.get("runningDuration", {}).get("value", 0),
                        "spark_app_id": s.get("sparkApplicationId"),
                        "cancellation_reason": s.get("cancellationReason"),
                    }
                break
        time.sleep(POLL_INTERVAL_SEC)
    return {"state": "TIMEOUT", "running_sec": int(time.time() - start)}


def main() -> int:
    api_token = get_token("https://api.fabric.microsoft.com")
    storage_token = get_token("https://storage.azure.com")

    print(f"Pre-flight: deleting any existing spindle_* tables...")
    n = delete_spindle_tables(storage_token)
    print(f"  removed {n} pre-existing tables")

    results: list[dict] = []
    for i, domain in enumerate(DOMAINS, 1):
        print(f"\n[{i:>2}/13] domain={domain}")
        # Refresh tokens (may expire over a long run)
        api_token = get_token("https://api.fabric.microsoft.com")
        storage_token = get_token("https://storage.azure.com")

        # Pre-clean for this domain
        delete_spindle_tables(storage_token)

        t0 = time.time()
        sub = submit_demo_run(domain)
        if sub.get("submit_failed"):
            results.append({
                "domain": domain,
                "phase": "submit",
                "state": "Failed",
                "error": sub.get("stderr", "")[-500:],
                "elapsed_sec": int(time.time() - t0),
            })
            print(f"  SUBMIT FAILED: {sub.get('stderr','')[-200:]}")
            continue

        print(f"  submitted: session={sub['session_id']}, run={sub['fabric_run_id']}")
        if not sub.get("fabric_run_id"):
            results.append({
                "domain": domain,
                "phase": "submit",
                "state": "Failed",
                "error": "no fabric_run_id in manifest",
                "elapsed_sec": int(time.time() - t0),
            })
            continue

        poll = poll_until_done(api_token, sub["notebook_item_id"], sub["fabric_run_id"])
        elapsed = int(time.time() - t0)
        print(f"  state={poll['state']}, running={poll.get('running_sec', 0)}s, elapsed={elapsed}s")

        # Refresh storage token before listing
        storage_token = get_token("https://storage.azure.com")
        sizes = get_table_sizes(storage_token)
        total_bytes = sum(sizes.values())
        print(f"  tables_written={len(sizes)}, total_bytes={total_bytes:,}")

        results.append({
            "domain": domain,
            "phase": "complete",
            "state": poll["state"],
            "running_sec": poll.get("running_sec", 0),
            "elapsed_sec": elapsed,
            "spark_app_id": poll.get("spark_app_id"),
            "session_id": sub["session_id"],
            "fabric_run_id": sub["fabric_run_id"],
            "notebook_item_id": sub["notebook_item_id"],
            "tables_written": len(sizes),
            "table_sizes_bytes": sizes,
            "total_bytes": total_bytes,
            "cancellation_reason": poll.get("cancellation_reason"),
        })

    # Final cleanup
    print("\nFinal cleanup...")
    n = delete_spindle_tables(get_token("https://storage.azure.com"))
    print(f"  removed {n} tables")

    # Write results
    out = Path("/tmp/smoke_test_results.json")
    out.write_text(json.dumps(results, indent=2))
    print(f"\nResults written to {out}")

    # Summary
    print("\n=== SUMMARY ===")
    succeeded = sum(1 for r in results if r.get("state") == "Succeeded")
    failed = sum(1 for r in results if r.get("state") not in ("Succeeded",))
    print(f"Succeeded: {succeeded}/{len(results)}")
    print(f"Failed:    {failed}/{len(results)}")
    print()
    print(f"{'Domain':<20} {'State':<12} {'Running':>10} {'Elapsed':>10} {'Tables':>7} {'Bytes':>14}")
    print("-" * 78)
    for r in results:
        state = r.get("state", "?")
        run_s = r.get("running_sec", 0)
        elapsed = r.get("elapsed_sec", 0)
        tables = r.get("tables_written", 0)
        b = r.get("total_bytes", 0)
        print(f"{r['domain']:<20} {state:<12} {run_s:>9}s {elapsed:>9}s {tables:>7} {b:>14,}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
