#!/usr/bin/env python3
"""Parallel smoke-test of all 13 spindle domains via Phase 2 Fabric Spark path.

Submits every domain concurrently (each uses spindle_<domain>_<scale>_*
table prefix and spindle_temp/<run_id>.json schema path so they don't
collide). Polls all run-IDs concurrently. Wall-clock should be ~max(
domain run time) instead of sum.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

WORKSPACE_ID = "990dbc7b-f5d1-4bc8-a929-9dfd509a5d52"
LAKEHOUSE_ID = "ec851642-fa89-42bc-aebf-2742845d36fe"
CONNECTION = "fabric-demo"
ROWS = 50_000  # medium scale
SEED = 42

DOMAINS = [
    "retail", "healthcare", "financial", "supply_chain", "iot", "hr",
    "insurance", "marketing", "education", "real_estate",
    "manufacturing", "telecom", "capital_markets",
]

POLL_INTERVAL_SEC = 15
POLL_TIMEOUT_SEC = 900  # 15 min ceiling per domain


def get_token(resource: str) -> str:
    return subprocess.check_output(
        ["az", "account", "get-access-token", "--resource", resource,
         "--query", "accessToken", "-o", "tsv"], text=True,
    ).strip()


def submit_domain(domain: str) -> dict:
    """Submit one domain run via the demo CLI; return manifest fields."""
    cmd = [
        ".venv-mac/bin/python", "-m", "sqllocks_spindle.cli", "demo", "run", "retail",
        "--mode", "seeding", "--scale-mode", "spark",
        "--rows", str(ROWS), "--connection", CONNECTION,
        "--seed", str(SEED), "--domain", domain,
    ]
    cwd = Path(__file__).parent.parent
    t0 = time.time()
    try:
        proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=180)
    except subprocess.TimeoutExpired as e:
        return {"domain": domain, "submit_failed": True, "error": f"submit timeout: {e}"}
    if proc.returncode != 0:
        return {
            "domain": domain, "submit_failed": True,
            "error": proc.stderr[-400:],
        }
    sessions_dir = Path.home() / ".spindle" / "sessions"
    # Pick latest manifest matching this domain's submission time
    candidates = [p for p in sessions_dir.glob("demo-*.json") if p.stat().st_mtime >= t0]
    if not candidates:
        return {"domain": domain, "submit_failed": True, "error": "no fresh manifest"}
    manifest = json.loads(max(candidates, key=lambda p: p.stat().st_mtime).read_text())
    return {
        "domain": domain,
        "submit_failed": False,
        "submit_elapsed_sec": int(time.time() - t0),
        "session_id": manifest["session_id"],
        "fabric_run_id": manifest.get("fabric_run_id"),
        "notebook_item_id": manifest.get("notebook_item_id"),
    }


def poll_one(api_token: str, notebook_item_id: str, fabric_run_id: str) -> dict:
    url = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"
        f"/notebooks/{notebook_item_id}/livySessions"
    )
    headers = {"Authorization": f"Bearer {api_token}"}
    start = time.time()
    while time.time() - start < POLL_TIMEOUT_SEC:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            time.sleep(POLL_INTERVAL_SEC); continue
        for s in r.json().get("value", []):
            if s.get("jobInstanceId") == fabric_run_id:
                state = s.get("state")
                if state in ("Succeeded", "Failed", "Cancelled", "Dead", "Error"):
                    return {
                        "state": state,
                        "running_sec": s.get("runningDuration", {}).get("value", 0),
                        "spark_app_id": s.get("sparkApplicationId"),
                    }
                break
        time.sleep(POLL_INTERVAL_SEC)
    return {"state": "TIMEOUT", "running_sec": int(time.time() - start)}


def get_table_sizes(storage_token: str, prefix: str) -> dict[str, int]:
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
        if len(parts) >= 3 and parts[2].startswith(prefix) and p.get("isDirectory") != "true":
            sizes[parts[2]] = sizes.get(parts[2], 0) + int(p.get("contentLength", 0))
    return sizes


def delete_prefix(storage_token: str, prefix: str) -> int:
    """Delete every Tables/<prefix>* directory."""
    url = (
        f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}"
        f"?resource=filesystem&recursive=false"
        f"&directory={LAKEHOUSE_ID}/Tables&maxResults=2000"
    )
    r = requests.get(url, headers={"Authorization": f"Bearer {storage_token}"}, timeout=30)
    r.raise_for_status()
    deleted = 0
    for p in r.json().get("paths", []):
        name = p.get("name", "").split("/")[-1]
        if not name.startswith(prefix):
            continue
        d = requests.delete(
            f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{LAKEHOUSE_ID}"
            f"/Tables/{name}?recursive=true",
            headers={"Authorization": f"Bearer {storage_token}"}, timeout=30,
        )
        if d.status_code in (200, 202, 204):
            deleted += 1
    return deleted


def main() -> int:
    storage_token = get_token("https://storage.azure.com")
    print(f"Pre-flight: deleting any spindle_* tables...", flush=True)
    n = delete_prefix(storage_token, "spindle_")
    print(f"  removed {n}", flush=True)

    overall_start = time.time()

    # Submit all 13 in parallel
    print(f"\nSubmitting {len(DOMAINS)} domains in parallel...", flush=True)
    submit_results = {}
    with ThreadPoolExecutor(max_workers=len(DOMAINS)) as pool:
        futures = {pool.submit(submit_domain, d): d for d in DOMAINS}
        for fut in as_completed(futures):
            d = futures[fut]
            res = fut.result()
            submit_results[d] = res
            if res.get("submit_failed"):
                print(f"  [{d}] SUBMIT FAILED ({res.get('submit_elapsed_sec',0)}s): "
                      f"{res.get('error','')[-150:]}", flush=True)
            else:
                print(f"  [{d}] submitted ({res['submit_elapsed_sec']}s) "
                      f"run={res['fabric_run_id'][:8]}...", flush=True)

    submit_phase_sec = int(time.time() - overall_start)
    succeeded_submits = [d for d, r in submit_results.items() if not r.get("submit_failed")]
    print(f"\nSubmit phase: {len(succeeded_submits)}/{len(DOMAINS)} OK in {submit_phase_sec}s",
          flush=True)

    # Poll all in parallel
    print(f"\nPolling {len(succeeded_submits)} domains in parallel...", flush=True)
    api_token = get_token("https://api.fabric.microsoft.com")

    def poll_domain(d: str) -> tuple[str, dict]:
        sub = submit_results[d]
        if sub.get("submit_failed"):
            return d, {"state": "SubmitFailed"}
        return d, poll_one(api_token, sub["notebook_item_id"], sub["fabric_run_id"])

    poll_results = {}
    with ThreadPoolExecutor(max_workers=len(DOMAINS)) as pool:
        futures = [pool.submit(poll_domain, d) for d in succeeded_submits]
        for fut in as_completed(futures):
            d, p = fut.result()
            poll_results[d] = p
            print(f"  [{d}] state={p.get('state')} running={p.get('running_sec',0)}s",
                  flush=True)

    poll_phase_sec = int(time.time() - overall_start) - submit_phase_sec
    total_sec = int(time.time() - overall_start)

    # Tally per-domain table sizes
    print(f"\nReading table sizes...", flush=True)
    storage_token = get_token("https://storage.azure.com")

    full_results = []
    for d in DOMAINS:
        sub = submit_results.get(d, {})
        poll = poll_results.get(d, {})
        prefix = f"spindle_{d}_medium_"  # _rows_to_scale(50_000) = "medium"
        sizes = get_table_sizes(storage_token, prefix) if not sub.get("submit_failed") else {}
        full_results.append({
            "domain": d,
            "submit_failed": sub.get("submit_failed", False),
            "submit_error": sub.get("error"),
            "state": poll.get("state", "?"),
            "running_sec": poll.get("running_sec", 0),
            "spark_app_id": poll.get("spark_app_id"),
            "session_id": sub.get("session_id"),
            "fabric_run_id": sub.get("fabric_run_id"),
            "table_count": len(sizes),
            "total_bytes": sum(sizes.values()),
            "tables": sizes,
        })

    # Summary
    out = Path("/tmp/smoke_test_parallel_results.json")
    out.write_text(json.dumps(full_results, indent=2))
    print(f"\nResults: {out}", flush=True)

    print("\n=== SUMMARY ===", flush=True)
    succeeded = sum(1 for r in full_results if r["state"] == "Succeeded")
    failed = len(full_results) - succeeded
    print(f"Succeeded: {succeeded}/{len(full_results)}", flush=True)
    print(f"Failed:    {failed}/{len(full_results)}", flush=True)
    print(f"Submit phase:  {submit_phase_sec}s", flush=True)
    print(f"Poll  phase:   {poll_phase_sec}s", flush=True)
    print(f"Total wall:    {total_sec}s", flush=True)
    print()
    print(f"{'Domain':<20} {'State':<14} {'Running':>10} {'Tables':>7} {'Bytes':>14}",
          flush=True)
    print("-" * 70, flush=True)
    for r in full_results:
        print(f"{r['domain']:<20} {r['state']:<14} {r['running_sec']:>9}s "
              f"{r['table_count']:>7} {r['total_bytes']:>14,}", flush=True)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
