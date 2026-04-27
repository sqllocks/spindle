#!/usr/bin/env python3
"""Parallel smoke-test of all 13 spindle domains via Phase 2 Fabric Spark path.

Submits every domain concurrently (each uses spindle_<domain>_<scale>_*
table prefix and spindle_temp/<run_id>.json schema path so they don't
collide). Polls all run-IDs concurrently. Wall-clock should be ~max(
domain run time) instead of sum.

Pre-creates the spindle_spark_worker notebook synchronously before the
parallel phase to avoid concurrent create-item 409 races.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
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
POLL_TIMEOUT_SEC = 900

T_START = time.time()


def ts() -> str:
    """Wall-clock HH:MM:SS plus elapsed since script start."""
    elapsed = int(time.time() - T_START)
    return f"{datetime.now().strftime('%H:%M:%S')} +{elapsed:>4}s"


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


def get_token(resource: str) -> str:
    return subprocess.check_output(
        ["az", "account", "get-access-token", "--resource", resource,
         "--query", "accessToken", "-o", "tsv"], text=True,
    ).strip()


def precreate_notebook() -> None:
    """Run a single submit synchronously so the spindle_spark_worker notebook
    exists before parallel submits start. Avoids concurrent 409 create races."""
    log("warmup: pre-creating spindle_spark_worker notebook...")
    res = submit_domain("retail", quiet=True)
    if res.get("submit_failed"):
        log(f"warmup: FAILED — {res.get('error','')[-200:]}")
        sys.exit(1)
    log(f"warmup: notebook ready, run={res['fabric_run_id'][:8]} "
        f"(this submission is part of the smoke test, count it once)")
    return res


def submit_domain(domain: str, quiet: bool = False) -> dict:
    cmd = [
        ".venv-mac/bin/python", "-m", "sqllocks_spindle.cli", "demo", "run", "retail",
        "--mode", "seeding", "--scale-mode", "spark",
        "--rows", str(ROWS), "--connection", CONNECTION,
        "--seed", str(SEED), "--domain", domain,
    ]
    cwd = Path(__file__).parent.parent
    t0 = time.time()
    if not quiet:
        log(f"  [{domain}] submit START")
    try:
        proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=180)
    except subprocess.TimeoutExpired as e:
        return {"domain": domain, "submit_failed": True, "error": f"submit timeout: {e}"}
    if proc.returncode != 0:
        return {
            "domain": domain, "submit_failed": True,
            "submit_elapsed_sec": int(time.time() - t0),
            "error": proc.stderr[-400:],
        }
    sessions_dir = Path.home() / ".spindle" / "sessions"
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


def poll_one(api_token: str, notebook_item_id: str, fabric_run_id: str, domain: str) -> dict:
    url = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"
        f"/notebooks/{notebook_item_id}/livySessions"
    )
    headers = {"Authorization": f"Bearer {api_token}"}
    start = time.time()
    last_state = None
    while time.time() - start < POLL_TIMEOUT_SEC:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            time.sleep(POLL_INTERVAL_SEC); continue
        for s in r.json().get("value", []):
            if s.get("jobInstanceId") == fabric_run_id:
                state = s.get("state")
                running = s.get("runningDuration", {}).get("value", 0)
                if state != last_state:
                    log(f"  [{domain}] state={state} running={running}s")
                    last_state = state
                if state in ("Succeeded", "Failed", "Cancelled", "Dead", "Error"):
                    return {
                        "state": state, "running_sec": running,
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
    log("pre-flight: deleting any spindle_* tables...")
    n = delete_prefix(storage_token, "spindle_")
    log(f"  removed {n}")

    # Warmup: synchronous submit of retail. This creates the notebook item
    # and starts retail's run. We track it alongside the parallel batch.
    warmup = precreate_notebook()
    submit_results = {"retail": warmup}

    # Parallel submit for the remaining 12 domains. Cap at 6 concurrent — OneLake
    # DFS rate-limits writes to the same lakehouse and returned connection
    # timeouts on 13-way parallel uploads. With retry+backoff in
    # _upload_schema, 6-way is the safe ceiling.
    remaining = [d for d in DOMAINS if d != "retail"]
    SUBMIT_CONCURRENCY = 6
    log(f"submitting {len(remaining)} more domains in parallel "
        f"(max_workers={SUBMIT_CONCURRENCY})...")
    with ThreadPoolExecutor(max_workers=SUBMIT_CONCURRENCY) as pool:
        futures = {pool.submit(submit_domain, d): d for d in remaining}
        for fut in as_completed(futures):
            d = futures[fut]
            res = fut.result()
            submit_results[d] = res
            if res.get("submit_failed"):
                log(f"  [{d}] SUBMIT FAILED ({res.get('submit_elapsed_sec',0)}s): "
                    f"{res.get('error','')[-150:]}")
            else:
                log(f"  [{d}] submitted ({res['submit_elapsed_sec']}s) "
                    f"run={res['fabric_run_id'][:8]}")

    submit_phase_sec = int(time.time() - T_START)
    succeeded_submits = [d for d, r in submit_results.items() if not r.get("submit_failed")]
    log(f"submit phase done: {len(succeeded_submits)}/{len(DOMAINS)} OK at "
        f"+{submit_phase_sec}s")

    # Poll all in parallel
    log(f"polling {len(succeeded_submits)} domains in parallel...")
    api_token = get_token("https://api.fabric.microsoft.com")

    def poll_domain(d: str) -> tuple[str, dict]:
        sub = submit_results[d]
        if sub.get("submit_failed"):
            return d, {"state": "SubmitFailed"}
        return d, poll_one(api_token, sub["notebook_item_id"], sub["fabric_run_id"], d)

    poll_results = {}
    with ThreadPoolExecutor(max_workers=len(DOMAINS)) as pool:
        futures = [pool.submit(poll_domain, d) for d in succeeded_submits]
        for fut in as_completed(futures):
            d, p = fut.result()
            poll_results[d] = p

    poll_phase_sec = int(time.time() - T_START) - submit_phase_sec
    total_sec = int(time.time() - T_START)

    log("reading table sizes...")
    storage_token = get_token("https://storage.azure.com")

    full_results = []
    for d in DOMAINS:
        sub = submit_results.get(d, {})
        poll = poll_results.get(d, {})
        prefix = f"spindle_{d}_medium_"
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

    out = Path("/tmp/smoke_test_parallel_results.json")
    out.write_text(json.dumps(full_results, indent=2))
    log(f"results: {out}")

    succeeded = sum(1 for r in full_results if r["state"] == "Succeeded")
    failed = len(full_results) - succeeded

    print()
    print("=== SUMMARY ===", flush=True)
    print(f"Succeeded: {succeeded}/{len(full_results)}", flush=True)
    print(f"Failed:    {failed}/{len(full_results)}", flush=True)
    print(f"Submit phase: {submit_phase_sec}s", flush=True)
    print(f"Poll  phase:  {poll_phase_sec}s", flush=True)
    print(f"Total wall:   {total_sec}s", flush=True)
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
