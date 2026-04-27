#!/usr/bin/env python3
"""Parallel smoke-test of all 13 spindle domains via Phase 2 Fabric Spark path.

Architecture (three phases):
  Phase A — prepare() per domain: static-gen + OneLake upload + notebook find/create.
             Slow, I/O-heavy, throttle-prone. Run at PREPARE_CONCURRENCY (4-way).
  Phase B — submit_run() per domain: single REST call per domain.
             Fast. Run fully parallel (13-way).
  Phase C — poll all livySessions concurrently until terminal state.
             Run fully parallel.

Wall-clock target: ~max(domain Spark run time) + ~prepare overhead.
"""
from __future__ import annotations

import dataclasses
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

WORKSPACE_ID = "990dbc7b-f5d1-4bc8-a929-9dfd509a5d52"
LAKEHOUSE_ID = "ec851642-fa89-42bc-aebf-2742845d36fe"
CONNECTION = "fabric-demo"
ROWS = 50_000
SEED = 42

DOMAINS = [
    "retail", "healthcare", "financial", "supply_chain", "iot", "hr",
    "insurance", "marketing", "education", "real_estate",
    "manufacturing", "telecom", "capital_markets",
]

PREPARE_CONCURRENCY = 4   # OneLake DFS throttle ceiling
POLL_INTERVAL_SEC = 15
POLL_TIMEOUT_SEC = 900

T_START = time.time()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def ts() -> str:
    elapsed = int(time.time() - T_START)
    return f"{datetime.now().strftime('%H:%M:%S')} +{elapsed:>4}s"


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


def get_token(resource: str) -> str:
    return subprocess.check_output(
        ["az", "account", "get-access-token", "--resource", resource,
         "--query", "accessToken", "-o", "tsv"], text=True,
    ).strip()


def _load_connection():
    """Load the fabric-demo connection profile."""
    from sqllocks_spindle.demo.connections import get_connection
    return get_connection(CONNECTION)


def _build_schema(domain_name: str) -> dict:
    """Build a schema dict for the given domain at ROWS scale."""
    import importlib
    import pkgutil
    import sqllocks_spindle.domains as _pkg
    from sqllocks_spindle.domains.base import Domain
    from sqllocks_spindle.engine.generator import Spindle

    domain = None
    for _, mod_name, is_pkg in pkgutil.iter_modules(_pkg.__path__, _pkg.__name__ + "."):
        if not is_pkg:
            continue
        try:
            module = importlib.import_module(mod_name)
        except Exception:
            continue
        for attr in getattr(module, "__all__", dir(module)):
            cls = getattr(module, attr, None)
            if isinstance(cls, type) and issubclass(cls, Domain) and cls is not Domain:
                try:
                    inst = cls.__new__(cls)
                    if cls.name.fget(inst) == domain_name:
                        domain = cls()
                        break
                except Exception:
                    pass
        if domain is not None:
            break

    if domain is None:
        raise ValueError(f"Domain '{domain_name}' not found")

    sp = Spindle()
    parsed = sp._resolve_schema(domain, None)

    rows = ROWS
    if rows <= 2_000:
        scale = "small"
    elif rows <= 50_000:
        scale = "medium"
    elif rows <= 500_000:
        scale = "large"
    else:
        scale = "xlarge"

    parsed.generation.scale = scale
    parsed.model.seed = SEED
    schema_dict = dataclasses.asdict(parsed)
    if hasattr(domain, "domain_path"):
        schema_dict["_domain_path"] = str(domain.domain_path)
    return schema_dict, scale


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
            time.sleep(POLL_INTERVAL_SEC)
            continue
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    # Pre-flight: clean OneLake
    storage_token = get_token("https://storage.azure.com")
    log("pre-flight: deleting any spindle_* tables...")
    n = delete_prefix(storage_token, "spindle_")
    log(f"  removed {n}")

    # Load connection + acquire token once
    conn = _load_connection()
    api_token = get_token("https://api.fabric.microsoft.com")

    from sqllocks_spindle.engine.spark_router import FabricSparkRouter

    # ------------------------------------------------------------------
    # Phase A — prepare() at controlled concurrency (4-way)
    # Each call: static-gen + OneLake upload + notebook find/create
    # ------------------------------------------------------------------
    log(f"Phase A: prepare all {len(DOMAINS)} domains (max_workers={PREPARE_CONCURRENCY})...")

    prepare_results: dict[str, dict | Exception] = {}

    def do_prepare(domain: str) -> tuple[str, dict | Exception]:
        t0 = time.time()
        try:
            schema_dict, scale_label = _build_schema(domain)
            table_prefix = f"spindle_{domain}_{scale_label}_"
            router = FabricSparkRouter(
                workspace_id=conn.workspace_id,
                lakehouse_id=conn.lakehouse_id,
                token=api_token,
                sinks=["lakehouse"],
                sink_config={
                    "workspace_id": conn.workspace_id,
                    "lakehouse_id": conn.lakehouse_id,
                    "token": api_token,
                },
                chunk_size=500_000,
                table_prefix=table_prefix,
            )
            prepared = router.prepare(schema_dict, total_rows=ROWS, seed=SEED)
            elapsed = int(time.time() - t0)
            log(f"  [{domain}] prepare OK ({elapsed}s) path={prepared['schema_path']}")
            prepared["_router"] = router
            prepared["_domain"] = domain
            return domain, prepared
        except Exception as exc:
            elapsed = int(time.time() - t0)
            log(f"  [{domain}] prepare FAILED ({elapsed}s): {exc}")
            return domain, exc

    with ThreadPoolExecutor(max_workers=PREPARE_CONCURRENCY) as pool:
        futures = {pool.submit(do_prepare, d): d for d in DOMAINS}
        for fut in as_completed(futures):
            d, result = fut.result()
            prepare_results[d] = result

    prepare_phase_sec = int(time.time() - T_START)
    ok_prepares = [d for d, r in prepare_results.items() if not isinstance(r, Exception)]
    log(f"Phase A done: {len(ok_prepares)}/{len(DOMAINS)} OK at +{prepare_phase_sec}s")

    # ------------------------------------------------------------------
    # Phase B — submit_run() fully parallel (one REST call per domain)
    # ------------------------------------------------------------------
    log(f"Phase B: submit_run all {len(ok_prepares)} domains in parallel...")

    submit_results: dict[str, dict | Exception] = {}

    def do_submit(domain: str) -> tuple[str, dict | Exception]:
        prepared = prepare_results[domain]
        router: FabricSparkRouter = prepared["_router"]
        t0 = time.time()
        try:
            job = router.submit_run(prepared)
            elapsed = int(time.time() - t0)
            log(f"  [{domain}] submit_run OK ({elapsed}s) run={job.fabric_run_id[:8]}")
            return domain, {
                "job": job,
                "fabric_run_id": job.fabric_run_id,
                "notebook_item_id": job.notebook_item_id,
            }
        except Exception as exc:
            elapsed = int(time.time() - t0)
            log(f"  [{domain}] submit_run FAILED ({elapsed}s): {exc}")
            return domain, exc

    with ThreadPoolExecutor(max_workers=len(DOMAINS)) as pool:
        futures = {pool.submit(do_submit, d): d for d in ok_prepares}
        for fut in as_completed(futures):
            d, result = fut.result()
            submit_results[d] = result

    submit_phase_sec = int(time.time() - T_START) - prepare_phase_sec
    ok_submits = [d for d, r in submit_results.items() if not isinstance(r, Exception)]
    log(f"Phase B done: {len(ok_submits)}/{len(DOMAINS)} OK at +{int(time.time()-T_START)}s")

    # ------------------------------------------------------------------
    # Phase C — poll all in parallel
    # ------------------------------------------------------------------
    log(f"Phase C: polling {len(ok_submits)} domains in parallel...")

    poll_results: dict[str, dict] = {}

    def do_poll(domain: str) -> tuple[str, dict]:
        sr = submit_results[domain]
        return domain, poll_one(
            api_token,
            sr["notebook_item_id"],
            sr["fabric_run_id"],
            domain,
        )

    with ThreadPoolExecutor(max_workers=len(DOMAINS)) as pool:
        futures = [pool.submit(do_poll, d) for d in ok_submits]
        for fut in as_completed(futures):
            d, p = fut.result()
            poll_results[d] = p

    total_sec = int(time.time() - T_START)

    # ------------------------------------------------------------------
    # Table size check
    # ------------------------------------------------------------------
    log("reading table sizes...")
    storage_token = get_token("https://storage.azure.com")

    full_results = []
    for d in DOMAINS:
        prep = prepare_results.get(d)
        sub = submit_results.get(d)
        poll = poll_results.get(d, {})

        prep_failed = isinstance(prep, Exception)
        sub_failed = not prep_failed and isinstance(sub, Exception)

        scale_label = "medium"  # ROWS=50k → medium
        prefix = f"spindle_{d}_{scale_label}_"
        sizes = get_table_sizes(storage_token, prefix) if not prep_failed and not sub_failed else {}

        state = poll.get("state", "PrepFailed" if prep_failed else "SubmitFailed")
        full_results.append({
            "domain": d,
            "prepare_failed": prep_failed,
            "submit_failed": sub_failed,
            "prepare_error": str(prep) if prep_failed else None,
            "submit_error": str(sub) if sub_failed else None,
            "state": state,
            "running_sec": poll.get("running_sec", 0),
            "spark_app_id": poll.get("spark_app_id"),
            "fabric_run_id": sub["fabric_run_id"] if not sub_failed and not prep_failed else None,
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
    print(f"Prepare phase: {prepare_phase_sec}s", flush=True)
    print(f"Submit  phase: {submit_phase_sec}s", flush=True)
    print(f"Total   wall:  {total_sec}s", flush=True)
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
