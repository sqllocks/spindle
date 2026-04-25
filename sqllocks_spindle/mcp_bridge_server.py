"""Persistent MCP bridge — JSON-lines protocol for warm-start.

Instead of spawning a new Python process per MCP call (cold-start ~2.5s),
this runs as a long-lived process that reads JSON-lines from stdin and writes
responses to stdout. The import cost is paid once at startup.

Usage:
    python -m sqllocks_spindle.mcp_bridge_server

Protocol:
    Input:  one JSON object per line on stdin
    Output: one JSON object per line on stdout

Commands:
    ping       — health check, returns {"status": "ok", "data": {"pong": true}}
    shutdown   — graceful exit
    (all mcp_bridge.COMMANDS) — list, describe, generate, dry_run, validate, preview, profile_info
"""

from __future__ import annotations

import json
import sys
import traceback


def main():
    """Run the persistent bridge loop."""
    # Pre-import everything on startup to pay the cost once
    from sqllocks_spindle.mcp_bridge import COMMANDS  # noqa: F401
    from sqllocks_spindle.engine.generator import Spindle

    # Pre-build the strategy registry (warms up NativeStrategy data arrays)
    _warmup = Spindle()
    del _warmup

    # Signal ready
    sys.stdout.write(json.dumps({"status": "ok", "data": {"ready": True}}) + "\n")
    sys.stdout.flush()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            request = json.loads(line)
        except json.JSONDecodeError as e:
            _respond({"status": "error", "error": f"Invalid JSON: {e}"})
            continue

        command = request.get("command", "")

        if command == "shutdown":
            _respond({"status": "ok", "data": {"shutdown": True}})
            break

        if command == "ping":
            _respond({"status": "ok", "data": {"pong": True}})
            continue

        if command not in COMMANDS:
            _respond({"status": "error", "error": f"Unknown command: '{command}'. Available: {', '.join(COMMANDS.keys())}"})
            continue

        try:
            params = request.get("params", {})
            result = COMMANDS[command](params)
            _respond({"status": "ok", "data": result})
        except Exception as e:
            _respond({
                "status": "error",
                "error": f"{type(e).__name__}: {e}",
                "trace": traceback.format_exc(),
            })


def _respond(obj: dict) -> None:
    """Write a JSON response line to stdout."""
    sys.stdout.write(json.dumps(obj, default=str) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
