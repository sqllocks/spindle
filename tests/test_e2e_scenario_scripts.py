"""E2E tests: run all 22 example scenario scripts, assert they exit cleanly."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

SCENARIOS_DIR = Path(__file__).parent.parent / "examples" / "scenarios"

# Collect all .py files in scenarios/
SCENARIO_FILES = sorted(SCENARIOS_DIR.glob("*.py")) if SCENARIOS_DIR.exists() else []

# Scripts that need live Fabric / external connections — skip locally
SKIP_SCRIPTS = {
    "11_streaming_eventhub_kafka.py",  # Requires Event Hub / Kafka connection
    "22_fabric_integration.py",        # Requires Fabric endpoints
}

# Scripts with known API drift that need broader rewrite
XFAIL_SCRIPTS = {
    "19_scenario_packs.py",  # PackLoader API changed, script references old attrs
}


@pytest.mark.parametrize(
    "script_path",
    [s for s in SCENARIO_FILES if s.name not in SKIP_SCRIPTS],
    ids=[s.stem for s in SCENARIO_FILES if s.name not in SKIP_SCRIPTS],
)
def test_scenario_script_runs(script_path, tmp_path):
    """Execute each scenario script and verify exit code 0."""
    if script_path.name in XFAIL_SCRIPTS:
        pytest.xfail(f"Known issue: {script_path.name}")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=str(tmp_path),
        env={
            **dict(__import__("os").environ),
            "SPINDLE_OUTPUT_DIR": str(tmp_path),
            "PYTHONIOENCODING": "utf-8",
        },
    )
    assert result.returncode == 0, (
        f"Script {script_path.name} failed with exit code {result.returncode}.\n"
        f"STDERR:\n{result.stderr[-500:]}"
    )
