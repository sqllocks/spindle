"""STORY-010 — `profile validate --safe` structural static scanner (ADR-006 / E3).

Acceptance criteria covered:

AC1  Structural scanner walks every node recursively (not just top-level
     ``tables``); list-shaped or legacy-shaped (``RegistryProfile``, no
     ``tables``) artifacts are fully scanned.
AC2  Deny-by-shape rules: list of >k raw strings under ANY key; numeric
     extreme-pair under ANY parent EXCEPT the closed safe set
     {string_length, length_dist} (Option A); any PII-regex match anywhere;
     unsafe=true stamp. Deny rules never depend on names.
AC3  Fail-closed: missing/unknown row_count or undeterminable node → FLAG.
AC4  ``spindle profile validate --safe <path>`` exits 0 only on a proven-clean
     artifact, non-zero on any leak; ``--json`` machine-readable; artifact-only.
AC5  The 5 documented bypasses are regression tests that MUST exit non-zero.
"""

from __future__ import annotations

import json
import subprocess
import sys

import numpy as np
import pandas as pd
import pytest
from click.testing import CliRunner

from sqllocks_spindle.cli import main
from sqllocks_spindle.inference import (
    DataProfiler,
    ProfileStore,
    SafeProfile,
    SafeProfileValidator,
)
from sqllocks_spindle.inference.safe_validator import (
    SAFE_MINMAX_CONTAINERS,
    SAFE_SCHEMA_MARKERS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate(data) -> "object":
    return SafeProfileValidator().validate_data(data)


def _safe_marker_envelope(tables: dict) -> dict:
    """A SafeProfile-shaped envelope (has schema markers) wrapping ``tables``."""
    return {
        "schema_version": 3,
        "unsafe": False,
        "tables": tables,
        "relationships": [],
        "redaction_manifest": {},
    }


@pytest.fixture
def real_safe_artifact(tmp_path):
    """A real, scrubbed SafeProfile artifact written via ProfileStore.

    Includes a PII column (so ``length_dist`` is populated) and a categorical
    (so ``string_length`` is populated) — the two safe length containers that
    legitimately carry bare min/max and MUST NOT be flagged.
    """
    rng = np.random.default_rng(7)
    n = 500
    df = pd.DataFrame(
        {
            "amount": rng.normal(100, 15, n).round(2),
            "category": rng.choice(["A", "B", "C"], size=n, p=[0.5, 0.3, 0.2]),
            "ssn_col": [
                f"{rng.integers(100, 999)}-{rng.integers(10, 99)}-"
                f"{rng.integers(1000, 9999)}"
                for _ in range(n)
            ],
        }
    )
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)
    path = tmp_path / "safe.json"
    ProfileStore.save(safe, path)
    return path


# ---------------------------------------------------------------------------
# AC4 — a real safe artifact passes clean (no false positives)
# ---------------------------------------------------------------------------


def test_real_safe_artifact_is_clean(real_safe_artifact):
    result = SafeProfileValidator().validate_file(real_safe_artifact)
    assert result.is_clean, [f.to_dict() for f in result.findings]
    assert result.exit_code == 0


def test_safe_length_containers_minmax_exempt():
    """``min``/``max`` under string_length / length_dist (Option A) is NOT a leak."""
    for container in SAFE_MINMAX_CONTAINERS:
        env = _safe_marker_envelope(
            {
                "t": {
                    "row_count": 100,
                    "columns": {"c": {container: {"min": 3, "max": 12}}},
                }
            }
        )
        result = _validate(env)
        assert result.is_clean, (container, [f.to_dict() for f in result.findings])


# ---------------------------------------------------------------------------
# AC5 — the 5 documented bypass-corpus regression tests (MUST exit non-zero)
# ---------------------------------------------------------------------------


def test_bypass_1_legacy_no_tables_is_flagged():
    """Legacy RegistryProfile JSON: top-level ``columns``, no ``tables``."""
    legacy = {
        "system": "s",
        "table": "t",
        "name": "n",
        "columns": {
            "age": {"dtype": "int", "min": 18, "max": 97},
            "city": {"top_values": ["NYC", "LA", "SF", "Boston", "Reno"]},
        },
    }
    result = _validate(legacy)
    assert result.exit_code != 0
    rules = {f.rule for f in result.findings}
    # Walked despite no ``tables`` key: caught the bare min/max AND the raw list.
    assert "extreme-pair" in rules
    assert "raw-string-list" in rules
    # And flagged as not-a-SafeProfile (fail-closed on missing markers).
    assert "not-safe-profile" in rules


def test_bypass_2_bounds_minmax_is_flagged():
    """``bounds.min``/``bounds.max`` — min/max under a non-safe parent."""
    env = _safe_marker_envelope(
        {
            "t": {
                "row_count": 100,
                "columns": {"x": {"bounds": {"min": 1.0, "max": 9.0}}},
            }
        }
    )
    result = _validate(env)
    assert result.exit_code != 0
    assert any(f.rule == "extreme-pair" for f in result.findings)


def test_bypass_3_top_values_and_samples_lists_flagged():
    """Raw string lists under any key (top_values / samples)."""
    for key in ("top_values", "samples", "any_other_key"):
        env = _safe_marker_envelope(
            {
                "t": {
                    "row_count": 100,
                    "columns": {"c": {key: ["a", "b", "c", "d"]}},
                }
            }
        )
        result = _validate(env)
        assert result.exit_code != 0, key
        assert any(f.rule == "raw-string-list" for f in result.findings), key


def test_bypass_4_list_shaped_columns_flagged():
    """Columns serialized as a LIST (not a dict) are still fully walked."""
    env = _safe_marker_envelope(
        {
            "t": {
                "row_count": 100,
                "columns": [
                    {"name": "age", "min": 18, "max": 99},
                    {"name": "city", "top_values": ["a", "b", "c", "d"]},
                ],
            }
        }
    )
    result = _validate(env)
    assert result.exit_code != 0
    rules = {f.rule for f in result.findings}
    assert "extreme-pair" in rules
    assert "raw-string-list" in rules


def test_bypass_5_absent_row_count_flagged():
    """Fail-closed: a table with no row_count is undeterminable → FLAG."""
    env = {
        "schema_version": 3,
        "redaction_manifest": {},
        "tables": {"t": {"columns": {"x": {"mean": 5.0}}}},  # no row_count
    }
    result = _validate(env)
    assert result.exit_code != 0
    assert any(f.rule == "row-count-missing" for f in result.findings)


# ---------------------------------------------------------------------------
# AC2 — additional deny-by-shape coverage
# ---------------------------------------------------------------------------


def test_unsafe_stamp_is_flagged():
    env = {
        "schema_version": 3,
        "redaction_manifest": {},
        "unsafe": True,
        "tables": {"t": {"row_count": 100, "columns": {}}},
    }
    result = _validate(env)
    assert result.exit_code != 0
    assert any(f.rule == "unsafe-stamp" for f in result.findings)


@pytest.mark.parametrize(
    "value",
    [
        "contact me at jdoe@example.com please",  # email
        "ssn is 123-45-6789 on file",  # ssn
        "server at 192.168.10.254 down",  # ip
        "iban GB82WEST12345698765432 ok",  # iban
        "call +1 415-555-2671 now",  # phone
    ],
)
def test_pii_regex_anywhere_is_flagged(value):
    """PII embedded inside ANY string value, under ANY key, is flagged."""
    env = _safe_marker_envelope(
        {
            "t": {
                "row_count": 100,
                "columns": {"c_47": {"note": value}},  # oddly-named key
            }
        }
    )
    result = _validate(env)
    assert result.exit_code != 0
    assert any(f.rule == "pii-regex" for f in result.findings), value


def test_minmax_under_arbitrary_parent_flagged_name_independent():
    """An extreme-pair under a parent named anything (not a safe container)."""
    env = _safe_marker_envelope(
        {
            "t": {
                "row_count": 100,
                "columns": {"c": {"min_value": 1, "max_value": 9}},
            }
        }
    )
    result = _validate(env)
    assert result.exit_code != 0
    assert any(f.rule == "extreme-pair" for f in result.findings)


def test_safe_schema_markers_required():
    """An artifact lacking schema markers is flagged fail-closed."""
    assert SAFE_SCHEMA_MARKERS  # sanity
    result = _validate({"foo": "bar"})
    assert result.exit_code != 0
    assert any(f.rule == "not-safe-profile" for f in result.findings)


def test_malformed_json_file_fails_closed(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{ not json", encoding="utf-8")
    result = SafeProfileValidator().validate_file(bad)
    assert result.exit_code != 0
    assert any(f.rule == "malformed" for f in result.findings)


# ---------------------------------------------------------------------------
# AC4 — CLI integration: exit codes + --json
# ---------------------------------------------------------------------------


def test_cli_clean_artifact_exits_zero(real_safe_artifact):
    runner = CliRunner()
    res = runner.invoke(
        main, ["profile", "validate", "--safe", str(real_safe_artifact)]
    )
    assert res.exit_code == 0, res.output
    assert "CLEAN" in res.output


def test_cli_leak_artifact_exits_nonzero(tmp_path):
    leak = tmp_path / "leak.json"
    leak.write_text(
        json.dumps(
            {
                "system": "s",
                "table": "t",
                "name": "n",
                "columns": {"age": {"min": 18, "max": 97}},
            }
        ),
        encoding="utf-8",
    )
    runner = CliRunner()
    res = runner.invoke(main, ["profile", "validate", "--safe", str(leak)])
    assert res.exit_code != 0


def test_cli_json_output_machine_readable(tmp_path):
    leak = tmp_path / "leak.json"
    leak.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "redaction_manifest": {},
                "tables": {
                    "t": {
                        "row_count": 100,
                        "columns": {"x": {"bounds": {"min": 1, "max": 9}}},
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    runner = CliRunner()
    res = runner.invoke(
        main, ["profile", "validate", "--safe", "--json", str(leak)]
    )
    assert res.exit_code != 0
    payload = json.loads(res.output)
    assert payload["clean"] is False
    assert payload["exit_code"] != 0
    assert any(f["rule"] == "extreme-pair" for f in payload["findings"])


def test_cli_missing_safe_flag_errors(real_safe_artifact):
    runner = CliRunner()
    res = runner.invoke(main, ["profile", "validate", str(real_safe_artifact)])
    assert res.exit_code == 2


def test_cli_as_subprocess_real_exit_code(real_safe_artifact):
    """End-to-end via the installed console script semantics (real process exit)."""
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "sqllocks_spindle.cli",
            "profile",
            "validate",
            "--safe",
            str(real_safe_artifact),
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
