"""STORY-009 — Safe-by-default + opt-out + redaction manifest (ADR-005 / E2).

Acceptance criteria covered:

AC1  Scrub (winsorize + k-anon + PII gate) runs on ``save`` by default.
AC2  ``unsafe_full_fidelity`` persists raw-fidelity values and stamps
     ``unsafe=true`` in the artifact.
AC3  Embedded ``redaction_manifest``: rare categories dropped (per column),
     bounds winsorized (per column), pattern-only columns, k used, sensitive
     flag.
AC4  Manifest is accurate against what was actually suppressed.

Story-specified tests:
- Default save -> safe artifact with accurate manifest.
- ``--unsafe-full-fidelity`` -> artifact stamped ``unsafe=true``.
"""

from __future__ import annotations

import json
import os
import tempfile

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference import (
    DataProfiler,
    ProfileStore,
    SafeProfile,
    build_redaction_manifest,
)
from sqllocks_spindle.inference.safe_profile import (
    K_DEFAULT,
    K_SENSITIVE,
    OTHER_BUCKET,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _dataset_with_rare_categories(n=4000, seed=11):
    rng = np.random.default_rng(seed)
    common = rng.choice(["alpha", "beta", "gamma"], size=n - 30, p=[0.5, 0.3, 0.2])
    rare = [f"rare_{i}" for i in range(30)]  # each appears exactly once
    values = np.concatenate([common, np.array(rare, dtype=object)])
    rng.shuffle(values)
    return pd.DataFrame({"category": values})


def _numeric_heavy_tail(n=3000, seed=7):
    rng = np.random.default_rng(seed)
    return pd.DataFrame({"amount": rng.lognormal(2.0, 1.0, size=n)})


def _save_load_text(safe: SafeProfile):
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "p.safe.json")
        ProfileStore.save(safe, path)
        with open(path) as fh:
            raw = fh.read()
    return json.loads(raw), raw


# ---------------------------------------------------------------------------
# AC1 — scrub runs on save by default
# ---------------------------------------------------------------------------


def test_default_save_produces_safe_artifact():
    """Default (no opt-out) save: unsafe=false, no sub-k category survives."""
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)  # default -> scrub ON
    data, raw = _save_load_text(safe)

    assert data["unsafe"] is False
    # No rare singleton survived: scrub folded them into __OTHER__.
    assert "rare_" not in raw
    weights = data["tables"]["t"]["columns"]["category"]["categorical_weights"]
    assert OTHER_BUCKET in weights
    assert not any(v.startswith("rare_") for v in weights if v != OTHER_BUCKET)


def test_default_save_winsorizes_numeric_bounds():
    """Default save: numeric column carries winsorized bounds, no raw min/max."""
    df = _numeric_heavy_tail()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)
    data, raw = _save_load_text(safe)

    col = data["tables"]["t"]["columns"]["amount"]
    assert col["bounds"] is not None
    assert "lo" in col["bounds"] and "hi" in col["bounds"]
    # ADR-007 / ADR-002 — no raw extreme field names anywhere.
    assert "min_value" not in raw and "max_value" not in raw


# ---------------------------------------------------------------------------
# AC2 — opt-out stamps unsafe=true and disables disclosure controls
# ---------------------------------------------------------------------------


def test_unsafe_full_fidelity_stamps_unsafe_true():
    """Story test: --unsafe-full-fidelity -> artifact stamped unsafe=true."""
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich, unsafe_full_fidelity=True)

    assert safe.unsafe is True
    data, _ = _save_load_text(safe)
    assert data["unsafe"] is True
    # The stamp survives a load round-trip too.
    assert SafeProfile.from_safe_dict(data).unsafe is True


def test_unsafe_full_fidelity_disables_k_anon_suppression():
    """Opt-out keeps full-fidelity categories: no __OTHER__ folding, no drops."""
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich, unsafe_full_fidelity=True)

    col = safe.tables["t"].columns["category"]
    # k=1 disables suppression -> every category survives, nothing folded.
    assert col.suppressed_category_count == 0
    weights = col.categorical_weights or {}
    assert OTHER_BUCKET not in weights
    # The rare singletons are PRESENT in full-fidelity mode (the unsafe point).
    assert any(v.startswith("rare_") for v in weights)


def test_unsafe_full_fidelity_disables_pii_gate():
    """Opt-out turns off the PII gate so a PII column is not pattern-only."""
    df = pd.DataFrame(
        {"c_47": [f"{i:03d}-{i:02d}-{i:04d}" for i in range(1000)]}  # SSN-like
    )
    rich = DataProfiler().profile_dataset({"t": df})

    safe_default = SafeProfile.from_dataset_profile(rich)
    safe_unsafe = SafeProfile.from_dataset_profile(rich, unsafe_full_fidelity=True)

    man_default = safe_default.redaction_manifest["tables"]["t"]["c_47"]
    man_unsafe = safe_unsafe.redaction_manifest["tables"]["t"]["c_47"]
    # Default: gate fires (pattern-only). Unsafe: gate disabled.
    assert man_default["pattern_only"] is True
    assert man_unsafe["pattern_only"] is False


def test_unsafe_config_not_mutated():
    """The opt-out must not mutate the caller's config dict."""
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    cfg = {"k": 5}
    SafeProfile.from_dataset_profile(rich, config=cfg, unsafe_full_fidelity=True)
    assert cfg == {"k": 5}  # untouched


# ---------------------------------------------------------------------------
# AC3 / AC4 — embedded, accurate redaction manifest
# ---------------------------------------------------------------------------


def test_manifest_present_in_artifact_with_expected_shape():
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)
    data, _ = _save_load_text(safe)

    man = data["redaction_manifest"]
    assert man["unsafe"] is False
    assert man["k_default"] == K_DEFAULT
    entry = man["tables"]["t"]["category"]
    assert set(entry) == {
        "categories_dropped",
        "bounds_winsorized",
        "pattern_only",
        "k",
        "sensitive",
    }


def test_manifest_categories_dropped_matches_actual_suppression():
    """AC4: dropped count == the count the k-anon hook actually recorded."""
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)

    safe_col = safe.tables["t"].columns["category"]
    man_entry = safe.redaction_manifest["tables"]["t"]["category"]
    assert man_entry["categories_dropped"] == safe_col.suppressed_category_count == 30
    assert man_entry["bounds_winsorized"] is False  # categorical, no bounds
    assert man_entry["pattern_only"] is False
    assert man_entry["k"] == K_DEFAULT
    assert man_entry["sensitive"] is False


def test_manifest_bounds_winsorized_flag_for_numeric():
    df = _numeric_heavy_tail()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)
    entry = safe.redaction_manifest["tables"]["t"]["amount"]
    assert entry["bounds_winsorized"] is True
    assert entry["categories_dropped"] == 0


def test_manifest_records_sensitive_k():
    """AC3: sensitive flag raises k to 11 and the manifest reports it."""
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich, config={"sensitive": True})
    entry = safe.redaction_manifest["tables"]["t"]["category"]
    assert entry["k"] == K_SENSITIVE
    assert entry["sensitive"] is True
    assert safe.redaction_manifest["k_default"] == K_SENSITIVE


def test_manifest_records_per_column_k_override_not_sensitive():
    """An explicit per-column k is reported and is NOT flagged sensitive."""
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    cfg = {"sensitive": True, "columns": {"category": {"k": 3}}}
    safe = SafeProfile.from_dataset_profile(rich, config=cfg)
    entry = safe.redaction_manifest["tables"]["t"]["category"]
    assert entry["k"] == 3
    # Explicit k beats the profile sensitive flag -> not sensitive-driven.
    assert entry["sensitive"] is False


def test_manifest_pattern_only_for_pii_column():
    """AC3/AC4: a value-detected PII column is reported pattern_only."""
    df = pd.DataFrame(
        {"notes": [f"user{i}@example.com" for i in range(1000)]}  # emails
    )
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)
    entry = safe.redaction_manifest["tables"]["t"]["notes"]
    assert entry["pattern_only"] is True
    # And the column persisted no values (categorical weights dropped).
    safe_col = safe.tables["t"].columns["notes"]
    assert safe_col.categorical_weights is None


def test_manifest_unsafe_mode_reports_no_suppression():
    """AC4: in unsafe mode the manifest accurately reports zero suppression."""
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich, unsafe_full_fidelity=True)
    man = safe.redaction_manifest
    assert man["unsafe"] is True
    entry = man["tables"]["t"]["category"]
    assert entry["categories_dropped"] == 0
    assert entry["pattern_only"] is False


def test_build_redaction_manifest_is_exported_and_callable():
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)
    man = build_redaction_manifest(rich, safe, config=None, unsafe=False)
    assert man["tables"]["t"]["category"]["categories_dropped"] == 30


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
