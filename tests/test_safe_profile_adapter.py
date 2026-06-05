"""STORY-005 — Load SafeProfile -> generate(fidelity_profile) bridge.

Tests the adapter that lets a *loaded* SafeProfile drive generation:
  profile -> SafeProfile.from_dataset_profile -> ProfileStore.save/load
  -> safe_profile_to_schema -> Spindle().generate(schema=..., fidelity_profile=loaded)

Fidelity >=90% assertion is STORY-011; here we assert the path RUNS and is
shaped correctly (AC3), and that numeric generation clips to bounds while
categorical generation samples categorical_weights incl. ``__OTHER__`` (AC2).
"""

from __future__ import annotations

import os
import tempfile

import pytest

from sqllocks_spindle import RetailDomain, Spindle
from sqllocks_spindle.inference import (
    DataProfiler,
    ProfileStore,
    SafeColumnProfile,
    SafeProfile,
    SafeProfileAdapter,
    SafeTableProfile,
    safe_profile_to_schema,
)


# ---------------------------------------------------------------------------
# AC1 / AC2 — synthetic SafeProfile: distribution+bounds clip, weighted __OTHER__
# ---------------------------------------------------------------------------


def _synthetic_profile() -> SafeProfile:
    """A loaded-shape SafeProfile with an explicit clip-forcing numeric column
    and a categorical column carrying an ``__OTHER__`` bucket."""
    amount = SafeColumnProfile(
        name="amount",
        dtype="float",
        null_rate=0.0,
        cardinality=900,
        mean=100.0,
        std=50.0,
        distribution="normal",
        distribution_params={"loc": 100.0, "scale": 50.0},
        bounds={"lo": 10.0, "hi": 20.0},  # narrow -> forces clipping
    )
    tier = SafeColumnProfile(
        name="tier",
        dtype="string",
        null_rate=0.0,
        cardinality=3,
        categorical_weights={"gold": 0.5, "silver": 0.3, "__OTHER__": 0.2},
    )
    table = SafeTableProfile(
        name="t",
        row_count=1000,
        columns={"amount": amount, "tier": tier},
        primary_key=[],  # adapter injects a synthetic _row_id PK
    )
    return SafeProfile(tables={"t": table})


def test_adapter_numeric_uses_distribution_and_bounds():
    """AC2: numeric generation uses distribution+params and threads bounds in
    as min/max for clipping."""
    schema = safe_profile_to_schema(_synthetic_profile())
    gen = schema.tables["t"].columns["amount"].generator
    assert gen["strategy"] == "distribution"
    assert gen["distribution"] == "normal"
    # bounds -> min/max so the engine clips
    assert gen["params"]["min"] == 10.0
    assert gen["params"]["max"] == 20.0
    # distribution params carried (translated from scipy loc/scale)
    assert gen["params"]["mean"] == 100.0
    assert gen["params"]["std_dev"] == 50.0


def test_adapter_categorical_uses_weighted_enum_with_other():
    """AC2: categorical generation samples categorical_weights incl __OTHER__."""
    schema = safe_profile_to_schema(_synthetic_profile())
    gen = schema.tables["t"].columns["tier"].generator
    assert gen["strategy"] == "weighted_enum"
    assert "__OTHER__" in gen["values"]
    assert set(gen["values"]) == {"gold", "silver", "__OTHER__"}


def test_generated_numeric_clipped_to_bounds():
    """AC2 (behavior): regenerated numerics lie within the winsorized bounds."""
    schema = safe_profile_to_schema(_synthetic_profile())
    out = Spindle().generate(schema=schema, scale="small", seed=1)
    res = out[0] if isinstance(out, tuple) else out
    amt = res.tables["t"]["amount"].astype(float)
    assert amt.min() >= 10.0 - 1e-9
    assert amt.max() <= 20.0 + 1e-9


def test_generated_categorical_samples_only_known_values():
    """AC2 (behavior): only the persisted categories (incl __OTHER__) appear."""
    schema = safe_profile_to_schema(_synthetic_profile())
    out = Spindle().generate(schema=schema, scale="small", seed=1)
    res = out[0] if isinstance(out, tuple) else out
    values = set(res.tables["t"]["tier"].dropna().unique())
    assert "__OTHER__" in values
    assert values <= {"gold", "silver", "__OTHER__"}


# ---------------------------------------------------------------------------
# AC3 — end-to-end on retail: profile -> save -> load -> generate
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def retail_tables():
    result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
    return result.tables


def test_retail_profile_save_load_generate_end_to_end(retail_tables):
    """AC1 + AC3: a loaded SafeProfile drives generation without a live
    in-memory profile; result is populated with the expected schema, and the
    SAME loaded object serves as fidelity_profile."""
    rich = DataProfiler().profile_dataset(retail_tables)
    safe = SafeProfile.from_dataset_profile(rich)

    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "retail.safe.json")
        ProfileStore.save(safe, path)
        loaded = ProfileStore.load(path)

    # adapter builds a generatable schema from the loaded (not in-memory) object
    schema = SafeProfileAdapter().to_schema(loaded, domain_name="retail")
    assert set(schema.tables) == set(loaded.tables)

    out = Spindle().generate(schema=schema, scale="small", seed=7, fidelity_profile=loaded)
    # fidelity_profile=... -> (result, report) tuple (AC1: loaded SafeProfile
    # satisfies the fidelity_profile structural contract).
    assert isinstance(out, tuple)
    result, report = out
    assert report is not None
    assert report.overall_score >= 0.0  # threshold gated in STORY-011

    # every profiled table is populated with its expected columns
    for tname, tprof in loaded.tables.items():
        assert tname in result.tables
        df = result.tables[tname]
        assert len(df) > 0, f"{tname} generated empty"
        for cname in tprof.columns:
            assert cname in df.columns, f"{tname}.{cname} missing from output"


def test_retail_generate_without_fidelity_profile(retail_tables):
    """AC1: the adapted schema also generates standalone (no fidelity_profile)."""
    rich = DataProfiler().profile_dataset(retail_tables)
    safe = SafeProfile.from_dataset_profile(rich)
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "retail.safe.json")
        ProfileStore.save(safe, path)
        loaded = ProfileStore.load(path)
    schema = safe_profile_to_schema(loaded)
    out = Spindle().generate(schema=schema, scale="small", seed=3)
    result = out[0] if isinstance(out, tuple) else out
    assert result.tables
    assert all(len(df) > 0 for df in result.tables.values())
