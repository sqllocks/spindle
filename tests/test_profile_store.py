"""Tests for ProfileStore.save/load (STORY-003).

Covers the acceptance criteria:
- ``ProfileStore.save`` writes JSON; ``ProfileStore.load`` returns a SafeProfile.
- save -> load identity on a SafeProfile.
- load of a ``schema_version=0`` (legacy/unknown) fixture warns and returns a
  degraded-but-usable object (no crash).
- ProfileStore is exported as the public on-disk entrypoint.
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import pytest

from sqllocks_spindle.inference import ProfileStore as ProfileStoreFromPkg
from sqllocks_spindle.inference.profile_store import ProfileStore
from sqllocks_spindle.inference.safe_profile import (
    SCHEMA_VERSION,
    SafeColumnProfile,
    SafeProfile,
    SafeTableProfile,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _sample_profile() -> SafeProfile:
    """A populated SafeProfile across numeric / categorical / temporal cols."""
    balance = SafeColumnProfile(
        name="balance",
        dtype="float",
        null_rate=0.01,
        cardinality=2950,
        mean=1234.5,
        std=678.9,
        quantiles={"p1": 10.0, "p50": 1000.0, "p99": 5000.0},
        distribution="lognormal",
        distribution_params={"mu": 7.0, "sigma": 0.5},
        bounds={"lo": 10.0, "hi": 5000.0},
    )
    tier = SafeColumnProfile(
        name="tier",
        dtype="string",
        null_rate=0.0,
        cardinality=3,
        categorical_weights={"bronze": 0.6, "silver": 0.3, "gold": 0.1},
    )
    signup = SafeColumnProfile(
        name="signup_at",
        dtype="datetime",
        null_rate=0.0,
        cardinality=3000,
        hour_histogram=[1.0 / 24] * 24,
        dow_histogram=[1.0 / 7] * 7,
    )
    customers = SafeTableProfile(
        name="customers",
        row_count=3000,
        columns={"balance": balance, "tier": tier, "signup_at": signup},
        primary_key=["customer_id"],
        detected_fks={},
        correlation_matrix={"balance": {"balance": 1.0}},
    )
    return SafeProfile(
        tables={"customers": customers},
        relationships=[{"from": "orders.customer_id", "to": "customers.customer_id"}],
        schema_version=SCHEMA_VERSION,
        redaction_manifest={},
    )


# ---------------------------------------------------------------------------
# AC1 — save writes JSON, load returns a SafeProfile
# ---------------------------------------------------------------------------


def test_save_writes_json_file(tmp_path: Path) -> None:
    path = tmp_path / "profile.json"
    out = ProfileStore.save(_sample_profile(), path)
    assert out == path
    assert path.exists()
    # The file is valid JSON and round-trips through json.load.
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["schema_version"] == SCHEMA_VERSION
    assert "customers" in data["tables"]


def test_save_creates_parent_dirs(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "deeper" / "profile.json"
    ProfileStore.save(_sample_profile(), path)
    assert path.exists()


def test_load_returns_safeprofile(tmp_path: Path) -> None:
    path = tmp_path / "profile.json"
    ProfileStore.save(_sample_profile(), path)
    loaded = ProfileStore.load(path)
    assert isinstance(loaded, SafeProfile)


# ---------------------------------------------------------------------------
# Tests — save -> load identity
# ---------------------------------------------------------------------------


def test_save_load_identity(tmp_path: Path) -> None:
    """A SafeProfile round-trips through save -> load byte-stably."""
    original = _sample_profile()
    path = tmp_path / "profile.json"
    ProfileStore.save(original, path)
    loaded = ProfileStore.load(path)
    # Dict form is the canonical equality check (byte-stable per STORY-001).
    assert loaded.to_safe_dict() == original.to_safe_dict()


def test_save_load_string_path(tmp_path: Path) -> None:
    """save/load accept a str path as well as a Path."""
    original = _sample_profile()
    path = str(tmp_path / "profile.json")
    ProfileStore.save(original, path)
    loaded = ProfileStore.load(path)
    assert loaded.to_safe_dict() == original.to_safe_dict()


# ---------------------------------------------------------------------------
# AC3 — legacy / unknown schema_version handled read-only with a warning
# ---------------------------------------------------------------------------


def test_load_legacy_version_zero_warns_and_loads(tmp_path: Path) -> None:
    """A schema_version=0 artifact warns and returns a usable object."""
    legacy = {
        "schema_version": 0,
        "tables": {
            "customers": {
                "name": "customers",
                "row_count": 100,
                "columns": {
                    "tier": {
                        "name": "tier",
                        "dtype": "string",
                        "null_rate": 0.0,
                        "cardinality": 3,
                        "categorical_weights": {"a": 0.5, "b": 0.5},
                    }
                },
                "primary_key": [],
                "detected_fks": {},
                "correlation_matrix": None,
            }
        },
        "relationships": [],
        "redaction_manifest": {},
    }
    path = tmp_path / "legacy.json"
    path.write_text(json.dumps(legacy), encoding="utf-8")

    with pytest.warns(UserWarning):
        loaded = ProfileStore.load(path)

    # Degraded-but-usable: the version is preserved (0), not silently bumped,
    # and the data that was present loaded.
    assert loaded.schema_version == 0
    assert "customers" in loaded.tables
    assert loaded.tables["customers"].columns["tier"].categorical_weights == {
        "a": 0.5,
        "b": 0.5,
    }


def test_load_missing_version_treated_as_legacy_zero(tmp_path: Path) -> None:
    """An artifact with NO schema_version key loads read-only as version 0."""
    legacy = {"tables": {}, "relationships": []}
    path = tmp_path / "noversion.json"
    path.write_text(json.dumps(legacy), encoding="utf-8")

    with pytest.warns(UserWarning):
        loaded = ProfileStore.load(path)
    assert loaded.schema_version == 0


def test_load_future_version_warns(tmp_path: Path) -> None:
    """A future (unknown) schema_version also warns, does not crash."""
    future = {
        "schema_version": SCHEMA_VERSION + 99,
        "tables": {},
        "relationships": [],
        "redaction_manifest": {},
    }
    path = tmp_path / "future.json"
    path.write_text(json.dumps(future), encoding="utf-8")

    with pytest.warns(UserWarning):
        loaded = ProfileStore.load(path)
    assert loaded.schema_version == SCHEMA_VERSION + 99


def test_load_current_version_does_not_warn(tmp_path: Path) -> None:
    """A current-version artifact loads silently (no warning)."""
    path = tmp_path / "profile.json"
    ProfileStore.save(_sample_profile(), path)
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # any warning becomes an error
        loaded = ProfileStore.load(path)
    assert loaded.schema_version == SCHEMA_VERSION


# ---------------------------------------------------------------------------
# AC2 — ProfileStore is the public on-disk entrypoint (exported)
# ---------------------------------------------------------------------------


def test_profile_store_exported_from_package() -> None:
    assert ProfileStoreFromPkg is ProfileStore
