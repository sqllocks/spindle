"""Tests for the SafeProfile transport model (STORY-001).

Covers:
- Construct a SafeProfile, round-trip via dict, assert equality.
- to_safe_dict -> from_safe_dict round-trips byte-stably.
- Introspection: no safe dataclass declares a raw-bearing field name.
- schema_version defaults to 1; redaction_manifest field present.
- The forbidden raw fields are absent from every safe dataclass.
"""

from __future__ import annotations

import dataclasses
import json

import pytest

from sqllocks_spindle.inference.safe_profile import (
    FORBIDDEN_RAW_FIELDS,
    SCHEMA_VERSION,
    SafeColumnProfile,
    SafeProfile,
    SafeTableProfile,
)


def _sample_profile() -> SafeProfile:
    col_num = SafeColumnProfile(
        name="balance",
        dtype="float",
        null_rate=0.01,
        cardinality=4521,
        mean=1234.56,
        std=789.0,
        quantiles={"p1": 1.0, "p50": 1200.0, "p99": 9000.0},
        distribution="lognormal",
        distribution_params={"s": 0.5, "loc": 0.0, "scale": 1200.0},
        bounds={"lo": 1.0, "hi": 9000.0},
        pattern=None,
        length_dist=None,
        string_length=None,
        hour_histogram=None,
        dow_histogram=None,
    )
    col_cat = SafeColumnProfile(
        name="status",
        dtype="string",
        null_rate=0.0,
        cardinality=3,
        categorical_weights={"active": 0.7, "inactive": 0.25, "__OTHER__": 0.05},
        pattern=None,
    )
    col_dt = SafeColumnProfile(
        name="created_at",
        dtype="datetime",
        null_rate=0.0,
        cardinality=10000,
        hour_histogram=[round(1.0 / 24, 6)] * 24,
        dow_histogram=[round(1.0 / 7, 6)] * 7,
    )
    table = SafeTableProfile(
        name="accounts",
        row_count=10000,
        columns={"balance": col_num, "status": col_cat, "created_at": col_dt},
        primary_key=["account_id"],
        detected_fks={"customer_id": "customers"},
        correlation_matrix={"balance": {"limit": 0.82}},
    )
    return SafeProfile(
        tables={"accounts": table},
        relationships=[
            {
                "name": "fk_accounts_customer_id",
                "parent": "customers",
                "child": "accounts",
                "type": "one_to_many",
            }
        ],
    )


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


def test_roundtrip_equality():
    """Construct a SafeProfile, round-trip via dict, assert equality."""
    profile = _sample_profile()
    restored = SafeProfile.from_safe_dict(profile.to_safe_dict())
    assert restored == profile


def test_roundtrip_byte_stable():
    """to_safe_dict -> from_safe_dict round-trips byte-stably (stable JSON)."""
    profile = _sample_profile()
    first = json.dumps(profile.to_safe_dict(), sort_keys=False)
    restored = SafeProfile.from_safe_dict(json.loads(first))
    second = json.dumps(restored.to_safe_dict(), sort_keys=False)
    # Byte-stable: serialize -> load -> serialize yields identical bytes.
    assert first == second
    # And idempotent across a second round-trip.
    third = json.dumps(
        SafeProfile.from_safe_dict(json.loads(second)).to_safe_dict(),
        sort_keys=False,
    )
    assert second == third


def test_roundtrip_empty_profile():
    """An empty SafeProfile round-trips and keeps schema_version + manifest."""
    profile = SafeProfile()
    restored = SafeProfile.from_safe_dict(profile.to_safe_dict())
    assert restored == profile
    assert restored.schema_version == SCHEMA_VERSION
    assert restored.redaction_manifest == {}


def test_column_roundtrip_equality():
    col = SafeColumnProfile(
        name="x", dtype="integer", null_rate=0.0, cardinality=5, mean=2.0
    )
    assert SafeColumnProfile.from_safe_dict(col.to_safe_dict()) == col


def test_table_roundtrip_equality():
    table = _sample_profile().tables["accounts"]
    assert SafeTableProfile.from_safe_dict(table.to_safe_dict()) == table


# ---------------------------------------------------------------------------
# schema_version + redaction_manifest fields present
# ---------------------------------------------------------------------------


def test_schema_version_default_is_1():
    assert SafeProfile().schema_version == 1
    assert SCHEMA_VERSION == 1


def test_redaction_manifest_field_present():
    profile = SafeProfile()
    assert hasattr(profile, "redaction_manifest")
    assert profile.redaction_manifest == {}
    assert "redaction_manifest" in profile.to_safe_dict()


def test_schema_version_survives_roundtrip_when_legacy():
    """A legacy schema_version=0 artifact loads with that version preserved."""
    data = SafeProfile().to_safe_dict()
    data["schema_version"] = 0
    restored = SafeProfile.from_safe_dict(data)
    assert restored.schema_version == 0


# ---------------------------------------------------------------------------
# No raw-bearing fields (ADR-007 introspection)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "klass", [SafeColumnProfile, SafeTableProfile, SafeProfile]
)
def test_no_raw_bearing_field_declared(klass):
    """No safe dataclass declares min_value/max_value/enum_values/value_counts_ext."""
    declared = {f.name for f in dataclasses.fields(klass)}
    leaked = declared & FORBIDDEN_RAW_FIELDS
    assert not leaked, f"{klass.__name__} leaks raw-bearing field(s): {sorted(leaked)}"


def test_forbidden_set_matches_adr007():
    assert FORBIDDEN_RAW_FIELDS == frozenset(
        {"min_value", "max_value", "enum_values", "value_counts_ext"}
    )


@pytest.mark.parametrize(
    "klass", [SafeColumnProfile, SafeTableProfile, SafeProfile]
)
def test_serialized_dict_has_no_raw_keys(klass):
    """No serialized key matches a forbidden raw-bearing field name, recursively."""
    profile = _sample_profile()
    blob = json.dumps(profile.to_safe_dict())
    for forbidden in FORBIDDEN_RAW_FIELDS:
        # match as a JSON key, e.g. "min_value":
        assert f'"{forbidden}"' not in blob


# ---------------------------------------------------------------------------
# SafeColumnProfile carries ONLY the allowed safe statistic set
# ---------------------------------------------------------------------------


def test_safe_column_field_set_exact():
    """SafeColumnProfile declares exactly the AC-specified safe fields (+ name)."""
    declared = {f.name for f in dataclasses.fields(SafeColumnProfile)}
    expected = {
        "name",
        "dtype",
        "null_rate",
        "cardinality",
        "mean",
        "std",
        "quantiles",
        "distribution",
        "distribution_params",
        "bounds",
        "categorical_weights",
        "pattern",
        "length_dist",
        "string_length",
        "hour_histogram",
        "dow_histogram",
    }
    assert declared == expected
