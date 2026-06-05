"""STORY-008 — Value-pattern PII gate at serialize (ADR-004 / E2).

Acceptance criteria covered:

AC1  Columns whose values match profiler PII patterns
     (email/ssn/cc/phone/ip/iban/postal) OR whose cardinality ≈ row_count
     persist ``pattern`` + ``length_dist`` ONLY — no values, no categorical
     weights.
AC2  Detection is NAME-INDEPENDENT (catches PII in ``notes`` / ``c_47``).
AC3  Defense-in-depth — documented as NOT a completeness guarantee
     (ADR-004 / ADR-011). Asserted against the source docstrings + the
     PII_PATTERNS / PII_CARDINALITY_RATIO knobs.

Story-specified tests:
- A fixture with SSNs in a column named ``c_47`` produces an artifact with no
  SSN values. (``validate --safe`` is STORY-010 and not yet implemented; the
  substance of "passes on it" is verified here directly: the serialized artifact
  carries no SSN value and no categorical weights for the gated column.)
- A high-cardinality free-text column persists pattern/length only.
"""

from __future__ import annotations

import ast
import inspect
import json
import os
import tempfile
import textwrap

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference import DataProfiler, ProfileStore, SafeProfile
from sqllocks_spindle.inference.safe_profile import (
    PII_CARDINALITY_RATIO,
    PII_PATTERNS,
    SafeColumnProfile,
)


# ---------------------------------------------------------------------------
# Unit-level: the gate decision hook
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("pat", sorted(PII_PATTERNS))
def test_gate_fires_on_every_pii_pattern(pat):
    # cardinality well below the backstop ratio -> only the pattern can fire it.
    assert SafeColumnProfile._pii_gate_hook(
        pattern=pat, cardinality=3, row_count=1000, config=None
    )


def test_gate_does_not_fire_on_safe_low_card_pattern():
    # A non-PII detected pattern (e.g. currency_code) on a low-card column.
    assert not SafeColumnProfile._pii_gate_hook(
        pattern="currency_code", cardinality=3, row_count=1000, config=None
    )
    # No pattern, low cardinality -> safe.
    assert not SafeColumnProfile._pii_gate_hook(
        pattern=None, cardinality=3, row_count=1000, config=None
    )


def test_gate_fires_on_cardinality_backstop():
    # ~unique free-text: cardinality / row_count >= 0.95 -> gate, no pattern.
    assert SafeColumnProfile._pii_gate_hook(
        pattern=None, cardinality=980, row_count=1000, config=None
    )
    # Just below the ratio -> does not fire on the backstop alone.
    assert not SafeColumnProfile._pii_gate_hook(
        pattern=None, cardinality=940, row_count=1000, config=None
    )


def test_gate_cardinality_ratio_configurable():
    # Lower the ratio so a 0.8-cardinality column is gated.
    assert SafeColumnProfile._pii_gate_hook(
        pattern=None,
        cardinality=800,
        row_count=1000,
        config={"pii_cardinality_ratio": 0.75},
    )


def test_gate_can_be_disabled_via_config():
    # --unsafe-full-fidelity (STORY-009) disables the gate.
    assert not SafeColumnProfile._pii_gate_hook(
        pattern="ssn", cardinality=1000, row_count=1000, config={"pii_gate": False}
    )


def test_gate_no_row_count_only_pattern_can_fire():
    # Without a row count the backstop cannot be evaluated; pattern still fires.
    assert SafeColumnProfile._pii_gate_hook(
        pattern="email", cardinality=10, row_count=None, config=None
    )
    assert not SafeColumnProfile._pii_gate_hook(
        pattern=None, cardinality=10, row_count=None, config=None
    )


def test_length_dist_derived_from_summary_and_is_a_copy():
    summary = {"min": 11.0, "mean": 11.0, "max": 11.0, "p95": 11.0}
    out = SafeColumnProfile._length_dist_from_summary(summary)
    assert out == summary
    assert out is not summary  # never alias the rich profile's dict
    assert SafeColumnProfile._length_dist_from_summary(None) is None


# ---------------------------------------------------------------------------
# AC1 + AC2 — story test: SSNs in a column named ``c_47`` -> no SSN values
# ---------------------------------------------------------------------------


def _ssn(i: int) -> str:
    return f"{(i % 900) + 100:03d}-{(i % 90) + 10:02d}-{(i % 9000) + 1000:04d}"


def _dataset_with_ssn_in_oddly_named_column(n: int = 2000) -> pd.DataFrame:
    # Column NAME gives no hint it is PII -> tests name-independence (AC2).
    return pd.DataFrame(
        {
            "id": range(1, n + 1),
            "c_47": [_ssn(i) for i in range(n)],  # SSNs, innocuous name
        }
    )


def test_ssn_in_c_47_is_detected_as_ssn():
    df = _dataset_with_ssn_in_oddly_named_column()
    rich = DataProfiler().profile_dataset({"t": df})
    # Value-based detection labels it ssn despite the column name.
    assert rich.tables["t"].columns["c_47"].pattern == "ssn"


def test_artifact_has_no_ssn_values_and_no_weights():
    """Story test: SSNs in ``c_47`` produce an artifact with no SSN values."""
    df = _dataset_with_ssn_in_oddly_named_column()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)

    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "p.safe.json")
        ProfileStore.save(safe, path)
        with open(path) as fh:
            raw_text = fh.read()
        data = json.loads(raw_text)

    col = data["tables"]["t"]["columns"]["c_47"]
    # Gated: pattern + length_dist only; no values, no categorical weights.
    assert col["pattern"] == "ssn"
    assert col["length_dist"] is not None
    assert col["categorical_weights"] is None
    assert col["suppressed_category_count"] is None

    # No literal SSN value anywhere in the serialized artifact.
    import re

    assert not re.search(r"\d{3}-\d{2}-\d{4}", raw_text), (
        "an SSN value leaked into the serialized artifact"
    )


def test_gate_drops_weights_for_pii_pattern_even_when_enum():
    """A low-card PII column that would otherwise seed categorical weights is
    still reduced to pattern+length (no weights).
    """
    # Few distinct SSNs -> profiler would treat it as an enum and seed weights;
    # the PII gate must still strip those weights.
    rng = np.random.default_rng(1)
    pool = [_ssn(i) for i in range(8)]
    df = pd.DataFrame({"x_99": rng.choice(pool, size=3000)})
    rich = DataProfiler().profile_dataset({"t": df})
    col_rich = rich.tables["t"].columns["x_99"]
    assert col_rich.pattern == "ssn"
    assert col_rich.is_enum  # would have seeded categorical_weights pre-gate

    safe = SafeProfile.from_dataset_profile(rich)
    col = safe.tables["t"].columns["x_99"]
    assert col.categorical_weights is None
    assert col.pattern == "ssn"
    assert col.length_dist is not None


# ---------------------------------------------------------------------------
# AC1 — story test: a high-cardinality free-text column -> pattern/length only
# ---------------------------------------------------------------------------


def test_high_cardinality_free_text_persists_length_only():
    """Story test: a high-cardinality free-text column persists pattern/length
    only (no values), via the cardinality≈row_count backstop.
    """
    rng = np.random.default_rng(5)
    # Unique-ish free-text 'notes' the regexes do NOT recognise as a PII class.
    notes = [
        f"note {rng.integers(0, 10**9)}-{rng.integers(0, 10**9)}" for _ in range(2000)
    ]
    df = pd.DataFrame({"notes": notes})
    rich = DataProfiler().profile_dataset({"t": df})
    col_rich = rich.tables["t"].columns["notes"]
    # No PII regex matches -> only the cardinality backstop can gate it.
    assert col_rich.pattern not in PII_PATTERNS
    assert col_rich.cardinality / rich.tables["t"].row_count >= PII_CARDINALITY_RATIO

    safe = SafeProfile.from_dataset_profile(rich)
    col = safe.tables["t"].columns["notes"]
    assert col.length_dist is not None
    assert col.categorical_weights is None

    # Serialize and confirm no raw note text survives.
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "p.safe.json")
        ProfileStore.save(safe, path)
        with open(path) as fh:
            raw_text = fh.read()
    assert "note " not in raw_text


def test_non_pii_low_card_column_keeps_weights():
    """Regression: an ordinary low-card categorical is NOT gated."""
    df = pd.DataFrame({"tier": np.repeat(["bronze", "silver", "gold"], 1000)})
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)
    col = safe.tables["t"].columns["tier"]
    assert col.categorical_weights is not None
    assert set(col.categorical_weights) == {"bronze", "silver", "gold"}


# ---------------------------------------------------------------------------
# AC3 — defense-in-depth, NOT a completeness guarantee (documented)
# ---------------------------------------------------------------------------


def test_gate_documented_as_defense_in_depth():
    """AC3: the gate is documented as defense-in-depth, not a guarantee."""
    src = inspect.getsource(SafeColumnProfile._pii_gate_hook).lower()
    assert "defense-in-depth" in src
    assert "not a completeness guarantee" in src
    assert "adr-004" in src and "adr-011" in src


def test_name_independence_documented():
    """AC2: name-independence is an explicit, documented property."""
    src = inspect.getsource(SafeColumnProfile._pii_gate_hook).lower()
    assert "name-independent" in src


# ---------------------------------------------------------------------------
# Source guard — the gate hook reads ONLY real attributes (B2 regression class)
# ---------------------------------------------------------------------------


def test_gate_reads_only_real_profile_attributes():
    """The gate path added to ``from_column_profile`` must read only fields that
    exist on the rich ``ColumnProfile`` (guards the B2 attribute-mismatch bug).
    """
    from sqllocks_spindle.inference.profiler import ColumnProfile

    real = {f.name for f in ColumnProfile.__dataclass_fields__.values()}
    src = "\n".join(
        textwrap.dedent(inspect.getsource(fn))
        for fn in (
            SafeColumnProfile.from_column_profile,
            SafeColumnProfile._pii_gate_hook,
            SafeColumnProfile._length_dist_from_summary,
        )
    )
    tree = ast.parse(src)
    getattr_targets = {
        node.args[1].value
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
        )
    }
    # Reads off the rich profile must be real fields. (cardinality is the only
    # new rich-profile read the gate introduces in from_column_profile.)
    assert "cardinality" in getattr_targets
    rich_reads = getattr_targets & {
        "cardinality",
        "cardinality_ratio",
        "pattern",
        "string_length",
    }
    for name in rich_reads:
        assert name in real, f"gate reads non-existent rich attr '{name}'"


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
