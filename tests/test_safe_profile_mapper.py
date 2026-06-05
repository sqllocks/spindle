"""Tests for the Rich -> Safe mapper (STORY-002).

Covers the acceptance criteria:
- ``SafeProfile.from_dataset_profile`` builds one ``SafeTableProfile`` per
  table and one ``SafeColumnProfile`` per column.
- Numerics carry mean/std/quantiles/distribution/distribution_params; bounds
  are stubbed from quantiles p1/p99 (ADR-002 — never raw min/max); table-level
  correlation_matrix + per-column hour/dow histograms carried.
- Categoricals seed ``categorical_weights`` from ``enum_values`` /
  ``value_counts_ext``.
- Strings carry ``pattern`` + ``string_length``; no raw values.
- The mapper reads ONLY real attribute names — a regression test fails if the
  mapper source accesses a non-existent attribute (the B2 bug: ``.min`` /
  ``.max`` / ``.top_values``).
"""

from __future__ import annotations

import ast
import inspect
import json
import textwrap

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference import safe_profile as safe_profile_module
from sqllocks_spindle.inference.profiler import DataProfiler
from sqllocks_spindle.inference.safe_profile import (
    FORBIDDEN_RAW_FIELDS,
    SCHEMA_VERSION,
    SafeColumnProfile,
    SafeProfile,
    SafeTableProfile,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _known_dataset() -> dict[str, pd.DataFrame]:
    """A two-table dataset with known statistical shape across all dtypes."""
    rng = np.random.default_rng(7)
    n = 3000
    customers = pd.DataFrame(
        {
            "customer_id": range(1, n + 1),
            "balance": rng.lognormal(7.0, 0.5, n),
            "limit": rng.normal(5000, 1500, n),
            "tier": rng.choice(
                ["bronze", "silver", "gold"], n, p=[0.6, 0.3, 0.1]
            ),
            "signup_at": pd.date_range("2021-01-01", periods=n, freq="h"),
            "email": [f"user{i}@example.com" for i in range(n)],
        }
    )
    orders = pd.DataFrame(
        {
            "order_id": range(1, n + 1),
            "customer_id": rng.integers(1, n + 1, n),
            "amount": rng.gamma(2.0, 50.0, n),
        }
    )
    return {"customers": customers, "orders": orders}


@pytest.fixture(scope="module")
def safe_profile() -> SafeProfile:
    dataset_profile = DataProfiler().profile_dataset(_known_dataset())
    return SafeProfile.from_dataset_profile(dataset_profile)


@pytest.fixture(scope="module")
def rich_profile():
    return DataProfiler().profile_dataset(_known_dataset())


# ---------------------------------------------------------------------------
# AC1 — structure: one SafeTableProfile/table, one SafeColumnProfile/column
# ---------------------------------------------------------------------------


def test_from_dataset_profile_returns_safe_profile(safe_profile):
    assert isinstance(safe_profile, SafeProfile)
    assert safe_profile.schema_version == SCHEMA_VERSION


def test_one_table_profile_per_table(safe_profile, rich_profile):
    assert set(safe_profile.tables) == set(rich_profile.tables)
    for tprofile in safe_profile.tables.values():
        assert isinstance(tprofile, SafeTableProfile)


def test_one_column_profile_per_column(safe_profile, rich_profile):
    for tname, rich_table in rich_profile.tables.items():
        safe_table = safe_profile.tables[tname]
        assert set(safe_table.columns) == set(rich_table.columns)
        for col in safe_table.columns.values():
            assert isinstance(col, SafeColumnProfile)


def test_row_count_and_relationships_carried(safe_profile, rich_profile):
    for tname, rich_table in rich_profile.tables.items():
        assert safe_profile.tables[tname].row_count == rich_table.row_count
    # relationships carried through verbatim
    assert safe_profile.relationships == rich_profile.relationships


# ---------------------------------------------------------------------------
# AC2 — numerics: mean/std/quantiles/distribution carried; bounds from p1/p99;
#       correlation_matrix at table level; histograms carried
# ---------------------------------------------------------------------------


def test_numeric_summary_stats_carried(safe_profile):
    balance = safe_profile.tables["customers"].columns["balance"]
    assert balance.mean is not None
    assert balance.std is not None
    assert balance.quantiles is not None
    assert "p1" in balance.quantiles and "p99" in balance.quantiles
    assert balance.distribution is not None


def test_bounds_stub_from_quantiles_p1_p99(safe_profile):
    """ADR-002: bounds derive from quantiles p1/p99 — NOT raw min/max."""
    balance = safe_profile.tables["customers"].columns["balance"]
    assert balance.bounds is not None
    assert balance.bounds["lo"] == balance.quantiles["p1"]
    assert balance.bounds["hi"] == balance.quantiles["p99"]


def test_bounds_configurable_percentiles():
    """The STORY-006 bounds hook honors configured percentile keys."""
    q = {"p1": 1.0, "p5": 5.0, "p95": 95.0, "p99": 99.0}
    default = SafeColumnProfile._winsorized_bounds_hook(q)
    assert default == {"lo": 1.0, "hi": 99.0}
    widened = SafeColumnProfile._winsorized_bounds_hook(
        q, {"bounds_lo_quantile": "p5", "bounds_hi_quantile": "p95"}
    )
    assert widened == {"lo": 5.0, "hi": 95.0}


def test_correlation_matrix_carried_at_table_level(safe_profile):
    # customers has >=2 numeric cols (balance, limit, customer_id) -> matrix present
    matrix = safe_profile.tables["customers"].correlation_matrix
    assert matrix is not None
    assert isinstance(matrix, dict)


def test_datetime_histograms_carried(safe_profile):
    signup = safe_profile.tables["customers"].columns["signup_at"]
    assert signup.hour_histogram is not None
    assert len(signup.hour_histogram) == 24
    assert signup.dow_histogram is not None
    assert len(signup.dow_histogram) == 7


# ---------------------------------------------------------------------------
# AC3 — categoricals: weights seeded from enum_values / value_counts_ext
# ---------------------------------------------------------------------------


def test_categorical_weights_seeded(safe_profile, rich_profile):
    tier_safe = safe_profile.tables["customers"].columns["tier"]
    tier_rich = rich_profile.tables["customers"].columns["tier"]
    assert tier_safe.categorical_weights is not None
    # Stub passthrough (no suppression yet) -> identical keys to enum_values.
    assert set(tier_safe.categorical_weights) == set(tier_rich.enum_values)
    # Weights copied, not the same dict object (no mutation of the rich profile).
    assert tier_safe.categorical_weights is not tier_rich.enum_values


def test_categorical_weights_none_for_non_enum(safe_profile):
    # balance is a continuous numeric -> not enum -> no categorical weights.
    balance = safe_profile.tables["customers"].columns["balance"]
    assert balance.categorical_weights is None


# ---------------------------------------------------------------------------
# AC4 — strings: pattern + length stats only, no raw values
# ---------------------------------------------------------------------------


def test_string_pattern_and_length_carried(safe_profile):
    email = safe_profile.tables["customers"].columns["email"]
    assert email.pattern == "email"
    assert email.string_length is not None
    assert {"min", "mean", "max", "p95"} <= set(email.string_length)


# ---------------------------------------------------------------------------
# Safe-by-construction: zero raw-value fields present anywhere
# ---------------------------------------------------------------------------


def test_no_raw_bearing_keys_in_serialized_output(safe_profile):
    blob = json.dumps(safe_profile.to_safe_dict())
    for forbidden in FORBIDDEN_RAW_FIELDS:
        assert f'"{forbidden}"' not in blob


def test_every_expected_field_populated(safe_profile):
    """Spot-check: each dtype class yields its expected populated fields."""
    cols = safe_profile.tables["customers"].columns
    # numeric
    assert cols["balance"].mean is not None and cols["balance"].bounds is not None
    # categorical
    assert cols["tier"].categorical_weights is not None
    # datetime
    assert cols["signup_at"].hour_histogram is not None
    # string/pattern
    assert cols["email"].pattern is not None


# ---------------------------------------------------------------------------
# AC5 — mapper reads ONLY real attribute names (regression guard for B2 bug)
# ---------------------------------------------------------------------------

# Attribute / subscript names that the OLD broken code read but that do NOT
# exist on the rich ColumnProfile. Reading any of these is the B2 bug.
_OLD_BUG_NAMES = frozenset({"min", "max", "top_values", "min_value", "max_value"})

# Real attribute names the mapper is allowed to read on the rich profile.
_REAL_COLUMN_PROFILE_ATTRS = frozenset(
    {
        "name",
        "dtype",
        "null_rate",
        "cardinality",
        "mean",
        "std",
        "quantiles",
        "distribution",
        "distribution_params",
        "is_enum",
        "enum_values",
        "value_counts_ext",
        "pattern",
        "string_length",
        "hour_histogram",
        "temporal_histogram",
        "dow_histogram",
    }
)


def _mapper_source() -> str:
    """Source of every mapping method on the safe model."""
    return "\n".join(
        textwrap.dedent(inspect.getsource(fn))
        for fn in (
            SafeColumnProfile.from_column_profile,
            SafeColumnProfile._winsorized_bounds_hook,
            SafeColumnProfile._suppress_categories_hook,
            SafeTableProfile.from_table_profile,
            SafeProfile.from_dataset_profile,
        )
    )


def _string_literals_read_via_getattr(tree: ast.AST) -> set[str]:
    """Names passed as the 2nd arg of getattr(obj, '<name>', ...)."""
    found: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
        ):
            found.add(node.args[1].value)
    return found


def _attribute_accesses(tree: ast.AST) -> set[str]:
    """Attribute names accessed via the . operator (obj.attr)."""
    return {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }


def test_mapper_does_not_read_old_bug_attributes():
    """Negative test: the mapper never reads .min/.max/.top_values/min_value.

    Parses the mapper SOURCE with ast (so docstrings/comments are ignored) and
    asserts no getattr-string and no attribute-access matches an old-bug name.
    """
    tree = ast.parse(_mapper_source())
    via_getattr = _string_literals_read_via_getattr(tree)
    via_dot = _attribute_accesses(tree)

    leaked_getattr = via_getattr & _OLD_BUG_NAMES
    assert not leaked_getattr, (
        f"mapper reads non-existent attr via getattr: {sorted(leaked_getattr)}"
    )
    leaked_dot = via_dot & _OLD_BUG_NAMES
    assert not leaked_dot, (
        f"mapper reads non-existent attr via dot access: {sorted(leaked_dot)}"
    )


def test_mapper_getattr_targets_are_real_attributes():
    """Every attribute the mapper pulls off the rich profile actually exists.

    A typo'd accessor (the B2 class) would name an attribute not on
    ColumnProfile/TableProfile/DatasetProfile -> caught here.
    """
    from sqllocks_spindle.inference.profiler import (
        ColumnProfile,
        DatasetProfile,
        TableProfile,
    )

    real_attrs = (
        {f.name for f in ColumnProfile.__dataclass_fields__.values()}
        | {f.name for f in TableProfile.__dataclass_fields__.values()}
        | {f.name for f in DatasetProfile.__dataclass_fields__.values()}
    )

    tree = ast.parse(_mapper_source())
    # getattr string targets that look like rich-profile reads must be real.
    # (config-dict keys go through dict.get, not getattr, so they're excluded.)
    for name in _string_literals_read_via_getattr(tree):
        assert name in real_attrs, (
            f"mapper getattr reads '{name}' which is not a field on the rich "
            f"profile dataclasses (B2 attribute-mismatch regression)"
        )


def test_mapper_only_uses_whitelisted_column_attrs():
    """Defense-in-depth: getattr reads stay within the known-real attr set."""
    tree = ast.parse(_mapper_source())
    reads = _string_literals_read_via_getattr(tree)
    # row_count/primary_key/detected_fks/correlation_matrix/relationships are
    # table/dataset-level reals; allow them alongside the column set.
    table_level = {
        "row_count",
        "primary_key",
        "detected_fks",
        "correlation_matrix",
        "relationships",
    }
    unexpected = reads - _REAL_COLUMN_PROFILE_ATTRS - table_level
    assert not unexpected, f"mapper reads unexpected attrs: {sorted(unexpected)}"


def test_module_imports_without_runtime_profiler_dependency():
    """The mapper module must not import profiler at runtime (TYPE_CHECKING)."""
    src = inspect.getsource(safe_profile_module)
    tree = ast.parse(src)
    # Find any top-level (module-scope) import of profiler.
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            mod = getattr(node, "module", "") or ""
            names = [a.name for a in getattr(node, "names", [])]
            assert "profiler" not in mod and "profiler" not in names, (
                "profiler must be imported only under TYPE_CHECKING"
            )
