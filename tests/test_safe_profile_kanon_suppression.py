"""STORY-007 — k-anon categorical suppression (__OTHER__) (ADR-003 / E2).

Acceptance criteria covered:

AC1  Categorical weights fold any value with count < k into a single
     ``__OTHER__`` bucket (aggregate weight). Default k=5; k=11 when
     ``sensitive=True``; per-column override honored.
AC2  Suppressed values never persisted — only surviving categories + ``__OTHER__``.
AC3  Per-column suppressed-category count recorded (for the STORY-009 manifest).
AC4  High-frequency category mass preserved within tolerance after suppression.

Story-specified tests:
- A column with rare values yields no sub-k category in the artifact.
- chi^2 on the surviving categories stays within fidelity tolerance.
"""

from __future__ import annotations

import json
import os
import tempfile

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference import DataProfiler, ProfileStore, SafeProfile
from sqllocks_spindle.inference.safe_profile import (
    K_DEFAULT,
    K_SENSITIVE,
    OTHER_BUCKET,
    SafeColumnProfile,
)


# ---------------------------------------------------------------------------
# Unit-level: the suppression hook (count = proportion * row_count)
# ---------------------------------------------------------------------------


def test_hook_folds_sub_k_values_into_other_default_k():
    # row_count=100: counts are weight*100. k=5 default -> anything <5 folded.
    weights = {
        "common": 0.90,   # count 90 -> survives
        "mid": 0.06,      # count  6 -> survives
        "rare_a": 0.02,   # count  2 -> suppressed
        "rare_b": 0.02,   # count  2 -> suppressed
    }
    out, suppressed = SafeColumnProfile._suppress_categories_hook(
        weights, config=None, row_count=100, column_name="c"
    )
    assert suppressed == 2
    assert set(out) == {"common", "mid", OTHER_BUCKET}
    # __OTHER__ carries the aggregate weight of the suppressed values.
    assert out[OTHER_BUCKET] == pytest.approx(0.04)
    # Surviving categories keep their original weights.
    assert out["common"] == pytest.approx(0.90)
    assert out["mid"] == pytest.approx(0.06)


def test_hook_no_suppression_when_all_above_k():
    weights = {"a": 0.5, "b": 0.3, "c": 0.2}  # counts 500/300/200 at rc=1000
    out, suppressed = SafeColumnProfile._suppress_categories_hook(
        weights, config=None, row_count=1000, column_name="c"
    )
    assert suppressed == 0
    assert OTHER_BUCKET not in out
    assert out == weights
    assert out is not weights  # never mutate the caller's dict


def test_hook_sensitive_flag_raises_k_to_11():
    # count=10 for "x": survives at k=5, suppressed at k=11 (sensitive).
    weights = {"bulk": 0.90, "x": 0.10}  # counts 90 / 10 at rc=100
    out_def, sup_def = SafeColumnProfile._suppress_categories_hook(
        weights, config=None, row_count=100, column_name="c"
    )
    assert sup_def == 0  # 10 >= 5

    out_sens, sup_sens = SafeColumnProfile._suppress_categories_hook(
        weights, config={"sensitive": True}, row_count=100, column_name="c"
    )
    assert sup_sens == 1  # 10 < 11
    assert "x" not in out_sens
    assert out_sens[OTHER_BUCKET] == pytest.approx(0.10)


def test_hook_per_column_k_override_honored():
    weights = {"bulk": 0.92, "y": 0.08}  # counts 92 / 8 at rc=100
    # Profile default k=5 -> 8 survives. Per-column k=10 -> 8 suppressed.
    cfg = {"k": 5, "columns": {"city": {"k": 10}}}
    out, suppressed = SafeColumnProfile._suppress_categories_hook(
        weights, config=cfg, row_count=100, column_name="city"
    )
    assert suppressed == 1
    assert "y" not in out
    # A different column falls back to profile-level k=5 (8 survives).
    out2, sup2 = SafeColumnProfile._suppress_categories_hook(
        weights, config=cfg, row_count=100, column_name="other"
    )
    assert sup2 == 0


def test_resolve_k_precedence():
    # Per-column k beats profile sensitive.
    cfg = {"sensitive": True, "columns": {"c": {"k": 3}}}
    assert SafeColumnProfile._resolve_k(cfg, "c") == 3
    # Per-column sensitive used when no per-column k.
    cfg2 = {"columns": {"c": {"sensitive": True}}}
    assert SafeColumnProfile._resolve_k(cfg2, "c") == K_SENSITIVE
    # Profile-level k.
    assert SafeColumnProfile._resolve_k({"k": 8}, "c") == 8
    # Default.
    assert SafeColumnProfile._resolve_k(None, "c") == K_DEFAULT


def test_hook_without_row_count_passes_through():
    # Cannot derive counts from proportions without a row count -> no suppression,
    # suppressed=0 (never fabricate a count we can't compute).
    weights = {"a": 0.99, "rare": 0.01}
    out, suppressed = SafeColumnProfile._suppress_categories_hook(
        weights, config=None, row_count=None, column_name="c"
    )
    assert suppressed == 0
    assert out == weights


# ---------------------------------------------------------------------------
# AC1 / AC2 — rare values yield NO sub-k category in the persisted artifact
# ---------------------------------------------------------------------------


def _dataset_with_rare_categories(n=4000, seed=11):
    rng = np.random.default_rng(seed)
    # 3 high-frequency categories + a long tail of singleton rare codes.
    common = rng.choice(["alpha", "beta", "gamma"], size=n - 30, p=[0.5, 0.3, 0.2])
    rare = [f"rare_{i}" for i in range(30)]  # each appears exactly once
    values = np.concatenate([common, np.array(rare, dtype=object)])
    rng.shuffle(values)
    return pd.DataFrame({"category": values})


def test_no_subk_category_survives_in_artifact():
    """Story test: a column with rare values yields no sub-k category persisted."""
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    row_count = rich.tables["t"].row_count
    safe = SafeProfile.from_dataset_profile(rich)

    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "p.safe.json")
        ProfileStore.save(safe, path)
        with open(path) as fh:
            data = json.loads(fh.read())

    col = data["tables"]["t"]["columns"]["category"]
    weights = col["categorical_weights"]
    assert weights is not None
    # Reconstruct counts; every surviving non-OTHER category must be >= k.
    for value, weight in weights.items():
        if value == OTHER_BUCKET:
            continue
        count = round(weight * row_count)
        assert count >= K_DEFAULT, (value, count)
    # The 30 singleton rare codes are gone; __OTHER__ absorbed them.
    assert OTHER_BUCKET in weights
    assert not any(v.startswith("rare_") for v in weights if v != OTHER_BUCKET)


def test_suppressed_values_never_persisted():
    """AC2: no raw rare value string appears anywhere in the serialized text."""
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "p.safe.json")
        ProfileStore.save(safe, path)
        with open(path) as fh:
            raw_text = fh.read()
    assert "rare_" not in raw_text


# ---------------------------------------------------------------------------
# AC3 — per-column suppressed-category count recorded
# ---------------------------------------------------------------------------


def test_suppressed_category_count_recorded():
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)
    col = safe.tables["t"].columns["category"]
    # 30 singleton rare categories, all count 1 < k=5 -> all suppressed.
    assert col.suppressed_category_count == 30


def test_suppressed_count_zero_when_nothing_rare():
    df = pd.DataFrame({"tier": np.repeat(["a", "b", "c"], 1000)})
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)
    col = safe.tables["t"].columns["tier"]
    assert col.suppressed_category_count == 0
    assert OTHER_BUCKET not in (col.categorical_weights or {})


def test_suppressed_count_survives_round_trip():
    df = _dataset_with_rare_categories()
    rich = DataProfiler().profile_dataset({"t": df})
    safe = SafeProfile.from_dataset_profile(rich)
    restored = SafeProfile.from_safe_dict(safe.to_safe_dict())
    assert (
        restored.tables["t"].columns["category"].suppressed_category_count
        == safe.tables["t"].columns["category"].suppressed_category_count
        == 30
    )


# ---------------------------------------------------------------------------
# AC4 — high-frequency mass preserved; chi^2 within tolerance
# ---------------------------------------------------------------------------


def test_high_frequency_mass_preserved_chi2_within_tolerance():
    """Story test: chi^2 on the surviving categories stays within tolerance.

    The high-frequency categories keep their exact pre-suppression proportions;
    only the rare tail is folded into __OTHER__. A chi^2 goodness-of-fit on the
    surviving high-frequency categories (renormalized) must show the post-
    suppression distribution is statistically indistinguishable from the
    pre-suppression one on that mass.
    """
    df = _dataset_with_rare_categories(n=6000, seed=3)
    rich = DataProfiler().profile_dataset({"t": df})
    col = rich.tables["t"].columns["category"]
    pre = col.enum_values or col.value_counts_ext
    assert pre is not None

    safe = SafeProfile.from_dataset_profile(rich)
    post = safe.tables["t"].columns["category"].categorical_weights

    survivors = [v for v in post if v != OTHER_BUCKET]
    assert survivors, "expected high-frequency survivors"

    # On the surviving categories, weights are preserved EXACTLY (suppression
    # only removed the rare tail) -> chi^2 contribution is ~0.
    for v in survivors:
        assert post[v] == pytest.approx(pre[v])

    # The high-frequency mass (sum of survivor weights) is the bulk of the
    # distribution; the suppressed tail is small.
    survivor_mass = sum(post[v] for v in survivors)
    assert survivor_mass > 0.98  # rare tail < 2% of mass

    # chi^2 statistic on survivors (observed=post, expected=pre, scaled to a
    # nominal sample size) is ~0 because the survivor weights are identical.
    from scipy.stats import chisquare

    n = 1000.0
    observed = np.array([post[v] for v in survivors]) * n
    expected = np.array([pre[v] for v in survivors]) * n
    # Renormalize expected to match observed total (chisquare requires equal sums).
    expected *= observed.sum() / expected.sum()
    stat, pvalue = chisquare(f_obs=observed, f_exp=expected)
    assert stat == pytest.approx(0.0, abs=1e-6)
    assert pvalue > 0.99


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
