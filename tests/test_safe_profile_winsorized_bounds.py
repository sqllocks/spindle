"""STORY-006 — Winsorized quantile bounds + generator clip (ADR-002 / E2).

Acceptance criteria covered:

AC1  Mapper sets ``bounds = {lo: p1, hi: p99}`` (percentiles configurable;
     default p1/p99; widening fallback p0.5/p99.5).
AC2  No ``min_value`` / ``max_value`` appear in the safe artifact.
AC3  Generator clips regenerated numerics to ``[lo, hi]``.
AC4  On a heavy-tailed fixture, p1/p99 winsorization drops fidelity below
     tolerance; widening to p0.5/p99.5 recovers it — and the widened bounds
     stay non-literal (strictly inside the real min/max).

The recovery mechanism is the std-ratio component of the composite fidelity
score: clipping a heavy tail at p1/p99 cuts variance materially; widening to
p0.5/p99.5 recovers tail mass (and variance) while never persisting the raw
extremes.
"""

from __future__ import annotations

import json
import os
import tempfile

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.inference import (
    DataProfiler,
    ProfileStore,
    SafeProfile,
    safe_profile_to_schema,
)
from sqllocks_spindle.inference.comparator import FidelityComparator
from sqllocks_spindle.inference.safe_profile import (
    SCHEMA_VERSION,
    SafeColumnProfile,
    SafeProfile as _SafeProfile,
    SafeTableProfile,
)


# ---------------------------------------------------------------------------
# AC1 — bounds from quantiles: default p1/p99, configurable, p0.5/p99.5 fallback
# ---------------------------------------------------------------------------


def test_profiler_quantiles_include_widening_endpoints():
    """The quantile fingerprint carries p0_5/p99_5 so the widening fallback is
    derivable from aggregate quantiles alone (never raw min/max)."""
    rng = np.random.default_rng(1)
    df = pd.DataFrame({"x": rng.lognormal(2.0, 1.0, size=3000)})
    rich = DataProfiler().profile_dataset({"t": df})
    q = rich.tables["t"].columns["x"].quantiles
    for k in ("p1", "p99", "p0_5", "p99_5"):
        assert k in q and q[k] is not None
    # Widening endpoints strictly bracket the p1/p99 window.
    assert q["p0_5"] <= q["p1"]
    assert q["p99_5"] >= q["p99"]


def test_schema_version_bumped_for_widening_endpoints():
    """Adding p0_5/p99_5 is a persisted-statistic addition -> schema_version
    bumped (ARCHITECTURE invariant)."""
    assert SCHEMA_VERSION >= 2


def test_bounds_default_p1_p99():
    q = {"p1": 1.0, "p50": 50.0, "p99": 99.0, "p0_5": 0.5, "p99_5": 99.5}
    assert SafeColumnProfile._winsorized_bounds_hook(q) == {"lo": 1.0, "hi": 99.0}


def test_bounds_configurable_percentiles():
    q = {"p1": 1.0, "p5": 5.0, "p95": 95.0, "p99": 99.0}
    widened = SafeColumnProfile._winsorized_bounds_hook(
        q, {"bounds_lo_quantile": "p5", "bounds_hi_quantile": "p95"}
    )
    assert widened == {"lo": 5.0, "hi": 95.0}


def test_bounds_widening_fallback_uses_p0_5_p99_5():
    q = {"p1": 1.0, "p99": 99.0, "p0_5": 0.5, "p99_5": 99.5}
    widened = SafeColumnProfile._winsorized_bounds_hook(
        q, SafeColumnProfile.WIDEN_BOUNDS_CONFIG
    )
    assert widened == {"lo": 0.5, "hi": 99.5}


def test_bounds_widening_request_degrades_gracefully_on_legacy_fingerprint():
    """A widened request against a v1 fingerprint lacking p0_5/p99_5 falls back
    to the default p1/p99 rather than returning None."""
    q = {"p1": 1.0, "p99": 99.0}  # no widening endpoints
    widened = SafeColumnProfile._winsorized_bounds_hook(
        q, SafeColumnProfile.WIDEN_BOUNDS_CONFIG
    )
    assert widened == {"lo": 1.0, "hi": 99.0}


def test_bounds_never_read_raw_min_max():
    """ADR-002: bounds derive from quantiles, never from min_value/max_value.
    A quantiles dict with no min/max keys still yields bounds."""
    q = {"p1": 3.3, "p99": 88.8}
    out = SafeColumnProfile._winsorized_bounds_hook(q)
    assert out == {"lo": 3.3, "hi": 88.8}


# ---------------------------------------------------------------------------
# AC2 — no literal min/max in the safe artifact
# ---------------------------------------------------------------------------


def test_safe_artifact_has_no_min_max_fields():
    """Serialized SafeProfile (and every nested column) carries no
    ``min_value`` / ``max_value`` (nor bare ``min`` / ``max``) keys."""
    rng = np.random.default_rng(2)
    df = pd.DataFrame(
        {
            "amount": rng.lognormal(3.0, 1.2, size=2000),
            "tier": rng.choice(["a", "b", "c"], size=2000),
        }
    )
    rich = DataProfiler().profile_dataset({"sales": df})
    safe = SafeProfile.from_dataset_profile(rich)

    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "p.safe.json")
        ProfileStore.save(safe, path)
        with open(path) as fh:
            raw_text = fh.read()
        data = json.loads(raw_text)

    forbidden = {"min_value", "max_value"}

    def _walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                assert k not in forbidden, f"forbidden key {k!r} in artifact"
                # bare min/max only legal nested under a winsorized bounds dict
                _walk(v)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(data)
    # The only place lo/hi live is the bounds dict; assert min/max are absent
    # as standalone column statistics.
    col = data["tables"]["sales"]["columns"]["amount"]
    assert "min_value" not in col and "max_value" not in col
    assert col["bounds"] is not None and set(col["bounds"]) == {"lo", "hi"}


# ---------------------------------------------------------------------------
# AC3 — generator clips regenerated numerics to [lo, hi]
# ---------------------------------------------------------------------------


def _single_numeric_profile(distribution, params, bounds, strategy_hint="dist"):
    col = SafeColumnProfile(
        name="x",
        dtype="float",
        null_rate=0.0,
        cardinality=5000,
        mean=100.0,
        std=50.0,
        distribution=distribution,
        distribution_params=params,
        quantiles={
            "p1": 5.0, "p5": 10.0, "p10": 20.0, "p25": 40.0, "p50": 60.0,
            "p75": 80.0, "p90": 95.0, "p95": 110.0, "p99": 140.0,
        },
        bounds=bounds,
    )
    if strategy_hint == "empirical":
        col.distribution = None
        col.distribution_params = None
    return _SafeProfile(
        tables={
            "t": SafeTableProfile(
                name="t", row_count=5000, columns={"x": col}, primary_key=[]
            )
        }
    )


def test_distribution_path_clips_to_bounds():
    """Numeric via the fitted-distribution path is clipped to [lo, hi].

    The distribution path is taken when the column has NO full p1..p99 quantile
    fingerprint (with one, the adapter prefers empirical inverse-CDF — STORY-019 /
    ADR-016). So this fixture drops quantiles to exercise the fitted-distribution
    branch; the empirical-path clip is covered by ``test_empirical_path_clips_to_bounds``.
    """
    safe = _single_numeric_profile(
        "normal", {"loc": 100.0, "scale": 50.0}, {"lo": 10.0, "hi": 20.0}
    )
    # No quantile fingerprint -> adapter routes to the fitted-distribution path.
    safe.tables["t"].columns["x"].quantiles = None
    schema = safe_profile_to_schema(safe)
    gen = schema.tables["t"].columns["x"].generator
    assert gen["strategy"] == "distribution"
    assert gen["params"]["min"] == 10.0 and gen["params"]["max"] == 20.0
    out = Spindle().generate(schema=schema, scale="small", seed=4)
    res = out[0] if isinstance(out, tuple) else out
    v = res.tables["t"]["x"].astype(float)
    assert v.min() >= 10.0 - 1e-9
    assert v.max() <= 20.0 + 1e-9


def test_empirical_path_clips_to_bounds():
    """Numeric via the empirical (quantile-interpolation) path threads bounds in
    and clips to [lo, hi]."""
    # Narrow the bounds well inside the p1..p99 quantile span so clipping bites.
    safe = _single_numeric_profile(
        None, None, {"lo": 30.0, "hi": 70.0}, strategy_hint="empirical"
    )
    schema = safe_profile_to_schema(safe)
    gen = schema.tables["t"].columns["x"].generator
    assert gen["strategy"] == "empirical"
    assert gen["min"] == 30.0 and gen["max"] == 70.0
    out = Spindle().generate(schema=schema, scale="small", seed=4)
    res = out[0] if isinstance(out, tuple) else out
    v = res.tables["t"]["x"].astype(float)
    assert v.min() >= 30.0 - 1e-9
    assert v.max() <= 70.0 + 1e-9


# ---------------------------------------------------------------------------
# AC4 — heavy-tailed fixture: widening p0.5/p99.5 recovers fidelity (non-literal)
# ---------------------------------------------------------------------------


def _heavy_tailed_real(seed=7):
    rng = np.random.default_rng(seed)
    return rng.lognormal(mean=3.0, sigma=1.6, size=6000)


def _score_for_config(real, rich, cfg, gen_seed=11):
    safe = SafeProfile.from_dataset_profile(rich, cfg)
    schema = safe_profile_to_schema(safe)
    out = Spindle().generate(schema=schema, scale="medium", seed=gen_seed)
    res = out[0] if isinstance(out, tuple) else out
    rep = FidelityComparator().compare(
        {"t": pd.DataFrame({"amount": real})}, {"t": res.tables["t"]}
    )
    cf = rep.tables["t"].columns["amount"]
    bounds = safe.tables["t"].columns["amount"].bounds
    return cf.score, cf.std_ratio, cf.ks_statistic, bounds


def test_widening_recovers_fidelity_on_heavy_tail_nonliteral():
    real = _heavy_tailed_real()
    rich = DataProfiler().profile_dataset({"t": pd.DataFrame({"amount": real})})

    score_def, sr_def, _ks_def, bounds_def = _score_for_config(real, rich, None)
    score_wide, sr_wide, _ks_wide, bounds_wide = _score_for_config(
        real, rich, SafeColumnProfile.WIDEN_BOUNDS_CONFIG
    )

    # p1/p99 winsorization drops fidelity below tolerance: the heavy tail's
    # variance is severely clipped (std-ratio well below 1) and the composite
    # score sits below the 90% round-trip target.
    tolerance = 90.0
    assert score_def < tolerance, score_def
    assert sr_def < 0.7, sr_def

    # Widening to p0.5/p99.5 recovers tail mass: higher composite score AND a
    # std-ratio closer to 1.0.
    assert score_wide > score_def
    assert sr_wide > sr_def

    # Still non-literal: the widened bounds stay strictly inside the real
    # min/max — the raw extremes never leave prod (ADR-002).
    real_min, real_max = float(real.min()), float(real.max())
    assert bounds_def["lo"] > real_min and bounds_def["hi"] < real_max
    assert bounds_wide["lo"] > real_min and bounds_wide["hi"] < real_max
    # Widened window is wider than the default (more tail captured).
    assert bounds_wide["hi"] > bounds_def["hi"]


def test_normal_fixture_within_tolerance_under_default_bounds():
    """A well-behaved (normal) fixture: default p1/p99 winsorization preserves
    the distribution within tolerance — widening is only needed for heavy tails.

    STORY-006 governs the *distributional* fidelity that bounds affect
    (std-ratio, KS, mean) — not the overall composite, whose cardinality term is
    a separate concern (the continuous regenerator yields more distinct floats).
    So we assert the bounds-relevant signals directly.
    """
    rng = np.random.default_rng(3)
    real = rng.normal(500.0, 80.0, size=6000)
    rich = DataProfiler().profile_dataset({"t": pd.DataFrame({"amount": real})})
    _score_def, sr_def, ks_def, _bounds = _score_for_config(real, rich, None)
    # Light-tailed: default winsorization keeps std-ratio near 1 and the KS gap
    # tiny — the distribution survives the default bounds within tolerance.
    assert sr_def > 0.9, sr_def
    assert ks_def < 0.05, ks_def


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
