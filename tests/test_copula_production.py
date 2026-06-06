"""Copula joint-fidelity lever: the production generate() path recovers the
correlation_matrix on correlated data, via SafeProfileAdapter -> correlated_columns
-> GaussianCopula post-pass. Correlations are aggregate -> safe.
"""

import numpy as np
import pandas as pd

from sqllocks_spindle.engine.generator import Spindle
from sqllocks_spindle.inference.profiler import DataProfiler
from sqllocks_spindle.inference.safe_profile import SafeProfile
from sqllocks_spindle.inference.safe_profile_adapter import SafeProfileAdapter


def _correlated_df(n=2000, seed=0):
    r = np.random.RandomState(seed)
    age = r.normal(45, 12, n).clip(18, 90)
    income = (age * 900 + r.normal(20000, 8000, n)).clip(15000, 300000)
    spend = (income * 0.3 + r.normal(0, 5000, n)).clip(0, None)
    return pd.DataFrame({"age": age, "income": income, "spend": spend})


def test_adapter_emits_correlated_columns():
    safe = SafeProfile.from_dataset_profile(DataProfiler().profile_dataset({"t": _correlated_df()}))
    schema = SafeProfileAdapter().to_schema(safe, domain_name="x")
    assert "t" in schema.correlated_columns
    pairs = {tuple(sorted((p[0], p[1]))) for p in schema.correlated_columns["t"]}
    assert ("age", "income") in pairs  # strong correlation captured


def test_production_generate_recovers_correlation_target():
    """STORY-019 AC#2/AC#3: the production generate() path tracks the real pairwise
    correlation (~0.80) instead of collapsing to 1.0. Fixed by empirical inverse-CDF
    marginals (ADR-016): faithful marginals preserve the rank structure the
    GaussianCopula post-pass reorders, so age/income recovers the target r.
    """
    orig = _correlated_df()
    safe = SafeProfile.from_dataset_profile(DataProfiler().profile_dataset({"t": orig}))
    schema = SafeProfileAdapter().to_schema(safe, domain_name="x")
    out = Spindle().generate(schema=schema, scale="small", seed=7, fidelity_profile=safe)
    gen = (out[0] if isinstance(out, tuple) else out).tables["t"]
    gen_corr = gen["age"].corr(gen["income"])
    # target: track the real ~0.80, NOT collapse to 1.0 (the old over-correlation bug)
    assert 0.6 < gen_corr < 0.95


def test_production_roundtrip_fidelity_on_correlated_fixture():
    """STORY-019 AC#4: round-trip fidelity on a correlated continuous fixture is
    >= 90% (the safe stats were already sufficient; empirical-first unlocks it)."""
    from sqllocks_spindle.inference.comparator import FidelityComparator

    orig = _correlated_df()
    safe = SafeProfile.from_dataset_profile(DataProfiler().profile_dataset({"t": orig}))
    schema = SafeProfileAdapter().to_schema(safe, domain_name="x")
    out = Spindle().generate(schema=schema, scale="small", seed=7, fidelity_profile=safe)
    gen = (out[0] if isinstance(out, tuple) else out).tables["t"]
    rep = FidelityComparator().compare({"t": orig}, {"t": gen})
    assert rep.tables["t"].score >= 90.0, rep.tables["t"].score
