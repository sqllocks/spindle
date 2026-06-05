"""STORY-015: registry.validate() reconstructs reference from the SafeProfile keys."""

import tempfile
import types

import numpy as np
import pandas as pd

from sqllocks_spindle.inference.profiler import DataProfiler
from sqllocks_spindle.profiles import ProfileRegistry


def _reg():
    return ProfileRegistry(root=tempfile.mkdtemp())


def test_reconstruct_reads_safe_keys_not_min_max_top_values():
    """Unit: _reconstruct_reference uses categorical_weights/histogram/bounds and
    spindle dtype names, producing a populated (non-null) reference frame."""
    reg = _reg()
    df = pd.DataFrame({"tier": ["bronze"] * 60 + ["gold"] * 40,
                       "income": list(np.random.RandomState(0).randint(30000, 90000, 100))})
    rich = DataProfiler().profile_dataset({"t": df})
    reg.save_from_dataset_profile(rich, system="s", name="n")
    prof = reg.load("s/t/n")
    ref = ProfileRegistry._reconstruct_reference(prof, n_rows=200)
    # categorical column reconstructed from categorical_weights (real labels)
    assert set(ref["tier"].dropna().unique()) <= {"bronze", "gold"}
    assert ref["tier"].notna().sum() > 0
    # numeric column reconstructed (not all-null, the old degenerate failure)
    assert ref["income"].notna().sum() > 0


def test_validate_produces_meaningful_score():
    """registry.validate() against a real generation result yields a non-degenerate
    fidelity score (the old code reconstructed an all-null reference -> ~0)."""
    reg = _reg()
    df = pd.DataFrame({"tier": ["bronze"] * 60 + ["gold"] * 40,
                       "income": list(np.random.RandomState(1).randint(30000, 90000, 100))})
    rich = DataProfiler().profile_dataset({"t": df})
    reg.save_from_dataset_profile(rich, system="s", name="n")
    result = types.SimpleNamespace(tables={"t": df})  # compare against the original
    report = reg.validate("s/t/n", result, sample_rows=200)
    assert report.overall_score > 20.0, f"degenerate score {report.overall_score}"
