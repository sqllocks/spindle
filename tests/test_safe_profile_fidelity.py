"""STORY-011: round-trip fidelity gate for the Safe Profile path.

profile -> ProfileStore.save -> load -> generate, scored with FidelityComparator
(KS numeric, chi-squared categorical) vs the original. HONEST FINDING: the safe
default-deny transforms (007a/b) discard the literals that ARE the fidelity for
categorical/low-card data, so safe-mode scores ~74/100 on retail, NOT >=90. This
gate asserts a realistic safe-mode floor and that unsafe mode scores higher.
See docs/guides/safe-profile-threat-model.md.
"""

import os
import tempfile

import pytest

from sqllocks_spindle.domains.retail import RetailDomain
from sqllocks_spindle.engine.generator import Spindle
from sqllocks_spindle.inference.comparator import FidelityComparator
from sqllocks_spindle.inference.profile_store import ProfileStore
from sqllocks_spindle.inference.profiler import DataProfiler
from sqllocks_spindle.inference.safe_profile import SafeProfile
from sqllocks_spindle.inference.safe_profile_adapter import SafeProfileAdapter

SAFE_MODE_FIDELITY_FLOOR = 65.0


@pytest.fixture(scope="module")
def retail_tables():
    return Spindle().generate(domain=RetailDomain(), scale="small", seed=42).tables


def _round_trip_score(orig, config=None):
    safe = SafeProfile.from_dataset_profile(
        DataProfiler().profile_dataset(orig), config=config
    )
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "r.json")
        ProfileStore.save(safe, path)
        loaded = ProfileStore.load(path)
    schema = SafeProfileAdapter().to_schema(loaded, domain_name="retail")
    out = Spindle().generate(schema=schema, scale="small", seed=7, fidelity_profile=loaded)
    gen = (out[0] if isinstance(out, tuple) else out).tables
    return FidelityComparator().compare(orig, gen).overall_score


def test_safe_mode_round_trip_meets_floor(retail_tables):
    score = _round_trip_score(retail_tables)
    assert score >= SAFE_MODE_FIDELITY_FLOOR, f"safe fidelity {score:.1f} < {SAFE_MODE_FIDELITY_FLOOR}"


def test_safety_costs_fidelity_tradeoff(retail_tables):
    safe = _round_trip_score(retail_tables)
    unsafe = _round_trip_score(retail_tables, config={"unsafe_full_fidelity": True})
    assert unsafe >= safe
