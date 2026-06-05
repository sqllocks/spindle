"""STORY-014: ProfileRegistry.save_from_dataset_profile re-pointed to SafeProfile."""

import json
import tempfile

import pandas as pd

from sqllocks_spindle.inference.profiler import DataProfiler
from sqllocks_spindle.profiles import ProfileRegistry
from sqllocks_spindle.profiles.profile import RegistryProfile


def _reg():
    return ProfileRegistry(root=tempfile.mkdtemp())


def test_save_persists_safe_stats_no_raw_values():
    reg = _reg()
    df = pd.DataFrame({"salary": [55000] * 20 + [88000] * 20 + [123456] * 20,
                       "tier": ["bronze"] * 30 + ["gold"] * 30})
    rich = DataProfiler().profile_dataset({"t": df})
    saved = reg.save_from_dataset_profile(rich, system="hr", name="prod-q2", tags=["pii"])
    assert len(saved) == 1
    loaded = reg.load("hr/t/prod-q2")
    blob = json.dumps(loaded.to_dict(), default=str)
    # no raw salary literal; safe keys present; B2 bug gone (no min/max/top_values)
    for needle in ("123456", "88000", "55000"):
        assert needle not in blob, f"raw value {needle} in registry record"
    cols = loaded.columns
    assert "min" not in cols["salary"] and "max" not in cols["salary"]
    assert "top_values" not in cols["tier"]
    assert cols["tier"].get("categorical_weights")  # safe labels persisted


def test_read_side_intact_after_repoint():
    reg = _reg()
    rich = DataProfiler().profile_dataset({"t": pd.DataFrame({"a": [1, 2, 3] * 10})})
    reg.save_from_dataset_profile(rich, system="s", name="n", tags=["x"])
    # load / search / tag / reindex all work (need system/name/tags metadata)
    assert reg.exists("s/t/n")
    assert reg.search(system="s")
    reg.add_tags("s/t/n", ["y"])
    assert "y" in reg.load("s/t/n").tags
    assert reg.reindex() >= 1


def test_legacy_record_still_loads():
    reg = _reg()
    legacy = RegistryProfile(system="old", table="t", name="v0",
                             columns={"c": {"dtype": "integer", "min": 1, "max": 9,
                                            "top_values": {"a": 0.5}}},
                             tags=[], source_rows=10)
    reg.save(legacy)
    back = reg.load("old/t/v0")  # old-format columns load read-only, no crash
    assert back.columns["c"]["min"] == 1
