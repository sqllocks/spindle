"""Datetime fidelity lever: safe coarse temporal histogram (year range + seasonality).

Proves the period is captured (so regenerated dates land in the right era, not the
engine default 2022-2025) while NO individual raw date is persisted.
"""

import json
import os
import tempfile

import pandas as pd

from sqllocks_spindle.inference.profiler import DataProfiler
from sqllocks_spindle.inference.profile_store import ProfileStore
from sqllocks_spindle.inference.safe_profile import SafeProfile


def _safe(df):
    return SafeProfile.from_dataset_profile(DataProfiler().profile_dataset({"t": df}))


def test_temporal_histogram_captures_era_no_raw_date():
    df = pd.DataFrame({"dob": pd.to_datetime(
        ["1965-03-09", "1980-05-15", "1991-07-02"] * 30 + ["2001-12-25"] * 10)})
    safe = _safe(df)
    th = safe.tables["t"].columns["dob"].temporal_histogram
    assert th["lo_year"] == 1965 and th["hi_year"] == 2001  # real era, not 2022-25
    assert len(th["month_weights"]) == 12 and abs(sum(th["month_weights"]) - 1.0) < 1e-6
    blob = json.dumps(safe.to_safe_dict(), default=str)
    for raw in ("1980-05-15", "1991-07-02", "2001-12-25"):
        assert raw not in blob  # no individual raw date persisted


def test_temporal_histogram_survives_store_roundtrip():
    df = pd.DataFrame({"hire": pd.to_datetime(["2010-06-01", "2012-09-15"] * 50)})
    safe = _safe(df)
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "p.json")
        ProfileStore.save(safe, p)
        loaded = ProfileStore.load(p)
    th = loaded.tables["t"].columns["hire"].temporal_histogram
    assert th["lo_year"] == 2010 and th["hi_year"] == 2012
