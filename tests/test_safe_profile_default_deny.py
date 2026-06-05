"""STORY-007a/b: default-DENY categorical key routing - bypass corpus.

Every shape that previously refuted (memory id=3626/3635) must now leave NO
literal value anywhere in the serialized artifact, while genuine string labels
still persist and high-cardinality numerics keep their quantiles.
"""

import datetime as dt
import json

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference.profiler import DataProfiler
from sqllocks_spindle.inference.safe_profile import SCHEMA_VERSION, SafeProfile


def _artifact(df: pd.DataFrame):
    prof = DataProfiler().profile_dataset({"t": df})
    a = SafeProfile.from_dataset_profile(prof).to_safe_dict()
    return a, json.dumps(a, default=str)


def test_schema_version_is_4():
    assert SCHEMA_VERSION == 4


@pytest.mark.parametrize(
    "df, needles",
    [
        (pd.DataFrame({"salary": [55000] * 20 + [88000] * 20 + [123456] * 20}),
         ["55000", "88000", "123456"]),
        (pd.DataFrame({"dob": pd.to_datetime(
            [dt.date(1980, 5, 15)] * 20 + [dt.date(1991, 7, 2)] * 20 + [dt.date(1965, 3, 9)] * 20)}),
         ["1980-05-15", "1991-07-02", "1965-03-09"]),
        (pd.DataFrame({"c_47": ["555-123-4567"] * 20 + ["555-987-6543"] * 20 + ["555-111-2222"] * 20}),
         ["555-123-4567", "555-987-6543", "5551112222"]),
        (pd.DataFrame({"x": ["MRN0012345"] * 20 + ["MRN0067890"] * 20 + ["MRN0099999"] * 20}),
         ["MRN0012345", "MRN0067890"]),
        (pd.DataFrame({"z": ["90210-1234"] * 20 + ["10001-5678"] * 20 + ["60601-0001"] * 20}),
         ["90210-1234", "10001-5678"]),
        (pd.DataFrame({"acct": ["1234-5678"] * 20 + ["8765-4321"] * 20 + ["1111-2222"] * 20}),
         ["1234-5678", "8765-4321"]),
        (pd.DataFrame({"bd": ["05/15/1980"] * 20 + ["07/02/1991"] * 20 + ["03/09/1965"] * 20}),
         ["05/15/1980", "07/02/1991"]),
        (pd.DataFrame({"ssn": [123456789] * 20 + [987654321] * 20 + [555443322] * 20}),
         ["123456789", "987654321", "555443322"]),
    ],
)
def test_no_literal_leaks(df, needles):
    _, blob = _artifact(df)
    leaked = [n for n in needles if n in blob]
    assert leaked == [], f"literal PII leaked into artifact: {leaked}"


def test_safe_labels_persist():
    a, _ = _artifact(pd.DataFrame(
        {"tier": ["bronze"] * 30 + ["silver"] * 30 + ["gold"] * 10,
         "state": ["CA"] * 30 + ["NY"] * 30 + ["TX"] * 10}))
    tier = a["tables"]["t"]["columns"]["tier"]["categorical_weights"]
    state = a["tables"]["t"]["columns"]["state"]["categorical_weights"]
    assert tier and "bronze" in tier
    assert state and "CA" in state


def test_high_card_numeric_keeps_quantiles():
    a, _ = _artifact(pd.DataFrame({"income": list(np.random.RandomState(0).randint(30000, 90000, 500))}))
    assert a["tables"]["t"]["columns"]["income"]["quantiles"] is not None


def test_low_card_numeric_routes_to_histogram_and_suppresses_quantiles():
    a, _ = _artifact(pd.DataFrame({"salary": [55000] * 20 + [88000] * 20 + [123456] * 20}))
    col = a["tables"]["t"]["columns"]["salary"]
    assert col["categorical_histogram"] is not None
    assert col["categorical_weights"] is None
    assert col["quantiles"] is None and col["bounds"] is None
