"""STORY-012: empirical gates - suppression-utility, leak/fuzz (0 FN), conformance."""

import dataclasses
import json

import pandas as pd

from sqllocks_spindle.inference.profiler import DataProfiler
from sqllocks_spindle.inference.safe_profile import SafeProfile
from sqllocks_spindle.inference.safe_validator import SafeProfileValidator

_RAW_FIELDS = ("min_value", "max_value", "enum_values", "value_counts_ext")


def _artifact(df):
    a = SafeProfile.from_dataset_profile(DataProfiler().profile_dataset({"t": df})).to_safe_dict()
    return a, json.dumps(a, default=str)


def test_suppression_utility_high_freq_mass_preserved():
    """Post-k-anon, the high-frequency label weights stay within tolerance."""
    df = pd.DataFrame({"tier": ["bronze"] * 500 + ["silver"] * 300 + ["gold"] * 150
                       + [f"rare{i}" for i in range(50)]})
    a, _ = _artifact(df)
    w = a["tables"]["t"]["columns"]["tier"]["categorical_weights"]
    # high-freq labels survive with ~their original proportion (1000 total)
    assert abs(w["bronze"] - 0.5) < 0.02
    assert abs(w["silver"] - 0.3) < 0.02
    assert abs(w["gold"] - 0.15) < 0.02
    assert "__OTHER__" in w  # the 50 rare singletons folded away


def test_leak_fuzz_zero_false_negatives():
    """PII in oddly-named columns / odd encodings: ZERO literals reach the artifact
    AND validate --safe passes the resulting (clean) artifact."""
    fuzz = {
        "c_47": [f"{100+i:03d}-12-3456" for i in range(60)],      # SSN
        "zzz": [f"555-{200+i:03d}-1234" for i in range(60)],       # phone
        "blob": [f"u{i}@mail.com" for i in range(60)],             # email
        "n9": [123456789, 987654321, 555443322] * 20,             # integer SSN
        "d": ["05/15/1980", "07/02/1991", "03/09/1965"] * 20,     # slash DOB
        "mrn": ["MRN0012345", "MRN0067890", "MRN0099999"] * 20,   # MRN
        "z4": ["90210-1234", "10001-5678", "60601-0001"] * 20,    # ZIP+4
    }
    needles = []
    for vals in fuzz.values():
        needles += [str(v) for v in set(vals)]
    a, blob = _artifact(pd.DataFrame(fuzz))
    false_negatives = [n for n in needles if n in blob]
    assert false_negatives == [], f"PII literals leaked: {false_negatives[:5]}"
    assert SafeProfileValidator().validate_data(a).is_clean is True


def test_serialization_conformance_no_raw_fields():
    """STORY-004 guarantee: the rich DatasetProfile never emits the raw-bearing
    FIELD NAMES via asdict / json / pickle / repr (the fields are InitVar-demoted,
    so they are absent from dataclasses.fields and every serialization path).
    (Aggregate stats like quantiles legitimately remain on the rich profile;
    they are suppressed only on the SAFE profile, covered by other gates.)"""
    df = pd.DataFrame({"name": [f"person {i}" for i in range(60)],
                       "email": [f"u{i}@x.com" for i in range(60)]})
    rich = DataProfiler().profile_dataset({"t": df})
    asdict_blob = json.dumps(dataclasses.asdict(rich), default=str)
    repr_blob = "".join(repr(c) for c in rich.tables["t"].columns.values())
    for f in _RAW_FIELDS:
        assert f not in asdict_blob, f"raw field name {f} leaked via asdict"
        assert f not in repr_blob, f"raw field name {f} leaked via repr"
    # a real value that lives ONLY in a raw field (enum/value_counts) must not
    # appear in asdict (the raw fields are excluded from serialization).
    assert "person 7" not in asdict_blob
