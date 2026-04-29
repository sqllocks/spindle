"""Tests for FidelityReport.to_html() and ProfileRegistry.validate()."""

from __future__ import annotations

import pytest

try:
    import scipy  # noqa: F401
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

import numpy as np
import pandas as pd

from sqllocks_spindle.inference.comparator import FidelityReport


pytestmark = pytest.mark.skipif(not HAS_SCIPY, reason="scipy required")


# ---------------------------------------------------------------------------
# to_html
# ---------------------------------------------------------------------------

def _make_report() -> FidelityReport:
    real = pd.DataFrame({
        "age": np.random.randint(18, 80, 200).astype(float),
        "segment": (["A"] * 80 + ["B"] * 80 + ["C"] * 40),
    })
    synth = pd.DataFrame({
        "age": np.random.randint(18, 80, 200).astype(float),
        "segment": (["A"] * 80 + ["B"] * 80 + ["C"] * 40),
    })
    return FidelityReport.score(real, synth, table_name="users")


def test_to_html_returns_string():
    html = _make_report().to_html()
    assert isinstance(html, str)


def test_to_html_is_valid_html():
    html = _make_report().to_html()
    assert "<!DOCTYPE html>" in html
    assert "<html" in html
    assert "</html>" in html


def test_to_html_contains_table_name():
    html = _make_report().to_html()
    assert "users" in html


def test_to_html_contains_score():
    report = _make_report()
    html = report.to_html()
    assert f"{report.overall_score:.1f}" in html


def test_to_html_contains_all_columns():
    html = _make_report().to_html()
    assert "age" in html
    assert "segment" in html


def test_to_html_custom_title():
    html = _make_report().to_html(title="My Custom Report")
    assert "My Custom Report" in html


def test_to_html_score_colors():
    html = _make_report().to_html()
    # At least one color band should appear
    assert any(c in html for c in ["#2d7d46", "#b45309", "#c0392b"])


def test_to_html_progress_bars():
    html = _make_report().to_html()
    assert "border-radius:3px" in html


def test_to_html_multi_table():
    real_a = pd.DataFrame({"x": np.random.randn(100)})
    synth_a = pd.DataFrame({"x": np.random.randn(100)})
    real_b = pd.DataFrame({"y": list("AB") * 50})
    synth_b = pd.DataFrame({"y": list("AB") * 50})

    from sqllocks_spindle.inference.comparator import FidelityComparator
    report = FidelityComparator().compare(
        {"sales": real_a, "segments": real_b},
        {"sales": synth_a, "segments": synth_b},
    )
    html = report.to_html()
    assert "sales" in html
    assert "segments" in html


def test_to_html_null_safe_ks():
    """Columns without numeric data should render em-dashes not crash."""
    real = pd.DataFrame({"cat": list("ABCD") * 25})
    synth = pd.DataFrame({"cat": list("ABCD") * 25})
    report = FidelityReport.score(real, synth, table_name="t")
    html = report.to_html()
    assert "&mdash;" in html


# ---------------------------------------------------------------------------
# ProfileRegistry.validate integration
# ---------------------------------------------------------------------------

@pytest.mark.slow
def test_registry_validate_returns_fidelity_report(tmp_path):
    """End-to-end: save a profile → generate data → validate → get report."""
    from sqllocks_spindle.profiles import ProfileRegistry, RegistryProfile
    from sqllocks_spindle.engine.generator import Spindle

    reg = ProfileRegistry(root=tmp_path / "profiles")
    spindle = Spindle()

    import importlib
    from sqllocks_spindle.cli import _get_domain_registry
    registry = _get_domain_registry()
    mod_path, cls_name, _ = registry["retail"]
    module = importlib.import_module(mod_path)
    domain = getattr(module, cls_name)(schema_mode="3nf")

    result = spindle.generate(domain=domain, scale="small", seed=42)
    first_table = next(iter(result.tables))
    ref_df = result.tables[first_table].head(100)

    from sqllocks_spindle.inference.profiler import DataProfiler, DatasetProfile
    profiler = DataProfiler(sample_rows=200)
    table_profile = profiler.profile(ref_df, table_name=first_table)
    dataset_profile = DatasetProfile(tables={first_table: table_profile})
    profiles = reg.save_from_dataset_profile(dataset_profile, system="test", name="v1")
    assert profiles

    identity = profiles[0].identity
    result2 = spindle.generate(domain=domain, scale="small", seed=99)
    report = reg.validate(identity, result2)
    assert report is not None
    assert report.overall_score >= 0
    assert report.overall_score <= 100
    html = report.to_html()
    assert "<!DOCTYPE html>" in html
