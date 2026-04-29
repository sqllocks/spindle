"""Parametrized validation matrix — all ~512 valid domain × sink × size × mode combos.

Runs in <90s with no credentials (mock sinks only).
"""
from __future__ import annotations

import importlib

import pytest

from sqllocks_spindle.cli import _get_domain_registry
from sqllocks_spindle.engine.generator import Spindle
from tests.fixtures.validation_matrix import (
    DOMAINS,
    INFERENCE_CAPABLE_DOMAINS,
    build_matrix,
)
from tests.fixtures.mock_sinks import MockSink, make_mock_sink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_domain(domain_name: str):
    registry = _get_domain_registry()
    module_path, class_name, _ = registry[domain_name]
    module = importlib.import_module(module_path)
    return getattr(module, class_name)(schema_mode="3nf")


def _assert_result_valid(result, domain_name: str, size: str) -> None:
    assert result.tables, f"{domain_name}: no tables generated"
    for table_name, df in result.tables.items():
        assert len(df) > 0, f"{domain_name}/{table_name}: 0 rows at size={size}"
    errors = result.verify_integrity()
    assert errors == [], f"{domain_name} FK integrity errors: {errors}"


# ---------------------------------------------------------------------------
# Parametrized matrix
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("domain,sink,size,mode", build_matrix())
def test_generation_to_sink(domain, sink, size, mode):
    """Generate synthetic data and write to mock sink — all valid combinations."""
    spindle = Spindle()
    mock_sink = make_mock_sink(sink)

    if mode == "seeding":
        domain_obj = _load_domain(domain)
        result = spindle.generate(domain=domain_obj, scale=size, seed=42)
        mock_sink.write(result)
        _assert_result_valid(result, domain, size)

    elif mode == "streaming":
        domain_obj = _load_domain(domain)
        tables_received: list[str] = []
        for table_name, df in spindle.generate_stream(domain=domain_obj, scale=size, seed=42):
            mock_sink.write_stream(table_name, df)
            tables_received.append(table_name)
            assert len(df) > 0, f"{domain}/{table_name}: 0 rows in stream at size={size}"
        assert tables_received, f"{domain}: no tables yielded in stream"

    elif mode == "inference":
        assert domain in INFERENCE_CAPABLE_DOMAINS
        try:
            from sqllocks_spindle.inference import DataProfiler
            from sqllocks_spindle.inference.schema_builder import SchemaBuilder
            from sqllocks_spindle.inference.profiler import DatasetProfile
        except ImportError:
            pytest.skip("Inference API not available")
            return

        domain_obj = _load_domain(domain)
        ref_result = spindle.generate(domain=domain_obj, scale="small", seed=42)
        first_table = next(iter(ref_result.tables))
        ref_df = ref_result.tables[first_table].head(200)

        profiler = DataProfiler(sample_rows=200)
        table_profile = profiler.profile(ref_df, table_name=first_table)
        dataset_profile = DatasetProfile(tables={first_table: table_profile})
        schema = SchemaBuilder().build(dataset_profile)
        result = spindle.generate(schema=schema, scale="small", seed=99)
        mock_sink.write(result)
        assert result.tables, f"{domain} inference: no tables generated"

    mock_sink.assert_written(min_rows=1)


# ---------------------------------------------------------------------------
# Matrix builder unit tests
# ---------------------------------------------------------------------------

def test_build_matrix_returns_nonempty_list():
    matrix = build_matrix()
    assert isinstance(matrix, list)
    assert len(matrix) > 100


def test_matrix_no_duplicates():
    matrix = build_matrix()
    assert len(matrix) == len(set(matrix))


def test_matrix_filters_streaming_sql_server():
    matrix = build_matrix()
    bad = [(d, s, sz, m) for d, s, sz, m in matrix if s == "sql-server" and m == "streaming"]
    assert bad == [], f"streaming+sql-server should be filtered: {bad}"


def test_matrix_filters_fabric_demo_sql_server():
    matrix = build_matrix()
    for d, s, sz, m in matrix:
        if s == "sql-server":
            assert sz != "fabric_demo", "fabric_demo+sql-server should be filtered"


def test_matrix_inference_only_capable_domains():
    matrix = build_matrix()
    for d, s, sz, m in matrix:
        if m == "inference":
            assert d in INFERENCE_CAPABLE_DOMAINS


def test_all_domains_in_matrix():
    matrix = build_matrix()
    domains_in_matrix = {d for d, _, _, _ in matrix}
    for domain in DOMAINS:
        assert domain in domains_in_matrix, f"{domain} missing from matrix"
