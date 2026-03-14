"""Tests for corrupt/poison files pack — new FileChaosMutator mutations (E3)."""

from __future__ import annotations

import numpy as np
import pytest

from sqllocks_spindle.chaos.categories import FileChaosMutator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mutator():
    return FileChaosMutator()


@pytest.fixture
def rng():
    return np.random.RandomState(42)


@pytest.fixture
def csv_data():
    return b"id,name,amount\n1,Alice,100.50\n2,Bob,200.75\n3,Carol,50.25\n"


@pytest.fixture
def jsonl_data():
    return b'{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n'


# ---------------------------------------------------------------------------
# Wrong delimiter
# ---------------------------------------------------------------------------

class TestWrongDelimiter:
    def test_replaces_commas_with_pipes(self, mutator, rng, csv_data):
        result = mutator._wrong_delimiter(csv_data, rng, 1.0)
        assert b"|" in result
        assert b"," not in result

    def test_returns_bytes(self, mutator, rng, csv_data):
        result = mutator._wrong_delimiter(csv_data, rng, 1.0)
        assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# Invalid JSON poison
# ---------------------------------------------------------------------------

class TestInvalidJsonPoison:
    def test_injects_poison_payload(self, mutator, rng, jsonl_data):
        result = mutator._invalid_json_poison(jsonl_data, rng, 1.0)
        assert len(result) > len(jsonl_data)

    def test_returns_bytes(self, mutator, rng, jsonl_data):
        result = mutator._invalid_json_poison(jsonl_data, rng, 1.0)
        assert isinstance(result, bytes)

    def test_works_on_short_data(self, mutator, rng):
        result = mutator._invalid_json_poison(b"x", rng, 1.0)
        assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# BOM injection
# ---------------------------------------------------------------------------

class TestBomInjection:
    def test_adds_bom_bytes(self, mutator, rng, csv_data):
        result = mutator._bom_injection(csv_data, rng, 1.0)
        assert b"\xef\xbb\xbf" in result

    def test_returns_bytes(self, mutator, rng, csv_data):
        result = mutator._bom_injection(csv_data, rng, 1.0)
        assert isinstance(result, bytes)

    def test_works_on_short_data(self, mutator, rng):
        result = mutator._bom_injection(b"x", rng, 1.0)
        assert b"\xef\xbb\xbf" in result


# ---------------------------------------------------------------------------
# Integration: mutate() can select new mutations
# ---------------------------------------------------------------------------

class TestMutateIntegration:
    def test_mutate_returns_bytes(self, mutator, rng, csv_data):
        result = mutator.mutate(csv_data, day=5, rng=rng, intensity_multiplier=1.0)
        assert isinstance(result, bytes)

    def test_mutate_across_many_seeds(self, mutator, csv_data):
        """Ensure the new mutations are reachable across different seeds."""
        results = set()
        for seed in range(100):
            rng = np.random.RandomState(seed)
            result = mutator.mutate(csv_data, day=5, rng=rng, intensity_multiplier=1.0)
            results.add(len(result))
        # Should see variety in output lengths (different mutations produce different lengths)
        assert len(results) > 3
