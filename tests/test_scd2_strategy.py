"""Tests for the SCD Type 2 strategy."""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import pytest

from sqllocks_spindle.engine.id_manager import IDManager
from sqllocks_spindle.engine.strategies.base import GenerationContext
from sqllocks_spindle.engine.strategies.scd2 import SCD2Strategy
from sqllocks_spindle.schema.parser import ColumnDef


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(name: str = "col", dtype: str = "date") -> ColumnDef:
    return ColumnDef(name=name, type=dtype, generator={})


def _ctx(row_count: int = 100, current_table: dict | None = None) -> GenerationContext:
    rng = np.random.default_rng(42)
    id_manager = IDManager(rng)
    ctx = GenerationContext(
        rng=rng,
        id_manager=id_manager,
        model_config={"date_range": {"start": "2022-01-01", "end": "2024-12-31"}},
        row_count=row_count,
    )
    if current_table:
        ctx.current_table = current_table
    return ctx


def _make_bk_array(keys_and_counts: dict[int, int]) -> np.ndarray:
    """Build a business key array. keys_and_counts maps bk_value -> repeat count."""
    parts = []
    for key, count in keys_and_counts.items():
        parts.extend([key] * count)
    return np.array(parts, dtype=object)


def _generate_all_roles(bk_values: np.ndarray, avg_versions: int = 3, min_gap_days: int = 1):
    """Generate all four SCD2 roles and return them as a dict."""
    strategy = SCD2Strategy()
    row_count = len(bk_values)
    base_config = {
        "strategy": "scd2",
        "business_key": "customer_id",
        "avg_versions": avg_versions,
        "min_gap_days": min_gap_days,
    }

    # effective_date
    ctx = _ctx(row_count, current_table={"customer_id": bk_values})
    eff_config = {**base_config, "role": "effective_date"}
    eff_dates = strategy.generate(_col("effective_date"), eff_config, ctx)
    ctx.current_table["effective_date"] = eff_dates

    # end_date
    end_config = {**base_config, "role": "end_date"}
    end_dates = strategy.generate(_col("end_date"), end_config, ctx)
    ctx.current_table["end_date"] = end_dates

    # is_current
    cur_config = {**base_config, "role": "is_current"}
    is_current = strategy.generate(_col("is_current"), cur_config, ctx)

    # version
    ver_config = {**base_config, "role": "version"}
    versions = strategy.generate(_col("version"), ver_config, ctx)

    return {
        "effective_date": eff_dates,
        "end_date": end_dates,
        "is_current": is_current,
        "version": versions,
        "bk": bk_values,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSCD2Strategy:
    def test_effective_dates_sorted_per_business_key(self):
        """Effective dates are sorted ascending within each BK group."""
        bk = _make_bk_array({1: 4, 2: 3, 3: 2})
        result = _generate_all_roles(bk)

        for key in (1, 2, 3):
            indices = [i for i, k in enumerate(bk) if k == key]
            dates = [result["effective_date"][i] for i in indices]
            assert dates == sorted(dates), f"BK {key}: dates not sorted: {dates}"

    def test_end_dates_match_next_effective(self):
        """end_date[i] == effective_date[i+1] - min_gap_days for each BK group."""
        min_gap = 1
        bk = _make_bk_array({10: 4, 20: 3})
        result = _generate_all_roles(bk, min_gap_days=min_gap)

        for key in (10, 20):
            indices = [i for i, k in enumerate(bk) if k == key]
            # Sort by effective date
            sorted_idx = sorted(indices, key=lambda i: result["effective_date"][i])

            for pos in range(len(sorted_idx) - 1):
                curr = sorted_idx[pos]
                nxt = sorted_idx[pos + 1]
                expected_end = result["effective_date"][nxt] - timedelta(days=min_gap)
                assert result["end_date"][curr] == expected_end, (
                    f"BK {key} version {pos+1}: end_date {result['end_date'][curr]} "
                    f"!= expected {expected_end}"
                )

    def test_last_version_has_null_end_date(self):
        """The most recent version per BK should have None end_date."""
        bk = _make_bk_array({1: 3, 2: 2, 3: 5})
        result = _generate_all_roles(bk)

        for key in (1, 2, 3):
            indices = [i for i, k in enumerate(bk) if k == key]
            sorted_idx = sorted(indices, key=lambda i: result["effective_date"][i])
            last_idx = sorted_idx[-1]
            assert result["end_date"][last_idx] is None, (
                f"BK {key}: last version end_date should be None, got {result['end_date'][last_idx]}"
            )

    def test_is_current_one_per_business_key(self):
        """Exactly one True per business key."""
        bk = _make_bk_array({1: 4, 2: 3, 3: 2, 4: 1})
        result = _generate_all_roles(bk)

        for key in (1, 2, 3, 4):
            indices = [i for i, k in enumerate(bk) if k == key]
            current_flags = [result["is_current"][i] for i in indices]
            assert current_flags.count(True) == 1, (
                f"BK {key}: expected exactly 1 is_current=True, got {current_flags.count(True)}"
            )
            assert current_flags.count(False) == len(indices) - 1

    def test_version_sequential(self):
        """Versions are 1, 2, 3, ... per business key, ordered by effective_date."""
        bk = _make_bk_array({1: 5, 2: 3})
        result = _generate_all_roles(bk)

        for key in (1, 2):
            indices = [i for i, k in enumerate(bk) if k == key]
            sorted_idx = sorted(indices, key=lambda i: result["effective_date"][i])
            versions = [result["version"][i] for i in sorted_idx]
            expected = list(range(1, len(indices) + 1))
            assert versions == expected, (
                f"BK {key}: versions {versions} != expected {expected}"
            )

    def test_single_version_business_key(self):
        """BK appearing once: version=1, is_current=True, end_date=None."""
        bk = _make_bk_array({99: 1})
        result = _generate_all_roles(bk)

        assert result["version"][0] == 1
        assert result["is_current"][0] is True
        assert result["end_date"][0] is None
        assert result["effective_date"][0] is not None

    def test_min_gap_respected(self):
        """Gap between end_date and next effective_date >= min_gap_days."""
        min_gap = 3
        bk = _make_bk_array({1: 5, 2: 4})
        result = _generate_all_roles(bk, min_gap_days=min_gap)

        for key in (1, 2):
            indices = [i for i, k in enumerate(bk) if k == key]
            sorted_idx = sorted(indices, key=lambda i: result["effective_date"][i])

            for pos in range(len(sorted_idx) - 1):
                curr = sorted_idx[pos]
                nxt = sorted_idx[pos + 1]
                end = result["end_date"][curr]
                next_eff = result["effective_date"][nxt]
                gap = (next_eff - end).days
                assert gap >= min_gap, (
                    f"BK {key}: gap {gap} days < min_gap {min_gap} "
                    f"(end={end}, next_eff={next_eff})"
                )

    def test_missing_business_key_raises(self):
        """Should raise if business_key column is not in current_table."""
        strategy = SCD2Strategy()
        ctx = _ctx(10, current_table={})
        config = {"role": "effective_date", "business_key": "missing_col"}
        with pytest.raises(ValueError, match="must be generated before"):
            strategy.generate(_col("eff"), config, ctx)

    def test_unknown_role_raises(self):
        """Should raise for unknown role."""
        strategy = SCD2Strategy()
        bk = np.array([1, 1, 2], dtype=object)
        ctx = _ctx(3, current_table={"cid": bk})
        config = {"role": "bogus", "business_key": "cid"}
        with pytest.raises(ValueError, match="unknown role"):
            strategy.generate(_col("x"), config, ctx)

    def test_empty_business_key_config_raises(self):
        """Should raise if business_key config is empty string."""
        strategy = SCD2Strategy()
        ctx = _ctx(5, current_table={})
        config = {"role": "effective_date", "business_key": ""}
        with pytest.raises(ValueError, match="requires 'business_key'"):
            strategy.generate(_col("x"), config, ctx)

    def test_result_lengths(self):
        """All generated arrays should match the input length."""
        bk = _make_bk_array({1: 10, 2: 5, 3: 8})
        result = _generate_all_roles(bk)

        total = len(bk)
        assert len(result["effective_date"]) == total
        assert len(result["end_date"]) == total
        assert len(result["is_current"]) == total
        assert len(result["version"]) == total
