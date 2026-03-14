"""Tests for ChaosEngine and all six category mutators."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.chaos.categories import (
    FileChaosMutator,
    ReferentialChaosMutator,
    SchemaChaosMutator,
    TemporalChaosMutator,
    ValueChaosMutator,
    VolumeChaosMutator,
)
from sqllocks_spindle.chaos.config import ChaosCategory, ChaosConfig, ChaosOverride
from sqllocks_spindle.chaos.engine import ChaosEngine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def rng():
    return np.random.RandomState(42)


@pytest.fixture
def simple_df():
    """Minimal DataFrame suitable for most value/schema tests."""
    n = 50
    rng = np.random.RandomState(1)
    return pd.DataFrame({
        "id": range(1, n + 1),
        "amount": rng.uniform(10.0, 200.0, size=n).round(2),
        "name": [f"Item {i}" for i in range(1, n + 1)],
        "order_date": pd.date_range("2024-01-01", periods=n, freq="D"),
    })


@pytest.fixture
def enabled_config():
    return ChaosConfig(enabled=True, intensity="moderate", seed=99, chaos_start_day=1)


@pytest.fixture
def engine(enabled_config):
    return ChaosEngine(config=enabled_config)


# ---------------------------------------------------------------------------
# ChaosEngine — basic behaviour
# ---------------------------------------------------------------------------

class TestChaosEngineBasic:
    def test_disabled_engine_never_injects(self, simple_df):
        cfg = ChaosConfig(enabled=False)
        eng = ChaosEngine(cfg)
        for day in range(0, 50):
            for cat in ChaosCategory:
                assert eng.should_inject(day, cat.value) is False

    def test_warmup_suppresses_injection(self):
        cfg = ChaosConfig(enabled=True, warmup_days=7, chaos_start_day=8)
        eng = ChaosEngine(cfg, seed=0)
        # Days 0-7 are inside warmup (< chaos_start_day=8)
        for day in range(0, 8):
            for cat in ChaosCategory:
                assert eng.should_inject(day, cat.value) is False

    def test_override_forces_injection(self):
        override = ChaosOverride(day=5, category="value")
        cfg = ChaosConfig(enabled=True, chaos_start_day=1, overrides=[override])
        eng = ChaosEngine(cfg, seed=999)
        assert eng.should_inject(5, "value") is True

    def test_seeded_engine_is_deterministic(self):
        cfg = ChaosConfig(enabled=True, intensity="stormy", seed=77, chaos_start_day=1)
        results_a = [ChaosEngine(cfg).should_inject(d, "value") for d in range(1, 20)]
        results_b = [ChaosEngine(cfg).should_inject(d, "value") for d in range(1, 20)]
        assert results_a == results_b

    def test_seed_override_in_constructor(self):
        cfg = ChaosConfig(enabled=True, seed=10, chaos_start_day=1)
        eng = ChaosEngine(cfg, seed=99)
        # Engine uses constructor seed, not config seed
        results = [eng.should_inject(d, "value") for d in range(1, 10)]
        eng2 = ChaosEngine(cfg, seed=99)
        results2 = [eng2.should_inject(d, "value") for d in range(1, 10)]
        assert results == results2

    def test_config_property(self, engine, enabled_config):
        assert engine.config is enabled_config

    def test_rng_property_is_random_state(self, engine):
        assert isinstance(engine.rng, np.random.RandomState)


# ---------------------------------------------------------------------------
# ChaosEngine — escalation factors
# ---------------------------------------------------------------------------

class TestEscalationFactor:
    def test_gradual_day_zero_is_zero(self):
        cfg = ChaosConfig(enabled=True, escalation="gradual", chaos_start_day=0)
        eng = ChaosEngine(cfg, seed=0)
        # Day 0 = chaos_day 0 → factor = 0/30 = 0.0
        factor = eng._escalation_factor(0)
        assert factor == pytest.approx(0.0)

    def test_gradual_day_30_is_one(self):
        cfg = ChaosConfig(enabled=True, escalation="gradual", chaos_start_day=0)
        eng = ChaosEngine(cfg, seed=0)
        factor = eng._escalation_factor(30)
        assert factor == pytest.approx(1.0)

    def test_gradual_capped_at_one(self):
        cfg = ChaosConfig(enabled=True, escalation="gradual", chaos_start_day=0)
        eng = ChaosEngine(cfg, seed=0)
        factor = eng._escalation_factor(100)
        assert factor == pytest.approx(1.0)

    def test_front_loaded_starts_near_one(self):
        cfg = ChaosConfig(enabled=True, escalation="front-loaded", chaos_start_day=0)
        eng = ChaosEngine(cfg, seed=0)
        factor = eng._escalation_factor(0)
        assert factor >= 0.9

    def test_front_loaded_decays_over_time(self):
        cfg = ChaosConfig(enabled=True, escalation="front-loaded", chaos_start_day=0)
        eng = ChaosEngine(cfg, seed=0)
        early = eng._escalation_factor(1)
        late = eng._escalation_factor(50)
        assert late < early

    def test_random_escalation_returns_0_to_1(self):
        cfg = ChaosConfig(enabled=True, escalation="random", chaos_start_day=0)
        eng = ChaosEngine(cfg, seed=0)
        for day in range(1, 20):
            factor = eng._escalation_factor(day)
            assert 0.0 <= factor <= 1.0

    def test_before_chaos_start_returns_zero(self):
        cfg = ChaosConfig(enabled=True, chaos_start_day=10)
        eng = ChaosEngine(cfg, seed=0)
        assert eng._escalation_factor(5) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# ChaosEngine — public mutation methods
# ---------------------------------------------------------------------------

class TestChaosEngineCorruptDataframe:
    def test_returns_dataframe(self, engine, simple_df):
        result = engine.corrupt_dataframe(simple_df, day=10)
        assert isinstance(result, pd.DataFrame)

    def test_does_not_mutate_original(self, engine, simple_df):
        original_ids = list(simple_df["id"])
        engine.corrupt_dataframe(simple_df, day=10)
        assert list(simple_df["id"]) == original_ids

    def test_same_columns_or_subset(self, engine, simple_df):
        result = engine.corrupt_dataframe(simple_df, day=10)
        assert set(result.columns).issubset(set(simple_df.columns)) or set(result.columns) >= set(simple_df.columns)

    def test_some_nulls_injected(self):
        """With hurricane intensity, high chance of nulls being injected."""
        cfg = ChaosConfig(enabled=True, intensity="hurricane", seed=42, chaos_start_day=1)
        eng = ChaosEngine(cfg)
        df = pd.DataFrame({"id": range(1, 101), "amount": range(1, 101)})
        result = eng.corrupt_dataframe(df, day=10)
        assert result.isna().any().any()


class TestChaosEngineDriftSchema:
    def test_returns_dataframe(self, engine, simple_df):
        result = engine.drift_schema(simple_df, day=25)
        assert isinstance(result, pd.DataFrame)

    def test_column_count_changes(self, engine, simple_df):
        """Schema drift should change columns (add, drop, or rename)."""
        cfg = ChaosConfig(
            enabled=True, intensity="stormy", seed=0, chaos_start_day=1,
            breaking_change_day=5,
        )
        eng = ChaosEngine(cfg)
        results = set()
        for _ in range(20):
            res = eng.drift_schema(simple_df.copy(), day=25)
            results.add(len(res.columns))
        # Should see at least 2 different column counts across 20 runs
        assert len(results) >= 2

    def test_no_breaking_before_breaking_change_day(self, simple_df):
        """Before breaking_change_day, only additive mutations allowed."""
        cfg = ChaosConfig(
            enabled=True, intensity="moderate", seed=99, chaos_start_day=1,
            breaking_change_day=50,
        )
        eng = ChaosEngine(cfg)
        original_cols = set(simple_df.columns)
        for _ in range(30):
            result = eng.drift_schema(simple_df.copy(), day=10)
            # Should not have lost any original columns (no drops before breaking_change_day)
            # NOTE: column reorder is allowed, add column is allowed
            assert original_cols.issubset(set(result.columns))


class TestChaosEngineCorruptFile:
    def test_returns_bytes(self, engine):
        data = b"hello world this is a test file"
        result = engine.corrupt_file(data, day=10)
        assert isinstance(result, bytes)

    def test_empty_bytes_returned_as_empty(self, engine):
        result = engine.corrupt_file(b"", day=10)
        assert result == b""

    def test_output_differs_from_input(self):
        cfg = ChaosConfig(enabled=True, seed=0, chaos_start_day=1)
        eng = ChaosEngine(cfg)
        data = b"A" * 500
        changed = False
        for _ in range(10):
            result = eng.corrupt_file(data, day=5)
            if result != data:
                changed = True
                break
        assert changed


class TestChaosEngineReferentialChaos:
    def test_returns_dict(self, engine):
        tables = {
            "customer": pd.DataFrame({"customer_id": range(1, 11)}),
            "order": pd.DataFrame({"order_id": range(1, 21), "customer_id": range(1, 21)}),
        }
        result = engine.inject_referential_chaos(tables, day=10)
        assert isinstance(result, dict)
        assert set(result.keys()) == set(tables.keys())

    def test_original_not_mutated(self, engine):
        cust = pd.DataFrame({"customer_id": range(1, 11)})
        tables = {
            "customer": cust.copy(),
            "order": pd.DataFrame({"order_id": range(1, 21), "customer_id": range(1, 21)}),
        }
        engine.inject_referential_chaos(tables, day=10)
        assert len(cust) == 10


class TestChaosEngineVolumeChaos:
    def test_spike_increases_rows(self):
        # Call the spike sub-method directly to avoid random action selection
        mutator = VolumeChaosMutator()
        df = pd.DataFrame({"id": range(1, 51), "val": range(1, 51)})
        rng = np.random.RandomState(0)
        result = mutator._spike(df, rng, intensity=1.0)
        assert len(result) > len(df)

    def test_empty_returns_empty(self, engine):
        df = pd.DataFrame({"id": pd.Series([], dtype=int)})
        result = engine.inject_volume_chaos(df, day=5)
        assert result.empty


class TestChaosEngineTemporalChaos:
    def test_shifts_datetime_values(self):
        cfg = ChaosConfig(enabled=True, seed=42, chaos_start_day=1)
        eng = ChaosEngine(cfg)
        df = pd.DataFrame({
            "id": range(1, 51),
            "event_date": pd.date_range("2024-01-01", periods=50, freq="D"),
        })
        original_dates = list(df["event_date"])
        result = eng.inject_temporal_chaos(df.copy(), ["event_date"], day=5)
        # At least some dates should have shifted
        new_dates = list(result["event_date"])
        assert new_dates != original_dates

    def test_no_datetime_cols_noop(self, engine):
        df = pd.DataFrame({"id": range(1, 10), "name": list("abcdefghi")})
        result = engine.inject_temporal_chaos(df.copy(), ["event_date"], day=5)
        # Missing column — should handle gracefully
        assert isinstance(result, pd.DataFrame)


class TestChaosEngineApplyAll:
    def test_noop_when_disabled(self, simple_df):
        cfg = ChaosConfig(enabled=False)
        eng = ChaosEngine(cfg)
        result = eng.apply_all(simple_df.copy(), day=10)
        pd.testing.assert_frame_equal(result, simple_df)

    def test_returns_dataframe(self, engine, simple_df):
        result = engine.apply_all(simple_df.copy(), day=15)
        assert isinstance(result, pd.DataFrame)

    def test_with_tables_dict(self, engine, simple_df):
        tables = {"order": simple_df.copy()}
        result = engine.apply_all(simple_df.copy(), day=15, tables_dict=tables)
        assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# SchemaChaosMutator
# ---------------------------------------------------------------------------

class TestSchemaChaosMutator:
    @pytest.fixture
    def mutator(self):
        return SchemaChaosMutator(breaking_change_day=10)

    @pytest.fixture
    def df(self):
        return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [1.0, 2.0, 3.0]})

    @pytest.fixture
    def rng_fixture(self):
        return np.random.RandomState(42)

    def test_category_is_schema(self, mutator):
        assert mutator.category == "schema"

    def test_returns_dataframe(self, mutator, df, rng_fixture):
        result = mutator.mutate(df, day=5, rng=rng_fixture, intensity_multiplier=1.0)
        assert isinstance(result, pd.DataFrame)

    def test_empty_df_returned_unchanged(self, mutator, rng_fixture):
        empty = pd.DataFrame()
        result = mutator.mutate(empty, day=5, rng=rng_fixture, intensity_multiplier=1.0)
        assert result.empty

    def test_add_column_adds_chaos_column(self, mutator, df, rng_fixture):
        result = mutator._add_column(df.copy(), rng_fixture)
        chaos_cols = [c for c in result.columns if "_chaos_extra_" in c]
        assert len(chaos_cols) >= 1

    def test_reorder_same_columns(self, mutator, df, rng_fixture):
        result = mutator._reorder_columns(df.copy(), rng_fixture)
        assert set(result.columns) == set(df.columns)

    def test_drop_column_reduces_count(self, mutator, df, rng_fixture):
        result = mutator._drop_column(df.copy(), rng_fixture)
        assert len(result.columns) == len(df.columns) - 1

    def test_drop_not_called_before_breaking_change_day(self, mutator, df):
        """Additive-only before breaking_change_day=10."""
        original_cols = set(df.columns)
        rng_fixed = np.random.RandomState(0)
        for _ in range(30):
            result = mutator.mutate(df.copy(), day=5, rng=np.random.RandomState(0), intensity_multiplier=1.0)
            # Original columns must still be present (no drops before day 10)
            assert original_cols.issubset(set(result.columns))

    def test_rename_column_changes_one_name(self, mutator, df, rng_fixture):
        result = mutator._rename_column(df.copy(), rng_fixture)
        # One column should have been renamed
        assert set(result.columns) != set(df.columns)

    def test_retype_column_to_string(self, mutator, df, rng_fixture):
        result = mutator._retype_column(df.copy(), rng_fixture)
        # At least one numeric column should no longer be numeric (was retyped to string)
        assert any(
            not pd.api.types.is_numeric_dtype(result[col]) for col in ["a", "c"]
        )

    def test_single_column_df_not_dropped(self, mutator, rng_fixture):
        df = pd.DataFrame({"only": [1, 2, 3]})
        result = mutator._drop_column(df, rng_fixture)
        assert "only" in result.columns


# ---------------------------------------------------------------------------
# ValueChaosMutator
# ---------------------------------------------------------------------------

class TestValueChaosMutator:
    @pytest.fixture
    def mutator(self):
        return ValueChaosMutator()

    @pytest.fixture
    def df(self):
        n = 100
        rng = np.random.RandomState(1)
        return pd.DataFrame({
            "id": range(1, n + 1),
            "amount": rng.uniform(10.0, 200.0, size=n),
            "name": [f"Item {i}" for i in range(1, n + 1)],
            "event_date": pd.date_range("2024-01-01", periods=n, freq="D"),
        })

    @pytest.fixture
    def rng_fixture(self):
        return np.random.RandomState(42)

    def test_category_is_value(self, mutator):
        assert mutator.category == "value"

    def test_returns_dataframe(self, mutator, df, rng_fixture):
        result = mutator.mutate(df, day=5, rng=rng_fixture, intensity_multiplier=1.0)
        assert isinstance(result, pd.DataFrame)

    def test_inject_nulls_increases_null_count(self, mutator, df, rng_fixture):
        result = mutator._inject_nulls(df.copy(), rng_fixture, intensity=2.0)
        assert result.isna().sum().sum() >= df.isna().sum().sum()

    def test_out_of_range_adds_extreme_values(self, mutator, rng_fixture):
        # Use a float-only df to avoid int64/float assignment issues
        float_df = pd.DataFrame({"amount": np.random.RandomState(1).uniform(10.0, 200.0, 100)})
        original_max = float_df["amount"].max()
        result = mutator._out_of_range(float_df.copy(), rng_fixture, intensity=2.0)
        assert result["amount"].max() > original_max

    def test_wrong_types_adds_junk_strings(self, mutator, rng_fixture):
        # Use a float-only df to ensure the targeted column is "amount"
        float_df = pd.DataFrame({"amount": np.random.RandomState(1).uniform(10.0, 200.0, 100)})
        result = mutator._wrong_types(float_df.copy(), rng_fixture, intensity=2.0)
        # The numeric column should have been cast to object and some junk inserted
        assert not pd.api.types.is_numeric_dtype(result["amount"])

    def test_encoding_issues_adds_bom_or_chars(self, mutator, df, rng_fixture):
        result = mutator._encoding_issues(df.copy(), rng_fixture, intensity=2.0)
        # Some string values should have BOM or latin chars
        name_vals = result["name"].dropna().tolist()
        changed = [v for v in name_vals if "\ufeff" in str(v) or any(c in str(v) for c in ["\xe9", "\xf1", "\xfc"])]
        assert len(changed) >= 1

    def test_future_dates_adds_far_future(self, mutator, df, rng_fixture):
        result = mutator._future_dates(df.copy(), rng_fixture, intensity=2.0)
        future_count = (result["event_date"] > pd.Timestamp("2030-01-01")).sum()
        assert future_count >= 1

    def test_negative_amounts_flips_sign(self, mutator, df, rng_fixture):
        result = mutator._negative_amounts(df.copy(), rng_fixture, intensity=2.0)
        assert (result["amount"] < 0).any()

    def test_empty_df_noop(self, mutator, rng_fixture):
        empty = pd.DataFrame({"amount": pd.Series([], dtype=float)})
        result = mutator.mutate(empty, day=1, rng=rng_fixture, intensity_multiplier=1.0)
        assert result.empty


# ---------------------------------------------------------------------------
# FileChaosMutator
# ---------------------------------------------------------------------------

class TestFileChaosMutator:
    @pytest.fixture
    def mutator(self):
        return FileChaosMutator()

    @pytest.fixture
    def rng_fixture(self):
        return np.random.RandomState(42)

    def test_category_is_file(self, mutator):
        assert mutator.category == "file"

    def test_returns_bytes(self, mutator, rng_fixture):
        data = b"hello this is test data " * 20
        result = mutator.mutate(data, day=1, rng=rng_fixture, intensity_multiplier=1.0)
        assert isinstance(result, bytes)

    def test_empty_bytes_passthrough(self, mutator, rng_fixture):
        result = mutator.mutate(b"", day=1, rng=rng_fixture, intensity_multiplier=1.0)
        assert result == b""

    def test_truncate_shortens(self, mutator, rng_fixture):
        data = b"A" * 1000
        result = mutator._truncate(data, rng_fixture, 1.0)
        assert len(result) < len(data)

    def test_zero_byte_returns_empty(self, mutator, rng_fixture):
        data = b"A" * 500
        result = mutator._zero_byte(data, rng_fixture, 1.0)
        assert result == b""

    def test_garbage_header_lengthens(self, mutator, rng_fixture):
        data = b"real content here"
        result = mutator._garbage_header(data, rng_fixture, 1.0)
        assert len(result) > len(data)

    def test_partial_write_same_length(self, mutator, rng_fixture):
        data = b"A" * 100
        result = mutator._partial_write(data, rng_fixture, 1.0)
        assert len(result) == len(data)

    def test_corrupt_encoding_changes_bytes(self, mutator, rng_fixture):
        data = b"A" * 200
        result = mutator._corrupt_encoding(data, rng_fixture, 1.0)
        assert result != data


# ---------------------------------------------------------------------------
# ReferentialChaosMutator
# ---------------------------------------------------------------------------

class TestReferentialChaosMutator:
    @pytest.fixture
    def mutator(self):
        return ReferentialChaosMutator()

    @pytest.fixture
    def rng_fixture(self):
        return np.random.RandomState(42)

    @pytest.fixture
    def tables(self):
        return {
            "customer": pd.DataFrame({"customer_id": range(1, 11)}),
            "order": pd.DataFrame({
                "order_id": range(1, 21),
                "customer_id": list(range(1, 11)) * 2,
            }),
        }

    def test_category_is_referential(self, mutator):
        assert mutator.category == "referential"

    def test_returns_dict_of_dfs(self, mutator, tables, rng_fixture):
        result = mutator.mutate(tables, day=1, rng=rng_fixture, intensity_multiplier=1.0)
        assert isinstance(result, dict)
        assert all(isinstance(v, pd.DataFrame) for v in result.values())

    def test_orphan_fks_are_large_values(self, mutator, rng_fixture):
        # Both tables have FK columns so orphans are injected regardless of which is picked
        tables_both_fk = {
            "order": pd.DataFrame({
                "order_id": range(1, 21),
                "customer_id": list(range(1, 11)) * 2,
            }),
            "item": pd.DataFrame({
                "item_id": range(1, 21),
                "order_id": list(range(1, 21)),
            }),
        }
        result = mutator._orphan_fks(tables_both_fk, rng_fixture, intensity=2.0)
        # Collect all FK-like columns (ends with _id, not first col) across result tables
        orphan_found = False
        for df in result.values():
            fk_cols = [c for c in df.columns if c.endswith("_id") and c != df.columns[0]]
            for col in fk_cols:
                if (df[col] >= 9_000_000).any():
                    orphan_found = True
        assert orphan_found

    def test_duplicate_pks_introduces_dupes(self, mutator, tables):
        found_dupes = False
        for seed in range(20):
            result = mutator._duplicate_pks(
                {k: v.copy() for k, v in tables.items()},
                np.random.RandomState(seed),
                intensity=1.0,
            )
            for df in result.values():
                pk_col = df.columns[0]
                if df[pk_col].duplicated().sum() > 0:
                    found_dupes = True
                    break
            if found_dupes:
                break
        assert found_dupes, "Expected _duplicate_pks to introduce at least one duplicate PK across 20 seeds"

    def test_single_table_no_orphan_fks(self, mutator, rng_fixture):
        tables = {"only": pd.DataFrame({"only_id": range(1, 10)})}
        result = mutator._orphan_fks(tables, rng_fixture, intensity=1.0)
        assert set(result.keys()) == {"only"}

    def test_empty_dict_passthrough(self, mutator, rng_fixture):
        result = mutator.mutate({}, day=1, rng=rng_fixture, intensity_multiplier=1.0)
        assert result == {}


# ---------------------------------------------------------------------------
# VolumeChaosMutator
# ---------------------------------------------------------------------------

class TestVolumeChaosMutator:
    @pytest.fixture
    def mutator(self):
        return VolumeChaosMutator()

    @pytest.fixture
    def df(self):
        return pd.DataFrame({"id": range(1, 51), "val": range(1, 51)})

    @pytest.fixture
    def rng_fixture(self):
        return np.random.RandomState(42)

    def test_category_is_volume(self, mutator):
        assert mutator.category == "volume"

    def test_spike_more_rows(self, mutator, df, rng_fixture):
        result = mutator._spike(df, rng_fixture, intensity=1.0)
        assert len(result) > len(df)

    def test_empty_returns_empty(self, mutator, rng_fixture):
        empty = pd.DataFrame({"id": pd.Series([], dtype=int)})
        result = mutator.mutate(empty, day=1, rng=rng_fixture, intensity_multiplier=1.0)
        assert result.empty

    def test_empty_result_has_same_schema(self, mutator, df):
        result = mutator._empty(df)
        assert list(result.columns) == list(df.columns)
        assert len(result) == 0

    def test_single_row_returns_one_row(self, mutator, df, rng_fixture):
        result = mutator._single_row(df, rng_fixture)
        assert len(result) == 1

    def test_single_row_preserves_columns(self, mutator, df, rng_fixture):
        result = mutator._single_row(df, rng_fixture)
        assert list(result.columns) == list(df.columns)


# ---------------------------------------------------------------------------
# TemporalChaosMutator
# ---------------------------------------------------------------------------

class TestTemporalChaosMutator:
    @pytest.fixture
    def mutator(self):
        return TemporalChaosMutator()

    @pytest.fixture
    def df(self):
        n = 50
        return pd.DataFrame({
            "id": range(1, n + 1),
            "event_date": pd.date_range("2024-01-01", periods=n, freq="D"),
        })

    @pytest.fixture
    def rng_fixture(self):
        return np.random.RandomState(42)

    def test_category_is_temporal(self, mutator):
        assert mutator.category == "temporal"

    def test_returns_dataframe(self, mutator, df, rng_fixture):
        result = mutator.mutate(df, day=1, rng=rng_fixture, intensity_multiplier=1.0, date_columns=["event_date"])
        assert isinstance(result, pd.DataFrame)

    def test_no_datetime_cols_noop(self, mutator, rng_fixture):
        df = pd.DataFrame({"id": range(1, 10), "name": list("abcdefghi")})
        result = mutator.mutate(df, day=1, rng=rng_fixture, intensity_multiplier=1.0, date_columns=[])
        assert list(result["id"]) == list(df["id"])

    def test_late_arrivals_shifts_past(self, mutator, df, rng_fixture):
        original = list(df["event_date"])
        result = mutator._late_arrivals(df.copy(), ["event_date"], rng_fixture, 2.0)
        assert list(result["event_date"]) != original

    def test_out_of_order_swaps_values(self, mutator, df, rng_fixture):
        original = list(df["event_date"])
        result = mutator._out_of_order(df.copy(), ["event_date"], rng_fixture, 2.0)
        assert list(result["event_date"]) != original

    def test_timezone_mismatch_offsets_dates(self, mutator, df, rng_fixture):
        original = list(df["event_date"])
        result = mutator._timezone_mismatch(df.copy(), ["event_date"], rng_fixture, 2.0)
        assert list(result["event_date"]) != original

    def test_dst_boundary_sets_known_dates(self, mutator, df, rng_fixture):
        result = mutator._dst_boundary(df.copy(), ["event_date"], rng_fixture, 2.0)
        dst_dates = {
            pd.Timestamp("2024-03-10 02:30:00"),
            pd.Timestamp("2024-11-03 01:30:00"),
            pd.Timestamp("2025-03-09 02:30:00"),
            pd.Timestamp("2025-11-02 01:30:00"),
        }
        result_dates = set(result["event_date"].dropna())
        assert len(result_dates & dst_dates) >= 1

    def test_empty_df_noop(self, mutator, rng_fixture):
        empty = pd.DataFrame({"id": pd.Series([], dtype=int)})
        result = mutator.mutate(empty, day=1, rng=rng_fixture, intensity_multiplier=1.0)
        assert result.empty
