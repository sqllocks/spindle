"""Tests for ChaosConfig, ChaosCategory, and related helpers."""

from __future__ import annotations

import pytest

from sqllocks_spindle.chaos.config import (
    INTENSITY_PRESETS,
    ChaosCategory,
    ChaosConfig,
    ChaosOverride,
)


# ---------------------------------------------------------------------------
# ChaosCategory
# ---------------------------------------------------------------------------

class TestChaosCategory:
    def test_all_six_values(self):
        names = {c.value for c in ChaosCategory}
        assert names == {"schema", "value", "file", "referential", "temporal", "volume"}

    def test_enum_members_accessible(self):
        assert ChaosCategory.SCHEMA.value == "schema"
        assert ChaosCategory.VALUE.value == "value"
        assert ChaosCategory.FILE.value == "file"
        assert ChaosCategory.REFERENTIAL.value == "referential"
        assert ChaosCategory.TEMPORAL.value == "temporal"
        assert ChaosCategory.VOLUME.value == "volume"


# ---------------------------------------------------------------------------
# INTENSITY_PRESETS
# ---------------------------------------------------------------------------

class TestIntensityPresets:
    def test_all_presets_present(self):
        assert set(INTENSITY_PRESETS.keys()) == {"calm", "moderate", "stormy", "hurricane"}

    def test_preset_ordering(self):
        assert INTENSITY_PRESETS["calm"] < INTENSITY_PRESETS["moderate"]
        assert INTENSITY_PRESETS["moderate"] < INTENSITY_PRESETS["stormy"]
        assert INTENSITY_PRESETS["stormy"] < INTENSITY_PRESETS["hurricane"]

    def test_calm_less_than_one(self):
        assert INTENSITY_PRESETS["calm"] < 1.0

    def test_moderate_is_one(self):
        assert INTENSITY_PRESETS["moderate"] == pytest.approx(1.0)

    def test_stormy_is_2_5(self):
        assert INTENSITY_PRESETS["stormy"] == pytest.approx(2.5)


# ---------------------------------------------------------------------------
# ChaosConfig defaults
# ---------------------------------------------------------------------------

class TestChaosConfigDefaults:
    def test_disabled_by_default(self):
        cfg = ChaosConfig()
        assert cfg.enabled is False

    def test_default_seed(self):
        assert ChaosConfig().seed == 42

    def test_default_warmup(self):
        assert ChaosConfig().warmup_days == 7

    def test_default_chaos_start_day(self):
        assert ChaosConfig().chaos_start_day == 8

    def test_default_intensity(self):
        assert ChaosConfig().intensity == "moderate"

    def test_default_escalation(self):
        assert ChaosConfig().escalation == "gradual"

    def test_default_breaking_change_day(self):
        assert ChaosConfig().breaking_change_day == 20

    def test_default_categories_has_all_six(self):
        cfg = ChaosConfig()
        for cat in ChaosCategory:
            assert cat.value in cfg.categories


# ---------------------------------------------------------------------------
# intensity_multiplier
# ---------------------------------------------------------------------------

class TestIntensityMultiplier:
    def test_known_intensity_stormy(self):
        cfg = ChaosConfig(intensity="stormy")
        assert cfg.intensity_multiplier == pytest.approx(2.5)

    def test_known_intensity_calm(self):
        cfg = ChaosConfig(intensity="calm")
        assert cfg.intensity_multiplier == pytest.approx(0.25)

    def test_known_intensity_hurricane(self):
        cfg = ChaosConfig(intensity="hurricane")
        assert cfg.intensity_multiplier == pytest.approx(5.0)

    def test_unknown_intensity_fallback(self):
        cfg = ChaosConfig(intensity="blizzard")
        assert cfg.intensity_multiplier == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# is_category_enabled / category_weight
# ---------------------------------------------------------------------------

class TestCategoryHelpers:
    def test_is_category_enabled_true(self):
        cfg = ChaosConfig()
        assert cfg.is_category_enabled("value") is True

    def test_is_category_enabled_disabled_category(self):
        cfg = ChaosConfig(
            categories={"value": {"enabled": False, "weight": 0.15}}
        )
        assert cfg.is_category_enabled("value") is False

    def test_is_category_enabled_missing_key(self):
        cfg = ChaosConfig(categories={})
        assert cfg.is_category_enabled("value") is False

    def test_category_weight_enabled(self):
        cfg = ChaosConfig()
        w = cfg.category_weight("value")
        assert 0.0 < w <= 1.0

    def test_category_weight_disabled_returns_zero(self):
        cfg = ChaosConfig(
            categories={"value": {"enabled": False, "weight": 0.15}}
        )
        assert cfg.category_weight("value") == pytest.approx(0.0)

    def test_category_weight_missing_returns_zero(self):
        cfg = ChaosConfig(categories={})
        assert cfg.category_weight("value") == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# overrides_for_day
# ---------------------------------------------------------------------------

class TestOverridesForDay:
    def test_returns_override_on_matching_day(self):
        override = ChaosOverride(day=5, category="value")
        cfg = ChaosConfig(overrides=[override])
        result = cfg.overrides_for_day(5)
        assert len(result) == 1
        assert result[0].category == "value"

    def test_returns_empty_on_non_matching_day(self):
        override = ChaosOverride(day=5, category="value")
        cfg = ChaosConfig(overrides=[override])
        assert cfg.overrides_for_day(6) == []

    def test_returns_multiple_overrides_same_day(self):
        overrides = [
            ChaosOverride(day=3, category="value"),
            ChaosOverride(day=3, category="schema"),
        ]
        cfg = ChaosConfig(overrides=overrides)
        result = cfg.overrides_for_day(3)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# validate()
# ---------------------------------------------------------------------------

class TestChaosConfigValidate:
    def test_default_config_is_valid(self):
        errors = ChaosConfig().validate()
        assert errors == []

    def test_bad_intensity_produces_error(self):
        cfg = ChaosConfig(intensity="blizzard")
        errors = cfg.validate()
        assert any("intensity" in e.lower() or "blizzard" in e for e in errors)

    def test_bad_escalation_produces_error(self):
        cfg = ChaosConfig(escalation="sudden")
        errors = cfg.validate()
        assert any("escalation" in e.lower() or "sudden" in e for e in errors)

    def test_chaos_start_day_equal_to_warmup_produces_error(self):
        cfg = ChaosConfig(warmup_days=10, chaos_start_day=10)
        errors = cfg.validate()
        assert any("chaos_start_day" in e or "warmup" in e for e in errors)

    def test_chaos_start_day_less_than_warmup_produces_error(self):
        cfg = ChaosConfig(warmup_days=10, chaos_start_day=5)
        errors = cfg.validate()
        assert len(errors) >= 1

    def test_unknown_category_name_produces_error(self):
        cfg = ChaosConfig(categories={"nonexistent": {"enabled": True, "weight": 0.1}})
        errors = cfg.validate()
        assert any("nonexistent" in e for e in errors)

    def test_multiple_errors_collected(self):
        cfg = ChaosConfig(intensity="blizzard", escalation="sudden")
        errors = cfg.validate()
        assert len(errors) >= 2
