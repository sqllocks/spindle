"""Tests for composite domain presets (C7)."""

import pytest

from sqllocks_spindle.presets import PresetRegistry, get_preset, list_presets
from sqllocks_spindle.presets.registry import PresetDef


class TestPresetRegistry:
    def test_register_and_get(self):
        reg = PresetRegistry()
        preset = PresetDef(name="test", description="Test", domains=["retail"])
        reg.register(preset)
        assert reg.get("test") == preset

    def test_unknown_preset_raises(self):
        reg = PresetRegistry()
        with pytest.raises(KeyError):
            reg.get("nonexistent")

    def test_list_presets(self):
        reg = PresetRegistry()
        reg.register(PresetDef(name="a", description="A", domains=["retail"]))
        reg.register(PresetDef(name="b", description="B", domains=["hr"]))
        assert len(reg.list()) == 2

    def test_available_names(self):
        reg = PresetRegistry()
        reg.register(PresetDef(name="x", description="X", domains=["retail"]))
        assert "x" in reg.available


class TestBuiltinPresets:
    def test_enterprise_exists(self):
        preset = get_preset("enterprise")
        assert "retail" in preset.domains
        assert "hr" in preset.domains
        assert "financial" in preset.domains

    def test_healthcare_system_exists(self):
        preset = get_preset("healthcare_system")
        assert "healthcare" in preset.domains
        assert "insurance" in preset.domains

    def test_smart_factory_exists(self):
        preset = get_preset("smart_factory")
        assert "manufacturing" in preset.domains

    def test_all_presets_have_valid_domains(self):
        from sqllocks_spindle.cli import _DOMAIN_REGISTRY

        for preset in list_presets():
            for domain_name in preset.domains:
                assert domain_name in _DOMAIN_REGISTRY, (
                    f"Preset '{preset.name}' references unknown domain '{domain_name}'"
                )

    def test_list_returns_all(self):
        presets = list_presets()
        assert len(presets) >= 6  # we defined 6 built-in presets
        names = [p.name for p in presets]
        assert "enterprise" in names
        assert "healthcare_system" in names

    def test_preset_builds_composite(self):
        """Integration test: verify preset can construct a CompositeDomain."""
        from sqllocks_spindle.domains.composite import CompositeDomain
        from sqllocks_spindle.cli import _resolve_domain

        preset = get_preset("enterprise")
        domains = [_resolve_domain(d, "3nf") for d in preset.domains]
        composite = CompositeDomain(
            domains=domains,
            shared_entities=preset.shared_entities if preset.shared_entities else None,
        )
        schema = composite.get_schema()
        assert len(schema.tables) > 0
        # Should have tables from all 3 domains (prefixed)
        table_names = set(schema.tables.keys())
        assert any("retail" in t for t in table_names)
        assert any("hr" in t for t in table_names)
        assert any("financial" in t for t in table_names)
