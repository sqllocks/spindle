"""E2E tests: composite domains — all 6 presets + ad-hoc combos + bridge FK validation."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle, get_preset, list_presets
from sqllocks_spindle.domains.composite import CompositeDomain
from sqllocks_spindle.domains.retail import RetailDomain
from sqllocks_spindle.domains.hr import HrDomain
from sqllocks_spindle.domains.financial import FinancialDomain

# Domain name → class mapping for preset resolution
_DOMAIN_MAP = {
    "retail": RetailDomain,
    "hr": HrDomain,
    "financial": FinancialDomain,
}

# Lazy import remaining domains only if needed
def _resolve_domain(name):
    if name in _DOMAIN_MAP:
        return _DOMAIN_MAP[name]()
    import importlib
    mod = importlib.import_module(f"sqllocks_spindle.domains.{name}")
    for attr in dir(mod):
        cls = getattr(mod, attr)
        if isinstance(cls, type) and hasattr(cls, 'get_schema') and attr.endswith('Domain'):
            return cls()
    raise ValueError(f"Cannot resolve domain: {name}")


PRESET_NAMES = ["enterprise", "healthcare_system", "smart_factory",
                "digital_commerce", "campus", "telecom_bundle"]


class TestPresetListing:
    def test_list_presets_returns_all(self):
        presets = list_presets()
        assert len(presets) >= 6

    def test_get_preset_returns_valid(self):
        for name in PRESET_NAMES:
            preset = get_preset(name)
            assert preset is not None
            assert len(preset.domains) >= 2


class TestPresetGeneration:
    @pytest.mark.parametrize("preset_name", PRESET_NAMES)
    def test_preset_generates_successfully(self, preset_name):
        preset = get_preset(preset_name)
        domains = [_resolve_domain(name) for name in preset.domains]
        comp = CompositeDomain(
            domains=domains,
            shared_entities=preset.shared_entities,
        )
        result = Spindle().generate(domain=comp, scale="small", seed=42)
        assert len(result.tables) > 0
        total_rows = sum(len(df) for df in result.tables.values())
        assert total_rows > 0, f"Preset {preset_name} generated 0 rows"


class TestAdHocComposites:
    def test_two_domain_via_campus_preset(self):
        """Education + HR via campus preset."""
        preset = get_preset("campus")
        domains = [_resolve_domain(name) for name in preset.domains]
        comp = CompositeDomain(domains=domains, shared_entities=preset.shared_entities)
        result = Spindle().generate(domain=comp, scale="small", seed=42)
        assert any("education_" in t for t in result.tables)
        assert any("hr_" in t for t in result.tables)
        errors = result.verify_integrity()
        assert errors == [], f"Campus preset FK errors: {errors}"

    def test_three_domain_with_preset(self):
        """Use the enterprise preset which properly configures retail+hr+financial."""
        preset = get_preset("enterprise")
        domains = [_resolve_domain(name) for name in preset.domains]
        comp = CompositeDomain(
            domains=domains,
            shared_entities=preset.shared_entities,
        )
        result = Spindle().generate(domain=comp, scale="small", seed=42)
        assert len(result.tables) > 0
        total_rows = sum(len(df) for df in result.tables.values())
        assert total_rows > 0


class TestBridgeFKColumns:
    def test_default_registry_creates_bridge_columns(self):
        """Cross-domain relationships via default registry must have bridge FK columns."""
        comp = CompositeDomain(domains=[RetailDomain(), HrDomain()])
        schema = comp.get_schema()
        xdomain_rels = [r for r in schema.relationships if r.name.startswith("xdomain_")]
        assert len(xdomain_rels) > 0, "No cross-domain relationships created"
        for rel in xdomain_rels:
            child_table = schema.tables.get(rel.child)
            assert child_table is not None
            for c_col in rel.child_columns:
                assert c_col in child_table.columns, (
                    f"Bridge column {c_col} missing from {rel.child}"
                )

    def test_bridge_fk_values_valid(self):
        """Bridge FK columns must reference valid parent PKs (via campus preset)."""
        preset = get_preset("campus")
        domains = [_resolve_domain(name) for name in preset.domains]
        comp = CompositeDomain(domains=domains, shared_entities=preset.shared_entities)
        result = Spindle().generate(domain=comp, scale="small", seed=42)
        errors = result.verify_integrity()
        assert errors == [], f"Bridge FK integrity errors: {errors}"
