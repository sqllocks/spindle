"""Tests for ScenarioCatalog."""
import pytest
from sqllocks_spindle.demo.catalog import ScenarioCatalog, ScenarioMeta


def test_builtin_scenarios():
    cat = ScenarioCatalog()
    names = [s.name for s in cat.list()]
    assert "retail" in names
    assert "adventureworks" in names
    assert "healthcare" in names
    assert "enterprise" in names


def test_get_known_scenario():
    cat = ScenarioCatalog()
    meta = cat.get("retail")
    assert meta.name == "retail"
    assert "inference" in meta.supported_modes


def test_get_unknown_raises():
    cat = ScenarioCatalog()
    with pytest.raises(KeyError):
        cat.get("nonexistent_scenario_xyz")


def test_register_custom():
    cat = ScenarioCatalog()
    custom = ScenarioMeta(
        name="my_custom", description="test",
        domains=["retail"], supported_modes=["inference"],
    )
    cat.register(custom)
    assert cat.get("my_custom").name == "my_custom"


def test_compose():
    cat = ScenarioCatalog()
    composed = cat.compose(["retail", "healthcare"], mode="seeding")
    assert "retail" in composed.domains
    assert "healthcare" in composed.domains
    assert "seeding" in composed.supported_modes


def test_adventureworks_meta():
    cat = ScenarioCatalog()
    aw = cat.get("adventureworks")
    assert "inference" in aw.supported_modes
    assert "conference" in aw.tags
