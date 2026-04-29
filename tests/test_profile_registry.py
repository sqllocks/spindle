"""Unit tests for ProfileRegistry and RegistryProfile."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sqllocks_spindle.profiles import ProfileRegistry, RegistryProfile


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def reg(tmp_path):
    return ProfileRegistry(root=tmp_path / "profiles")


def _make_profile(system="salesforce", table="customer", name="prod-2026Q2", tags=None):
    return RegistryProfile(
        system=system,
        table=table,
        name=name,
        columns={
            "customer_id": {"dtype": "int64", "null_rate": 0.0, "cardinality": 1000},
            "email": {"dtype": "object", "null_rate": 0.02, "cardinality": 998},
            "revenue": {"dtype": "float64", "null_rate": 0.0, "mean": 250.0, "std": 80.0},
        },
        tags=tags or ["prod", "q2"],
        description="Production customer profile",
        source_rows=50000,
    )


# ---------------------------------------------------------------------------
# RegistryProfile
# ---------------------------------------------------------------------------


def test_profile_identity():
    p = _make_profile()
    assert p.identity == "salesforce/customer/prod-2026Q2"


def test_profile_to_dict_roundtrip():
    p = _make_profile()
    d = p.to_dict()
    p2 = RegistryProfile.from_dict(d)
    assert p2.identity == p.identity
    assert p2.columns == p.columns
    assert p2.tags == p.tags


def test_profile_save_load(tmp_path):
    p = _make_profile()
    path = tmp_path / "salesforce" / "customer" / "prod-2026Q2.json"
    p.save(path)
    assert path.exists()
    p2 = RegistryProfile.load(path)
    assert p2.identity == p.identity
    assert p2.source_rows == 50000


# ---------------------------------------------------------------------------
# ProfileRegistry CRUD
# ---------------------------------------------------------------------------


def test_registry_save_and_load(reg):
    p = _make_profile()
    reg.save(p)
    loaded = reg.load("salesforce/customer/prod-2026Q2")
    assert loaded.identity == p.identity
    assert loaded.source_rows == p.source_rows


def test_registry_load_missing(reg):
    with pytest.raises(FileNotFoundError):
        reg.load("no/such/profile")


def test_registry_invalid_identity(reg):
    with pytest.raises(ValueError):
        reg.load("bad-identity")


def test_registry_exists(reg):
    assert not reg.exists("salesforce/customer/prod-2026Q2")
    reg.save(_make_profile())
    assert reg.exists("salesforce/customer/prod-2026Q2")


def test_registry_delete(reg):
    reg.save(_make_profile())
    assert reg.exists("salesforce/customer/prod-2026Q2")
    reg.delete("salesforce/customer/prod-2026Q2")
    assert not reg.exists("salesforce/customer/prod-2026Q2")


def test_registry_index_updated_on_save(reg):
    reg.save(_make_profile())
    idx_path = reg.root / "_index.json"
    assert idx_path.exists()
    idx = json.loads(idx_path.read_text())
    assert "salesforce/customer/prod-2026Q2" in idx


# ---------------------------------------------------------------------------
# ProfileRegistry listing and search
# ---------------------------------------------------------------------------


def test_registry_list_all(reg):
    reg.save(_make_profile(system="sf", table="account", name="v1"))
    reg.save(_make_profile(system="sf", table="contact", name="v1"))
    entries = reg.list_all()
    assert len(entries) == 2


def test_registry_list_systems(reg):
    reg.save(_make_profile(system="salesforce", table="account", name="v1"))
    reg.save(_make_profile(system="sap", table="vendor", name="v1"))
    assert sorted(reg.list_systems()) == ["salesforce", "sap"]


def test_registry_list_tables(reg):
    reg.save(_make_profile(system="sf", table="account", name="v1"))
    reg.save(_make_profile(system="sf", table="contact", name="v1"))
    assert sorted(reg.list_tables("sf")) == ["account", "contact"]


def test_registry_search_by_system(reg):
    reg.save(_make_profile(system="sf", table="account", name="v1"))
    reg.save(_make_profile(system="sap", table="vendor", name="v1"))
    results = reg.search(system="sf")
    assert len(results) == 1
    assert results[0]["system"] == "sf"


def test_registry_search_by_tag(reg):
    reg.save(_make_profile(system="sf", table="account", name="v1", tags=["prod"]))
    reg.save(_make_profile(system="sf", table="contact", name="v1", tags=["dev"]))
    results = reg.search(tags=["prod"])
    assert len(results) == 1


def test_registry_search_by_query(reg):
    p1 = _make_profile(system="salesforce", table="customer", name="prod-q2")
    p1.description = "Customer profile"
    p2 = _make_profile(system="salesforce", table="order", name="dev-q2")
    p2.description = "Order profile"
    reg.save(p1)
    reg.save(p2)
    results = reg.search(query="customer")
    assert len(results) == 1
    assert results[0]["table"] == "customer"


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------


def test_registry_add_tags(reg):
    reg.save(_make_profile(tags=["prod"]))
    reg.add_tags("salesforce/customer/prod-2026Q2", ["sensitive", "pii"])
    loaded = reg.load("salesforce/customer/prod-2026Q2")
    assert "sensitive" in loaded.tags
    assert "pii" in loaded.tags
    assert "prod" in loaded.tags


def test_registry_add_tags_no_duplicates(reg):
    reg.save(_make_profile(tags=["prod"]))
    reg.add_tags("salesforce/customer/prod-2026Q2", ["prod"])
    loaded = reg.load("salesforce/customer/prod-2026Q2")
    assert loaded.tags.count("prod") == 1


def test_registry_remove_tags(reg):
    reg.save(_make_profile(tags=["prod", "sensitive"]))
    reg.remove_tags("salesforce/customer/prod-2026Q2", ["sensitive"])
    loaded = reg.load("salesforce/customer/prod-2026Q2")
    assert "sensitive" not in loaded.tags
    assert "prod" in loaded.tags


# ---------------------------------------------------------------------------
# Bulk import
# ---------------------------------------------------------------------------


def test_registry_import_from_dir(reg, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    for name in ["v1", "v2", "v3"]:
        p = _make_profile(system="sf", table="account", name=name)
        dest = source / "sf" / "account" / f"{name}.json"
        p.save(dest)
    imported = reg.import_from_dir(source)
    assert len(imported) == 3


def test_registry_import_skips_existing(reg, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    p = _make_profile(system="sf", table="account", name="v1")
    dest = source / "sf" / "account" / "v1.json"
    p.save(dest)
    reg.save(p)
    imported = reg.import_from_dir(source, overwrite=False)
    assert len(imported) == 0


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------


def test_registry_diff_added_removed(reg):
    a = RegistryProfile(
        system="sf", table="customer", name="v1",
        columns={"id": {"dtype": "int64"}, "email": {"dtype": "object"}}
    )
    b = RegistryProfile(
        system="sf", table="customer", name="v2",
        columns={"id": {"dtype": "int64"}, "phone": {"dtype": "object"}}
    )
    reg.save(a)
    reg.save(b)
    result = reg.diff("sf/customer/v1", "sf/customer/v2")
    assert "email" in result["removed"]
    assert "phone" in result["added"]
    assert "changed" in result


def test_registry_diff_changed(reg):
    a = RegistryProfile(system="sf", table="customer", name="v1",
                        columns={"rev": {"dtype": "float64", "mean": 100.0}})
    b = RegistryProfile(system="sf", table="customer", name="v2",
                        columns={"rev": {"dtype": "float64", "mean": 200.0}})
    reg.save(a)
    reg.save(b)
    result = reg.diff("sf/customer/v1", "sf/customer/v2")
    assert "rev" in result["changed"]


def test_registry_diff_identical(reg):
    a = _make_profile(name="v1")
    b = _make_profile(name="v2")
    reg.save(a)
    reg.save(b)
    result = reg.diff("salesforce/customer/v1", "salesforce/customer/v2")
    assert not any(result.values())


# ---------------------------------------------------------------------------
# Reindex
# ---------------------------------------------------------------------------


def test_registry_reindex(reg, tmp_path):
    p = _make_profile()
    path = reg.root / "salesforce" / "customer" / "prod-2026Q2.json"
    p.save(path)
    (reg.root / "_index.json").unlink(missing_ok=True)
    count = reg.reindex()
    assert count == 1
    assert reg.exists("salesforce/customer/prod-2026Q2")
