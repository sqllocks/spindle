"""Integration tests for the supply chain domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.supply_chain import SupplyChainDomain


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=SupplyChainDomain(), scale="small", seed=42)


class TestSupplyChainStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "warehouse", "supplier", "material", "purchase_order",
            "purchase_order_line", "inventory", "shipment",
            "shipment_event", "quality_inspection", "demand_forecast",
        }
        assert expected == set(result_small.tables.keys())

    def test_row_counts_small_scale(self, result_small):
        r = result_small
        assert len(r["warehouse"]) == 50
        assert len(r["supplier"]) == 200
        assert len(r["material"]) == 300
        assert len(r["purchase_order"]) == 2000
        assert len(r["purchase_order_line"]) == 6000
        assert len(r["inventory"]) == 900
        assert len(r["shipment"]) == 2400
        assert len(r["shipment_event"]) == 9600
        assert len(r["quality_inspection"]) == 720
        assert len(r["demand_forecast"]) == 1800

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        assert order.index("supplier") < order.index("purchase_order")
        assert order.index("purchase_order") < order.index("purchase_order_line")
        assert order.index("material") < order.index("purchase_order_line")
        assert order.index("warehouse") < order.index("inventory")
        assert order.index("material") < order.index("inventory")
        assert order.index("purchase_order") < order.index("shipment")
        assert order.index("shipment") < order.index("shipment_event")
        assert order.index("shipment") < order.index("quality_inspection")
        assert order.index("material") < order.index("demand_forecast")


class TestSupplyChainIntegrity:
    def test_fk_integrity_passes(self, result_small):
        errors = result_small.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_po_id_is_unique(self, result_small):
        assert result_small["purchase_order"]["po_id"].is_unique

    def test_warehouse_id_is_unique(self, result_small):
        assert result_small["warehouse"]["warehouse_id"].is_unique

    def test_supplier_id_is_unique(self, result_small):
        assert result_small["supplier"]["supplier_id"].is_unique

    def test_po_supplier_fk_valid(self, result_small):
        supplier_ids = set(result_small["supplier"]["supplier_id"])
        po_supplier_ids = set(result_small["purchase_order"]["supplier_id"])
        assert po_supplier_ids.issubset(supplier_ids)

    def test_po_line_po_fk_valid(self, result_small):
        po_ids = set(result_small["purchase_order"]["po_id"])
        line_po_ids = set(result_small["purchase_order_line"]["po_id"])
        assert line_po_ids.issubset(po_ids)

    def test_po_line_material_fk_valid(self, result_small):
        material_ids = set(result_small["material"]["material_id"])
        line_material_ids = set(result_small["purchase_order_line"]["material_id"])
        assert line_material_ids.issubset(material_ids)

    def test_shipment_po_fk_valid(self, result_small):
        po_ids = set(result_small["purchase_order"]["po_id"])
        shipment_po_ids = set(result_small["shipment"]["po_id"])
        assert shipment_po_ids.issubset(po_ids)

    def test_shipment_warehouse_fk_valid(self, result_small):
        wh_ids = set(result_small["warehouse"]["warehouse_id"])
        shipment_wh_ids = set(result_small["shipment"]["warehouse_id"])
        assert shipment_wh_ids.issubset(wh_ids)

    def test_inventory_warehouse_fk_valid(self, result_small):
        wh_ids = set(result_small["warehouse"]["warehouse_id"])
        inv_wh_ids = set(result_small["inventory"]["warehouse_id"])
        assert inv_wh_ids.issubset(wh_ids)


class TestSupplyChainDistributions:
    def test_po_status_distribution(self, result_small):
        statuses = result_small["purchase_order"]["status"].value_counts(normalize=True)
        assert 0.25 <= statuses.get("Delivered", 0) <= 0.45

    def test_shipment_status_distribution(self, result_small):
        statuses = result_small["shipment"]["status"].value_counts(normalize=True)
        assert 0.45 <= statuses.get("Delivered", 0) <= 0.65

    def test_warehouse_type_distribution(self, result_small):
        types = result_small["warehouse"]["warehouse_type"].value_counts(normalize=True)
        assert 0.15 <= types.get("Distribution Center", 0) <= 0.60

    def test_quality_result_in_set(self, result_small):
        results = set(result_small["quality_inspection"]["result"].unique())
        valid = {"Pass", "Fail", "Conditional Pass", "Pending Retest"}
        assert results.issubset(valid)

    def test_warehouse_has_coordinates(self, result_small):
        wh = result_small["warehouse"]
        assert wh["lat"].between(17.0, 72.0).all()
        assert wh["lng"].between(-180.0, -65.0).all()


class TestSupplyChainBusinessRules:
    def test_received_leq_ordered(self, result_small):
        lines = result_small["purchase_order_line"]
        violations = (lines["quantity_received"] > lines["quantity_ordered"] + 0.01).sum()
        assert violations == 0, f"{violations} PO lines have received > ordered"

    def test_shipment_delivery_after_ship(self, result_small):
        shipments = result_small["shipment"]
        violations = (shipments["delivery_date"] < shipments["ship_date"]).sum()
        assert violations == 0, f"{violations} shipments delivered before shipped"

    def test_po_total_amount_positive(self, result_small):
        pos = result_small["purchase_order"]
        assert (pos["total_amount"] > 0).all()

    def test_inventory_reserved_leq_on_hand(self, result_small):
        inv = result_small["inventory"]
        violations = (inv["quantity_reserved"] > inv["quantity_on_hand"] + 0.01).sum()
        assert violations == 0, f"{violations} inventory rows have reserved > on_hand"


class TestSupplyChainReproducibility:
    def test_same_seed_same_output(self):
        s = Spindle()
        r1 = s.generate(domain=SupplyChainDomain(), scale="small", seed=99)
        r2 = s.generate(domain=SupplyChainDomain(), scale="small", seed=99)
        import pandas as pd
        for table in r1.tables:
            pd.testing.assert_frame_equal(r1[table], r2[table])
