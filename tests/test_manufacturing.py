"""Integration tests for the manufacturing domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.manufacturing import ManufacturingDomain


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=ManufacturingDomain(), scale="small", seed=42)


class TestManufacturingStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "product", "bom", "production_line", "equipment",
            "downtime_event", "work_order", "production_metric",
            "quality_check", "defect",
        }
        assert expected == set(result_small.tables.keys())

    def test_row_counts_small_scale(self, result_small):
        r = result_small
        assert len(r["product"]) == 100
        assert len(r["bom"]) == 500
        assert len(r["production_line"]) == 20
        assert len(r["equipment"]) == 80
        assert len(r["downtime_event"]) == 320
        assert len(r["work_order"]) == 500
        assert len(r["production_metric"]) == 2500
        assert len(r["quality_check"]) == 1500
        assert len(r["defect"]) == 450

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        assert order.index("product") < order.index("bom")
        assert order.index("product") < order.index("work_order")
        assert order.index("production_line") < order.index("work_order")
        assert order.index("production_line") < order.index("equipment")
        assert order.index("equipment") < order.index("downtime_event")
        assert order.index("work_order") < order.index("quality_check")
        assert order.index("quality_check") < order.index("defect")
        assert order.index("work_order") < order.index("production_metric")


class TestManufacturingIntegrity:
    def test_fk_integrity_passes(self, result_small):
        errors = result_small.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_product_id_is_unique(self, result_small):
        assert result_small["product"]["product_id"].is_unique

    def test_wo_id_is_unique(self, result_small):
        assert result_small["work_order"]["wo_id"].is_unique

    def test_line_id_is_unique(self, result_small):
        assert result_small["production_line"]["line_id"].is_unique

    def test_equipment_id_is_unique(self, result_small):
        assert result_small["equipment"]["equipment_id"].is_unique

    def test_check_id_is_unique(self, result_small):
        assert result_small["quality_check"]["check_id"].is_unique

    def test_bom_product_fk_valid(self, result_small):
        product_ids = set(result_small["product"]["product_id"])
        bom_product_ids = set(result_small["bom"]["product_id"])
        assert bom_product_ids.issubset(product_ids)

    def test_work_order_product_fk_valid(self, result_small):
        product_ids = set(result_small["product"]["product_id"])
        wo_product_ids = set(result_small["work_order"]["product_id"])
        assert wo_product_ids.issubset(product_ids)

    def test_work_order_production_line_fk_valid(self, result_small):
        line_ids = set(result_small["production_line"]["line_id"])
        wo_line_ids = set(result_small["work_order"]["production_line_id"])
        assert wo_line_ids.issubset(line_ids)

    def test_equipment_production_line_fk_valid(self, result_small):
        line_ids = set(result_small["production_line"]["line_id"])
        eq_line_ids = set(result_small["equipment"]["production_line_id"])
        assert eq_line_ids.issubset(line_ids)

    def test_downtime_event_equipment_fk_valid(self, result_small):
        eq_ids = set(result_small["equipment"]["equipment_id"])
        dt_eq_ids = set(result_small["downtime_event"]["equipment_id"])
        assert dt_eq_ids.issubset(eq_ids)

    def test_quality_check_work_order_fk_valid(self, result_small):
        wo_ids = set(result_small["work_order"]["wo_id"])
        qc_wo_ids = set(result_small["quality_check"]["wo_id"])
        assert qc_wo_ids.issubset(wo_ids)

    def test_defect_quality_check_fk_valid(self, result_small):
        check_ids = set(result_small["quality_check"]["check_id"])
        defect_check_ids = set(result_small["defect"]["check_id"])
        assert defect_check_ids.issubset(check_ids)

    def test_production_metric_work_order_fk_valid(self, result_small):
        wo_ids = set(result_small["work_order"]["wo_id"])
        pm_wo_ids = set(result_small["production_metric"]["wo_id"])
        assert pm_wo_ids.issubset(wo_ids)


class TestManufacturingDistributions:
    def test_quality_check_result_distribution(self, result_small):
        results = result_small["quality_check"]["result"].value_counts(normalize=True)
        assert 0.72 <= results.get("Pass", 0) <= 0.92
        assert 0.03 <= results.get("Fail", 0) <= 0.15

    def test_defect_severity_distribution(self, result_small):
        sevs = result_small["defect"]["severity"].value_counts(normalize=True)
        assert 0.40 <= sevs.get("Minor", 0) <= 0.60
        assert 0.20 <= sevs.get("Cosmetic", 0) <= 0.40

    def test_work_order_status_distribution(self, result_small):
        statuses = result_small["work_order"]["status"].value_counts(normalize=True)
        assert 0.42 <= statuses.get("Completed", 0) <= 0.68

    def test_defect_severity_values_in_set(self, result_small):
        sevs = set(result_small["defect"]["severity"].unique())
        valid = {"Critical", "Major", "Minor", "Cosmetic"}
        assert sevs.issubset(valid)

    def test_work_order_priority_values_in_set(self, result_small):
        priorities = set(result_small["work_order"]["priority"].unique())
        valid = {"High", "Medium", "Low"}
        assert priorities.issubset(valid)

    def test_equipment_type_distribution(self, result_small):
        # equipment table has 80 rows, use moderate tolerances
        types = result_small["equipment"]["equipment_type"].value_counts(normalize=True)
        assert 0.12 <= types.get("CNC", 0) <= 0.40

    def test_production_line_facility_distribution(self, result_small):
        # production_line has 20 rows, use very wide tolerances
        facs = result_small["production_line"]["facility"].value_counts(normalize=True)
        assert 0.05 <= facs.get("Plant A", 0) <= 0.60


class TestManufacturingBusinessRules:
    def test_quantity_produced_leq_planned(self, result_small):
        wo = result_small["work_order"]
        violations = (wo["quantity_produced"] > wo["quantity_planned"]).sum()
        assert violations == 0, f"{violations} work orders have produced > planned"

    def test_sell_price_gte_unit_cost(self, result_small):
        products = result_small["product"]
        violations = (products["sell_price"] < products["unit_cost"] - 0.01).sum()
        assert violations == 0, f"{violations} products have sell_price < unit_cost"

    def test_oee_score_range(self, result_small):
        oee = result_small["production_metric"]["oee_score"]
        assert (oee >= 0.0).all(), "Some OEE scores are < 0"
        assert (oee <= 1.01).all(), "Some OEE scores are > 1"

    def test_yield_rate_range(self, result_small):
        yr = result_small["production_metric"]["yield_rate"]
        assert (yr >= 0.0).all(), "Some yield rates are < 0"
        assert (yr <= 1.01).all(), "Some yield rates are > 1"

    def test_scrap_rate_range(self, result_small):
        sr = result_small["production_metric"]["scrap_rate"]
        assert (sr >= 0.0).all(), "Some scrap rates are < 0"
        assert (sr <= 0.11).all(), "Some scrap rates are > 0.10"

    def test_quantity_planned_positive(self, result_small):
        wo = result_small["work_order"]
        assert (wo["quantity_planned"] >= 1).all(), "Some quantity_planned values < 1"

    def test_bom_quantity_required_positive(self, result_small):
        bom = result_small["bom"]
        assert (bom["quantity_required"] >= 1).all(), "Some quantity_required values < 1"


class TestManufacturingReproducibility:
    def test_same_seed_same_output(self):
        s = Spindle()
        r1 = s.generate(domain=ManufacturingDomain(), scale="small", seed=99)
        r2 = s.generate(domain=ManufacturingDomain(), scale="small", seed=99)
        import pandas as pd
        for table in r1.tables:
            pd.testing.assert_frame_equal(r1[table], r2[table])
