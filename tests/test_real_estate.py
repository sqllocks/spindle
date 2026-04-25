"""Integration tests for the real_estate domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.real_estate import RealEstateDomain


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=RealEstateDomain(), scale="small", seed=42)


class TestRealEstateStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "agent", "neighborhood", "property", "listing",
            "offer", "showing", "transaction", "appraisal", "inspection",
        }
        assert expected == set(result_small.tables.keys())

    def test_row_counts_small_scale(self, result_small):
        r = result_small
        assert len(r["agent"]) == 100
        assert len(r["neighborhood"]) == 50
        assert len(r["property"]) == 1000
        assert len(r["listing"]) == 1500
        assert len(r["showing"]) >= 5000   # per_parent with ratio 5.0
        assert len(r["transaction"]) >= 300
        assert len(r["inspection"]) >= 200
        assert len(r["appraisal"]) >= 200

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        assert order.index("neighborhood") < order.index("property")
        assert order.index("property") < order.index("listing")
        assert order.index("listing") < order.index("showing")
        assert order.index("listing") < order.index("offer")
        assert order.index("listing") < order.index("transaction")
        assert order.index("transaction") < order.index("inspection")
        assert order.index("transaction") < order.index("appraisal")


class TestRealEstateIntegrity:
    def test_agent_id_is_unique(self, result_small):
        assert result_small["agent"]["agent_id"].is_unique

    def test_property_id_is_unique(self, result_small):
        assert result_small["property"]["property_id"].is_unique

    def test_listing_id_is_unique(self, result_small):
        assert result_small["listing"]["listing_id"].is_unique

    def test_neighborhood_id_is_unique(self, result_small):
        assert result_small["neighborhood"]["neighborhood_id"].is_unique

    def test_property_neighborhood_fk_valid(self, result_small):
        nb_ids = set(result_small["neighborhood"]["neighborhood_id"])
        prop_nb_ids = set(result_small["property"]["neighborhood_id"])
        assert prop_nb_ids.issubset(nb_ids)

    def test_listing_property_fk_valid(self, result_small):
        prop_ids = set(result_small["property"]["property_id"])
        listing_prop_ids = set(result_small["listing"]["property_id"])
        assert listing_prop_ids.issubset(prop_ids)

    def test_listing_agent_fk_valid(self, result_small):
        agent_ids = set(result_small["agent"]["agent_id"])
        listing_agent_ids = set(result_small["listing"]["agent_id"])
        assert listing_agent_ids.issubset(agent_ids)

    def test_showing_listing_fk_valid(self, result_small):
        listing_ids = set(result_small["listing"]["listing_id"])
        showing_listing_ids = set(result_small["showing"]["listing_id"])
        assert showing_listing_ids.issubset(listing_ids)

    def test_offer_listing_fk_valid(self, result_small):
        listing_ids = set(result_small["listing"]["listing_id"])
        offer_listing_ids = set(result_small["offer"]["listing_id"])
        assert offer_listing_ids.issubset(listing_ids)

    def test_transaction_listing_fk_valid(self, result_small):
        listing_ids = set(result_small["listing"]["listing_id"])
        trans_listing_ids = set(result_small["transaction"]["listing_id"])
        assert trans_listing_ids.issubset(listing_ids)

    def test_inspection_transaction_fk_valid(self, result_small):
        trans_ids = set(result_small["transaction"]["transaction_id"])
        insp_ids = set(result_small["inspection"]["transaction_id"])
        assert insp_ids.issubset(trans_ids)

    def test_appraisal_transaction_fk_valid(self, result_small):
        trans_ids = set(result_small["transaction"]["transaction_id"])
        appr_ids = set(result_small["appraisal"]["transaction_id"])
        assert appr_ids.issubset(trans_ids)


class TestRealEstateDistributions:
    def test_listing_status_in_set(self, result_small):
        valid = {"Active", "Pending", "Sold", "Withdrawn", "Expired"}
        actual = set(result_small["listing"]["status"].unique())
        assert actual.issubset(valid)

    def test_listing_status_distribution(self, result_small):
        dist = result_small["listing"]["status"].value_counts(normalize=True)
        # Sold should be the most common (~40-50%)
        assert 0.30 <= dist.get("Sold", 0) <= 0.60

    def test_agent_specialization_in_set(self, result_small):
        valid = {"Residential", "Commercial", "Luxury", "Investment", "New Construction"}
        actual = set(result_small["agent"]["specialization"].unique())
        assert actual.issubset(valid)

    def test_showing_buyer_interest_level_in_set(self, result_small):
        valid = {"High", "Medium", "Low", "None"}
        actual = set(result_small["showing"]["buyer_interest_level"].unique())
        assert actual.issubset(valid)

    def test_inspection_result_in_set(self, result_small):
        valid = {"Pass", "Fail", "Conditional"}
        actual = set(result_small["inspection"]["result"].unique())
        assert actual.issubset(valid)

    def test_appraisal_condition_in_set(self, result_small):
        valid = {"Excellent", "Good", "Fair", "Poor"}
        actual = set(result_small["appraisal"]["condition_rating"].unique())
        assert actual.issubset(valid)

    def test_property_has_location_data(self, result_small):
        nb = result_small["neighborhood"]
        assert "city" in nb.columns
        assert "state" in nb.columns
        assert nb["city"].notna().all()


class TestRealEstateBusinessRules:
    def test_list_price_positive(self, result_small):
        assert (result_small["listing"]["list_price"] > 0).all()

    def test_offer_amount_positive(self, result_small):
        assert (result_small["offer"]["offer_amount"] > 0).all()

    def test_sale_price_positive(self, result_small):
        assert (result_small["transaction"]["sale_price"] > 0).all()

    def test_days_on_market_positive(self, result_small):
        assert (result_small["listing"]["days_on_market"] > 0).all()

    def test_sqft_positive(self, result_small):
        assert (result_small["property"]["sqft"] > 0).all()

    def test_assessed_value_positive(self, result_small):
        assert (result_small["property"]["assessed_value"] > 0).all()

    def test_commission_rate_reasonable(self, result_small):
        rate = result_small["transaction"]["commission_rate"]
        assert (rate >= 1.0).all() and (rate <= 10.0).all()


class TestRealEstateReproducibility:
    def test_same_seed_same_output(self):
        s = Spindle()
        r1 = s.generate(domain=RealEstateDomain(), scale="small", seed=99)
        r2 = s.generate(domain=RealEstateDomain(), scale="small", seed=99)
        assert list(r1["property"]["property_id"]) == list(r2["property"]["property_id"])
        assert list(r1["listing"]["listing_id"]) == list(r2["listing"]["listing_id"])
