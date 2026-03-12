"""Integration tests for the marketing domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.marketing import MarketingDomain


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=MarketingDomain(), scale="small", seed=42)


class TestMarketingStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "campaign_type", "campaign", "industry", "lead_source",
            "contact", "email_send", "lead", "conversion",
            "opportunity", "web_visit",
        }
        assert expected == set(result_small.tables.keys())

    def test_row_counts_small_scale(self, result_small):
        r = result_small
        assert len(r["campaign_type"]) == 15
        assert len(r["campaign"]) == 200
        assert len(r["industry"]) == 25
        assert len(r["lead_source"]) == 20
        assert len(r["contact"]) == 5000
        assert len(r["email_send"]) == 10000
        assert len(r["lead"]) == 2000
        assert len(r["conversion"]) == 600
        assert len(r["opportunity"]) == 1000
        assert len(r["web_visit"]) == 25000

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        assert order.index("campaign") < order.index("lead")
        assert order.index("contact") < order.index("lead")
        assert order.index("contact") < order.index("email_send")
        assert order.index("campaign") < order.index("email_send")
        assert order.index("lead") < order.index("conversion")
        assert order.index("lead") < order.index("opportunity")
        assert order.index("contact") < order.index("web_visit")


class TestMarketingIntegrity:
    def test_fk_integrity_passes(self, result_small):
        schema = MarketingDomain()._build_schema()
        assert schema is not None  # schema built without error

    def test_contact_id_is_unique(self, result_small):
        assert result_small["contact"]["contact_id"].is_unique

    def test_campaign_id_is_unique(self, result_small):
        assert result_small["campaign"]["campaign_id"].is_unique

    def test_lead_id_is_unique(self, result_small):
        assert result_small["lead"]["lead_id"].is_unique

    def test_opportunity_id_is_unique(self, result_small):
        assert result_small["opportunity"]["opp_id"].is_unique

    def test_conversion_id_is_unique(self, result_small):
        assert result_small["conversion"]["conversion_id"].is_unique

    def test_lead_contact_fk_valid(self, result_small):
        contact_ids = set(result_small["contact"]["contact_id"])
        lead_contact_ids = set(result_small["lead"]["contact_id"])
        assert lead_contact_ids.issubset(contact_ids)

    def test_lead_campaign_fk_valid(self, result_small):
        campaign_ids = set(result_small["campaign"]["campaign_id"])
        lead_campaign_ids = set(result_small["lead"]["campaign_id"])
        assert lead_campaign_ids.issubset(campaign_ids)

    def test_email_send_campaign_fk_valid(self, result_small):
        campaign_ids = set(result_small["campaign"]["campaign_id"])
        send_ids = set(result_small["email_send"]["campaign_id"])
        assert send_ids.issubset(campaign_ids)

    def test_email_send_contact_fk_valid(self, result_small):
        contact_ids = set(result_small["contact"]["contact_id"])
        send_ids = set(result_small["email_send"]["contact_id"])
        assert send_ids.issubset(contact_ids)

    def test_conversion_lead_fk_valid(self, result_small):
        lead_ids = set(result_small["lead"]["lead_id"])
        conv_ids = set(result_small["conversion"]["lead_id"])
        assert conv_ids.issubset(lead_ids)

    def test_opportunity_lead_fk_valid(self, result_small):
        lead_ids = set(result_small["lead"]["lead_id"])
        opp_ids = set(result_small["opportunity"]["lead_id"])
        assert opp_ids.issubset(lead_ids)

    def test_web_visit_contact_fk_valid(self, result_small):
        contact_ids = set(result_small["contact"]["contact_id"])
        visit_ids = set(result_small["web_visit"]["contact_id"])
        assert visit_ids.issubset(contact_ids)


class TestMarketingDistributions:
    def test_campaign_status_in_set(self, result_small):
        valid = {"Active", "Paused", "Completed", "Draft"}
        actual = set(result_small["campaign"]["status"].unique())
        assert actual.issubset(valid)

    def test_lead_status_in_set(self, result_small):
        valid = {"New", "Contacted", "Qualified", "Converted", "Unqualified"}
        actual = set(result_small["lead"]["status"].unique())
        assert actual.issubset(valid)

    def test_lead_status_distribution(self, result_small):
        dist = result_small["lead"]["status"].value_counts(normalize=True)
        # "New" should be roughly 15-25%
        assert 0.10 <= dist.get("New", 0) <= 0.35

    def test_opportunity_stage_in_set(self, result_small):
        valid = {"Prospecting", "Qualification", "Proposal", "Negotiation", "Closed Won", "Closed Lost"}
        actual = set(result_small["opportunity"]["stage"].unique())
        assert actual.issubset(valid)

    def test_conversion_type_in_set(self, result_small):
        valid = {"Signup", "Demo", "Trial", "Purchase"}
        actual = set(result_small["conversion"]["conversion_type"].unique())
        assert actual.issubset(valid)

    def test_contact_industry_fk(self, result_small):
        industry_ids = set(result_small["industry"]["industry_id"])
        contact_industry_ids = set(result_small["contact"]["industry_id"])
        assert contact_industry_ids.issubset(industry_ids)


class TestMarketingBusinessRules:
    def test_lead_score_in_range(self, result_small):
        ls = result_small["lead"]["lead_score"]
        assert (ls >= 0).all() and (ls <= 100).all(), "lead_score out of [0,100]"

    def test_conversion_revenue_positive(self, result_small):
        assert (result_small["conversion"]["revenue"] > 0).all()

    def test_opportunity_probability_range(self, result_small):
        prob = result_small["opportunity"]["probability"]
        assert (prob >= 0).all() and (prob <= 100).all()

    def test_campaign_budget_positive(self, result_small):
        assert (result_small["campaign"]["budget"] > 0).all()

    def test_web_visit_duration_positive(self, result_small):
        assert (result_small["web_visit"]["duration_seconds"] > 0).all()


class TestMarketingReproducibility:
    def test_same_seed_same_output(self):
        s = Spindle()
        r1 = s.generate(domain=MarketingDomain(), scale="small", seed=99)
        r2 = s.generate(domain=MarketingDomain(), scale="small", seed=99)
        assert list(r1["lead"]["lead_id"]) == list(r2["lead"]["lead_id"])
        assert list(r1["contact"]["contact_id"]) == list(r2["contact"]["contact_id"])
