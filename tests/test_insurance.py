"""Integration tests for the insurance domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.insurance import InsuranceDomain


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=InsuranceDomain(), scale="small", seed=42)


class TestInsuranceStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "agent", "policy_type", "policyholder", "policy",
            "claim", "claim_payment", "coverage",
            "premium_payment", "underwriting",
        }
        assert expected == set(result_small.tables.keys())

    def test_row_counts_small_scale(self, result_small):
        r = result_small
        assert len(r["agent"]) == 100
        assert len(r["policy_type"]) == 30
        assert len(r["policyholder"]) == 1000
        assert len(r["policy"]) == 1800
        assert len(r["claim"]) >= 300
        assert len(r["claim_payment"]) >= 400
        assert len(r["coverage"]) >= 3000
        assert len(r["premium_payment"]) >= 8000
        assert len(r["underwriting"]) == 1800

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        assert order.index("agent") < order.index("policyholder")
        assert order.index("policyholder") < order.index("policy")
        assert order.index("policy") < order.index("claim")
        assert order.index("policy") < order.index("coverage")
        assert order.index("policy") < order.index("premium_payment")
        assert order.index("policy") < order.index("underwriting")
        assert order.index("claim") < order.index("claim_payment")


class TestInsuranceIntegrity:
    def test_fk_integrity_passes(self, result_small):
        errors = result_small.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_policyholder_id_is_unique(self, result_small):
        assert result_small["policyholder"]["policyholder_id"].is_unique

    def test_policy_id_is_unique(self, result_small):
        assert result_small["policy"]["policy_id"].is_unique

    def test_claim_id_is_unique(self, result_small):
        assert result_small["claim"]["claim_id"].is_unique

    def test_agent_id_is_unique(self, result_small):
        assert result_small["agent"]["agent_id"].is_unique

    def test_policyholder_agent_fk_valid(self, result_small):
        agent_ids = set(result_small["agent"]["agent_id"])
        ph_agent_ids = set(result_small["policyholder"]["agent_id"])
        assert ph_agent_ids.issubset(agent_ids)

    def test_policy_policyholder_fk_valid(self, result_small):
        ph_ids = set(result_small["policyholder"]["policyholder_id"])
        policy_ph_ids = set(result_small["policy"]["policyholder_id"])
        assert policy_ph_ids.issubset(ph_ids)

    def test_policy_policy_type_fk_valid(self, result_small):
        pt_ids = set(result_small["policy_type"]["policy_type_id"])
        policy_pt_ids = set(result_small["policy"]["policy_type_id"])
        assert policy_pt_ids.issubset(pt_ids)

    def test_claim_policy_fk_valid(self, result_small):
        policy_ids = set(result_small["policy"]["policy_id"])
        claim_policy_ids = set(result_small["claim"]["policy_id"])
        assert claim_policy_ids.issubset(policy_ids)

    def test_coverage_policy_fk_valid(self, result_small):
        policy_ids = set(result_small["policy"]["policy_id"])
        cov_policy_ids = set(result_small["coverage"]["policy_id"])
        assert cov_policy_ids.issubset(policy_ids)

    def test_premium_payment_policy_fk_valid(self, result_small):
        policy_ids = set(result_small["policy"]["policy_id"])
        pp_policy_ids = set(result_small["premium_payment"]["policy_id"])
        assert pp_policy_ids.issubset(policy_ids)

    def test_claim_payment_claim_fk_valid(self, result_small):
        claim_ids = set(result_small["claim"]["claim_id"])
        cp_claim_ids = set(result_small["claim_payment"]["claim_id"])
        assert cp_claim_ids.issubset(claim_ids)

    def test_underwriting_policy_fk_valid(self, result_small):
        policy_ids = set(result_small["policy"]["policy_id"])
        uw_policy_ids = set(result_small["underwriting"]["policy_id"])
        assert uw_policy_ids.issubset(policy_ids)


class TestInsuranceDistributions:
    def test_policy_status_distribution(self, result_small):
        statuses = result_small["policy"]["status"].value_counts(normalize=True)
        assert 0.60 <= statuses.get("Active", 0) <= 0.88

    def test_policy_status_in_set(self, result_small):
        valid = {"Active", "Expired", "Cancelled", "Lapsed", "Pending"}
        assert set(result_small["policy"]["status"].unique()).issubset(valid)

    def test_claim_status_in_set(self, result_small):
        valid = {"Open", "Under Review", "Approved", "Paid", "Denied", "Closed"}
        assert set(result_small["claim"]["status"].unique()).issubset(valid)

    def test_claim_status_distribution(self, result_small):
        statuses = result_small["claim"]["status"].value_counts(normalize=True)
        assert 0.25 <= statuses.get("Approved", 0) <= 0.55

    def test_underwriting_risk_tier_distribution(self, result_small):
        tiers = result_small["underwriting"]["risk_tier"].value_counts(normalize=True)
        # "Low" should be the most common
        assert 0.25 <= tiers.get("Low", 0) <= 0.50

    def test_premium_payment_method_distribution(self, result_small):
        methods = result_small["premium_payment"]["payment_method"].value_counts(normalize=True)
        assert 0.30 <= methods.get("Auto-Pay", 0) <= 0.60

    def test_agent_specialization_in_set(self, result_small):
        valid = {"P&C", "Life", "Health", "Commercial", "Multi-Line"}
        assert set(result_small["agent"]["specialization"].unique()).issubset(valid)


class TestInsuranceBusinessRules:
    def test_claim_payment_amount_positive(self, result_small):
        assert (result_small["claim_payment"]["payment_amount"] > 0).all()

    def test_premium_payment_amount_positive(self, result_small):
        assert (result_small["premium_payment"]["amount"] > 0).all()

    def test_policy_premium_positive(self, result_small):
        assert (result_small["policy"]["premium_amount"] > 0).all()

    def test_coverage_limit_positive(self, result_small):
        assert (result_small["coverage"]["coverage_limit"] > 0).all()

    def test_underwriting_risk_score_range(self, result_small):
        rs = result_small["underwriting"]["risk_score"]
        assert (rs >= 0).all() and (rs <= 100).all()


class TestInsuranceReproducibility:
    def test_same_seed_same_output(self):
        s = Spindle()
        r1 = s.generate(domain=InsuranceDomain(), scale="small", seed=99)
        r2 = s.generate(domain=InsuranceDomain(), scale="small", seed=99)
        import pandas as pd
        for table in r1.tables:
            pd.testing.assert_frame_equal(r1[table], r2[table])
