"""Integration tests for the healthcare domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import HealthcareDomain, Spindle


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=HealthcareDomain(), scale="small", seed=42)


class TestHealthcareStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "provider", "facility", "patient", "encounter",
            "diagnosis", "procedure", "medication", "claim", "claim_line",
        }
        assert expected == set(result_small.tables.keys())

    def test_row_counts_small_scale(self, result_small):
        r = result_small
        assert len(r["patient"]) == 500
        assert len(r["provider"]) == 100
        assert len(r["facility"]) == 50
        assert len(r["encounter"]) == 2500
        assert len(r["diagnosis"]) == 4500
        assert len(r["procedure"]) == 3000
        assert len(r["medication"]) == 2250
        assert len(r["claim"]) == 2375

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        # Parents before children
        assert order.index("patient") < order.index("encounter")
        assert order.index("provider") < order.index("encounter")
        assert order.index("facility") < order.index("encounter")
        assert order.index("encounter") < order.index("diagnosis")
        assert order.index("encounter") < order.index("procedure")
        assert order.index("encounter") < order.index("medication")
        assert order.index("encounter") < order.index("claim")
        assert order.index("claim") < order.index("claim_line")


class TestHealthcareIntegrity:
    def test_fk_integrity_passes(self, result_small):
        errors = result_small.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_patient_id_is_unique(self, result_small):
        assert result_small["patient"]["patient_id"].is_unique

    def test_provider_id_is_unique(self, result_small):
        assert result_small["provider"]["provider_id"].is_unique

    def test_encounter_patient_fk_valid(self, result_small):
        patient_ids = set(result_small["patient"]["patient_id"])
        encounter_patient_ids = set(result_small["encounter"]["patient_id"])
        assert encounter_patient_ids.issubset(patient_ids)

    def test_encounter_provider_fk_valid(self, result_small):
        provider_ids = set(result_small["provider"]["provider_id"])
        encounter_provider_ids = set(result_small["encounter"]["provider_id"])
        assert encounter_provider_ids.issubset(provider_ids)

    def test_encounter_facility_fk_valid(self, result_small):
        facility_ids = set(result_small["facility"]["facility_id"])
        encounter_facility_ids = set(result_small["encounter"]["facility_id"])
        assert encounter_facility_ids.issubset(facility_ids)

    def test_diagnosis_encounter_fk_valid(self, result_small):
        encounter_ids = set(result_small["encounter"]["encounter_id"])
        diagnosis_encounter_ids = set(result_small["diagnosis"]["encounter_id"])
        assert diagnosis_encounter_ids.issubset(encounter_ids)

    def test_claim_encounter_fk_valid(self, result_small):
        encounter_ids = set(result_small["encounter"]["encounter_id"])
        claim_encounter_ids = set(result_small["claim"]["encounter_id"])
        assert claim_encounter_ids.issubset(encounter_ids)

    def test_claim_line_claim_fk_valid(self, result_small):
        claim_ids = set(result_small["claim"]["claim_id"])
        cl_claim_ids = set(result_small["claim_line"]["claim_id"])
        assert cl_claim_ids.issubset(claim_ids)

    def test_claim_line_procedure_fk_valid(self, result_small):
        procedure_ids = set(result_small["procedure"]["procedure_id"])
        cl_procedure_ids = set(result_small["claim_line"]["procedure_id"])
        assert cl_procedure_ids.issubset(procedure_ids)

    def test_medication_encounter_fk_valid(self, result_small):
        encounter_ids = set(result_small["encounter"]["encounter_id"])
        med_encounter_ids = set(result_small["medication"]["encounter_id"])
        assert med_encounter_ids.issubset(encounter_ids)


class TestHealthcareDistributions:
    def test_pareto_patient_max_encounters(self, result_small):
        counts = result_small["encounter"]["patient_id"].value_counts()
        assert counts.max() <= 80

    def test_encounter_type_distribution(self, result_small):
        types = result_small["encounter"]["encounter_type"].value_counts(normalize=True)
        # Outpatient should be ~70% (CDC NCHS/HCUP 2022)
        assert 0.63 <= types.get("Outpatient", 0) <= 0.77

    def test_credential_distribution(self, result_small):
        creds = result_small["provider"]["credential"].value_counts(normalize=True)
        # MD should be ~55% (wide tolerance for N=100)
        assert 0.35 <= creds.get("MD", 0) <= 0.75

    def test_claim_status_distribution(self, result_small):
        statuses = result_small["claim"]["status"].value_counts(normalize=True)
        # Paid should be ~72% (KFF/CMS 2023)
        assert 0.62 <= statuses.get("Paid", 0) <= 0.82

    def test_facility_has_real_coordinates(self, result_small):
        fac = result_small["facility"]
        assert fac["lat"].between(17.0, 72.0).all(), "Latitudes outside US range"
        assert fac["lng"].between(-180.0, -65.0).all(), "Longitudes outside US range"

    def test_patient_has_real_addresses(self, result_small):
        pat = result_small["patient"]
        assert pat["state"].str.len().max() == 2
        assert pat["state"].str.isupper().all()
        assert pat["zip_code"].str.match(r"^\d{5}$").all()


class TestHealthcareBusinessRules:
    def test_encounter_date_after_registration(self, result_small):
        encounters = result_small["encounter"]
        patients = result_small["patient"]
        merged = encounters.merge(
            patients[["patient_id", "registration_date"]], on="patient_id", how="left"
        )
        import pandas as pd
        violations = (
            pd.to_datetime(merged["encounter_date"]) < pd.to_datetime(merged["registration_date"])
        ).sum()
        assert violations == 0, f"{violations} encounters before patient registration"

    def test_claim_filing_after_encounter(self, result_small):
        claims = result_small["claim"]
        encounters = result_small["encounter"]
        merged = claims.merge(
            encounters[["encounter_id", "encounter_date"]], on="encounter_id", how="left"
        )
        import pandas as pd
        violations = (
            pd.to_datetime(merged["filing_date"]) < pd.to_datetime(merged["encounter_date"])
        ).sum()
        assert violations == 0, f"{violations} claims filed before encounter date"

    def test_allowed_leq_charge_on_claim_line(self, result_small):
        cl = result_small["claim_line"]
        violations = (cl["allowed_amount"] > cl["charge_amount"] + 0.01).sum()
        assert violations == 0, f"{violations} claim lines have allowed > charge"

    def test_paid_leq_allowed_on_claim_line(self, result_small):
        cl = result_small["claim_line"]
        violations = (cl["paid_amount"] > cl["allowed_amount"] + 0.01).sum()
        assert violations == 0, f"{violations} claim lines have paid > allowed"

    def test_charge_amount_positive(self, result_small):
        cl = result_small["claim_line"]
        assert (cl["charge_amount"] > 0).all(), "Some charge amounts are <= 0"


class TestHealthcareComputedColumns:
    def test_claim_total_amount_backfilled(self, result_small):
        claim = result_small["claim"]
        positive_rate = (claim["total_amount"] > 0).mean()
        assert positive_rate >= 0.85, f"Only {positive_rate:.1%} of claims have total_amount > 0"

    def test_claim_allowed_amount_backfilled(self, result_small):
        claim = result_small["claim"]
        positive_rate = (claim["allowed_amount"] > 0).mean()
        assert positive_rate >= 0.85, f"Only {positive_rate:.1%} of claims have allowed_amount > 0"

    def test_claim_paid_amount_backfilled(self, result_small):
        claim = result_small["claim"]
        positive_rate = (claim["paid_amount"] > 0).mean()
        assert positive_rate >= 0.85, f"Only {positive_rate:.1%} of claims have paid_amount > 0"

    def test_claim_allowed_leq_total(self, result_small):
        claim = result_small["claim"]
        violations = (claim["allowed_amount"] > claim["total_amount"] + 0.01).sum()
        assert violations == 0, f"{violations} claims have allowed > total"


class TestHealthcareReferenceData:
    def test_icd10_codes_from_dataset(self, result_small):
        codes = set(result_small["diagnosis"]["icd10_code"].unique())
        # Should have a reasonable variety of ICD-10 codes
        assert len(codes) >= 10, f"Only {len(codes)} unique ICD-10 codes"

    def test_cpt_codes_from_dataset(self, result_small):
        codes = set(result_small["procedure"]["cpt_code"].unique())
        assert len(codes) >= 10, f"Only {len(codes)} unique CPT codes"

    def test_diagnosis_description_correlated(self, result_small):
        diag = result_small["diagnosis"]
        # Pick a known code and verify its description matches
        hypertension = diag[diag["icd10_code"] == "I10"]
        if len(hypertension) > 0:
            assert (hypertension["description"] == "Essential (primary) hypertension").all()

    def test_procedure_charge_correlated(self, result_small):
        proc = result_small["procedure"]
        # CPT 99213 should have charge of 150.00
        office_visits = proc[proc["cpt_code"] == "99213"]
        if len(office_visits) > 0:
            assert (office_visits["standard_charge"] == 150.00).all()


class TestHealthcareReproducibility:
    def test_same_seed_same_output(self):
        s = Spindle()
        r1 = s.generate(domain=HealthcareDomain(), scale="small", seed=99)
        r2 = s.generate(domain=HealthcareDomain(), scale="small", seed=99)
        import pandas as pd
        for table in r1.tables:
            pd.testing.assert_frame_equal(r1[table], r2[table])

    def test_different_seeds_different_output(self):
        s = Spindle()
        r1 = s.generate(domain=HealthcareDomain(), scale="small", seed=1)
        r2 = s.generate(domain=HealthcareDomain(), scale="small", seed=2)
        assert not r1["patient"]["email"].equals(r2["patient"]["email"])
