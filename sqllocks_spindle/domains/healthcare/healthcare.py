"""Healthcare domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class HealthcareDomain(Domain):
    """Healthcare domain — clinical encounters, claims, and medications.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships

    Tables (3NF mode):
        - provider: Doctors, nurses, and other healthcare professionals
        - facility: Hospitals, clinics, and care sites
        - patient: Patient demographics and insurance
        - encounter: Patient visits/admissions
        - diagnosis: ICD-10 diagnoses linked to encounters
        - procedure: CPT procedures linked to encounters
        - medication: Prescriptions linked to encounters
        - claim: Insurance claims linked to encounters
        - claim_line: Line items on claims linked to procedures
    """

    @property
    def name(self) -> str:
        return "healthcare"

    @property
    def description(self) -> str:
        return "Healthcare domain with patients, encounters, diagnoses, procedures, and claims"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build healthcare 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        _allowed_rate = self._dist("claim_line.allowed_rate", {"factor_min": 0.55, "factor_max": 0.90})
        _paid_rate = self._dist("claim_line.paid_rate", {"factor_min": 0.70, "factor_max": 1.00})

        schema_dict = {
            "model": {
                "name": f"healthcare_{self._schema_mode}",
                "description": f"Healthcare domain — {self._schema_mode} schema",
                "domain": "healthcare",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2022-01-01", "end": "2025-12-31"},
            },
            "tables": {
                # ── provider ──────────────────────────────────────
                "provider": {
                    "description": "Healthcare providers (doctors, nurses, etc.)",
                    "primary_key": ["provider_id"],
                    "columns": {
                        "provider_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "npi": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "pattern",
                                "format": "{seq:10}",
                            },
                        },
                        "first_name": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {"strategy": "faker", "provider": "first_name"},
                        },
                        "last_name": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {"strategy": "faker", "provider": "last_name"},
                        },
                        "credential": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("provider.credential", {
                                    "MD": 0.55,
                                    "DO": 0.11,
                                    "NP": 0.20,
                                    "PA": 0.10,
                                    "RN": 0.04,
                                }),
                            },
                        },
                        "specialty": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "specialties",
                                "field": "name",
                                "weight_field": "weight",
                            },
                        },
                        "is_active": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("provider.is_active", {"true": 0.90, "false": 0.10}),
                            },
                        },
                    },
                },

                # ── facility ──────────────────────────────────────
                "facility": {
                    "description": "Healthcare facilities (hospitals, clinics, etc.)",
                    "primary_key": ["facility_id"],
                    "columns": {
                        "facility_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "facility_name": {
                            "type": "string",
                            "max_length": 150,
                            "generator": {
                                "strategy": "pattern",
                                "format": "Medical Center #{seq:4}",
                            },
                        },
                        "facility_type": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("facility.facility_type", {
                                    "Hospital": 0.25,
                                    "Clinic": 0.35,
                                    "Urgent Care": 0.15,
                                    "Surgery Center": 0.10,
                                    "Rehabilitation": 0.08,
                                    "Psychiatric": 0.04,
                                    "Long-Term Care": 0.03,
                                }),
                            },
                        },
                        "bed_count": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "params": {"mean": 4.5, "sigma": 1.0, "min": 5, "max": 1500},
                            },
                        },
                        "street": {
                            "type": "string",
                            "max_length": 200,
                            "generator": {"strategy": "faker", "provider": "street_address"},
                        },
                        "city": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "record_sample",
                                "dataset": "us_zip_locations",
                                "field": "city",
                            },
                        },
                        "state": {
                            "type": "string",
                            "max_length": 2,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "state",
                            },
                        },
                        "zip_code": {
                            "type": "string",
                            "max_length": 5,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "zip",
                            },
                        },
                        "lat": {
                            "type": "decimal",
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "lat",
                            },
                        },
                        "lng": {
                            "type": "decimal",
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "lng",
                            },
                        },
                        "phone": {
                            "type": "string",
                            "max_length": 15,
                            "generator": {"strategy": "faker", "provider": "phone_number"},
                        },
                    },
                },

                # ── patient ───────────────────────────────────────
                "patient": {
                    "description": "Patient demographics and insurance",
                    "primary_key": ["patient_id"],
                    "columns": {
                        "patient_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "mrn": {
                            "type": "string",
                            "max_length": 12,
                            "generator": {
                                "strategy": "pattern",
                                "format": "MRN{seq:8}",
                            },
                        },
                        "first_name": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {"strategy": "faker", "provider": "first_name"},
                        },
                        "last_name": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {"strategy": "faker", "provider": "last_name"},
                        },
                        "date_of_birth": {
                            "type": "timestamp",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "uniform",
                                "range": {"start": "1930-01-01", "end": "2020-12-31"},
                            },
                        },
                        "gender": {
                            "type": "string",
                            "max_length": 5,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("patient.gender", {"M": 0.49, "F": 0.50, "NB": 0.01}),
                            },
                        },
                        "race": {
                            "type": "string",
                            "max_length": 30,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("patient.race", {
                                    "White": 0.578,
                                    "Hispanic": 0.187,
                                    "Black": 0.124,
                                    "Asian": 0.060,
                                    "Other": 0.051,
                                }),
                            },
                        },
                        "street": {
                            "type": "string",
                            "max_length": 200,
                            "generator": {"strategy": "faker", "provider": "street_address"},
                        },
                        "city": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "record_sample",
                                "dataset": "us_zip_locations",
                                "field": "city",
                            },
                        },
                        "state": {
                            "type": "string",
                            "max_length": 2,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "state",
                            },
                        },
                        "zip_code": {
                            "type": "string",
                            "max_length": 5,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "zip",
                            },
                        },
                        "phone": {
                            "type": "string",
                            "max_length": 15,
                            "generator": {"strategy": "faker", "provider": "phone_number"},
                        },
                        "email": {
                            "type": "string",
                            "max_length": 255,
                            "nullable": True,
                            "null_rate": 0.15,
                            "generator": {"strategy": "faker", "provider": "email"},
                        },
                        "insurance_plan": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "insurance_plans",
                                "field": "name",
                                "weight_field": "weight",
                            },
                        },
                        "insurance_member_id": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "pattern",
                                "format": "INS{seq:9}",
                            },
                        },
                        "registration_date": {
                            "type": "timestamp",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "uniform",
                                "range_ref": "model.date_range",
                            },
                        },
                        "is_active": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("patient.is_active", {"true": 0.88, "false": 0.12}),
                            },
                        },
                    },
                },

                # ── encounter ─────────────────────────────────────
                "encounter": {
                    "description": "Patient visits and admissions",
                    "primary_key": ["encounter_id"],
                    "columns": {
                        "encounter_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "patient_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "patient.patient_id",
                                "distribution": self._dist("encounter.patient_id", {"distribution": "pareto", "alpha": 1.05, "max_per_parent": 80}).get("distribution", "pareto"),
                                "params": {
                                    "alpha": self._dist("encounter.patient_id", {"distribution": "pareto", "alpha": 1.05, "max_per_parent": 80}).get("alpha", 1.05),
                                    "max_per_parent": self._dist("encounter.patient_id", {"distribution": "pareto", "alpha": 1.05, "max_per_parent": 80}).get("max_per_parent", 80),
                                },
                            },
                        },
                        "provider_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "provider.provider_id",
                                "distribution": self._dist("encounter.provider_id", {"distribution": "zipf", "alpha": 1.2}).get("distribution", "zipf"),
                                "params": {"alpha": self._dist("encounter.provider_id", {"distribution": "zipf", "alpha": 1.2}).get("alpha", 1.2)},
                            },
                        },
                        "facility_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "facility.facility_id",
                                "distribution": self._dist("encounter.facility_id", {"distribution": "zipf", "alpha": 1.4}).get("distribution", "zipf"),
                                "params": {"alpha": self._dist("encounter.facility_id", {"distribution": "zipf", "alpha": 1.4}).get("alpha", 1.4)},
                            },
                        },
                        "encounter_type": {
                            "type": "string",
                            "max_length": 30,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("encounter.encounter_type", {
                                    "Outpatient": 0.70,
                                    "Emergency": 0.10,
                                    "Inpatient": 0.08,
                                    "Telehealth": 0.08,
                                    "Observation": 0.04,
                                }),
                            },
                        },
                        "encounter_date": {
                            "type": "timestamp",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "seasonal",
                                "range_ref": "model.date_range",
                                "profiles": {
                                    "month": self._dist("encounter.encounter_date.month", {
                                        "Jan": 0.096, "Feb": 0.091, "Mar": 0.087,
                                        "Apr": 0.083, "May": 0.079, "Jun": 0.075,
                                        "Jul": 0.075, "Aug": 0.079, "Sep": 0.083,
                                        "Oct": 0.087, "Nov": 0.079, "Dec": 0.086,
                                    }),
                                    "day_of_week": self._dist("encounter.encounter_date.day_of_week", {
                                        "Mon": 0.195, "Tue": 0.175, "Wed": 0.175,
                                        "Thu": 0.175, "Fri": 0.165, "Sat": 0.065,
                                        "Sun": 0.050,
                                    }),
                                    "hour_of_day": self._dist("encounter.encounter_date.hour_of_day", {
                                        "distribution": "bimodal",
                                        "peaks": [9, 14],
                                        "std_dev": 2,
                                    }),
                                },
                            },
                        },
                        "status": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("encounter.status", {
                                    "Completed": 0.85,
                                    "In Progress": 0.05,
                                    "Cancelled": 0.05,
                                    "No Show": 0.05,
                                }),
                            },
                        },
                    },
                },

                # ── diagnosis ─────────────────────────────────────
                "diagnosis": {
                    "description": "ICD-10 diagnoses linked to encounters",
                    "primary_key": ["diagnosis_id"],
                    "columns": {
                        "diagnosis_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "encounter_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "encounter.encounter_id",
                            },
                        },
                        "icd10_code": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "record_sample",
                                "dataset": "icd10_codes",
                                "field": "code",
                                "weight_field": "weight",
                            },
                        },
                        "description": {
                            "type": "string",
                            "max_length": 255,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "icd10_codes",
                                "field": "description",
                            },
                        },
                        "diagnosis_type": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("diagnosis.diagnosis_type", {
                                    "Primary": 0.35,
                                    "Secondary": 0.40,
                                    "Admitting": 0.15,
                                    "External Cause": 0.10,
                                }),
                            },
                        },
                        "is_chronic": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("diagnosis.is_chronic", {"true": 0.30, "false": 0.70}),
                            },
                        },
                    },
                },

                # ── procedure ─────────────────────────────────────
                "procedure": {
                    "description": "CPT procedures linked to encounters",
                    "primary_key": ["procedure_id"],
                    "columns": {
                        "procedure_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "encounter_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "encounter.encounter_id",
                            },
                        },
                        "cpt_code": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "record_sample",
                                "dataset": "cpt_codes",
                                "field": "code",
                                "weight_field": "weight",
                            },
                        },
                        "cpt_description": {
                            "type": "string",
                            "max_length": 255,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "cpt_codes",
                                "field": "description",
                            },
                        },
                        "standard_charge": {
                            "type": "decimal",
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "cpt_codes",
                                "field": "charge",
                            },
                        },
                        "performing_provider_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "provider.provider_id",
                                "distribution": self._dist("procedure.performing_provider_id", {"distribution": "zipf", "alpha": 1.3}).get("distribution", "zipf"),
                                "params": {"alpha": self._dist("procedure.performing_provider_id", {"distribution": "zipf", "alpha": 1.3}).get("alpha", 1.3)},
                            },
                        },
                        "modifier": {
                            "type": "string",
                            "max_length": 5,
                            "nullable": True,
                            "null_rate": 0.70,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("procedure.modifier", {
                                    "26": 0.30,
                                    "TC": 0.25,
                                    "59": 0.20,
                                    "25": 0.15,
                                    "76": 0.10,
                                }),
                            },
                        },
                    },
                },

                # ── medication ────────────────────────────────────
                "medication": {
                    "description": "Prescriptions linked to encounters",
                    "primary_key": ["medication_id"],
                    "columns": {
                        "medication_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "encounter_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "encounter.encounter_id",
                            },
                        },
                        "patient_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "lookup",
                                "source_table": "encounter",
                                "source_column": "patient_id",
                                "via": "encounter_id",
                            },
                        },
                        "medication_name": {
                            "type": "string",
                            "max_length": 150,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "medication_names",
                                "field": "name",
                                "weight_field": "weight",
                            },
                        },
                        "dosage": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("medication.dosage", {
                                    "10mg": 0.20,
                                    "20mg": 0.15,
                                    "25mg": 0.12,
                                    "50mg": 0.15,
                                    "100mg": 0.12,
                                    "250mg": 0.10,
                                    "500mg": 0.10,
                                    "1000mg": 0.06,
                                }),
                            },
                        },
                        "frequency": {
                            "type": "string",
                            "max_length": 30,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("medication.frequency", {
                                    "Once daily": 0.35,
                                    "Twice daily": 0.25,
                                    "Three times daily": 0.10,
                                    "Four times daily": 0.05,
                                    "As needed": 0.15,
                                    "Weekly": 0.05,
                                    "Monthly": 0.05,
                                }),
                            },
                        },
                        "days_supply": {
                            "type": "integer",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("medication.days_supply", {
                                    30: 0.50,
                                    90: 0.25,
                                    14: 0.10,
                                    7: 0.08,
                                    60: 0.07,
                                }),
                            },
                        },
                    },
                },

                # ── claim ─────────────────────────────────────────
                "claim": {
                    "description": "Insurance claims linked to encounters",
                    "primary_key": ["claim_id"],
                    "columns": {
                        "claim_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "encounter_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "encounter.encounter_id",
                            },
                        },
                        "patient_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "lookup",
                                "source_table": "encounter",
                                "source_column": "patient_id",
                                "via": "encounter_id",
                            },
                        },
                        "claim_number": {
                            "type": "string",
                            "max_length": 15,
                            "generator": {
                                "strategy": "pattern",
                                "format": "CLM{seq:9}",
                            },
                        },
                        "claim_type": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("claim.claim_type", {
                                    "Professional": 0.55,
                                    "Institutional": 0.30,
                                    "Pharmacy": 0.15,
                                }),
                            },
                        },
                        "filing_date": {
                            "type": "timestamp",
                            "generator": {
                                "strategy": "derived",
                                "source": "encounter.encounter_date",
                                "via": "encounter_id",
                                "rule": "add_days",
                                "params": {
                                    "distribution": "log_normal",
                                    "mean": 1.5,
                                    "sigma": 0.6,
                                    "min": 0,
                                    "max": 30,
                                },
                            },
                        },
                        "status": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("claim.status", {
                                    "Paid": 0.72,
                                    "Denied": 0.15,
                                    "Pending": 0.05,
                                    "Partially Paid": 0.05,
                                    "Appealed": 0.03,
                                }),
                            },
                        },
                        "total_amount": {
                            "type": "decimal",
                            "generator": {
                                "strategy": "computed",
                                "rule": "sum_children",
                                "child_table": "claim_line",
                                "child_column": "charge_amount",
                            },
                        },
                        "allowed_amount": {
                            "type": "decimal",
                            "generator": {
                                "strategy": "computed",
                                "rule": "sum_children",
                                "child_table": "claim_line",
                                "child_column": "allowed_amount",
                            },
                        },
                        "paid_amount": {
                            "type": "decimal",
                            "generator": {
                                "strategy": "computed",
                                "rule": "sum_children",
                                "child_table": "claim_line",
                                "child_column": "paid_amount",
                            },
                        },
                    },
                },

                # ── claim_line ────────────────────────────────────
                "claim_line": {
                    "description": "Line items on insurance claims",
                    "primary_key": ["claim_line_id"],
                    "columns": {
                        "claim_line_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "claim_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "claim.claim_id",
                            },
                        },
                        "procedure_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "procedure.procedure_id",
                                "distribution": "zipf",
                                "params": {"alpha": 1.3},
                            },
                        },
                        "cpt_code": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "lookup",
                                "source_table": "procedure",
                                "source_column": "cpt_code",
                                "via": "procedure_id",
                            },
                        },
                        "charge_amount": {
                            "type": "decimal",
                            "generator": {
                                "strategy": "lookup",
                                "source_table": "procedure",
                                "source_column": "standard_charge",
                                "via": "procedure_id",
                            },
                        },
                        "allowed_amount": {
                            "type": "decimal",
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "charge_amount",
                                "rule": "multiply",
                                "params": {"factor_min": _allowed_rate["factor_min"], "factor_max": _allowed_rate["factor_max"]},
                            },
                        },
                        "paid_amount": {
                            "type": "decimal",
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "allowed_amount",
                                "rule": "multiply",
                                "params": {"factor_min": _paid_rate["factor_min"], "factor_max": _paid_rate["factor_max"]},
                            },
                        },
                        "patient_copay": {
                            "type": "decimal",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("claim_line.patient_copay", {
                                    0.00: 0.10,
                                    15.00: 0.15,
                                    25.00: 0.20,
                                    30.00: 0.15,
                                    40.00: 0.15,
                                    50.00: 0.10,
                                    75.00: 0.10,
                                    100.00: 0.05,
                                }),
                            },
                        },
                    },
                },
            },

            # ── relationships ─────────────────────────────────
            "relationships": [
                {
                    "name": "patient_encounters",
                    "parent": "patient",
                    "child": "encounter",
                    "parent_columns": ["patient_id"],
                    "child_columns": ["patient_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "provider_encounters",
                    "parent": "provider",
                    "child": "encounter",
                    "parent_columns": ["provider_id"],
                    "child_columns": ["provider_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "facility_encounters",
                    "parent": "facility",
                    "child": "encounter",
                    "parent_columns": ["facility_id"],
                    "child_columns": ["facility_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "encounter_diagnoses",
                    "parent": "encounter",
                    "child": "diagnosis",
                    "parent_columns": ["encounter_id"],
                    "child_columns": ["encounter_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "encounter_procedures",
                    "parent": "encounter",
                    "child": "procedure",
                    "parent_columns": ["encounter_id"],
                    "child_columns": ["encounter_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "encounter_medications",
                    "parent": "encounter",
                    "child": "medication",
                    "parent_columns": ["encounter_id"],
                    "child_columns": ["encounter_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "encounter_claims",
                    "parent": "encounter",
                    "child": "claim",
                    "parent_columns": ["encounter_id"],
                    "child_columns": ["encounter_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "claim_lines",
                    "parent": "claim",
                    "child": "claim_line",
                    "parent_columns": ["claim_id"],
                    "child_columns": ["claim_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "claim_line_procedures",
                    "parent": "procedure",
                    "child": "claim_line",
                    "parent_columns": ["procedure_id"],
                    "child_columns": ["procedure_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "provider_procedures",
                    "parent": "provider",
                    "child": "procedure",
                    "parent_columns": ["provider_id"],
                    "child_columns": ["performing_provider_id"],
                    "type": "one_to_many",
                },
            ],

            # ── business rules ────────────────────────────────
            "business_rules": [
                {
                    "name": "encounter_after_registration",
                    "type": "cross_table",
                    "rule": "encounter.encounter_date >= patient.registration_date",
                    "via": "patient_id",
                },
                {
                    "name": "claim_filed_after_encounter",
                    "type": "cross_table",
                    "rule": "claim.filing_date >= encounter.encounter_date",
                    "via": "encounter_id",
                },
                {
                    "name": "allowed_leq_charge",
                    "type": "cross_column",
                    "table": "claim_line",
                    "rule": "allowed_amount <= charge_amount",
                },
                {
                    "name": "paid_leq_allowed",
                    "type": "cross_column",
                    "table": "claim_line",
                    "rule": "paid_amount <= allowed_amount",
                },
                {
                    "name": "charge_amount_positive",
                    "type": "constraint",
                    "table": "claim_line",
                    "rule": "charge_amount > 0",
                },
            ],

            # ── generation config ─────────────────────────────
            "generation": {
                "scale": "small",
                "scales": {
                    "fabric_demo": {"patient": 100, "provider": 20, "encounter": 500},
                    "small": {"patient": 500, "provider": 100, "encounter": 2500},
                    "medium": {"patient": 25000, "provider": 2000, "encounter": 150000},
                    "large": {"patient": 250000, "provider": 15000, "encounter": 1500000},
                    "xlarge": {"patient": 2500000, "provider": 100000, "encounter": 20000000},
                    "warehouse": {"patient": 500000, "provider": 30000, "encounter": 5000000},
                    "xxl": {"patient": 10000000, "provider": 200000, "encounter": 100000000},
                    "xxxl": {"patient": 50000000, "provider": 500000, "encounter": 1000000000},
                },
                "derived_counts": {
                    "facility": {"fixed": 50},
                    "diagnosis": {"per_parent": "encounter", "ratio": self._ratio("diagnosis_per_encounter", 1.8)},
                    "procedure": {"per_parent": "encounter", "ratio": self._ratio("procedure_per_encounter", 1.2)},
                    "medication": {"per_parent": "encounter", "ratio": self._ratio("medication_per_encounter", 0.9)},
                    "claim": {"per_parent": "encounter", "ratio": self._ratio("claim_per_encounter", 0.95)},
                    "claim_line": {"per_parent": "claim", "ratio": self._ratio("claim_line_per_claim", 2.5)},
                },
                "output": {"format": "dataframe"},
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)

    def star_schema_map(self):
        """Return the star schema mapping for the Healthcare domain.

        Produces:
          - dim_patient    (from patient)
          - dim_provider   (from provider)
          - dim_facility   (from facility)
          - dim_date       (generated from encounter dates)
          - fact_encounter (from encounter)
          - fact_claim     (from claim + claim_line)
        """
        from sqllocks_spindle.transform.star_schema import DimSpec, FactSpec, StarSchemaMap

        return StarSchemaMap(
            dims={
                "dim_patient": DimSpec(
                    source="patient",
                    sk="sk_patient",
                    nk="patient_id",
                ),
                "dim_provider": DimSpec(
                    source="provider",
                    sk="sk_provider",
                    nk="provider_id",
                ),
                "dim_facility": DimSpec(
                    source="facility",
                    sk="sk_facility",
                    nk="facility_id",
                ),
            },
            facts={
                "fact_encounter": FactSpec(
                    primary="encounter",
                    fk_map={
                        "patient_id": "dim_patient",
                        "provider_id": "dim_provider",
                        "facility_id": "dim_facility",
                    },
                    date_cols=["encounter_date"],
                ),
                "fact_claim": FactSpec(
                    primary="claim_line",
                    joins=[{"table": "claim", "left_on": "claim_id", "right_on": "claim_id"}],
                    fk_map={
                        "patient_id": "dim_patient",
                        "provider_id": "dim_provider",
                        "facility_id": "dim_facility",
                    },
                    date_cols=["service_date"],
                ),
            },
        )

    def cdm_map(self):
        """Return the CDM entity map for the Healthcare domain."""
        from sqllocks_spindle.transform.cdm_mapper import CdmEntityMap

        return CdmEntityMap({
            "patient": "Patient",
            "provider": "Practitioner",
            "facility": "Account",
            "encounter": "Appointment",
            "diagnosis": "Condition",
            "procedure": "Procedure",
            "medication": "MedicationRequest",
            "claim": "Invoice",
            "claim_line": "InvoiceDetail",
        })
