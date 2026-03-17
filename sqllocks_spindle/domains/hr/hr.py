"""Human resources domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class HrDomain(Domain):
    """Human resources domain — employees, departments, compensation, and performance.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships

    Tables (3NF mode):
        - department: Organizational departments (fixed: 30)
        - position: Job titles and levels (fixed: 80)
        - employee: Employees with manager hierarchy (anchor: 500 small)
        - compensation: Salary history per employee
        - performance_review: Annual performance reviews
        - time_off_request: PTO, sick, and personal leave
        - training: Training course catalog (fixed: 100)
        - training_enrollment: Employee training enrollments
        - termination: Employee terminations
    """

    @property
    def name(self) -> str:
        return "hr"

    @property
    def description(self) -> str:
        return "Human resources domain with employees, departments, compensation, and performance"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build HR 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = {
            "model": {
                "name": f"hr_{self._schema_mode}",
                "description": f"Human resources domain — {self._schema_mode} schema",
                "domain": "hr",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2020-01-01", "end": "2025-12-31"},
            },
            "tables": {
                # ── department ─────────────────────────────────
                "department": {
                    "description": "Organizational departments",
                    "primary_key": ["department_id"],
                    "columns": {
                        "department_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "department_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "department_names",
                            },
                        },
                        "cost_center": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "pattern",
                                "format": "CC-{seq:4}",
                            },
                        },
                        "is_active": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("department.is_active", {"true": 0.93, "false": 0.07}),
                            },
                        },
                    },
                },

                # ── position ───────────────────────────────────
                "position": {
                    "description": "Job titles and levels",
                    "primary_key": ["position_id"],
                    "columns": {
                        "position_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "position_title": {
                            "type": "string",
                            "max_length": 150,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "position_titles",
                            },
                        },
                        "pay_grade": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("position.pay_grade", {
                                    "G1": 0.15,
                                    "G2": 0.20,
                                    "G3": 0.25,
                                    "G4": 0.20,
                                    "G5": 0.12,
                                    "G6": 0.05,
                                    "G7": 0.03,
                                }),
                            },
                        },
                        "is_exempt": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("position.is_exempt", {"true": 0.60, "false": 0.40}),
                            },
                        },
                        "min_salary": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 30000.0,
                                "max": 120000.0,
                                "round": 2,
                            },
                        },
                        "max_salary": {
                            "type": "float",
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "min_salary",
                                "operation": "multiply",
                                "params": {"factor_min": 1.30, "factor_max": 1.80},
                            },
                        },
                    },
                },

                # ── employee ───────────────────────────────────
                "employee": {
                    "description": "Employees with department and position assignments",
                    "primary_key": ["employee_id"],
                    "columns": {
                        "employee_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
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
                        "email": {
                            "type": "string",
                            "max_length": 255,
                            "generator": {"strategy": "faker", "provider": "email"},
                        },
                        "phone": {
                            "type": "string",
                            "max_length": 20,
                            "nullable": True,
                            "null_rate": 0.08,
                            "generator": {"strategy": "faker", "provider": "phone_number"},
                        },
                        "hire_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2015-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "department_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "department.department_id",
                                "distribution": "zipf",
                                "alpha": 1.3,
                            },
                        },
                        "position_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "position.position_id",
                                "distribution": "zipf",
                                "alpha": 1.2,
                            },
                        },
                        "manager_id": {
                            "type": "integer",
                            "nullable": True,
                            "generator": {
                                "strategy": "self_referencing",
                                "pk_column": "employee_id",
                                "root_count": 5,
                                "max_depth": 4,
                            },
                        },
                        "employment_status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("employee.employment_status", {
                                    "Active": 0.82,
                                    "On Leave": 0.05,
                                    "Terminated": 0.10,
                                    "Retired": 0.03,
                                }),
                            },
                        },
                        "employment_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("employee.employment_type", {
                                    "Full-Time": 0.75,
                                    "Part-Time": 0.12,
                                    "Contract": 0.08,
                                    "Temporary": 0.05,
                                }),
                            },
                        },
                    },
                },

                # ── compensation ───────────────────────────────
                "compensation": {
                    "description": "Salary history with effective dates",
                    "primary_key": ["compensation_id"],
                    "columns": {
                        "compensation_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "employee_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "employee.employee_id",
                            },
                        },
                        "effective_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2015-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "base_salary": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 11.0,
                                "sigma": 0.5,
                                "min": 28000.0,
                                "max": 350000.0,
                                "round": 2,
                            },
                        },
                        "bonus_pct": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0.0,
                                "max": 25.0,
                                "round": 1,
                            },
                        },
                        "change_reason": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("compensation.change_reason", {
                                    "Annual Review": 0.40,
                                    "Promotion": 0.25,
                                    "Market Adjustment": 0.15,
                                    "New Hire": 0.12,
                                    "Transfer": 0.08,
                                }),
                            },
                        },
                    },
                },

                # ── performance_review ─────────────────────────
                "performance_review": {
                    "description": "Annual performance reviews with ratings",
                    "primary_key": ["review_id"],
                    "columns": {
                        "review_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "employee_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "employee.employee_id",
                            },
                        },
                        "review_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "review_period_year": {
                            "type": "integer",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("performance_review.review_period_year", {
                                    "2020": 0.15,
                                    "2021": 0.18,
                                    "2022": 0.20,
                                    "2023": 0.22,
                                    "2024": 0.25,
                                }),
                            },
                        },
                        "rating": {
                            "type": "integer",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("performance_review.rating", {
                                    "1": 0.05,
                                    "2": 0.12,
                                    "3": 0.45,
                                    "4": 0.28,
                                    "5": 0.10,
                                }),
                            },
                        },
                        "reviewer_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "employee.employee_id",
                            },
                        },
                        "comments": {
                            "type": "string",
                            "nullable": True,
                            "null_rate": 0.20,
                            "generator": {"strategy": "faker", "provider": "sentence"},
                        },
                    },
                },

                # ── time_off_request ───────────────────────────
                "time_off_request": {
                    "description": "PTO, sick, and personal leave requests",
                    "primary_key": ["request_id"],
                    "columns": {
                        "request_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "employee_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "employee.employee_id",
                            },
                        },
                        "leave_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("time_off_request.leave_type", {
                                    "PTO": 0.45,
                                    "Sick": 0.25,
                                    "Personal": 0.12,
                                    "Bereavement": 0.04,
                                    "Jury Duty": 0.03,
                                    "Parental": 0.06,
                                    "Unpaid": 0.05,
                                }),
                            },
                        },
                        "start_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "end_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "derived",
                                "source": "start_date",
                                "rule": "add_days",
                                "params": {"distribution": "uniform", "min": 1, "max": 14},
                            },
                        },
                        "hours_requested": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 4.0,
                                "max": 80.0,
                                "round": 1,
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("time_off_request.status", {
                                    "Approved": 0.75,
                                    "Pending": 0.10,
                                    "Denied": 0.08,
                                    "Cancelled": 0.07,
                                }),
                            },
                        },
                    },
                },

                # ── training ───────────────────────────────────
                "training": {
                    "description": "Training course catalog",
                    "primary_key": ["training_id"],
                    "columns": {
                        "training_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "course_name": {
                            "type": "string",
                            "max_length": 200,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "training_courses",
                            },
                        },
                        "category": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("training.category", {
                                    "Compliance": 0.25,
                                    "Technical": 0.22,
                                    "Leadership": 0.15,
                                    "Safety": 0.13,
                                    "Soft Skills": 0.12,
                                    "Onboarding": 0.08,
                                    "Diversity": 0.05,
                                }),
                            },
                        },
                        "duration_hours": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0.5,
                                "max": 40.0,
                                "round": 1,
                            },
                        },
                        "is_mandatory": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("training.is_mandatory", {"true": 0.35, "false": 0.65}),
                            },
                        },
                    },
                },

                # ── training_enrollment ────────────────────────
                "training_enrollment": {
                    "description": "Employee training enrollments",
                    "primary_key": ["enrollment_id"],
                    "columns": {
                        "enrollment_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "employee_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "employee.employee_id",
                            },
                        },
                        "training_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "training.training_id",
                                "distribution": "zipf",
                                "alpha": 1.2,
                            },
                        },
                        "enrollment_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "completion_date": {
                            "type": "date",
                            "nullable": True,
                            "null_rate": 0.15,
                            "generator": {
                                "strategy": "derived",
                                "source": "enrollment_date",
                                "rule": "add_days",
                                "params": {"distribution": "uniform", "min": 1, "max": 90},
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("training_enrollment.status", {
                                    "Completed": 0.65,
                                    "In Progress": 0.15,
                                    "Enrolled": 0.10,
                                    "Dropped": 0.07,
                                    "Failed": 0.03,
                                }),
                            },
                        },
                        "score": {
                            "type": "float",
                            "nullable": True,
                            "null_rate": 0.25,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 50.0,
                                "max": 100.0,
                                "round": 1,
                            },
                        },
                    },
                },

                # ── termination ────────────────────────────────
                "termination": {
                    "description": "Employee terminations",
                    "primary_key": ["termination_id"],
                    "columns": {
                        "termination_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "employee_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "employee.employee_id",
                            },
                        },
                        "termination_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "termination_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("termination.termination_type", {
                                    "Voluntary": 0.55,
                                    "Involuntary": 0.25,
                                    "Layoff": 0.10,
                                    "Retirement": 0.07,
                                    "End of Contract": 0.03,
                                }),
                            },
                        },
                        "reason": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("termination.reason", {
                                    "Better Opportunity": 0.30,
                                    "Relocation": 0.12,
                                    "Performance": 0.15,
                                    "Conduct": 0.08,
                                    "Restructuring": 0.10,
                                    "Personal": 0.10,
                                    "Retirement": 0.07,
                                    "Other": 0.08,
                                }),
                            },
                        },
                        "eligible_for_rehire": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("termination.eligible_for_rehire", {"true": 0.70, "false": 0.30}),
                            },
                        },
                    },
                },
            },

            # ── relationships ───────────────────────────────
            "relationships": [
                {
                    "name": "employee_department",
                    "parent": "department",
                    "child": "employee",
                    "parent_key": "department_id",
                    "child_key": "department_id",
                },
                {
                    "name": "employee_position",
                    "parent": "position",
                    "child": "employee",
                    "parent_key": "position_id",
                    "child_key": "position_id",
                },
                {
                    "name": "employee_compensation",
                    "parent": "employee",
                    "child": "compensation",
                    "parent_key": "employee_id",
                    "child_key": "employee_id",
                },
                {
                    "name": "employee_reviews",
                    "parent": "employee",
                    "child": "performance_review",
                    "parent_key": "employee_id",
                    "child_key": "employee_id",
                },
                {
                    "name": "employee_time_off",
                    "parent": "employee",
                    "child": "time_off_request",
                    "parent_key": "employee_id",
                    "child_key": "employee_id",
                },
                {
                    "name": "employee_training_enrollment",
                    "parent": "employee",
                    "child": "training_enrollment",
                    "parent_key": "employee_id",
                    "child_key": "employee_id",
                },
                {
                    "name": "training_enrollment_course",
                    "parent": "training",
                    "child": "training_enrollment",
                    "parent_key": "training_id",
                    "child_key": "training_id",
                },
                {
                    "name": "employee_termination",
                    "parent": "employee",
                    "child": "termination",
                    "parent_key": "employee_id",
                    "child_key": "employee_id",
                },
            ],

            # ── business rules ──────────────────────────────
            "business_rules": [
                {
                    "name": "max_salary_exceeds_min",
                    "description": "Position max_salary must be >= min_salary",
                    "type": "column_comparison",
                    "table": "position",
                    "left": "max_salary",
                    "operator": ">=",
                    "right": "min_salary",
                },
                {
                    "name": "rating_in_range",
                    "description": "Performance rating must be 1-5",
                    "type": "constraint",
                    "table": "performance_review",
                    "rule": "rating >= 1 AND rating <= 5",
                },
                {
                    "name": "hours_positive",
                    "description": "Time-off hours must be positive",
                    "type": "constraint",
                    "table": "time_off_request",
                    "rule": "hours_requested > 0",
                },
            ],

            # ── generation config ───────────────────────────
            "generation": {
                "scales": {
                    "fabric_demo": {"employee": 100},
                    "small": {"employee": 500},
                    "medium": {"employee": 5000},
                    "large": {"employee": 50000},
                    "xlarge": {"employee": 500000},
                    "warehouse": {"employee": 5000000},
                    "xxl": {"employee": 20000000},
                    "xxxl": {"employee": 100000000},
                },
                "derived_counts": {
                    "department": {"fixed": 30},
                    "position": {"fixed": 80},
                    "training": {"fixed": 100},
                    "compensation": {"per_parent": "employee", "ratio": self._ratio("compensation_per_employee", 3.0)},
                    "performance_review": {"per_parent": "employee", "ratio": self._ratio("review_per_employee", 2.5)},
                    "time_off_request": {"per_parent": "employee", "ratio": self._ratio("time_off_per_employee", 5.0)},
                    "training_enrollment": {"per_parent": "employee", "ratio": self._ratio("enrollment_per_employee", 4.0)},
                    "termination": {"per_parent": "employee", "ratio": self._ratio("termination_per_employee", 0.15)},
                },
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)

    def star_schema_map(self):
        """Return the star schema mapping for the HR domain.

        Produces:
          - dim_employee   (from employee)
          - dim_department (from department)
          - dim_position   (from position)
          - fact_compensation (from compensation)
          - fact_performance  (from performance_review)
          - fact_time_off     (from time_off_request)
        """
        from sqllocks_spindle.transform.star_schema import DimSpec, FactSpec, StarSchemaMap

        return StarSchemaMap(
            dims={
                "dim_employee": DimSpec(
                    source="employee",
                    sk="sk_employee",
                    nk="employee_id",
                ),
                "dim_department": DimSpec(
                    source="department",
                    sk="sk_department",
                    nk="department_id",
                ),
                "dim_position": DimSpec(
                    source="position",
                    sk="sk_position",
                    nk="position_id",
                ),
            },
            facts={
                "fact_compensation": FactSpec(
                    primary="compensation",
                    fk_map={"employee_id": "dim_employee"},
                    date_cols=["effective_date"],
                ),
                "fact_performance": FactSpec(
                    primary="performance_review",
                    fk_map={"employee_id": "dim_employee"},
                    date_cols=["review_date"],
                ),
                "fact_time_off": FactSpec(
                    primary="time_off_request",
                    fk_map={"employee_id": "dim_employee"},
                    date_cols=["start_date"],
                ),
            },
        )

    def cdm_map(self):
        """Return the CDM entity map for the HR domain.

        Maps source table names to Microsoft Common Data Model entity names.
        """
        from sqllocks_spindle.transform.cdm_mapper import CdmEntityMap

        return CdmEntityMap({
            "department": "BusinessUnit",
            "position": "Position",
            "employee": "Worker",
            "compensation": "Compensation",
            "performance_review": "PerformanceReview",
            "time_off_request": "LeaveRequest",
            "training": "Course",
            "training_enrollment": "CourseEnrollment",
            "termination": "Termination",
        })
