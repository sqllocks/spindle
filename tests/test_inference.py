"""Tests for the Spindle inference engine (DataProfiler + SchemaBuilder)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.inference.profiler import DataProfiler, DatasetProfile, HAS_SCIPY
from sqllocks_spindle.inference.schema_builder import SchemaBuilder


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _simple_df() -> pd.DataFrame:
    """A small DataFrame with known types for basic profiling tests."""
    return pd.DataFrame(
        {
            "id": range(1, 101),
            "name": [f"user_{i}" for i in range(1, 101)],
            "score": np.random.default_rng(42).normal(50, 10, 100).round(2),
            "active": [True, False] * 50,
            "category": np.random.default_rng(42).choice(
                ["A", "B", "C", "D"], size=100
            ),
        }
    )


# ---------------------------------------------------------------------------
# TestDataProfiler
# ---------------------------------------------------------------------------


class TestDataProfiler:
    def test_profile_simple_dataframe(self):
        profiler = DataProfiler()
        df = _simple_df()
        profile = profiler.profile_dataframe(df, table_name="users")

        assert profile.name == "users"
        assert profile.row_count == 100
        assert "id" in profile.columns
        assert "name" in profile.columns
        assert "score" in profile.columns
        assert "active" in profile.columns
        assert "category" in profile.columns

        # Check inferred types
        assert profile.columns["id"].dtype == "integer"
        assert profile.columns["score"].dtype == "float"
        assert profile.columns["active"].dtype == "boolean"
        assert profile.columns["category"].dtype == "string"

    def test_null_rate_detection(self):
        profiler = DataProfiler()
        df = pd.DataFrame(
            {
                "id": range(1, 101),
                "value": [None if i % 4 == 0 else float(i) for i in range(1, 101)],
            }
        )
        profile = profiler.profile_dataframe(df, table_name="test")

        col = profile.columns["value"]
        assert col.null_count == 25
        assert abs(col.null_rate - 0.25) < 0.01

    def test_enum_detection(self):
        profiler = DataProfiler()
        df = pd.DataFrame(
            {
                "id": range(1, 201),
                "status": np.random.default_rng(42).choice(
                    ["open", "closed", "pending"], size=200
                ),
            }
        )
        profile = profiler.profile_dataframe(df, table_name="tickets")

        col = profile.columns["status"]
        assert col.is_enum is True
        assert col.enum_values is not None
        assert "open" in col.enum_values
        assert "closed" in col.enum_values
        assert "pending" in col.enum_values
        # Probabilities should sum to ~1
        assert abs(sum(col.enum_values.values()) - 1.0) < 0.01

    def test_primary_key_detection(self):
        profiler = DataProfiler()
        df = pd.DataFrame(
            {
                "id": range(1, 51),
                "name": [f"item_{i}" for i in range(1, 51)],
            }
        )
        profile = profiler.profile_dataframe(df, table_name="items")

        assert profile.primary_key == ["id"]
        assert profile.columns["id"].is_primary_key is True

    def test_foreign_key_detection(self):
        profiler = DataProfiler()
        customers = pd.DataFrame(
            {
                "id": range(1, 11),
                "name": [f"cust_{i}" for i in range(1, 11)],
            }
        )
        orders = pd.DataFrame(
            {
                "id": range(1, 51),
                "customer_id": np.random.default_rng(42).choice(range(1, 11), size=50),
                "amount": np.random.default_rng(42).uniform(10, 500, 50).round(2),
            }
        )
        tables = {"customer": customers, "order": orders}
        dataset = profiler.profile_dataset(tables)

        order_profile = dataset.tables["order"]
        assert "customer_id" in order_profile.detected_fks
        assert order_profile.detected_fks["customer_id"] == "customer"
        assert order_profile.columns["customer_id"].is_foreign_key is True
        assert order_profile.columns["customer_id"].fk_ref_table == "customer"

        # Relationship should be recorded
        assert len(dataset.relationships) >= 1
        rel = dataset.relationships[0]
        assert rel["parent"] == "customer"
        assert rel["child"] == "order"

    @pytest.mark.skipif(not HAS_SCIPY, reason="scipy not installed")
    def test_distribution_fitting(self):
        profiler = DataProfiler()
        rng = np.random.default_rng(42)
        values = rng.normal(0, 1, 500)
        df = pd.DataFrame({"id": range(1, 501), "measurement": values})
        profile = profiler.profile_dataframe(df, table_name="measurements")

        col = profile.columns["measurement"]
        assert col.distribution is not None
        # Distribution fitting should identify a continuous distribution
        assert col.distribution in ("normal", "lognormal", "uniform", "exponential")
        assert col.distribution_params is not None
        assert "loc" in col.distribution_params
        assert "scale" in col.distribution_params

    def test_pattern_detection(self):
        profiler = DataProfiler()
        emails = [f"user{i}@example.com" for i in range(1, 101)]
        df = pd.DataFrame({"id": range(1, 101), "email": emails})
        profile = profiler.profile_dataframe(df, table_name="contacts")

        col = profile.columns["email"]
        assert col.pattern == "email"

    def test_profile_dataset(self):
        profiler = DataProfiler()
        department = pd.DataFrame(
            {
                "id": range(1, 6),
                "name": ["Engineering", "Sales", "HR", "Finance", "Marketing"],
            }
        )
        employee = pd.DataFrame(
            {
                "id": range(1, 21),
                "department_id": np.random.default_rng(42).choice(range(1, 6), size=20),
                "name": [f"emp_{i}" for i in range(1, 21)],
            }
        )
        tables = {"department": department, "employee": employee}
        dataset = profiler.profile_dataset(tables)

        assert "department" in dataset.tables
        assert "employee" in dataset.tables
        assert dataset.tables["department"].row_count == 5
        assert dataset.tables["employee"].row_count == 20

        # FK should be detected
        emp = dataset.tables["employee"]
        assert "department_id" in emp.detected_fks

    def test_quantiles_captured(self):
        profiler = DataProfiler()
        df = pd.DataFrame({"score": np.random.default_rng(42).normal(50, 10, 500)})
        profile = profiler.profile_dataframe(df)
        col = profile.columns["score"]
        assert col.quantiles is not None
        assert set(col.quantiles.keys()) == {"p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99"}
        assert col.quantiles["p50"] == pytest.approx(df["score"].median(), abs=2.0)

    def test_outlier_rate_captured(self):
        profiler = DataProfiler()
        rng = np.random.default_rng(42)
        values = np.concatenate([rng.normal(50, 5, 95), [200.0, 210.0, 220.0, 230.0, 240.0]])
        df = pd.DataFrame({"val": values})
        profile = profiler.profile_dataframe(df)
        col = profile.columns["val"]
        assert col.outlier_rate is not None
        assert 0.0 <= col.outlier_rate <= 1.0

    def test_value_counts_ext_captured(self):
        profiler = DataProfiler()
        df = pd.DataFrame({"cat": ["A"] * 60 + ["B"] * 30 + ["C"] * 10})
        profile = profiler.profile_dataframe(df)
        col = profile.columns["cat"]
        assert col.value_counts_ext is not None
        assert abs(col.value_counts_ext["A"] - 0.6) < 0.01

    def test_string_length_captured(self):
        profiler = DataProfiler()
        # range(10, 10000): "user_10" (7 chars) .. "user_9999" (9 chars) → min=7, max=9
        df = pd.DataFrame({"name": [f"user_{i}" for i in range(10, 10000)]})
        profile = profiler.profile_dataframe(df)
        col = profile.columns["name"]
        assert col.string_length is not None
        assert col.string_length["min"] == 7
        assert col.string_length["max"] == 9

    def test_fit_score_captured(self):
        profiler = DataProfiler()
        df = pd.DataFrame({"val": np.random.default_rng(42).normal(0, 1, 200)})
        profile = profiler.profile_dataframe(df)
        col = profile.columns["val"]
        # fit_score is None when scipy not available, or a float in [0, 1]
        if HAS_SCIPY:
            assert col.fit_score is not None
            assert 0.0 <= col.fit_score <= 1.0


# ---------------------------------------------------------------------------
# TestDataProfilerEntryPoints
# ---------------------------------------------------------------------------


class TestDataProfilerEntryPoints:
    def test_profile_alias(self):
        """profile() is an alias for profile_dataframe()."""
        profiler = DataProfiler()
        df = pd.DataFrame({"x": [1, 2, 3]})
        result = profiler.profile(df, table_name="t")
        assert result.name == "t"
        assert "x" in result.columns

    def test_from_csv(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        csv_path = tmp_path / "test.csv"
        df.to_csv(csv_path, index=False)
        profile = DataProfiler.from_csv(str(csv_path))
        assert profile.name == "test"
        assert "a" in profile.columns
        assert "b" in profile.columns

    def test_from_csv_with_sample_rows(self, tmp_path):
        df = pd.DataFrame({"v": range(1000)})
        csv_path = tmp_path / "big.csv"
        df.to_csv(csv_path, index=False)
        profile = DataProfiler.from_csv(str(csv_path), sample_rows=100)
        assert profile.row_count == 100

    def test_constructor_kwargs(self):
        profiler = DataProfiler(fit_threshold=0.9, top_n_values=10, outlier_iqr_factor=2.0)
        assert profiler.fit_threshold == 0.9
        assert profiler.top_n_values == 10
        assert profiler.outlier_iqr_factor == 2.0


# ---------------------------------------------------------------------------
# TestSchemaBuilder
# ---------------------------------------------------------------------------


class TestSchemaBuilder:
    def _make_profile(self) -> DatasetProfile:
        """Build a small DatasetProfile for schema building tests."""
        profiler = DataProfiler()
        customer = pd.DataFrame(
            {
                "id": range(1, 11),
                "name": [f"customer_{i}" for i in range(1, 11)],
                "email": [f"c{i}@example.com" for i in range(1, 11)],
                "status": np.random.default_rng(42).choice(
                    ["active", "inactive"], size=10
                ),
            }
        )
        order = pd.DataFrame(
            {
                "id": range(1, 51),
                "customer_id": np.random.default_rng(42).choice(range(1, 11), size=50),
                "amount": np.random.default_rng(42).uniform(10, 500, 50).round(2),
            }
        )
        return profiler.profile_dataset({"customer": customer, "order": order})

    def test_build_from_profile(self):
        builder = SchemaBuilder()
        profile = self._make_profile()
        schema = builder.build(profile, domain_name="test_shop")

        assert schema.model.domain == "test_shop"
        assert "customer" in schema.tables
        assert "order" in schema.tables
        assert len(schema.tables["customer"].columns) == 4
        assert len(schema.tables["order"].columns) == 3

        # Generation config should have scales
        assert "small" in schema.generation.scales
        assert "medium" in schema.generation.scales
        assert "large" in schema.generation.scales

    def test_enum_to_weighted_enum(self):
        builder = SchemaBuilder()
        profile = self._make_profile()
        schema = builder.build(profile)

        # status column has 2 distinct values — should be enum
        status_gen = schema.tables["customer"].columns["status"].generator
        assert status_gen["strategy"] == "weighted_enum"
        assert "values" in status_gen

    def test_fk_to_foreign_key(self):
        builder = SchemaBuilder()
        profile = self._make_profile()
        schema = builder.build(profile)

        fk_gen = schema.tables["order"].columns["customer_id"].generator
        assert fk_gen["strategy"] == "foreign_key"
        assert "ref" in fk_gen

    def test_pk_to_sequence(self):
        builder = SchemaBuilder()
        profile = self._make_profile()
        schema = builder.build(profile)

        pk_gen = schema.tables["customer"].columns["id"].generator
        assert pk_gen["strategy"] == "sequence"
        assert pk_gen.get("start", 1) == 1


# ---------------------------------------------------------------------------
# TestExtendedPatternDetection
# ---------------------------------------------------------------------------


class TestExtendedPatternDetection:
    """New pattern detection for Phase 3B."""

    def _profiler_detect(self, values: list[str]) -> str | None:
        profiler = DataProfiler()
        s = pd.Series(values)
        return profiler._detect_pattern(s)

    def test_detects_ssn(self):
        ssns = [f"123-{i:02d}-{j:04d}" for i in range(1, 10) for j in range(1, 12)]
        assert self._profiler_detect(ssns) == "ssn"

    def test_detects_ip_v4(self):
        ips = [f"192.168.{i}.{j}" for i in range(10) for j in range(10)]
        assert self._profiler_detect(ips) == "ip_address"

    def test_detects_mac_address(self):
        macs = [f"00:1A:{i:02X}:{j:02X}:{k:02X}:FF"
                for i in range(10) for j in range(10) for k in range(10)][:100]
        assert self._profiler_detect(macs) == "mac_address"

    def test_detects_currency_code(self):
        codes = ["USD", "EUR", "GBP", "JPY", "CAD"] * 20
        assert self._profiler_detect(codes) == "currency_code"

    def test_detects_language_code(self):
        langs = ["en", "fr", "de", "es", "it"] * 20
        assert self._profiler_detect(langs) == "language_code"


class TestSchemaBuilderV2:
    """Tests for Phase 3B SchemaBuilder enhancements."""

    def _make_profile_with_field(self, **col_kwargs) -> "DatasetProfile":
        from sqllocks_spindle.inference.profiler import (
            ColumnProfile, TableProfile, DatasetProfile
        )
        # Base required fields; callers may override any of them via col_kwargs
        base = dict(
            name="val", dtype="float",
            null_count=0, null_rate=0.0,
            cardinality=100, cardinality_ratio=1.0,
            is_unique=False, is_enum=False, enum_values=None,
            min_value=0.0, max_value=100.0, mean=50.0, std=10.0,
            distribution=None, distribution_params=None, pattern=None,
            is_primary_key=False, is_foreign_key=False, fk_ref_table=None,
        )
        base.update(col_kwargs)
        col = ColumnProfile(**base)
        table = TableProfile(name="t", row_count=100, columns={"val": col},
                             primary_key=[], detected_fks={})
        return DatasetProfile(tables={"t": table})

    def test_empirical_strategy_selected_when_fit_score_low(self):
        profile = self._make_profile_with_field(
            distribution="normal",
            distribution_params={"loc": 50.0, "scale": 10.0},
            fit_score=0.60,  # below default 0.80 threshold
            quantiles={"p1": 10.0, "p5": 20.0, "p10": 25.0, "p25": 35.0,
                       "p50": 50.0, "p75": 65.0, "p90": 75.0, "p95": 80.0, "p99": 90.0},
        )
        builder = SchemaBuilder()
        schema = builder.build(profile, fit_threshold=0.80)
        gen = schema.tables["t"].columns["val"].generator
        assert gen["strategy"] == "empirical"
        assert "quantiles" in gen

    def test_parametric_strategy_when_fit_score_high(self):
        profile = self._make_profile_with_field(
            distribution="normal",
            distribution_params={"loc": 50.0, "scale": 10.0},
            fit_score=0.92,
        )
        builder = SchemaBuilder()
        schema = builder.build(profile, fit_threshold=0.80)
        gen = schema.tables["t"].columns["val"].generator
        assert gen["strategy"] == "distribution"

    def test_value_counts_ext_used_for_weighted_enum(self):
        from sqllocks_spindle.inference.profiler import (
            ColumnProfile, TableProfile, DatasetProfile
        )
        col = ColumnProfile(
            name="cat", dtype="string",
            null_count=0, null_rate=0.0,
            cardinality=3, cardinality_ratio=0.03,
            is_unique=False, is_enum=True,
            enum_values={"A": 0.6, "B": 0.3, "C": 0.1},
            min_value=None, max_value=None, mean=None, std=None,
            distribution=None, distribution_params=None, pattern=None,
            is_primary_key=False, is_foreign_key=False, fk_ref_table=None,
            value_counts_ext={"A": 0.6, "B": 0.3, "C": 0.1},
        )
        table = TableProfile(name="t", row_count=100, columns={"cat": col},
                             primary_key=[], detected_fks={})
        profile = DatasetProfile(tables={"t": table})
        schema = SchemaBuilder().build(profile)
        gen = schema.tables["t"].columns["cat"].generator
        assert gen["strategy"] == "weighted_enum"
        assert abs(gen["values"]["A"] - 0.6) < 0.01

    def test_correlated_columns_emitted_in_schema(self):
        from sqllocks_spindle.inference.profiler import (
            ColumnProfile, TableProfile, DatasetProfile
        )
        def _num_col(name):
            return ColumnProfile(
                name=name, dtype="float",
                null_count=0, null_rate=0.0,
                cardinality=100, cardinality_ratio=1.0,
                is_unique=False, is_enum=False, enum_values=None,
                min_value=0.0, max_value=100.0, mean=50.0, std=10.0,
                distribution="normal", distribution_params={"loc": 50.0, "scale": 10.0},
                pattern=None, is_primary_key=False, is_foreign_key=False, fk_ref_table=None,
                fit_score=0.95,
            )
        table = TableProfile(
            name="t", row_count=100,
            columns={"a": _num_col("a"), "b": _num_col("b")},
            primary_key=[], detected_fks={},
            correlation_matrix={"a": {"b": 0.75}, "b": {"a": 0.75}},
        )
        profile = DatasetProfile(tables={"t": table})
        schema = SchemaBuilder().build(profile, correlation_threshold=0.5)
        # correlated_columns stored in model metadata or schema extra
        assert hasattr(schema, "correlated_columns") or schema.model.extra.get("correlated_columns")

    def test_include_anomaly_registry_false_returns_single_value(self):
        from sqllocks_spindle.inference.profiler import (
            ColumnProfile, TableProfile, DatasetProfile
        )
        col = ColumnProfile(
            name="v", dtype="float",
            null_count=0, null_rate=0.0, cardinality=100, cardinality_ratio=1.0,
            is_unique=False, is_enum=False, enum_values=None,
            min_value=0.0, max_value=10.0, mean=5.0, std=1.0,
            distribution=None, distribution_params=None, pattern=None,
            is_primary_key=False, is_foreign_key=False, fk_ref_table=None,
        )
        table = TableProfile(name="t", row_count=100, columns={"v": col},
                             primary_key=[], detected_fks={})
        profile = DatasetProfile(tables={"t": table})
        result = SchemaBuilder().build(profile, include_anomaly_registry=False)
        # Single return value (not tuple)
        from sqllocks_spindle.schema.parser import SpindleSchema
        assert isinstance(result, SpindleSchema)

    def test_include_anomaly_registry_true_returns_tuple(self):
        from sqllocks_spindle.inference.profiler import (
            ColumnProfile, TableProfile, DatasetProfile
        )
        col = ColumnProfile(
            name="v", dtype="float",
            null_count=5, null_rate=0.05, cardinality=100, cardinality_ratio=1.0,
            is_unique=False, is_enum=False, enum_values=None,
            min_value=0.0, max_value=10.0, mean=5.0, std=1.0,
            distribution=None, distribution_params=None, pattern=None,
            is_primary_key=False, is_foreign_key=False, fk_ref_table=None,
            outlier_rate=0.05,
        )
        table = TableProfile(name="t", row_count=100, columns={"v": col},
                             primary_key=[], detected_fks={})
        profile = DatasetProfile(tables={"t": table})
        result = SchemaBuilder().build(profile, include_anomaly_registry=True)
        assert isinstance(result, tuple)
        schema, registry = result
        from sqllocks_spindle.schema.parser import SpindleSchema
        assert isinstance(schema, SpindleSchema)
        assert registry is not None
