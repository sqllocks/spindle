"""PII masking — replace sensitive data with synthetic values.

Detects PII columns (email, phone, name, SSN, etc.) via column name
heuristics and profiler pattern detection, then replaces values with
realistic synthetic data while preserving null patterns and distributions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker


@dataclass
class MaskConfig:
    """Configuration for data masking."""

    seed: int = 42
    locale: str = "en_US"
    preserve_nulls: bool = True  # keep NULL positions unchanged
    preserve_distributions: bool = True  # match numeric distributions
    preserve_fks: bool = True  # maintain FK relationships
    pii_columns: dict[str, str] | None = None  # explicit {col: type} overrides
    # e.g., {"email": "email", "phone": "phone", "ssn": "ssn"}
    exclude_columns: list[str] = field(default_factory=list)  # columns to leave untouched


@dataclass
class MaskResult:
    """Result of masking operation."""

    tables: dict[str, pd.DataFrame]  # masked DataFrames
    columns_masked: dict[str, list[str]]  # {table: [columns that were masked]}
    stats: dict[str, dict[str, int]]  # {table: {total_cols, masked_cols, rows}}

    def summary(self) -> str:
        """Return a human-readable summary of the masking result."""
        lines = ["Masking Result", "=" * 50]
        for table, s in self.stats.items():
            cols = self.columns_masked.get(table, [])
            lines.append(
                f"  {table}: {s['masked_cols']}/{s['total_cols']} columns masked ({s['rows']} rows)"
            )
            if cols:
                lines.append(f"    Masked: {', '.join(cols)}")
        return "\n".join(lines)


class DataMasker:
    """Replace PII in real data with synthetic values preserving distributions."""

    # Column name patterns that indicate PII
    PII_NAME_PATTERNS: dict[str, list[str]] = {
        "email": ["email", "email_address", "e_mail"],
        "phone": ["phone", "phone_number", "telephone", "mobile", "cell"],
        "name": [
            "first_name",
            "last_name",
            "full_name",
            "name",
            "given_name",
            "surname",
            "family_name",
        ],
        "first_name": ["first_name", "given_name", "fname"],
        "last_name": ["last_name", "surname", "family_name", "lname"],
        "address": [
            "address",
            "street",
            "street_address",
            "address_line",
            "address_1",
            "address_2",
        ],
        "city": ["city", "town"],
        "state": ["state", "province", "region"],
        "zip": ["zip", "zip_code", "zipcode", "postal_code", "postcode"],
        "ssn": ["ssn", "social_security", "social_security_number", "sin"],
        "credit_card": [
            "credit_card",
            "card_number",
            "cc_number",
            "card_num",
        ],
        "ip_address": ["ip_address", "ip", "ip_addr"],
        "username": ["username", "user_name", "login"],
        "date_of_birth": ["date_of_birth", "dob", "birth_date", "birthdate"],
    }

    def mask(
        self,
        tables: dict[str, pd.DataFrame],
        config: MaskConfig | None = None,
    ) -> MaskResult:
        """Mask PII columns across all tables.

        Parameters
        ----------
        tables:
            Mapping of table name to DataFrame.
        config:
            Optional masking configuration.  Defaults are sensible.

        Returns
        -------
        MaskResult with masked DataFrames and statistics.
        """
        config = config or MaskConfig()
        fake = Faker(config.locale)
        Faker.seed(config.seed)
        rng = np.random.default_rng(config.seed)

        from sqllocks_spindle.inference.profiler import DataProfiler

        profiler = DataProfiler()

        masked_tables: dict[str, pd.DataFrame] = {}
        columns_masked: dict[str, list[str]] = {}
        stats: dict[str, dict[str, int]] = {}

        # First pass: profile and identify PII
        profiles: dict[str, Any] = {}
        for table_name, df in tables.items():
            profiles[table_name] = profiler.profile_dataframe(df, table_name)

        # Track PK mappings for FK consistency
        pk_maps: dict[str, dict[str, dict]] = {}  # {table.col: {old_val: new_val}}

        for table_name, df in tables.items():
            profile = profiles[table_name]
            masked_df, masked_cols = self._mask_table(
                df, profile, config, fake, rng, pk_maps, tables
            )
            masked_tables[table_name] = masked_df
            columns_masked[table_name] = masked_cols
            stats[table_name] = {
                "total_cols": len(df.columns),
                "masked_cols": len(masked_cols),
                "rows": len(df),
            }

        return MaskResult(
            tables=masked_tables,
            columns_masked=columns_masked,
            stats=stats,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _mask_table(
        self,
        df: pd.DataFrame,
        profile: Any,
        config: MaskConfig,
        fake: Faker,
        rng: np.random.Generator,
        pk_maps: dict[str, dict[str, dict]],
        all_tables: dict[str, pd.DataFrame],
    ) -> tuple[pd.DataFrame, list[str]]:
        """Mask PII columns in a single table."""
        masked_df = df.copy()
        masked_cols: list[str] = []

        for col_name in df.columns:
            if col_name in config.exclude_columns:
                continue

            col_profile = profile.columns.get(col_name)
            pii_type = self._detect_pii_type(col_name, col_profile, config)

            if pii_type is None:
                continue

            # Check if this is a FK column — if so, apply mapping from parent
            if col_profile and col_profile.is_foreign_key and config.preserve_fks:
                ref_table = col_profile.fk_ref_table
                if ref_table and ref_table in pk_maps:
                    # Apply parent's PK mapping
                    for pk_col, mapping in pk_maps[ref_table].items():
                        if mapping:  # only if parent was masked
                            masked_df[col_name] = (
                                df[col_name].map(mapping).fillna(df[col_name])
                            )
                            masked_cols.append(col_name)
                continue

            # Generate replacement values
            new_values = self._generate_replacements(
                df[col_name], pii_type, config, fake, rng
            )

            # Preserve nulls if configured
            if config.preserve_nulls:
                null_mask = df[col_name].isna()
                new_values[null_mask] = None

            # Track PK mappings for FK consistency
            if col_profile and col_profile.is_primary_key:
                old_vals = df[col_name].values
                pk_maps.setdefault(profile.name, {})[col_name] = dict(
                    zip(old_vals, new_values)
                )

            masked_df[col_name] = new_values
            masked_cols.append(col_name)

        return masked_df, masked_cols

    def _detect_pii_type(
        self,
        col_name: str,
        col_profile: Any | None,
        config: MaskConfig,
    ) -> str | None:
        """Detect if a column contains PII based on name and profile.

        Returns the PII type string (e.g. ``"email"``, ``"phone"``) or
        ``None`` if the column is not identified as PII.
        """
        # Explicit override
        if config.pii_columns and col_name in config.pii_columns:
            return config.pii_columns[col_name]

        # Name-based detection
        col_lower = col_name.lower()
        for pii_type, patterns in self.PII_NAME_PATTERNS.items():
            if col_lower in patterns or any(p in col_lower for p in patterns):
                return pii_type

        # Pattern-based detection from profiler
        if col_profile and col_profile.pattern:
            if col_profile.pattern == "email":
                return "email"
            if col_profile.pattern == "phone":
                return "phone"
            if col_profile.pattern == "uuid":
                return None  # UUIDs are not PII

        return None

    def _generate_replacements(
        self,
        series: pd.Series,
        pii_type: str,
        config: MaskConfig,
        fake: Faker,
        rng: np.random.Generator,
    ) -> pd.array:
        """Generate synthetic replacement values for a PII column."""
        n = len(series)

        generators: dict[str, Any] = {
            "email": lambda: fake.email(),
            "phone": lambda: fake.phone_number(),
            "first_name": lambda: fake.first_name(),
            "last_name": lambda: fake.last_name(),
            "name": lambda: fake.name(),
            "address": lambda: fake.street_address(),
            "city": lambda: fake.city(),
            "state": lambda: fake.state_abbr(),
            "zip": lambda: fake.zipcode(),
            "ssn": lambda: fake.ssn(),
            "credit_card": lambda: fake.credit_card_number(),
            "ip_address": lambda: fake.ipv4(),
            "username": lambda: fake.user_name(),
            "date_of_birth": lambda: fake.date_of_birth().isoformat(),
        }

        gen = generators.get(pii_type)
        if gen:
            return pd.array([gen() for _ in range(n)], dtype=object)

        # Fallback for numeric PII: preserve distribution
        if config.preserve_distributions and pd.api.types.is_numeric_dtype(series):
            mean = series.dropna().mean()
            std = series.dropna().std()
            if std > 0:
                return pd.array(rng.normal(mean, std, n), dtype=series.dtype)

        # Fallback: random string
        return pd.array([fake.lexify("????") for _ in range(n)], dtype=object)
