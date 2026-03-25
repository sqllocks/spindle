"""Native vectorized strategy — replaces Faker for built-in providers.

Uses pre-built data arrays and numpy vectorized operations for 10-100x speedup
over Faker's per-call generation. All 11 providers used across 12 built-in
domains are handled here.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef

# Lazy-load data arrays (loaded once on first use, ~0.5MB total)
_DATA_CACHE: dict[str, np.ndarray] = {}


def _get_array(name: str) -> np.ndarray:
    """Lazy-load and cache a data array as numpy object array."""
    if name not in _DATA_CACHE:
        from sqllocks_spindle.engine.data import names

        arrays = {
            "first_names": names.FIRST_NAMES,
            "last_names": names.LAST_NAMES,
            "company_names": names.COMPANY_NAMES,
            "street_names": names.STREET_NAMES,
            "email_domains": names.EMAIL_DOMAINS,
            "sentences": names.SENTENCES,
            "uri_paths": names.URI_PATHS,
            "uri_domains": names.URI_DOMAINS,
        }
        # Cache all at once on first access
        for key, data in arrays.items():
            _DATA_CACHE[key] = np.array(data, dtype=object)

    return _DATA_CACHE[name]


# US state abbreviations (public domain, no external dependency)
_US_STATES = (
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
)
_US_STATES_ARRAY = np.array(_US_STATES, dtype=object)

# Common US city names (public domain Census data)
_US_CITIES = (
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
    "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
    "Fort Worth", "Columbus", "Charlotte", "Indianapolis", "San Francisco",
    "Seattle", "Denver", "Washington", "Nashville", "Oklahoma City", "El Paso",
    "Boston", "Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore",
    "Milwaukee", "Albuquerque", "Tucson", "Fresno", "Sacramento", "Mesa",
    "Kansas City", "Atlanta", "Omaha", "Colorado Springs", "Raleigh",
    "Long Beach", "Virginia Beach", "Miami", "Oakland", "Minneapolis",
    "Tampa", "Tulsa", "Arlington", "New Orleans", "Wichita", "Cleveland",
    "Bakersfield", "Aurora", "Anaheim", "Honolulu", "Santa Ana", "Riverside",
    "Corpus Christi", "Lexington", "Henderson", "Stockton", "Saint Paul",
    "Cincinnati", "St. Louis", "Pittsburgh", "Greensboro", "Lincoln",
    "Anchorage", "Plano", "Orlando", "Irvine", "Newark", "Durham",
    "Chula Vista", "Toledo", "Fort Wayne", "St. Petersburg", "Laredo",
    "Jersey City", "Chandler", "Madison", "Lubbock", "Scottsdale",
    "Reno", "Buffalo", "Gilbert", "Glendale", "North Las Vegas",
    "Winston-Salem", "Chesapeake", "Norfolk", "Fremont", "Garland",
    "Irving", "Hialeah", "Richmond", "Boise", "Spokane", "Baton Rouge",
)
_US_CITIES_ARRAY = np.array(_US_CITIES, dtype=object)

_STREET_SUFFIXES = np.array(
    ["St", "Ave", "Blvd", "Dr", "Ln", "Way", "Ct", "Pl", "Rd", "Cir"],
    dtype=object,
)


class NativeStrategy(Strategy):
    """Vectorized native data generation — replaces Faker for built-in providers."""

    # Provider dispatch table — maps Faker provider names to generator methods
    _GENERATORS: dict[str, str] = {
        "first_name": "_gen_first_name",
        "last_name": "_gen_last_name",
        "name": "_gen_name",
        "email": "_gen_email",
        "phone_number": "_gen_phone",
        "company": "_gen_company",
        "street_address": "_gen_street_address",
        "sentence": "_gen_sentence",
        "city": "_gen_city",
        "state_abbr": "_gen_state",
        "uri": "_gen_uri",
        "company_email": "_gen_company_email",
    }

    def can_handle(self, provider: str) -> bool:
        """Check if this strategy handles the given Faker provider."""
        return provider in self._GENERATORS

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        provider = config.get("provider", "word")
        method_name = self._GENERATORS.get(provider)
        if method_name is None:
            raise ValueError(f"NativeStrategy does not handle provider '{provider}'")

        method = getattr(self, method_name)
        values = method(ctx)

        # Apply max_length truncation (vectorized for strings)
        if column.max_length:
            ml = column.max_length
            values = np.array(
                [str(v)[:ml] if v is not None else v for v in values],
                dtype=object,
            )

        return values

    # ── Individual generators ──────────────────────────────────────────────

    def _gen_first_name(self, ctx: GenerationContext) -> np.ndarray:
        return ctx.rng.choice(_get_array("first_names"), size=ctx.row_count)

    def _gen_last_name(self, ctx: GenerationContext) -> np.ndarray:
        return ctx.rng.choice(_get_array("last_names"), size=ctx.row_count)

    def _gen_name(self, ctx: GenerationContext) -> np.ndarray:
        firsts = ctx.rng.choice(_get_array("first_names"), size=ctx.row_count)
        lasts = ctx.rng.choice(_get_array("last_names"), size=ctx.row_count)
        return np.array(
            [f"{f} {l}" for f, l in zip(firsts, lasts)],
            dtype=object,
        )

    def _gen_email(self, ctx: GenerationContext) -> np.ndarray:
        # Reuse first/last from current table if already generated
        table = ctx.current_table
        if "first_name" in table and "last_name" in table:
            firsts = table["first_name"]
            lasts = table["last_name"]
        else:
            firsts = ctx.rng.choice(_get_array("first_names"), size=ctx.row_count)
            lasts = ctx.rng.choice(_get_array("last_names"), size=ctx.row_count)

        domains = ctx.rng.choice(_get_array("email_domains"), size=ctx.row_count)
        # Add numeric suffix to improve uniqueness
        suffixes = ctx.rng.integers(1, 999, size=ctx.row_count)
        return np.array(
            [
                f"{str(f).lower().replace(' ', '')}.{str(l).lower().replace(' ', '')}{s}@{d}"
                for f, l, s, d in zip(firsts, lasts, suffixes, domains)
            ],
            dtype=object,
        )

    def _gen_phone(self, ctx: GenerationContext) -> np.ndarray:
        area = ctx.rng.integers(200, 999, size=ctx.row_count)
        exchange = ctx.rng.integers(200, 999, size=ctx.row_count)
        subscriber = ctx.rng.integers(1000, 9999, size=ctx.row_count)
        return np.array(
            [f"({a}) {e}-{s}" for a, e, s in zip(area, exchange, subscriber)],
            dtype=object,
        )

    def _gen_company(self, ctx: GenerationContext) -> np.ndarray:
        return ctx.rng.choice(_get_array("company_names"), size=ctx.row_count)

    def _gen_street_address(self, ctx: GenerationContext) -> np.ndarray:
        numbers = ctx.rng.integers(100, 9999, size=ctx.row_count)
        streets = ctx.rng.choice(_get_array("street_names"), size=ctx.row_count)
        suffixes = ctx.rng.choice(_STREET_SUFFIXES, size=ctx.row_count)
        return np.array(
            [f"{n} {s} {sfx}" for n, s, sfx in zip(numbers, streets, suffixes)],
            dtype=object,
        )

    def _gen_sentence(self, ctx: GenerationContext) -> np.ndarray:
        return ctx.rng.choice(_get_array("sentences"), size=ctx.row_count)

    def _gen_city(self, ctx: GenerationContext) -> np.ndarray:
        return ctx.rng.choice(_US_CITIES_ARRAY, size=ctx.row_count)

    def _gen_state(self, ctx: GenerationContext) -> np.ndarray:
        return ctx.rng.choice(_US_STATES_ARRAY, size=ctx.row_count)

    def _gen_uri(self, ctx: GenerationContext) -> np.ndarray:
        domains = ctx.rng.choice(_get_array("uri_domains"), size=ctx.row_count)
        paths = ctx.rng.choice(_get_array("uri_paths"), size=ctx.row_count)
        return np.array(
            [f"https://{d}/{p}" for d, p in zip(domains, paths)],
            dtype=object,
        )

    def _gen_company_email(self, ctx: GenerationContext) -> np.ndarray:
        firsts = ctx.rng.choice(_get_array("first_names"), size=ctx.row_count)
        lasts = ctx.rng.choice(_get_array("last_names"), size=ctx.row_count)
        companies = ctx.rng.choice(_get_array("company_names"), size=ctx.row_count)
        return np.array(
            [
                f"{str(f).lower().replace(' ', '')}.{str(l).lower().replace(' ', '')}@"
                f"{str(c).lower().replace(' ', '').replace(',', '').replace('.', '')[:20]}.com"
                for f, l, c in zip(firsts, lasts, companies)
            ],
            dtype=object,
        )
