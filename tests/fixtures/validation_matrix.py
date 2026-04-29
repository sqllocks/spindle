"""Validation matrix builder for Spindle domain × sink × size × mode combinations."""
from __future__ import annotations

DOMAINS = [
    "capital_markets",
    "education",
    "financial",
    "healthcare",
    "hr",
    "insurance",
    "iot",
    "manufacturing",
    "marketing",
    "real_estate",
    "retail",
    "supply_chain",
    "telecom",
]

SINKS = ["lakehouse", "warehouse", "eventhouse", "sql-database", "sql-server"]

SIZES = ["small", "medium", "large", "fabric_demo"]

MODES = ["seeding", "streaming", "inference"]

# Domains with bundled profiles available for inference mode testing.
INFERENCE_CAPABLE_DOMAINS = {"retail", "financial", "healthcare"}

# Fabric-only sinks that support all sizes including fabric_demo.
FABRIC_SINKS = {"lakehouse", "warehouse", "eventhouse", "sql-database"}


def build_matrix() -> list[tuple[str, str, str, str]]:
    """Return all valid (domain, sink, size, mode) tuples.

    Filters applied:
    - streaming + sql-server → skip (chunked writer deferred to Phase 6)
    - fabric_demo + sql-server → skip (no Spark path for on-prem)
    - inference mode for non-INFERENCE_CAPABLE_DOMAINS → skip
    - deduplicate after filters
    """
    seen: set[tuple[str, str, str, str]] = set()
    result: list[tuple[str, str, str, str]] = []

    for domain in DOMAINS:
        for sink in SINKS:
            for size in SIZES:
                for mode in MODES:
                    if sink == "sql-server" and mode == "streaming":
                        continue
                    if sink == "sql-server" and size == "fabric_demo":
                        continue
                    if mode == "inference" and domain not in INFERENCE_CAPABLE_DOMAINS:
                        continue

                    combo = (domain, sink, size, mode)
                    if combo not in seen:
                        seen.add(combo)
                        result.append(combo)

    return result
