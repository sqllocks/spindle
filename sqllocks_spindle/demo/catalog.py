"""ScenarioCatalog — register and discover demo scenarios."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional

from sqllocks_spindle.demo.params import DemoMode


@dataclass
class ScenarioMeta:
    name: str
    description: str
    domains: list
    supported_modes: list
    default_rows: int = 100_000
    tags: list = field(default_factory=list)
    author: str = "Spindle"


class ScenarioCatalog:
    def __init__(self):
        self._scenarios: dict = {}
        self._load_builtins()

    def _load_builtins(self) -> None:
        builtins = [
            ScenarioMeta(
                name="retail",
                description="Retail domain — customers, products, orders, order lines.",
                domains=["retail"],
                supported_modes=["inference", "streaming", "seeding"],
                default_rows=100_000,
                tags=["retail", "beginner"],
            ),
            ScenarioMeta(
                name="adventureworks",
                description=(
                    "AdventureWorks-compatible schema — DimCustomer, DimProduct, "
                    "FactSalesOrder, FactSalesOrderLine. Conference 'Retire AdventureWorks' demo."
                ),
                domains=["retail"],
                supported_modes=["inference", "seeding"],
                default_rows=50_000,
                tags=["adventureworks", "conference", "inference"],
            ),
            ScenarioMeta(
                name="healthcare",
                description="Healthcare domain — patients, encounters, medications, claims. PHI-safe.",
                domains=["healthcare"],
                supported_modes=["inference", "streaming", "seeding"],
                default_rows=50_000,
                tags=["healthcare", "compliance", "phi"],
            ),
            ScenarioMeta(
                name="enterprise",
                description=(
                    "Enterprise composite — retail + hr + financial across all 4 Fabric targets. "
                    "The '60-second workspace' demo."
                ),
                domains=["retail", "hr", "financial"],
                supported_modes=["seeding"],
                default_rows=200_000,
                tags=["enterprise", "composite", "all-targets"],
            ),
        ]
        for s in builtins:
            self._scenarios[s.name] = s

    def register(self, meta: ScenarioMeta) -> None:
        self._scenarios[meta.name] = meta

    def get(self, name: str) -> ScenarioMeta:
        if name not in self._scenarios:
            available = ", ".join(sorted(self._scenarios))
            raise KeyError(f"Scenario '{name}' not found. Available: {available}")
        return self._scenarios[name]

    def list(self) -> list:
        return list(self._scenarios.values())

    def compose(self, domains: list, mode: str, name: Optional[str] = None) -> ScenarioMeta:
        composed_name = name or f"custom_{'_'.join(domains)}"
        return ScenarioMeta(
            name=composed_name,
            description=f"Dynamic composite: {', '.join(domains)}",
            domains=domains,
            supported_modes=[mode],
            tags=["dynamic", "custom"],
        )


_catalog: Optional[ScenarioCatalog] = None


def get_catalog() -> ScenarioCatalog:
    global _catalog
    if _catalog is None:
        _catalog = ScenarioCatalog()
    return _catalog
