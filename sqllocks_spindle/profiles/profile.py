"""Profile registry data model."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class RegistryProfile:
    """A named, tagged data profile stored in the registry.

    Identity format: ``<system>/<table>/<profile_name>``
    e.g. ``salesforce/customer/prod-2026Q2``
    """

    system: str
    table: str
    name: str
    columns: dict[str, dict[str, Any]] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    description: str = ""
    source_rows: int = 0

    # ---------------------------------------------------------------------------
    # Derived identity
    # ---------------------------------------------------------------------------

    @property
    def identity(self) -> str:
        """Fully-qualified profile identity: ``system/table/name``."""
        return f"{self.system}/{self.table}/{self.name}"

    # ---------------------------------------------------------------------------
    # Serialisation
    # ---------------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "system": self.system,
            "table": self.table,
            "name": self.name,
            "description": self.description,
            "source_rows": self.source_rows,
            "tags": self.tags,
            "columns": self.columns,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RegistryProfile":
        return cls(
            system=data["system"],
            table=data["table"],
            name=data["name"],
            description=data.get("description", ""),
            source_rows=data.get("source_rows", 0),
            tags=data.get("tags", []),
            columns=data.get("columns", {}),
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2))

    @classmethod
    def load(cls, path: Path) -> "RegistryProfile":
        return cls.from_dict(json.loads(path.read_text()))
