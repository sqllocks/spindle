"""Base class for domain libraries."""

from __future__ import annotations

import json
import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from sqllocks_spindle.schema.parser import SchemaParser, SpindleSchema


class Domain(ABC):
    """Base class for all Spindle domains (Retail, Financial, etc.).

    Args:
        schema_mode: Schema layout variant (e.g., '3nf', 'star').
        profile: Name of a distribution profile to load from the domain's
            ``profiles/`` directory. Defaults to ``"default"``.
        overrides: Dict of ``"table.column"`` → value mappings that override
            any profile weights at generation time.

    Example::

        # Use the default profile with one tweak
        domain = RetailDomain(overrides={
            "customer.loyalty_tier": {"Basic": 0.60, "Silver": 0.20, "Gold": 0.15, "Platinum": 0.05},
        })

        # Use a named profile
        domain = HealthcareDomain(profile="medicare")
    """

    def __init__(
        self,
        schema_mode: str = "3nf",
        profile: str = "default",
        overrides: dict[str, Any] | None = None,
    ):
        self._schema_mode = schema_mode
        self._parser = SchemaParser()
        self._profile = self._load_profile(profile)
        if overrides:
            self._warn_unknown_keys(overrides)
            self._profile.setdefault("distributions", {}).update(overrides)

    def _load_profile(self, name: str) -> dict[str, Any]:
        """Load a named profile JSON from the domain's profiles/ directory."""
        path = self.domain_path / "profiles" / f"{name}.json"
        if path.exists():
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _warn_unknown_keys(self, overrides: dict[str, Any]) -> None:
        """Warn if override keys don't match any known profile distribution key."""
        known = set(self._profile.get("distributions", {}).keys())
        if not known:
            return
        for key in overrides:
            if key not in known:
                warnings.warn(
                    f"Override key '{key}' not found in {self.name} profile. "
                    f"It will still be applied, but may not match any _dist() lookup. "
                    f"Use 'spindle describe {self.name}' to see valid keys.",
                    stacklevel=3,
                )

    def _dist(self, key: str, default: Any = None) -> Any:
        """Look up a distribution weight from the active profile.

        Args:
            key: Dotted path like ``"customer.gender"`` or
                 ``"order.order_date.month"``.
            default: Fallback value if the key is not in the profile.

        Returns:
            The profile value for the key, or *default* if not found.
        """
        return self._profile.get("distributions", {}).get(key, default)

    def _ratio(self, key: str, default: float = 1.0) -> float:
        """Look up a derived-count ratio from the active profile."""
        return self._profile.get("ratios", {}).get(key, default)

    @property
    def profile_name(self) -> str:
        return self._profile.get("name", "default")

    @property
    def available_profiles(self) -> list[str]:
        """List available profile names for this domain."""
        profiles_dir = self.domain_path / "profiles"
        if profiles_dir.exists():
            return [f.stem for f in profiles_dir.glob("*.json")]
        return []

    @property
    @abstractmethod
    def name(self) -> str:
        """Domain name (e.g., 'retail', 'financial')."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """Brief description of the domain."""
        ...

    @property
    def schema_mode(self) -> str:
        return self._schema_mode

    @property
    def domain_path(self) -> Path:
        """Path to this domain's directory (for reference data lookup)."""
        return Path(__file__).parent / self.name

    @property
    def available_modes(self) -> list[str]:
        """List available schema modes for this domain."""
        modes = []
        domain_dir = self.domain_path
        if domain_dir.exists():
            for f in domain_dir.glob("*.spindle.json"):
                mode = f.stem.replace(f"{self.name}_", "")
                modes.append(mode)
        return modes

    def get_schema(self) -> SpindleSchema:
        """Load and return the schema for the current mode."""
        schema_file = self.domain_path / f"{self.name}_{self._schema_mode}.spindle.json"
        if schema_file.exists():
            schema = self._parser.parse_file(schema_file)
        else:
            schema = self._build_schema()

        # Inject domain path for reference data resolution
        return schema

    @abstractmethod
    def _build_schema(self) -> SpindleSchema:
        """Build schema programmatically (fallback if no JSON file)."""
        ...
