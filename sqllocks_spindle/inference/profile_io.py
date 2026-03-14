"""Profile I/O — export, import, and manage portable domain profiles.

Provides :class:`ProfileIO` for exporting domain profiles to standalone JSON
files, importing them into other domains, listing available profiles, and
inferring distribution profiles from raw DataFrames.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class ExportedProfile:
    """A portable profile that can be imported into any domain.

    Attributes:
        name: Profile identifier (e.g. ``"default"``, ``"high_volume"``).
        description: Human-readable description of what this profile represents.
        source_domain: Name of the domain this profile was exported from
            (or ``"inferred"`` when created via :meth:`ProfileIO.from_dataframe`).
        distributions: Mapping of ``"table.column"`` keys to value→weight dicts.
        ratios: Mapping of ratio names to float multipliers.
        metadata: Arbitrary extra information (row counts, column types, etc.).
    """

    name: str
    description: str
    source_domain: str
    distributions: dict[str, dict[str, float]]
    ratios: dict[str, float]
    metadata: dict[str, Any]


class ProfileIO:
    """Export, import, and list domain profiles.

    All public methods are stateless — no configuration is stored on the
    instance. Instantiate with ``ProfileIO()`` and call methods directly.

    Example::

        io = ProfileIO()
        io.export_profile(RetailDomain(), Path("retail_profile.json"))
        io.import_profile(Path("retail_profile.json"), HealthcareDomain(), save_as="from_retail")
        io.list_profiles(RetailDomain())
    """

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export_profile(
        self,
        domain: Any,
        output_path: str | Path,
        profile_name: str = "default",
    ) -> Path:
        """Export a domain's active profile to a standalone JSON file.

        Args:
            domain: A :class:`~sqllocks_spindle.domains.base.Domain` instance
                whose ``_profile`` dict will be serialised.
            output_path: Destination file path (created if it does not exist).
            profile_name: Label stored in the exported metadata.

        Returns:
            The resolved :class:`Path` the profile was written to.
        """
        output_path = Path(output_path)
        profile = domain._profile

        exported = ExportedProfile(
            name=profile.get("name", profile_name),
            description=profile.get("description", f"Exported from {domain.name}"),
            source_domain=domain.name,
            distributions=profile.get("distributions", {}),
            ratios=profile.get("ratios", {}),
            metadata={
                "exported_from": domain.name,
                "profile_name": profile_name,
            },
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(asdict(exported), f, indent=2)

        return output_path

    # ------------------------------------------------------------------
    # Import
    # ------------------------------------------------------------------

    def import_profile(
        self,
        profile_path: str | Path,
        target_domain: Any,
        save_as: str | None = None,
    ) -> str:
        """Import an exported profile into a target domain's ``profiles/`` directory.

        The imported file is converted to the standard domain profile format
        (i.e. metadata is stripped; only ``name``, ``description``,
        ``distributions``, and ``ratios`` are kept).

        Args:
            profile_path: Path to an exported profile JSON file.
            target_domain: The domain instance to import into.
            save_as: Override the profile name (and filename). When *None* the
                name is taken from the file's ``"name"`` field.

        Returns:
            The name the profile was saved as.
        """
        profile_path = Path(profile_path)
        with open(profile_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Determine save name
        if save_as:
            name = save_as
        else:
            name = data.get("name", profile_path.stem)

        # Write to target domain's profiles/ directory
        target_dir = target_domain.domain_path / "profiles"
        target_dir.mkdir(parents=True, exist_ok=True)

        # Convert to standard profile format (strip metadata, keep distributions/ratios)
        profile_data = {
            "name": name,
            "description": data.get(
                "description",
                f"Imported from {data.get('source_domain', 'unknown')}",
            ),
            "distributions": data.get("distributions", {}),
            "ratios": data.get("ratios", {}),
        }

        target_file = target_dir / f"{name}.json"
        with open(target_file, "w", encoding="utf-8") as f:
            json.dump(profile_data, f, indent=2)

        return name

    # ------------------------------------------------------------------
    # List
    # ------------------------------------------------------------------

    def list_profiles(self, domain: Any) -> list[dict[str, str | int]]:
        """List all profiles available for a domain.

        Args:
            domain: A :class:`~sqllocks_spindle.domains.base.Domain` instance.

        Returns:
            A list of dicts with keys ``name``, ``description``,
            ``distributions`` (count), and ``ratios`` (count).
        """
        profiles_dir = domain.domain_path / "profiles"
        result: list[dict[str, str | int]] = []
        if profiles_dir.exists():
            for f in sorted(profiles_dir.glob("*.json")):
                try:
                    with open(f, encoding="utf-8") as fp:
                        data = json.load(fp)
                    result.append(
                        {
                            "name": f.stem,
                            "description": data.get("description", ""),
                            "distributions": len(data.get("distributions", {})),
                            "ratios": len(data.get("ratios", {})),
                        }
                    )
                except (json.JSONDecodeError, KeyError):
                    result.append(
                        {
                            "name": f.stem,
                            "description": "(invalid JSON)",
                            "distributions": 0,
                            "ratios": 0,
                        }
                    )
        return result

    # ------------------------------------------------------------------
    # Infer from DataFrame
    # ------------------------------------------------------------------

    def from_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str = "table",
        name: str = "inferred",
    ) -> ExportedProfile:
        """Create a profile by inferring distributions from a DataFrame.

        Categorical columns (object dtype or low cardinality) are converted
        into normalised distribution weights. High-cardinality columns are
        skipped.

        Args:
            df: The source DataFrame.
            table_name: Prefix for distribution keys (``"table_name.column"``).
            name: Name to assign to the resulting profile.

        Returns:
            An :class:`ExportedProfile` ready to be serialised or imported.
        """
        distributions: dict[str, dict[str, float]] = {}
        ratios: dict[str, float] = {}

        for col in df.columns:
            series = df[col].dropna()
            if len(series) == 0:
                continue

            # Detect categorical columns (low cardinality)
            is_string = (
                series.dtype == object
                or pd.api.types.is_string_dtype(series)
                or hasattr(series, "cat")
            )
            if is_string:
                cardinality = series.nunique()
                if cardinality <= 50 or cardinality / len(series) < 0.05:
                    # Record distribution weights
                    counts = series.value_counts(normalize=True)
                    distributions[f"{table_name}.{col}"] = {
                        str(k): round(float(v), 4) for k, v in counts.items()
                    }

        return ExportedProfile(
            name=name,
            description=f"Inferred from {table_name} ({len(df)} rows)",
            source_domain="inferred",
            distributions=distributions,
            ratios=ratios,
            metadata={"row_count": len(df), "columns": list(df.columns)},
        )
