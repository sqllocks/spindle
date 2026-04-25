"""OneLake path construction for Fabric Lakehouse landing zones."""

from __future__ import annotations

import os
from pathlib import Path


class OneLakePaths:
    """Constructs proper OneLake paths for Fabric Lakehouse items.

    Supports the standard landing zone layout::

        Files/landing/<domain>/<entity>/dt=YYYY-MM-DD/hour=HH/
        Files/landing/<domain>/<entity>/_control/
        Files/quarantine/<domain>/<run_id>/

    Works both inside a Fabric runtime (auto-detected via ``FABRIC_RUNTIME``
    or ``TRIDENT_RUNTIME_VERSION`` environment variables) and locally with a
    configurable base path.

    Args:
        base_path: Root path for the Lakehouse Files area.  If *None*,
            auto-detects: inside Fabric uses ``/lakehouse/default/Files``,
            otherwise defaults to ``./lakehouse_files``.
    """

    _FABRIC_ENV_VARS = ("FABRIC_RUNTIME", "TRIDENT_RUNTIME_VERSION")

    def __init__(self, base_path: str | Path | None = None) -> None:
        self._base: Path | str = self._resolve_base(base_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def base(self) -> Path | str:
        """Return the resolved base path (Path for local, str URI for remote)."""
        return self._base

    def landing_zone_path(
        self,
        domain: str,
        entity: str,
        dt: str,
        hour: str | int | None = None,
    ) -> Path | str:
        """Build the landing zone partition path.

        Args:
            domain: Domain name (e.g. ``"retail"``).
            entity: Entity / table name (e.g. ``"order"``).
            dt: Date string in ``YYYY-MM-DD`` format.
            hour: Optional hour partition (``"00"``–``"23"``).  When provided
                the path includes an ``hour=HH`` segment.

        Returns:
            Fully qualified landing zone directory path.
        """
        path = self._join(self._base, "landing", domain, entity, f"dt={dt}")
        if hour is not None:
            path = self._join(path, f"hour={str(hour).zfill(2)}")
        return path

    def quarantine_path(self, domain: str, run_id: str) -> Path | str:
        """Build the quarantine directory for failed / rejected rows.

        Args:
            domain: Domain name.
            run_id: Unique identifier for the ingestion run.

        Returns:
            Quarantine directory path.
        """
        return self._join(self._base, "quarantine", domain, run_id)

    def tables_path(self, table_name: str) -> Path:
        """Build the Tables path for a Delta table (sibling of Files).

        This points to ``<lakehouse_root>/Tables/<table_name>`` — the
        standard location that DeltaWriter targets.

        Args:
            table_name: Name of the Delta table.

        Returns:
            Path to the table directory under Tables.
        """
        # Tables lives alongside Files, one level up from base (local only)
        assert isinstance(self._base, Path), "tables_path() is only supported for local paths"
        return self._base.parent / "Tables" / table_name

    def control_path(self, domain: str, entity: str) -> Path | str:
        """Build the control directory path for manifests and done flags.

        Args:
            domain: Domain name.
            entity: Entity / table name.

        Returns:
            Control directory path.
        """
        return self._join(self._base, "landing", domain, entity, "_control")

    def manifest_path(self, domain: str, entity: str, dt: str) -> Path | str:
        """Build the manifest file path for a given date partition.

        Args:
            domain: Domain name.
            entity: Entity / table name.
            dt: Date string in ``YYYY-MM-DD`` format.

        Returns:
            Path to the manifest JSON file.
        """
        return self._join(self.control_path(domain, entity), f"manifest_{dt}.json")

    def done_flag_path(self, domain: str, entity: str, dt: str) -> Path | str:
        """Build the done-flag (sentinel) file path for a given date partition.

        Args:
            domain: Domain name.
            entity: Entity / table name.
            dt: Date string in ``YYYY-MM-DD`` format.

        Returns:
            Path to the ``_SUCCESS`` sentinel file.
        """
        return self._join(self.control_path(domain, entity), f"_SUCCESS_{dt}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @classmethod
    def _is_fabric_runtime(cls) -> bool:
        """Detect whether we are running inside a Fabric Notebook."""
        return any(os.environ.get(v) for v in cls._FABRIC_ENV_VARS)

    @classmethod
    def _resolve_base(cls, base_path: str | Path | None) -> Path | str:
        """Resolve the base path for Files, auto-detecting Fabric if needed."""
        if base_path is not None:
            s = str(base_path)
            if s.startswith("abfss://") or s.startswith("wasbs://"):
                return s.rstrip("/")
            return Path(base_path)

        if cls._is_fabric_runtime():
            lakehouse = Path("/lakehouse/default")
            if lakehouse.exists() and lakehouse.is_dir():
                return lakehouse / "Files"
            # Fabric env var set but path doesn't exist — use it anyway
            return lakehouse / "Files"

        return Path("./lakehouse_files")

    @staticmethod
    def _join(base: Path | str, *parts: str) -> Path | str:
        """Join path parts onto a local Path or remote URI base."""
        if isinstance(base, str):
            return "/".join([base.rstrip("/")] + [str(p) for p in parts])
        return base.joinpath(*parts)
