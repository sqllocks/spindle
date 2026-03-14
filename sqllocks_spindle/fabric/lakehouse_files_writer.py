"""Lakehouse Files landing-zone writer for Fabric."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from sqllocks_spindle.fabric.onelake_paths import OneLakePaths


class LakehouseFilesWriter:
    """Write data files into Lakehouse Files landing zones.

    This writer targets the ``Files/`` area of a Fabric Lakehouse (flat files
    in landing zones), as opposed to :class:`~sqllocks_spindle.output.DeltaWriter`
    which writes Delta tables into the ``Tables/`` area.

    Uses :class:`OneLakePaths` for consistent path construction and works both
    inside a Fabric runtime and locally with a configurable base path.

    Args:
        base_path: Root path for the Lakehouse Files area.  Passed through to
            :class:`OneLakePaths`.  If *None*, auto-detects Fabric or falls
            back to ``./lakehouse_files``.
        default_format: Default output format — ``"parquet"``, ``"csv"``, or
            ``"jsonl"`` (default ``"parquet"``).

    Example::

        from sqllocks_spindle.fabric import LakehouseFilesWriter, OneLakePaths

        writer = LakehouseFilesWriter(base_path="./test_output")
        paths = OneLakePaths(base_path="./test_output")
        dest = paths.landing_zone_path("retail", "order", "2025-01-15", hour=10)

        writer.write_partition(df, dest, format="parquet")
        writer.write_done_flag(paths.done_flag_path("retail", "order", "2025-01-15"))
    """

    _SUPPORTED_FORMATS = ("csv", "parquet", "jsonl")

    def __init__(
        self,
        base_path: str | Path | None = None,
        default_format: str = "parquet",
    ) -> None:
        self._paths = OneLakePaths(base_path=base_path)
        self._default_format = self._validate_format(default_format)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def paths(self) -> OneLakePaths:
        """Return the underlying :class:`OneLakePaths` instance."""
        return self._paths

    def write_partition(
        self,
        df: pd.DataFrame,
        path: str | Path,
        format: str | None = None,
        file_naming_template: str | None = None,
    ) -> Path:
        """Write a DataFrame as a data file into a landing-zone partition.

        Args:
            df: DataFrame to write.
            path: Target directory for the partition.
            format: Output format — ``"csv"``, ``"parquet"``, or ``"jsonl"``.
                Falls back to ``default_format`` if *None*.
            file_naming_template: Template for the output file name.  Supports
                ``{format}`` placeholder.  Defaults to ``"part-0001.{format}"``.

        Returns:
            Path to the written file.

        Raises:
            ValueError: If format is unsupported.
        """
        fmt = self._validate_format(format or self._default_format)
        dest_dir = Path(path)
        dest_dir.mkdir(parents=True, exist_ok=True)

        ext = "jsonl" if fmt == "jsonl" else fmt
        if file_naming_template is not None:
            filename = file_naming_template.format(format=ext)
        else:
            filename = f"part-0001.{ext}"

        file_path = dest_dir / filename

        if fmt == "parquet":
            self._write_parquet(df, file_path)
        elif fmt == "csv":
            self._write_csv(df, file_path)
        elif fmt == "jsonl":
            self._write_jsonl(df, file_path)

        return file_path

    def write_manifest(
        self,
        manifest_dict: dict[str, Any],
        path: str | Path,
    ) -> Path:
        """Write a manifest JSON file to the control directory.

        Args:
            manifest_dict: Manifest contents (serialised as JSON).
            path: Full path to the manifest file.

        Returns:
            Path to the written manifest file.
        """
        dest = Path(path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(
            json.dumps(manifest_dict, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        return dest

    def write_done_flag(self, path: str | Path) -> Path:
        """Write an empty ``_SUCCESS`` sentinel file.

        Args:
            path: Full path to the done-flag file.

        Returns:
            Path to the written sentinel file.
        """
        dest = Path(path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("", encoding="utf-8")
        return dest

    # ------------------------------------------------------------------
    # Format writers
    # ------------------------------------------------------------------

    @staticmethod
    def _write_parquet(df: pd.DataFrame, path: Path) -> None:
        """Write a DataFrame as Parquet."""
        try:
            df.to_parquet(path, index=False, engine="pyarrow")
        except ImportError:
            raise ImportError(
                "The 'pyarrow' package is required for Parquet output. "
                "Install it with: pip install pyarrow"
            ) from None

    @staticmethod
    def _write_csv(df: pd.DataFrame, path: Path) -> None:
        """Write a DataFrame as CSV."""
        df.to_csv(path, index=False, encoding="utf-8")

    @staticmethod
    def _write_jsonl(df: pd.DataFrame, path: Path) -> None:
        """Write a DataFrame as JSON Lines (one JSON object per line)."""
        with open(path, "w", encoding="utf-8") as fh:
            for record in df.to_dict("records"):
                fh.write(json.dumps(record, default=str) + "\n")

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @classmethod
    def _validate_format(cls, fmt: str) -> str:
        """Validate and normalise the output format string."""
        fmt_lower = fmt.lower()
        if fmt_lower not in cls._SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format '{fmt}'. "
                f"Supported: {list(cls._SUPPORTED_FORMATS)}"
            )
        return fmt_lower
