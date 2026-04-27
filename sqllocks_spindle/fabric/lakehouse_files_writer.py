"""Lakehouse Files landing-zone writer for Fabric."""

from __future__ import annotations

import io
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd

from sqllocks_spindle.fabric.onelake_paths import OneLakePaths


@dataclass
class LakehouseWriteResult:
    """Result of a Lakehouse write_all() operation."""

    tables_written: int = 0
    rows_written: int = 0
    elapsed_seconds: float = 0.0
    per_table: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def summary(self) -> str:
        lines = [
            "Lakehouse Files Write Result",
            "=" * 40,
            f"Tables written: {self.tables_written}",
            f"Total rows:     {self.rows_written:,}",
            f"Elapsed:        {self.elapsed_seconds:.1f}s",
        ]
        if self.per_table:
            lines.append("")
            lines.append(f"{'Table':<30} {'Rows':>10}")
            lines.append("-" * 40)
            for tname, count in self.per_table.items():
                lines.append(f"{tname:<30} {count:>10,}")
        if self.errors:
            lines.append("")
            lines.append(f"Errors ({len(self.errors)}):")
            for err in self.errors:
                lines.append(f"  - {err}")
        return "\n".join(lines)


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

    def write_all(
        self,
        tables: dict[str, pd.DataFrame],
        format: str | None = None,
        **kwargs: Any,
    ) -> LakehouseWriteResult:
        """Write each table as a bulk file (Parquet/CSV/JSONL).

        Conforms to the SpindleWriter protocol so LakehouseFilesWriter
        can be used with MultiWriter.

        Args:
            tables: Mapping of table_name -> DataFrame.
            format: Output format (defaults to writer's default_format).
            **kwargs: Extra args passed to write_partition.

        Returns:
            LakehouseWriteResult with per-table row counts and any errors.
        """
        start = time.time()
        result = LakehouseWriteResult()

        if not tables:
            result.elapsed_seconds = time.time() - start
            return result

        fmt = format or self._default_format

        for table_name, df in tables.items():
            try:
                base = self._paths.base
                # Don't wrap abfss:// or wasbs:// URIs in Path() — Path normalizes
                # the double slash to a single one, breaking the URL scheme.
                if isinstance(base, str) and (
                    base.startswith("abfss://") or base.startswith("wasbs://")
                ):
                    dest_dir = base.rstrip("/") + "/" + table_name
                else:
                    dest_dir = Path(base) / table_name
                self.write_partition(df, dest_dir, format=fmt, **kwargs)
                result.per_table[table_name] = len(df)
                result.rows_written += len(df)
                result.tables_written += 1
            except Exception as exc:
                result.errors.append(f"{table_name}: {exc}")

        result.elapsed_seconds = time.time() - start
        return result

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
        ext = "jsonl" if fmt == "jsonl" else fmt
        filename = (
            file_naming_template.format(format=ext)
            if file_naming_template is not None
            else f"part-0001.{ext}"
        )

        if str(path).startswith("abfss://") or str(path).startswith("wasbs://"):
            return self._write_partition_remote(df, str(path), fmt, filename)

        dest_dir = Path(path)
        dest_dir.mkdir(parents=True, exist_ok=True)
        file_path = dest_dir / filename

        if fmt == "parquet":
            self._write_parquet(df, file_path)
        elif fmt == "csv":
            self._write_csv(df, file_path)
        elif fmt == "jsonl":
            self._write_jsonl(df, file_path)

        return file_path

    def _write_partition_remote(
        self,
        df: pd.DataFrame,
        dir_uri: str,
        fmt: str,
        filename: str,
    ) -> str:
        """Write to an abfss:// URI via azure-storage-file-datalake."""
        try:
            from azure.storage.filedatalake import DataLakeServiceClient
            from azure.identity import DefaultAzureCredential, AzureCliCredential
        except ImportError:
            raise ImportError(
                "azure-storage-file-datalake and azure-identity are required for "
                "abfss:// paths. Install with: pip install azure-storage-file-datalake azure-identity"
            )

        parsed = urlparse(dir_uri)
        # abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Files/...
        filesystem = parsed.username  # workspace ID
        host = parsed.hostname        # onelake.dfs.fabric.microsoft.com
        blob_dir = parsed.path.lstrip("/")

        # DefaultAzureCredential can break on machines with Azure Arc agent
        # (ManagedIdentityCredential raises Access Denied, halting the chain).
        # Try AzureCliCredential first; fall back to DefaultAzureCredential
        # with managed identity excluded.
        try:
            credential = AzureCliCredential()
            credential.get_token("https://storage.azure.com/.default")
        except Exception:
            credential = DefaultAzureCredential(
                exclude_managed_identity_credential=True,
            )

        service = DataLakeServiceClient(
            account_url=f"https://{host}",
            credential=credential,
        )
        fs = service.get_file_system_client(filesystem)
        file_client = fs.get_file_client(f"{blob_dir}/{filename}")

        buf = io.BytesIO()
        if fmt == "parquet":
            df.to_parquet(buf, index=False, engine="pyarrow")
        elif fmt == "csv":
            buf.write(df.to_csv(index=False, encoding="utf-8").encode("utf-8"))
        elif fmt == "jsonl":
            for record in df.to_dict("records"):
                buf.write((json.dumps(record, default=str) + "\n").encode("utf-8"))

        data = buf.getvalue()
        file_client.upload_data(data, overwrite=True, length=len(data))
        return f"{dir_uri}/{filename}"

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
