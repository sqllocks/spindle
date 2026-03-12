"""Delta Lake writer for Spindle output."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from sqllocks_spindle.output.fabric_utils import (
    detect_fabric_environment,
    parse_partition_spec,
)


class DeltaWriter:
    """Write generated tables as Delta Lake tables.

    Uses the ``deltalake`` (delta-rs) package — works both locally and inside
    Microsoft Fabric Notebooks without requiring Spark or JVM.

    Install the required extra::

        pip install sqllocks-spindle[fabric]

    Args:
        output_dir: Root directory for Delta tables.  Each table is written to
            a subdirectory ``{output_dir}/{table_name}/``.  If *None* and
            running inside a Fabric Notebook, defaults to
            ``/lakehouse/default/Tables/``.
        partition_by: Per-table partition specs from the schema output config.
            Keys are table names, values are lists of specs like
            ``["order_date:year", "order_date:month"]``.
        mode: Write mode — ``"overwrite"`` (default) or ``"append"``.

    Example::

        from sqllocks_spindle.output import DeltaWriter

        writer = DeltaWriter(output_dir="./delta_output")
        paths = writer.write_all(result.tables)
    """

    def __init__(
        self,
        output_dir: str | Path | None = None,
        partition_by: dict[str, list[str]] | None = None,
        mode: str = "overwrite",
    ) -> None:
        self._output_dir = self._resolve_output_dir(output_dir)
        self._partition_by = partition_by or {}
        self._mode = mode

    def write_all(
        self,
        tables: dict[str, pd.DataFrame],
    ) -> list[Path]:
        """Write all tables as Delta Lake tables.

        Args:
            tables: Mapping of table name to DataFrame (the standard Spindle
                output format from ``GenerationResult.tables``).

        Returns:
            List of paths to the written Delta table directories.
        """
        paths: list[Path] = []
        for table_name, df in tables.items():
            paths.append(self.write(table_name, df))
        return paths

    def write(
        self,
        table_name: str,
        df: pd.DataFrame,
    ) -> Path:
        """Write a single table as a Delta Lake table.

        Args:
            table_name: Name of the table (becomes the subdirectory name).
            df: DataFrame to write.

        Returns:
            Path to the written Delta table directory.
        """
        write_deltalake = self._import_deltalake()

        table_path = self._output_dir / table_name
        table_path.mkdir(parents=True, exist_ok=True)

        # Apply partition specs if configured for this table
        specs = self._partition_by.get(table_name, [])
        write_df, partition_cols = parse_partition_spec(specs, df)

        kwargs: dict[str, Any] = {
            "mode": self._mode,
        }
        if partition_cols:
            kwargs["partition_by"] = partition_cols

        write_deltalake(str(table_path), write_df, **kwargs)
        return table_path

    @staticmethod
    def _resolve_output_dir(output_dir: str | Path | None) -> Path:
        """Resolve output directory, auto-detecting Fabric if not specified."""
        if output_dir is not None:
            return Path(output_dir)

        env = detect_fabric_environment()
        if env.is_fabric and env.default_tables_path is not None:
            return env.default_tables_path

        raise ValueError(
            "No output_dir specified and not running in a Fabric Notebook. "
            "Pass output_dir explicitly, e.g.: DeltaWriter(output_dir='./delta_output')"
        )

    @staticmethod
    def _import_deltalake():
        """Import and return write_deltalake, with a helpful error if missing."""
        try:
            from deltalake import write_deltalake
        except ImportError:
            raise ImportError(
                "The 'deltalake' package is required for Delta Lake output. "
                "Install it with: pip install sqllocks-spindle[fabric]"
            ) from None
        return write_deltalake
