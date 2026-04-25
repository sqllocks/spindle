"""Delta Lake writer for Spindle output."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
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
        self._write_deltalake = None  # lazy-imported on first write

    def write_all(
        self,
        tables: dict[str, pd.DataFrame],
        max_workers: int = 4,
    ) -> list[Path]:
        """Write all tables as Delta Lake tables in parallel.

        delta-rs releases the GIL during Parquet/Delta writes, so
        ThreadPoolExecutor gives genuine parallelism here.

        Args:
            tables: Mapping of table name to DataFrame (the standard Spindle
                output format from ``GenerationResult.tables``).
            max_workers: Maximum parallel write threads (default 4).

        Returns:
            List of paths to the written Delta table directories.
        """
        # Warm the import cache before spawning threads
        if self._write_deltalake is None:
            self._write_deltalake = self._import_deltalake()

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self.write, name, df): name for name, df in tables.items()}
            return [f.result() for f in futures]

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
        if self._write_deltalake is None:
            self._write_deltalake = self._import_deltalake()

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

        try:
            self._write_deltalake(str(table_path), write_df, **kwargs)
        except (ValueError, TypeError) as e:
            if 'arrow_c_array' in str(e) or 'arrow_c_stream' in str(e):
                # Fabric Spark: deltalake pip conflicts with built-in pyarrow
                # Fall back to pyarrow parquet write (creates valid Delta via _delta_log)
                self._write_parquet_fallback(str(table_path), write_df, **kwargs)
            else:
                raise
        return table_path

    @staticmethod
    def _write_parquet_fallback(path: str, df: "pd.DataFrame", **kwargs) -> None:
        """Fallback: write as Parquet when deltalake has pyarrow conflicts."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        from pathlib import Path

        table_path = Path(path)
        table_path.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, str(table_path / "part-00000.parquet"))

        # Write minimal Delta log so it registers as a Delta table
        import json, time
        log_dir = table_path / "_delta_log"
        log_dir.mkdir(exist_ok=True)
        commit = {
            "commitInfo": {"timestamp": int(time.time() * 1000), "operation": "WRITE"},
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
            "metaData": {
                "schemaString": json.dumps({"type": "struct", "fields": [
                    {"name": c, "type": str(df[c].dtype), "nullable": True} for c in df.columns
                ]}),
                "partitionColumns": [],
                "format": {"provider": "parquet"},
            },
            "add": {
                "path": "part-00000.parquet",
                "size": (table_path / "part-00000.parquet").stat().st_size,
                "modificationTime": int(time.time() * 1000),
                "dataChange": True,
            },
        }
        log_file = log_dir / "00000000000000000000.json"
        log_file.write_text(chr(10).join(json.dumps(entry) for entry in [
            {"commitInfo": commit["commitInfo"]},
            {"protocol": commit["protocol"]},
            {"metaData": commit["metaData"]},
            {"add": commit["add"]},
        ]))

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
