"""CSV writer — write generated tables to CSV files."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


class CsvWriter:
    """Write generated tables to CSV files.

    Example:
        from sqllocks_spindle.output import CsvWriter

        writer = CsvWriter(output_dir="./output")
        writer.write_all(result.tables)
    """

    def __init__(
        self,
        output_dir: str | Path = "./output",
        separator: str = ",",
        encoding: str = "utf-8",
    ):
        self.output_dir = Path(output_dir)
        self.separator = separator
        self.encoding = encoding

    def write_all(
        self,
        tables: dict[str, pd.DataFrame],
    ) -> list[Path]:
        """Write all tables as separate CSV files.

        Returns a list of paths to the written files.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)

        written = []
        for table_name, df in tables.items():
            path = self.output_dir / f"{table_name}.csv"
            df.to_csv(path, index=False, sep=self.separator, encoding=self.encoding)
            written.append(path)

        return written

    def write(
        self,
        table_name: str,
        df: pd.DataFrame,
    ) -> Path:
        """Write a single table to CSV."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        path = self.output_dir / f"{table_name}.csv"
        df.to_csv(path, index=False, sep=self.separator, encoding=self.encoding)
        return path
