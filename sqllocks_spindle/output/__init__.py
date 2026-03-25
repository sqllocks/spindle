"""Output writers for various formats."""

from sqllocks_spindle.output.csv_writer import CsvWriter
from sqllocks_spindle.output.pandas_writer import PandasWriter

__all__ = ["CsvWriter", "DeltaWriter", "MultiStoreWriter", "MultiStoreResult", "PandasWriter"]


def __getattr__(name: str):
    if name == "DeltaWriter":
        from sqllocks_spindle.output.delta_writer import DeltaWriter
        return DeltaWriter
    if name in ("MultiStoreWriter", "MultiStoreResult"):
        from sqllocks_spindle.output.multi_store_writer import MultiStoreWriter, MultiStoreResult
        if name == "MultiStoreWriter":
            return MultiStoreWriter
        return MultiStoreResult
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
