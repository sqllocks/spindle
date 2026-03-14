"""Spindle Fabric integration — OneLake, Eventstream, Lakehouse, SQL Database, Semantic Model."""

from sqllocks_spindle.fabric.onelake_paths import OneLakePaths
from sqllocks_spindle.fabric.lakehouse_files_writer import LakehouseFilesWriter

__all__ = [
    "EventstreamClient",
    "FabricSqlDatabaseWriter",
    "FabricStreamWriter",
    "LakehouseFilesWriter",
    "OneLakePaths",
    "SemanticModelExporter",
    "WriteResult",
]


def __getattr__(name: str):
    if name == "EventstreamClient":
        from sqllocks_spindle.fabric.eventstream_client import EventstreamClient
        return EventstreamClient
    if name in ("FabricSqlDatabaseWriter", "WriteResult"):
        from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter, WriteResult
        if name == "FabricSqlDatabaseWriter":
            return FabricSqlDatabaseWriter
        return WriteResult
    if name == "SemanticModelExporter":
        from sqllocks_spindle.fabric.semantic_model_writer import SemanticModelExporter
        return SemanticModelExporter
    if name == "FabricStreamWriter":
        from sqllocks_spindle.fabric.stream_writer_convenience import FabricStreamWriter
        return FabricStreamWriter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
