"""Spindle Fabric integration — OneLake, Eventstream, Lakehouse, SQL Database, Semantic Model."""

from sqllocks_spindle.fabric.onelake_paths import OneLakePaths
from sqllocks_spindle.fabric.lakehouse_files_writer import LakehouseFilesWriter, LakehouseWriteResult
from sqllocks_spindle.fabric.multi_writer import MultiWriter, MultiWriteResult

__all__ = [
    "BulkWriteResult",
    "CredentialResolver",
    "CredentialError",
    "EventhouseWriter",
    "EventhouseWriteResult",
    "EventstreamClient",
    "FabricSqlDatabaseWriter",
    "FabricStreamWriter",
    "LakehouseFilesWriter",
    "LakehouseWriteResult",
    "MultiWriter",
    "MultiWriteResult",
    "OneLakePaths",
    "SemanticModelExporter",
    "WarehouseBulkWriter",
    "WriteResult",
]


def __getattr__(name: str):
    if name in ("CredentialResolver", "CredentialError"):
        from sqllocks_spindle.fabric.credentials import CredentialResolver, CredentialError
        if name == "CredentialResolver":
            return CredentialResolver
        return CredentialError
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
    if name in ("EventhouseWriter", "EventhouseWriteResult"):
        from sqllocks_spindle.fabric.eventhouse_writer import EventhouseWriter, EventhouseWriteResult
        if name == "EventhouseWriter":
            return EventhouseWriter
        return EventhouseWriteResult
    if name in ("WarehouseBulkWriter", "BulkWriteResult"):
        from sqllocks_spindle.fabric.warehouse_bulk_writer import WarehouseBulkWriter, BulkWriteResult
        if name == "WarehouseBulkWriter":
            return WarehouseBulkWriter
        return BulkWriteResult
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
