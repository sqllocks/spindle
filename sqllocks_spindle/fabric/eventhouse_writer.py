"""Write generated data to a Fabric Eventhouse (KQL database).

Uses the Azure Kusto SDK for table creation and managed streaming
ingestion into KQL databases hosted on Microsoft Fabric.

Requires the ``fabric-kusto`` extra::

    pip install sqllocks-spindle[fabric-kusto]
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class EventhouseWriteResult:
    """Result of an Eventhouse write operation."""

    tables_written: int
    rows_written: int
    errors: list[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0
    per_table: dict[str, int] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def summary(self) -> str:
        lines = [
            "Eventhouse Write Result",
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


class EventhouseWriter:
    """Write generated data to a Fabric Eventhouse (KQL database).

    Supports:
        - Fabric Eventhouse (\\*.kusto.fabric.microsoft.com)

    Authentication methods:
        - ``"cli"`` — Azure CLI (``az login``), default for local dev
        - ``"msi"`` — Managed Identity, for Fabric Notebooks / Azure VMs
        - ``"spn"`` — Service Principal, for CI/CD pipelines

    Usage::

        writer = EventhouseWriter(
            cluster_uri="https://my-eventhouse.z0.kusto.fabric.microsoft.com",
            database="MyKQLDatabase",
            auth_method="cli",
        )
        result = writer.write(generation_result, table_prefix="gen_")
    """

    def __init__(
        self,
        cluster_uri: str,
        database: str,
        auth_method: str = "cli",
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
    ):
        self._cluster_uri = cluster_uri.rstrip("/")
        self._database = database
        self._auth_method = auth_method
        self._client_id = client_id
        self._client_secret = client_secret
        self._tenant_id = tenant_id

        # Validate auth method
        valid_methods = ("cli", "msi", "spn", "fabric")
        if auth_method not in valid_methods:
            raise ValueError(f"auth_method must be one of {valid_methods}, got '{auth_method}'")

        if auth_method == "spn" and not all([client_id, client_secret, tenant_id]):
            raise ValueError("auth_method='spn' requires client_id, client_secret, and tenant_id")

        # Lazy-validate that Kusto SDK is available
        self._validate_sdk()

    def _validate_sdk(self) -> None:
        """Check that required Kusto packages are importable."""
        try:
            import azure.kusto.data  # noqa: F401
        except ImportError:
            raise ImportError(
                "azure-kusto-data is required for Eventhouse output. "
                "Install with: pip install azure-kusto-data azure-kusto-ingest azure-identity"
            )

        try:
            import azure.kusto.ingest  # noqa: F401
        except ImportError:
            raise ImportError(
                "azure-kusto-ingest is required for Eventhouse ingestion. "
                "Install with: pip install azure-kusto-ingest"
            )

    def write(
        self,
        result: Any,
        table_prefix: str = "",
        batch_size: int = 10_000,
    ) -> EventhouseWriteResult:
        """Write all tables from a GenerationResult or dict of DataFrames.

        Args:
            result: A GenerationResult or dict[str, DataFrame].
            table_prefix: Optional prefix for KQL table names.
            batch_size: Rows per ingestion batch.

        Returns:
            EventhouseWriteResult with per-table row counts and any errors.
        """
        import pandas as pd

        # Normalize input
        if isinstance(result, dict):
            tables: dict[str, pd.DataFrame] = result
            order = list(tables.keys())
        else:
            tables = result.tables
            order = getattr(result, "generation_order", list(tables.keys()))

        start = time.time()
        write_result = EventhouseWriteResult(tables_written=0, rows_written=0)

        try:
            client = self._get_client()
        except Exception as e:
            write_result.errors.append(f"Client creation failed: {e}")
            write_result.elapsed_seconds = time.time() - start
            return write_result

        try:
            ingest_client = self._get_ingest_client()
        except Exception as e:
            write_result.errors.append(f"Ingest client creation failed: {e}")
            write_result.elapsed_seconds = time.time() - start
            return write_result

        # Phase 1: Create all tables first
        table_map = {}  # kql_name -> (tname, df)
        for tname in order:
            if tname not in tables:
                continue
            df = tables[tname]
            kql_table_name = f"{table_prefix}{tname}"
            try:
                self._ensure_table(client, kql_table_name, df)
                table_map[kql_table_name] = (tname, df)
            except Exception as e:
                write_result.errors.append(f"{kql_table_name}: {e}")
                logger.error("Error creating table %s: %s", kql_table_name, e)

        # Brief pause for table metadata propagation
        if table_map:
            time.sleep(2)

        # Phase 2: Ingest all tables (retry once on EntityNotFound)
        for kql_table_name, (tname, df) in table_map.items():
            for attempt in range(2):
                try:
                    rows = self._ingest_table(ingest_client, kql_table_name, df, batch_size)
                    write_result.per_table[kql_table_name] = rows
                    write_result.rows_written += rows
                    write_result.tables_written += 1
                    logger.info("Wrote %d rows to %s.%s", rows, self._database, kql_table_name)
                    break
                except Exception as e:
                    if attempt == 0 and "EntityNotFound" in str(e):
                        logger.warning("Table %s not yet visible, retrying in 3s...", kql_table_name)
                        time.sleep(3)
                        continue
                    write_result.errors.append(f"{kql_table_name}: {e}")
                    logger.error("Error ingesting %s: %s", kql_table_name, e)
                    break

        write_result.elapsed_seconds = time.time() - start
        return write_result


    def write_all(self, tables: dict[str, Any], **kwargs: Any) -> EventhouseWriteResult:
        """Write all tables — protocol-compatible alias for write().

        Conforms to the SpindleWriter protocol so EventhouseWriter can be
        used with MultiStoreWriter.
        """
        return self.write(result=tables, **kwargs)

    # ----- internal: clients -----

    def _get_client(self):
        """Create an authenticated KustoClient for management commands."""
        from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

        kcsb = self._build_connection_string(KustoConnectionStringBuilder)
        return KustoClient(kcsb)

    def _get_ingest_client(self):
        """Create an authenticated managed streaming ingest client."""
        from azure.kusto.data import KustoConnectionStringBuilder
        from azure.kusto.ingest import ManagedStreamingIngestClient

        kcsb = self._build_connection_string(KustoConnectionStringBuilder)
        return ManagedStreamingIngestClient(engine_kcsb=kcsb)

    def _build_connection_string(self, kcsb_cls):
        """Build a KustoConnectionStringBuilder with the configured auth."""
        if self._auth_method == "cli":
            kcsb = kcsb_cls.with_az_cli_authentication(self._cluster_uri)
        elif self._auth_method == "msi":
            kcsb = kcsb_cls.with_aad_managed_service_identity_authentication(
                self._cluster_uri,
            )
        elif self._auth_method == "spn":
            kcsb = kcsb_cls.with_aad_application_key_authentication(
                connection_string=self._cluster_uri,
                aad_app_id=self._client_id,
                app_key=self._client_secret,
                authority_id=self._tenant_id,
            )
        elif self._auth_method == "fabric":
            # Fabric Notebook: use mssparkutils to get token
            try:
                from notebookutils import mssparkutils
                token = mssparkutils.credentials.getToken("https://kusto.kusto.windows.net")
            except ImportError:
                raise RuntimeError(
                    "auth_method='fabric' requires mssparkutils (only available in Fabric Notebooks)"
                )
            kcsb = kcsb_cls.with_aad_application_token_authentication(
                connection_string=self._cluster_uri,
                application_token=token,
            )
        else:
            raise ValueError(f"Unsupported auth_method: '{self._auth_method}'")

        return kcsb

    # ----- internal: table management -----

    def _ensure_table(self, client, table_name: str, df) -> None:
        """Create the KQL table if it doesn't already exist.

        Uses ``.create-merge table`` which is idempotent — it creates the
        table if missing or merges new columns into an existing table.
        """
        col_defs = []
        for col_name in df.columns:
            kql_type = self._pandas_dtype_to_kql(df[col_name].dtype)
            col_defs.append(f"['{col_name}']:{kql_type}")

        command = f".create-merge table ['{table_name}'] ({', '.join(col_defs)})"
        logger.debug("Executing KQL command: %s", command)

        client.execute_mgmt(self._database, command)

    def _ingest_table(self, ingest_client, table_name: str, df, batch_size: int) -> int:
        """Ingest a DataFrame into the KQL table. Returns row count."""
        import pandas as pd
        from azure.kusto.ingest import IngestionProperties

        if df.empty:
            return 0

        ingestion_props = IngestionProperties(
            database=self._database,
            table=table_name,
        )

        rows_written = 0
        for batch_start in range(0, len(df), batch_size):
            batch_df = df.iloc[batch_start : batch_start + batch_size]
            ingest_client.ingest_from_dataframe(
                batch_df,
                ingestion_properties=ingestion_props,
            )
            rows_written += len(batch_df)

        return rows_written

    # ----- internal: type mapping -----

    @staticmethod
    def _pandas_dtype_to_kql(dtype) -> str:
        """Map a pandas dtype to a KQL column type."""
        dtype_str = str(dtype)

        if dtype_str.startswith("int") or dtype_str.startswith("Int"):
            return "long"
        if dtype_str.startswith("float") or dtype_str.startswith("Float"):
            return "real"
        if dtype_str.startswith("datetime"):
            return "datetime"
        if dtype_str == "bool" or dtype_str == "boolean":
            return "bool"
        if dtype_str in ("object", "string", "category"):
            return "string"

        # Fallback for complex or unknown types
        return "dynamic"
