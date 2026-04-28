"""LakehouseProfiler — profile Fabric Lakehouse tables without a Spark session.

Uses the `deltalake` library (part of the [fabric] extra) to read Delta tables
locally via ABFSS. Falls back to a REST API for table listing when deltalake
is unavailable.

Requires: sqllocks-spindle[fabric] — deltalake>=0.17.0, pyarrow>=14.0
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from sqllocks_spindle.inference.profiler import DataProfiler, TableProfile

logger = logging.getLogger(__name__)

try:
    import deltalake as _deltalake
    HAS_DELTALAKE = True
except ImportError:
    HAS_DELTALAKE = False

try:
    from azure.identity import DefaultAzureCredential as _DefaultAzureCredential
    HAS_AZURE_IDENTITY = True
except ImportError:
    HAS_AZURE_IDENTITY = False


class LakehouseProfiler:
    """Profile Fabric Lakehouse Delta tables and return TableProfile objects.

    Args:
        workspace_id: Fabric workspace GUID.
        lakehouse_id: Fabric lakehouse GUID.
        token_provider: A callable returning an Azure access token string.
            Defaults to DefaultAzureCredential when azure-identity is installed.
        default_sample_rows: Row limit for profiling. Pass None to scan entire table.
    """

    def __init__(
        self,
        workspace_id: str,
        lakehouse_id: str,
        token_provider: Any | None = None,
        default_sample_rows: int | None = 100_000,
    ):
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.token_provider = token_provider
        self.default_sample_rows = default_sample_rows

    def profile_table(
        self,
        table_name: str,
        sample_rows: int | None | str = "default",
    ) -> TableProfile:
        """Profile a single Delta table."""
        if sample_rows == "default":
            sample_rows = self.default_sample_rows

        df = self._read_table(table_name, sample_rows=sample_rows)
        profiler = DataProfiler(sample_rows=None)
        return profiler.profile(df, table_name=table_name)

    def profile_all(
        self,
        sample_rows: int | None | str = "default",
    ) -> dict[str, TableProfile]:
        """Profile all tables in the lakehouse."""
        table_names = self._list_tables()
        profiles: dict[str, TableProfile] = {}
        for tname in table_names:
            try:
                profiles[tname] = self.profile_table(tname, sample_rows=sample_rows)
            except Exception as exc:
                logger.warning("Skipping table '%s': %s", tname, exc)
        return profiles

    def _abfss_tables_root(self) -> str:
        return (
            f"abfss://{self.workspace_id}"
            f"@onelake.dfs.fabric.microsoft.com"
            f"/{self.lakehouse_id}/Tables"
        )

    def _get_token(self) -> str | None:
        if self.token_provider is not None:
            return self.token_provider()
        if HAS_AZURE_IDENTITY:
            cred = _DefaultAzureCredential()
            token = cred.get_token("https://storage.azure.com/.default")
            return token.token
        return None

    def _storage_options(self) -> dict[str, str]:
        token = self._get_token()
        if token:
            return {"bearer_token": token, "use_emulator": "false"}
        return {}

    def _read_table(
        self,
        table_name: str,
        sample_rows: int | None = None,
    ) -> pd.DataFrame:
        """Read a Delta table into a pandas DataFrame."""
        if not HAS_DELTALAKE:
            raise ImportError(
                "LakehouseProfiler requires 'deltalake'. "
                "Install with: pip install 'sqllocks-spindle[fabric-inference]'"
            )

        table_uri = f"{self._abfss_tables_root()}/{table_name}"
        storage_options = self._storage_options()

        try:
            dt = _deltalake.DeltaTable(table_uri, storage_options=storage_options)
            if sample_rows is not None:
                df = dt.to_pandas(limit=sample_rows)
            else:
                df = dt.to_pandas()
            return df
        except Exception as exc:
            raise RuntimeError(
                f"Failed to read table '{table_name}' from {table_uri}: {exc}"
            ) from exc

    def _list_tables(self) -> list[str]:
        """List table names in the lakehouse."""
        if not HAS_DELTALAKE:
            logger.warning(
                "deltalake not installed — cannot list lakehouse tables. "
                "Install with: pip install 'sqllocks-spindle[fabric-inference]'"
            )
            return []

        root = self._abfss_tables_root()
        storage_options = self._storage_options()

        try:
            from pyarrow import fs as _fs
            account = f"{self.workspace_id}@onelake.dfs.fabric.microsoft.com"
            token = self._get_token()
            az_fs = _fs.AzureFileSystem(account=account, credential=token)
            file_info = az_fs.get_file_info(
                _fs.FileSelector(f"{self.lakehouse_id}/Tables", recursive=False)
            )
            return [fi.base_name for fi in file_info if fi.type.name == "Directory"]
        except Exception as exc:
            logger.warning("Could not list lakehouse tables: %s", exc)
            return []
