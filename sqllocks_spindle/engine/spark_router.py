"""FabricSparkRouter — submits Spindle generation jobs to Fabric Spark notebooks."""
from __future__ import annotations

import json
import logging
import uuid

import requests

from sqllocks_spindle.engine.async_job_store import JobRecord
from sqllocks_spindle.engine.scale_router import (
    _SpindleJSONEncoder,
    _classify_tables,
    _generate_static_tables,
)

logger = logging.getLogger(__name__)

_FABRIC_API = "https://api.fabric.microsoft.com/v1"
_ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"


class NotebookNotFoundError(RuntimeError):
    """Raised when the Spindle Spark worker notebook cannot be found or created."""


class FabricAPIError(RuntimeError):
    """Raised when the Fabric REST API returns an unexpected error."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"Fabric API error {status_code}: {message}")
        self.status_code = status_code


class FabricSparkRouter:
    """Submits Spindle generation jobs to a Fabric Spark notebook.

    Args:
        workspace_id: Fabric workspace GUID.
        lakehouse_id: Lakehouse GUID for temp file staging and output.
        token: Entra bearer token with Workspace.Write + Lakehouse.ReadWrite permissions.
        notebook_name: Display name of the Spark worker notebook (default: spindle_spark_worker).
        sinks: List of sink names to pass into the notebook (default: ["lakehouse"]).
        sink_config: Per-sink configuration dict passed as JSON to the notebook.
        chunk_size: Rows per Spark partition. Default 500_000.
    """

    def __init__(
        self,
        workspace_id: str,
        lakehouse_id: str,
        token: str,
        notebook_name: str = "spindle_spark_worker",
        sinks: list[str] | None = None,
        sink_config: dict | None = None,
        chunk_size: int = 500_000,
    ) -> None:
        self._workspace_id = workspace_id
        self._lakehouse_id = lakehouse_id
        self._token = token
        self._notebook_name = notebook_name
        self._sinks = sinks or ["lakehouse"]
        self._sink_config = sink_config or {}
        self._chunk_size = chunk_size
        self._storage_token: str | None = None

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    def _json_headers(self) -> dict[str, str]:
        return {**self._auth_headers(), "Content-Type": "application/json"}

    def _get_storage_token(self) -> str:
        """Acquire (and cache) a token for OneLake / ADLS Gen2.

        OneLake DFS requires the ``https://storage.azure.com/.default`` audience —
        distinct from the Fabric API token used for Items/Jobs endpoints.
        """
        if self._storage_token is None:
            from azure.identity import AzureCliCredential
            cred = AzureCliCredential()
            self._storage_token = cred.get_token("https://storage.azure.com/.default").token
        return self._storage_token

    def _storage_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._get_storage_token()}"}

    def _get_or_create_notebook(self) -> str:
        """Return the item ID for the Spark worker notebook, creating if absent."""
        url = f"{_FABRIC_API}/workspaces/{self._workspace_id}/notebooks"
        resp = requests.get(url, headers=self._json_headers(), timeout=30)
        resp.raise_for_status()
        for item in resp.json().get("value", []):
            if item.get("displayName") == self._notebook_name:
                logger.info("Found existing notebook '%s' (id=%s)", self._notebook_name, item["id"])
                return item["id"]
        logger.info("Notebook '%s' not found — creating.", self._notebook_name)
        return self._create_notebook()

    def _create_notebook(self) -> str:
        """Create the spindle_spark_worker notebook via the Fabric Items API.

        Item creation with a definition payload is a long-running operation. The
        API returns 202 with a Location header pointing at the operation status URL.
        We poll until the operation completes, then look up the new notebook by name.
        """
        import base64
        import time

        from sqllocks_spindle.notebooks import SPARK_WORKER_IPYNB

        payload_b64 = base64.b64encode(json.dumps(SPARK_WORKER_IPYNB).encode()).decode()
        body = {
            "displayName": self._notebook_name,
            "type": "Notebook",
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.ipynb",
                        "payload": payload_b64,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }
        url = f"{_FABRIC_API}/workspaces/{self._workspace_id}/items"
        resp = requests.post(url, headers=self._json_headers(), json=body, timeout=60)

        if resp.status_code == 201:
            item_id = (resp.json() or {}).get("id")
            if not item_id:
                raise NotebookNotFoundError(
                    f"Items API returned 201 with no 'id' in body: {resp.text[:200]}"
                )
            logger.info("Created notebook '%s' (id=%s)", self._notebook_name, item_id)
            return item_id

        if resp.status_code == 202:
            operation_url = resp.headers.get("Location", "")
            if not operation_url:
                raise NotebookNotFoundError(
                    "Items API returned 202 with no Location header for the operation"
                )
            logger.info("Notebook creation accepted (async); polling %s", operation_url)
            for _ in range(60):  # up to ~120 seconds
                time.sleep(2)
                op_resp = requests.get(
                    operation_url, headers=self._json_headers(), timeout=30,
                )
                op_resp.raise_for_status()
                op_body = op_resp.json() or {}
                op_status = op_body.get("status", "")
                if op_status == "Succeeded":
                    return self._find_notebook_by_name()
                if op_status == "Failed":
                    raise NotebookNotFoundError(
                        f"Notebook creation failed: {op_body.get('error', 'unknown')}"
                    )
            raise NotebookNotFoundError("Notebook creation timed out after ~120s")

        resp.raise_for_status()
        raise NotebookNotFoundError(
            f"Unexpected status {resp.status_code} from Items API: {resp.text[:200]}"
        )

    def _find_notebook_by_name(self) -> str:
        """Re-list workspace notebooks and find by display name."""
        url = f"{_FABRIC_API}/workspaces/{self._workspace_id}/notebooks"
        resp = requests.get(url, headers=self._json_headers(), timeout=30)
        resp.raise_for_status()
        for item in resp.json().get("value", []):
            if item.get("displayName") == self._notebook_name:
                logger.info(
                    "Found newly-created notebook '%s' (id=%s)",
                    self._notebook_name, item["id"],
                )
                return item["id"]
        raise NotebookNotFoundError(
            f"Notebook '{self._notebook_name}' not found after creation"
        )

    def _upload_schema(self, schema_dict: dict, run_id: str) -> str:
        """Upload augmented schema JSON to OneLake Files via the ADLS Gen2 DFS API.

        Three-step protocol: create → append → flush.
        Returns the OneLake-relative file path.
        """
        data: bytes = json.dumps(schema_dict, cls=_SpindleJSONEncoder).encode()
        rel_path = f"spindle_temp/{run_id}_schema.json"
        # OneLake DFS path: /<workspace>/<lakehouse_id>/Files/<path> — capital F is required.
        base_url = (
            f"{_ONELAKE_DFS}/{self._workspace_id}/{self._lakehouse_id}/Files/{rel_path}"
        )

        requests.put(
            f"{base_url}?resource=file",
            headers=self._storage_headers(),
            timeout=30,
        ).raise_for_status()

        requests.patch(
            f"{base_url}?action=append&position=0",
            headers={**self._storage_headers(), "Content-Length": str(len(data))},
            data=data,
            timeout=120,
        ).raise_for_status()

        requests.patch(
            f"{base_url}?action=flush&position={len(data)}",
            headers=self._storage_headers(),
            timeout=30,
        ).raise_for_status()

        logger.info("Schema uploaded to OneLake: %s", rel_path)
        return rel_path

    def _submit_notebook_run(
        self,
        notebook_item_id: str,
        schema_path: str,
        total_rows: int,
        seed: int,
    ) -> str:
        """Submit a Fabric notebook run and return the Fabric run ID."""
        url = (
            f"{_FABRIC_API}/workspaces/{self._workspace_id}"
            f"/items/{notebook_item_id}/jobs/instances?jobType=RunNotebook"
        )
        # Fabric notebook parameter format: each value needs a "type" field.
        # Valid types: string | int | float | bool. (cellLanguage is NOT a valid key.)
        body = {
            "executionData": {
                "parameters": {
                    "schema_path": {"value": schema_path, "type": "string"},
                    "chunk_size": {"value": self._chunk_size, "type": "int"},
                    "seed": {"value": seed, "type": "int"},
                    "total_rows": {"value": total_rows, "type": "int"},
                    "sinks_json": {
                        "value": json.dumps(
                            {"sinks": self._sinks, "sink_config": self._sink_config}
                        ),
                        "type": "string",
                    },
                    "workspace_id": {"value": self._workspace_id, "type": "string"},
                    "lakehouse_id": {"value": self._lakehouse_id, "type": "string"},
                }
            }
        }
        resp = requests.post(url, headers=self._json_headers(), json=body, timeout=30)
        if resp.status_code == 202:
            location = resp.headers.get("Location", "")
            return location.rstrip("/").split("/")[-1]
        if resp.status_code == 200:
            return resp.json().get("id", uuid.uuid4().hex)
        raise FabricAPIError(resp.status_code, resp.text)

    def submit(self, schema_dict: dict, total_rows: int, seed: int) -> JobRecord:
        """Generate static tables, upload schema, and submit a Fabric notebook run.

        Returns a JobRecord immediately.
        """
        from sqllocks_spindle.engine.generator import calculate_row_counts
        from sqllocks_spindle.schema.parser import SchemaParser

        schema = SchemaParser().parse_dict(schema_dict)
        schema_counts = calculate_row_counts(schema)
        static_tables, dynamic_tables = _classify_tables(schema_counts, self._chunk_size)

        if static_tables:
            logger.info("Generating %d static tables in main process.", len(static_tables))
            static_chunk = _generate_static_tables(
                schema_path="",
                static_tables=static_tables,
                schema_counts=schema_counts,
                seed=seed,
                schema_dict=schema_dict,
            )
            schema_dict["_static_tables"] = list(static_tables)
            schema_dict["_static_pk_data"] = static_chunk
            schema_dict["_dynamic_tables"] = list(dynamic_tables)
            schema_dict["_schema_counts"] = schema_counts
        else:
            schema_dict["_static_tables"] = []
            schema_dict["_static_pk_data"] = {}
            schema_dict["_dynamic_tables"] = list(dynamic_tables)
            schema_dict["_schema_counts"] = schema_counts

        schema_dict["_base_seed"] = seed

        run_id = uuid.uuid4().hex
        schema_path = self._upload_schema(schema_dict, run_id)
        notebook_item_id = self._get_or_create_notebook()
        fabric_run_id = self._submit_notebook_run(
            notebook_item_id=notebook_item_id,
            schema_path=schema_path,
            total_rows=total_rows,
            seed=seed,
        )

        job_id = f"spindle-{run_id[:8]}"
        return JobRecord(
            job_id=job_id,
            fabric_run_id=fabric_run_id,
            workspace_id=self._workspace_id,
            notebook_item_id=notebook_item_id,
            schema_temp_path=schema_path,
            lakehouse_id=self._lakehouse_id,
            token=self._token,
        )
