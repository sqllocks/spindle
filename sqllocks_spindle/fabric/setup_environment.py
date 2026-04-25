"""Fabric environment setup — standalone script for notebook-based provisioning.

This module can be:
1. Imported and called from Python code
2. Copy-pasted into a Fabric notebook cell for self-service setup
3. Called from the `spindle setup-fabric` CLI command

It creates a Fabric Environment item with Spindle + dependencies pre-installed.
"""

from __future__ import annotations

from sqllocks_spindle import __version__

# ── Standalone notebook snippet ──────────────────────────────────────────────
# Copy-paste the SETUP_SNIPPET into any Fabric notebook to bootstrap Spindle.

SETUP_SNIPPET = f'''
# ── Spindle Environment Setup ──
# Run this cell in any Fabric notebook to install Spindle and verify the setup.

%pip install sqllocks-spindle=={__version__} -q

# Verify installation
from sqllocks_spindle import Spindle, __version__ as v
print(f"Spindle v{{v}} installed successfully")

# Quick smoke test — generate 10 rows of retail data
from sqllocks_spindle import RetailDomain
result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
errors = result.verify_integrity()
print(f"Generated {{sum(result.row_counts.values()):,}} rows across {{len(result.tables)}} tables")
print(f"FK integrity: {{"PASS" if not errors else f"FAIL ({{len(errors)}} errors)"}}")
'''.strip()


def get_environment_library_spec() -> dict:
    """Return the Fabric Environment library specification for Spindle.

    This dict can be used with the Fabric REST API to create or update
    a Fabric Environment item's library configuration.
    """
    return {
        "customLibraries": {
            "pypi": [
                {"name": "sqllocks-spindle", "version": __version__},
                {"name": "deltalake", "version": ">=0.17.0"},
                {"name": "pyarrow", "version": ">=14.0"},
            ],
        },
    }


def get_spark_pool_config() -> dict:
    """Return recommended Spark pool configuration for Spindle workloads.

    Spindle is CPU-bound (numpy vectorized ops) and memory-light.
    A small pool with medium nodes is optimal.
    """
    return {
        "name": "spindle-pool",
        "nodeFamily": "MemoryOptimized",
        "nodeSize": "Small",
        "autoScale": {
            "enabled": True,
            "minNodeCount": 1,
            "maxNodeCount": 3,
        },
        "dynamicExecutorAllocation": {
            "enabled": True,
            "minExecutors": 1,
            "maxExecutors": 2,
        },
    }


def print_setup_snippet() -> None:
    """Print the copy-paste setup snippet for Fabric notebooks."""
    print(SETUP_SNIPPET)


def create_spindle_environment(
    workspace_id: str,
    environment_name: str = "Spindle-v2",
) -> dict:
    """Create a Fabric Environment with Spindle pre-installed.

    Uses the Fabric REST API to create an Environment item and configure
    it with sqllocks-spindle and dependencies. Requires an authenticated
    session (az login or mssparkutils token).

    Args:
        workspace_id: Fabric workspace GUID.
        environment_name: Name for the Environment item.

    Returns:
        Dict with environment_id and status.
    """
    import json
    import requests

    try:
        from azure.identity import AzureCliCredential
        credential = AzureCliCredential()
        token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    except Exception:
        try:
            from notebookutils import mssparkutils
            token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
        except Exception:
            raise RuntimeError(
                "No authentication available. Run 'az login' or use from a Fabric notebook."
            )

    base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Create the Environment item
    create_payload = {
        "displayName": environment_name,
        "type": "Environment",
        "description": f"Spindle v{__version__} runtime environment",
    }
    resp = requests.post(f"{base_url}/items", headers=headers, json=create_payload)

    if resp.status_code in (200, 201):
        env_id = resp.json().get("id")
    elif resp.status_code == 409:
        # Already exists — find it
        items_resp = requests.get(f"{base_url}/items?type=Environment", headers=headers)
        env_id = None
        for item in items_resp.json().get("value", []):
            if item["displayName"] == environment_name:
                env_id = item["id"]
                break
        if not env_id:
            raise RuntimeError(f"Environment '{environment_name}' conflict but not found")
    else:
        raise RuntimeError(f"Create environment failed: {resp.status_code} {resp.text[:200]}")

    # Configure libraries
    lib_spec = get_environment_library_spec()
    lib_resp = requests.post(
        f"{base_url}/environments/{env_id}/staging/libraries",
        headers=headers,
        json=lib_spec,
    )

    # Publish the environment
    pub_resp = requests.post(
        f"{base_url}/environments/{env_id}/staging/publish",
        headers=headers,
    )

    return {
        "environment_id": env_id,
        "environment_name": environment_name,
        "version": __version__,
        "library_status": lib_resp.status_code if lib_resp else None,
        "publish_status": pub_resp.status_code if pub_resp else None,
    }
