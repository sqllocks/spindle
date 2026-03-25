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
