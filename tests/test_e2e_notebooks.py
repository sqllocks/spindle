"""E2E tests: execute non-Fabric notebooks via nbclient.

Skips Fabric-connected notebooks (F01-F10, T08-T10) since they require
a Fabric runtime. All other notebooks should execute cleanly.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Try importing nbclient — skip all tests if not installed
nbformat = pytest.importorskip("nbformat")
nbclient = pytest.importorskip("nbclient")

NOTEBOOKS_DIR = Path(__file__).parent.parent / "examples" / "notebooks"

# Notebooks that require Fabric runtime — skip locally
FABRIC_NOTEBOOKS = {
    "T08_fabric_lakehouse.ipynb",
    "T09_fabric_quickstart.ipynb",
    "T10_fabric_sql_database.ipynb",
    "F01_medallion_architecture.ipynb",
    "F02_warehouse_dimensional.ipynb",
    "F03_sql_database_oltp.ipynb",
    "F04_realtime_streaming.ipynb",
    "F05_chaos_pipeline.ipynb",
    "F06_semantic_model.ipynb",
    "F07_healthcare_rcm.ipynb",
    "F08_filedrop_ingestion.ipynb",
    "F09_cross_domain_enterprise.ipynb",
    "F10_month_end_close.ipynb",
    "F11_integration_sweep.ipynb",
    "F12_billion_row_warehouse.ipynb",
    "F13_chaos_simulation_fabric.ipynb",
}

# Collect all .ipynb files recursively
ALL_NOTEBOOKS = sorted(NOTEBOOKS_DIR.rglob("*.ipynb")) if NOTEBOOKS_DIR.exists() else []
LOCAL_NOTEBOOKS = [
    nb for nb in ALL_NOTEBOOKS
    if nb.name not in FABRIC_NOTEBOOKS and ".ipynb_checkpoints" not in str(nb)
]


@pytest.mark.slow
@pytest.mark.parametrize(
    "notebook_path",
    LOCAL_NOTEBOOKS,
    ids=[nb.stem for nb in LOCAL_NOTEBOOKS],
)
def test_notebook_executes(notebook_path, tmp_path):
    """Execute a notebook and verify no cell raises an exception."""
    nb = nbformat.read(str(notebook_path), as_version=4)
    client = nbclient.NotebookClient(
        nb,
        timeout=300,
        kernel_name="python3",
        resources={"metadata": {"path": str(tmp_path)}},
    )
    try:
        client.execute()
    except nbclient.exceptions.CellExecutionError as e:
        pytest.fail(f"Notebook {notebook_path.name} failed:\n{str(e)[:500]}")
