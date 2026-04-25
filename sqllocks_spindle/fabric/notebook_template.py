"""Generate Fabric-compatible .ipynb notebooks for Spindle data generation.

Creates ready-to-run Jupyter notebooks that can be uploaded to Microsoft Fabric
or run locally. Notebooks install Spindle, generate data for a domain, and
optionally write to a Lakehouse.
"""

from __future__ import annotations

import json
from pathlib import Path

from sqllocks_spindle import __version__


def generate_notebook(
    domain: str,
    scale: str = "small",
    seed: int = 42,
    output_target: str = "lakehouse",
) -> dict:
    """Generate a ready-to-run Fabric notebook as a dict (ipynb format).

    Args:
        domain: Domain name (e.g. "retail", "healthcare").
        scale: Scale preset (small, medium, large, xlarge).
        seed: Random seed for reproducibility.
        output_target: Where to write output ("lakehouse", "display", "csv").

    Returns:
        A dict in Jupyter notebook (.ipynb) format.
    """
    domain_class = domain.replace("_", " ").title().replace(" ", "") + "Domain"

    cells = []

    # Cell 1: Markdown header
    cells.append(_markdown_cell(
        f"# Spindle Data Generation — {domain.title()}\n"
        f"\n"
        f"This notebook generates synthetic data using **Spindle v{__version__}**.\n"
        f"\n"
        f"- **Domain**: {domain}\n"
        f"- **Scale**: {scale}\n"
        f"- **Seed**: {seed}\n"
        f"- **Output**: {output_target}"
    ))

    # Cell 2: Install
    cells.append(_code_cell(
        f"%pip install sqllocks-spindle=={__version__} -q"
    ))

    # Cell 3: Generate
    cells.append(_code_cell(
        f"from sqllocks_spindle import Spindle, {domain_class}\n"
        f"\n"
        f"result = Spindle().generate(\n"
        f"    domain={domain_class}(),\n"
        f"    scale=\"{scale}\",\n"
        f"    seed={seed},\n"
        f")\n"
        f"\n"
        f"print(result.summary())\n"
        f"print()\n"
        f"\n"
        f"errors = result.verify_integrity()\n"
        f"if errors:\n"
        f"    print(\"FK Integrity Issues:\")\n"
        f"    for e in errors:\n"
        f"        print(f\"  WARNING: {{e}}\")\n"
        f"else:\n"
        f"    print(\"FK Integrity: PASS\")"
    ))

    # Cell 4: Output
    if output_target == "lakehouse":
        cells.append(_markdown_cell(
            "## Write to Lakehouse\n"
            "\n"
            "Writes all tables as Delta files to the default Lakehouse.\n"
            "Ensure a Lakehouse is attached to this notebook."
        ))
        cells.append(_code_cell(
            "import os\n"
            "\n"
            "# Auto-detect Fabric Lakehouse path\n"
            "lakehouse_path = os.environ.get(\n"
            "    'LAKEHOUSE_FILES_PATH',\n"
            "    '/lakehouse/default/Files'\n"
            ")\n"
            f"output_dir = f\"{{lakehouse_path}}/spindle/{domain}\"\n"
            "\n"
            "paths = result.to_parquet(output_dir)\n"
            "for p in paths:\n"
            "    print(f\"  Written: {p}\")\n"
            "\n"
            f"print(f\"\\n{{len(paths)}} tables written to {{output_dir}}\")"
        ))
    elif output_target == "csv":
        cells.append(_code_cell(
            f"paths = result.to_csv('./spindle_{domain}')\n"
            "for p in paths:\n"
            "    print(f\"  Written: {p}\")"
        ))
    else:
        # display mode — show sample data
        cells.append(_markdown_cell("## Sample Data"))
        cells.append(_code_cell(
            "for table_name in result.table_names[:5]:\n"
            "    print(f\"\\n--- {table_name} ---\")\n"
            "    display(result[table_name].head(5))"
        ))

    return _notebook_structure(cells)


def save_notebook(notebook: dict, output_path: str | Path) -> Path:
    """Save a notebook dict to a .ipynb file.

    Args:
        notebook: Notebook dict from generate_notebook().
        output_path: Path to write the .ipynb file.

    Returns:
        The Path where the file was written.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=1)
    return path


def _notebook_structure(cells: list[dict]) -> dict:
    """Wrap cells in a standard Jupyter notebook structure."""
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3.10.0",
            },
        },
        "cells": cells,
    }


def _code_cell(source: str) -> dict:
    """Create a code cell."""
    return {
        "cell_type": "code",
        "metadata": {},
        "source": source.splitlines(keepends=True),
        "outputs": [],
        "execution_count": None,
    }


def _markdown_cell(source: str) -> dict:
    """Create a markdown cell."""
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": source.splitlines(keepends=True),
    }
