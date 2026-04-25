"""NotebookGenerator — produce a Fabric PySpark .ipynb from a scenario definition."""
from __future__ import annotations
import json
import uuid
from pathlib import Path
from typing import Optional

from sqllocks_spindle.demo.catalog import ScenarioMeta


_CELL_TEMPLATE = {
    "cell_type": "code",
    "id": None,
    "source": [],
    "outputs": [],
    "execution_count": None,
    "metadata": {"microsoft": {"language": "python", "languageGroup": "synapse_pyspark"}},
}


def _code_cell(lines: list) -> dict:
    cell = dict(_CELL_TEMPLATE)
    cell["id"] = str(uuid.uuid4())
    cell["source"] = [line + "\n" for line in lines]
    return cell


def _markdown_cell(lines: list) -> dict:
    return {
        "cell_type": "markdown",
        "id": str(uuid.uuid4()),
        "source": [line + "\n" for line in lines],
        "metadata": {},
    }


class NotebookGenerator:
    def generate(self, scenario: ScenarioMeta, mode: str = "inference",
                 output_path: Optional[Path] = None) -> Path:
        cells = self._build_cells(scenario, mode)
        notebook = {
            "cells": cells,
            "metadata": {
                "language_info": {"name": "python"},
                "microsoft": {"language": "python",
                              "ms_spell_check": {"ms_spell_check_language": "en"}},
                "kernel_info": {"name": "synapse_pyspark"},
                "kernelspec": {"name": "synapse_pyspark",
                               "display_name": "Synapse PySpark",
                               "language": "Python"},
            },
            "nbformat": 4,
            "nbformat_minor": 5,
        }
        if output_path is None:
            output_path = Path(f"spindle_{scenario.name}_{mode}.ipynb")
        output_path.write_text(json.dumps(notebook, indent=2))
        return output_path

    def _build_cells(self, scenario: ScenarioMeta, mode: str) -> list:
        cells = []
        cells.append(_markdown_cell([
            f"# Spindle Demo — {scenario.name.replace('_', ' ').title()} ({mode})",
            "",
            f"> {scenario.description}",
            "",
            f"**Domains:** {', '.join(scenario.domains)}  ",
            f"**Mode:** {mode}  ",
        ]))
        cells.append(_code_cell([
            "%pip install sqllocks-spindle[fabric-sql] -q",
        ]))
        cells.append(_code_cell([
            "from sqllocks_spindle.demo import SpindleDemo",
            "from sqllocks_spindle.demo.params import DemoParams",
            "SEED = 42",
        ]))
        if mode == "inference":
            cells.extend(self._inference_cells(scenario))
        elif mode == "streaming":
            cells.extend(self._streaming_cells(scenario))
        elif mode == "seeding":
            cells.extend(self._seeding_cells(scenario))
        cells.append(_markdown_cell(["## Cleanup", "",
            "Run the cell below to remove all demo artifacts."]))
        cells.append(_code_cell([
            "# demo.cleanup()  # Uncomment to clean up",
        ]))
        return cells

    def _inference_cells(self, scenario: ScenarioMeta) -> list:
        domain = scenario.domains[0] if scenario.domains else "retail"
        return [
            _code_cell([
                f"params = DemoParams(",
                f"    scenario='{scenario.name}',",
                f"    mode='inference',",
                f"    domain='{domain}',",
                f"    rows=100_000,",
                f"    input_file=None,  # Set to 'path/to/data.csv' or 'live-db'",
                f"    output_formats=['terminal', 'charts'],",
                f"    seed=SEED,",
                f")",
            ]),
            _code_cell([
                "from sqllocks_spindle.demo.orchestrator import DemoOrchestrator",
                "orch = DemoOrchestrator()",
                "result = orch.run(params)",
                "print(f'Session ID: {result.session_id}')",
                "if result.fidelity_score: print(f'Fidelity: {result.fidelity_score:.1%}')",
            ]),
            _code_cell([
                "if result.manifest:",
                "    print(result.manifest.export('md'))",
            ]),
        ]

    def _streaming_cells(self, scenario: ScenarioMeta) -> list:
        domain = scenario.domains[0] if scenario.domains else "retail"
        return [
            _code_cell([
                f"params = DemoParams(",
                f"    scenario='{scenario.name}',",
                f"    mode='streaming',",
                f"    domain='{domain}',",
                f"    rows=10_000,",
                f"    seed=SEED,",
                f")",
            ]),
            _code_cell([
                "from sqllocks_spindle.demo.orchestrator import DemoOrchestrator",
                "result = DemoOrchestrator().run(params)",
            ]),
        ]

    def _seeding_cells(self, scenario: ScenarioMeta) -> list:
        domain = scenario.domains[0] if scenario.domains else "retail"
        return [
            _code_cell([
                f"params = DemoParams(",
                f"    scenario='{scenario.name}',",
                f"    mode='seeding',",
                f"    domain='{domain}',",
                f"    rows={scenario.default_rows:_},",
                f"    connection=None,  # Set to your connection profile name",
                f"    seed=SEED,",
                f")",
            ]),
            _code_cell([
                "from sqllocks_spindle.demo.orchestrator import DemoOrchestrator",
                "result = DemoOrchestrator().run(params)",
                "print(f'Session ID: {result.session_id}')",
            ]),
        ]
