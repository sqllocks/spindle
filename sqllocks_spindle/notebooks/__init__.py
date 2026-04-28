"""Fabric notebook templates bundled with Spindle."""
from __future__ import annotations

from pathlib import Path

_NOTEBOOKS_DIR = Path(__file__).parent


def _load_notebook(name: str) -> dict:
    import json
    path = _NOTEBOOKS_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Notebook template not found: {path}")
    with open(path) as f:
        return json.load(f)


def _get_spark_worker_ipynb() -> dict:
    try:
        return _load_notebook("spindle_spark_worker.ipynb")
    except FileNotFoundError:
        return {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {"kernelspec": {"display_name": "PySpark", "language": "python", "name": "synapse_pyspark"}},
            "cells": [],
        }


SPARK_WORKER_IPYNB: dict = _get_spark_worker_ipynb()
