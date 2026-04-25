"""SemanticModelOutput — wrap SemanticModelExporter for demo use."""
from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class SemanticModelOutput:
    def __init__(self, output_dir: Optional[Path] = None):
        self._output_dir = output_dir or Path.cwd()

    def generate(self, schema, output_name: str = "demo_model") -> Path:
        """Generate a .bim file for the given schema."""
        try:
            from sqllocks_spindle.fabric.semantic_model_writer import SemanticModelExporter
        except ImportError as e:
            raise ImportError(
                "SemanticModelExporter not found. "
                "Verify sqllocks_spindle/fabric/semantic_model_writer.py exists."
            ) from e

        output_path = self._output_dir / f"{output_name}.bim"
        exporter = SemanticModelExporter()
        result = exporter.export_bim(
            schema=schema,
            source_type="lakehouse",
            source_name="SpindleDemo",
            output_path=str(output_path),
            include_measures=True,
        )
        logger.info("Semantic model written to %s", output_path)
        return Path(result) if isinstance(result, str) else output_path

    def deploy_via_xmla(self, bim_path: Path, xmla_endpoint: str) -> bool:
        logger.warning(
            "XMLA deploy not implemented. Deploy %s manually via Tabular Editor or XMLA endpoint: %s",
            bim_path, xmla_endpoint
        )
        return False
