"""Spindle inference engine — profile existing data and infer schemas.

Provides DataProfiler for analysing DataFrames and SchemaBuilder for
converting profiles into ready-to-use SpindleSchema objects.  Also
includes FidelityComparator for comparing real vs synthetic data.
"""

from sqllocks_spindle.inference.masker import (
    DataMasker,
    MaskConfig,
    MaskResult,
)
from sqllocks_spindle.inference.comparator import (
    ColumnFidelity,
    FidelityComparator,
    FidelityReport,
    TableFidelity,
)
from sqllocks_spindle.inference.profiler import (
    ColumnProfile,
    DataProfiler,
    DatasetProfile,
    TableProfile,
)
from sqllocks_spindle.inference.profile_io import ExportedProfile, ProfileIO
from sqllocks_spindle.inference.schema_builder import SchemaBuilder

__all__ = [
    "ColumnFidelity",
    "ColumnProfile",
    "DataMasker",
    "DataProfiler",
    "DatasetProfile",
    "ExportedProfile",
    "FidelityComparator",
    "FidelityReport",
    "MaskConfig",
    "MaskResult",
    "ProfileIO",
    "SchemaBuilder",
    "TableFidelity",
    "TableProfile",
]
