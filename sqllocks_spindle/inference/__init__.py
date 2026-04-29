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
from sqllocks_spindle.inference.lakehouse_profiler import LakehouseProfiler
from sqllocks_spindle.inference.tier3_research import (
    BootstrapMode,
    BootstrapResult,
    BayesianEdge,
    ChowLiuNetwork,
    ChowLiuResult,
    CTGANWrapper,
    DifferentialPrivacy,
    DPResult,
    DriftMonitor,
    DriftReport,
    ColumnDriftResult,
)
from sqllocks_spindle.inference.tier2_profiler import (
    AnomalyRateResult,
    CardinalityConstraintChecker,
    CardinalityConstraintResult,
    FormatPreservationAnalyzer,
    FormatPreservationResult,
    StringSimilarityAnalyzer,
    StringSimilarityResult,
    Tier2Report,
    check_anomaly_rates,
    run_tier2,
)
from sqllocks_spindle.inference.advanced_profiler import (
    AdvancedProfiler,
    AdvancedTableProfile,
    AdversarialResult,
    ConditionalProfile,
    GMMFit,
    PeriodicityResult,
    TemporalProfile,
)

__all__ = [
    "ColumnFidelity",
    "ColumnProfile",
    "DataMasker",
    "DataProfiler",
    "DatasetProfile",
    "ExportedProfile",
    "FidelityComparator",
    "FidelityReport",
    "LakehouseProfiler",
    "MaskConfig",
    "MaskResult",
    "ProfileIO",
    "SchemaBuilder",
    "TableFidelity",
    "TableProfile",
    # Advanced profiler
    "AdvancedProfiler",
    "AdvancedTableProfile",
    "AdversarialResult",
    "ConditionalProfile",
    "GMMFit",
    "PeriodicityResult",
    "TemporalProfile",
    # Tier 2
    "AnomalyRateResult",
    "CardinalityConstraintChecker",
    "CardinalityConstraintResult",
    "FormatPreservationAnalyzer",
    "FormatPreservationResult",
    "StringSimilarityAnalyzer",
    "StringSimilarityResult",
    "Tier2Report",
    "check_anomaly_rates",
    "run_tier2",
    # Tier 3 research
    "BootstrapMode",
    "BootstrapResult",
    "BayesianEdge",
    "ChowLiuNetwork",
    "ChowLiuResult",
    "CTGANWrapper",
    "DifferentialPrivacy",
    "DPResult",
    "DriftMonitor",
    "DriftReport",
    "ColumnDriftResult",
]
