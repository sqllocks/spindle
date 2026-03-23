"""Spindle by SQLLocks — Multi-domain synthetic data generator for Microsoft Fabric."""

__version__ = "2.2.11"

from sqllocks_spindle.engine.generator import Spindle
from sqllocks_spindle.domains.retail import RetailDomain
from sqllocks_spindle.domains.healthcare import HealthcareDomain
from sqllocks_spindle.domains.financial import FinancialDomain
from sqllocks_spindle.domains.supply_chain import SupplyChainDomain
from sqllocks_spindle.domains.iot import IoTDomain
from sqllocks_spindle.domains.hr import HrDomain

# Convenience alias — both HrDomain and HRDomain work
HRDomain = HrDomain
from sqllocks_spindle.domains.insurance import InsuranceDomain
from sqllocks_spindle.domains.marketing import MarketingDomain
from sqllocks_spindle.domains.education import EducationDomain
from sqllocks_spindle.domains.real_estate import RealEstateDomain
from sqllocks_spindle.domains.manufacturing import ManufacturingDomain
from sqllocks_spindle.domains.telecom import TelecomDomain

from sqllocks_spindle.transform import (
    CdmEntityMap,
    CdmMapper,
    DimSpec,
    FactSpec,
    StarSchemaMap,
    StarSchemaResult,
    StarSchemaTransform,
)

from sqllocks_spindle.streaming import (
    Anomaly,
    AnomalyRegistry,
    BurstWindow,
    CollectiveAnomaly,
    ConsoleSink,
    ContextualAnomaly,
    FileSink,
    PointAnomaly,
    SpindleStreamer,
    StreamConfig,
    StreamResult,
    StreamWriter,
    TimePattern,
)

# Incremental (continue) engine
from sqllocks_spindle.incremental import ContinueEngine, ContinueConfig, DeltaResult

# Time-travel snapshots
from sqllocks_spindle.incremental import TimeTravelEngine, TimeTravelConfig, TimeTravelResult

# Inference (optional — requires [inference] extra)
try:
    from sqllocks_spindle.inference import DataMasker, DataProfiler, ExportedProfile, MaskConfig, ProfileIO, SchemaBuilder
except ImportError:
    pass

# Presets
from sqllocks_spindle.presets import get_preset, list_presets

__all__ = [
    # Core
    "Spindle",
    # Domains
    "RetailDomain",
    "HealthcareDomain",
    "FinancialDomain",
    "SupplyChainDomain",
    "IoTDomain",
    "HrDomain",
    "HRDomain",
    "InsuranceDomain",
    "MarketingDomain",
    "EducationDomain",
    "RealEstateDomain",
    "ManufacturingDomain",
    "TelecomDomain",
    # Streaming
    "SpindleStreamer",
    "StreamResult",
    "StreamConfig",
    "BurstWindow",
    "TimePattern",
    "StreamWriter",
    "ConsoleSink",
    "FileSink",
    # Anomaly
    "AnomalyRegistry",
    "PointAnomaly",
    "ContextualAnomaly",
    "CollectiveAnomaly",
    "Anomaly",
    # Transform
    "StarSchemaTransform",
    "StarSchemaResult",
    "StarSchemaMap",
    "DimSpec",
    "FactSpec",
    "CdmMapper",
    "CdmEntityMap",
    # Incremental
    "ContinueEngine",
    "ContinueConfig",
    "DeltaResult",
    # Time-travel
    "TimeTravelEngine",
    "TimeTravelConfig",
    "TimeTravelResult",
    # Inference
    "DataMasker",
    "DataProfiler",
    "ExportedProfile",
    "MaskConfig",
    "ProfileIO",
    "SchemaBuilder",
    # Presets
    "get_preset",
    "list_presets",
]
