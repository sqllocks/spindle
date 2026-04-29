"""Spindle by SQLLocks — Multi-domain synthetic data generator for Microsoft Fabric."""

import importlib as _importlib

__version__ = "2.11.0"

from sqllocks_spindle.engine.generator import Spindle
from sqllocks_spindle.engine.chunked_generator import ChunkedSpindle, ChunkedGenerationResult
from sqllocks_spindle.fabric.multi_writer import MultiWriter, MultiWriteResult
from sqllocks_spindle.fabric.lakehouse_files_writer import LakehouseWriteResult

# Lazy domain imports — domains are loaded on first access
_LAZY_IMPORTS = {
    "RetailDomain": "sqllocks_spindle.domains.retail",
    "HealthcareDomain": "sqllocks_spindle.domains.healthcare",
    "FinancialDomain": "sqllocks_spindle.domains.financial",
    "SupplyChainDomain": "sqllocks_spindle.domains.supply_chain",
    "IoTDomain": "sqllocks_spindle.domains.iot",
    "HrDomain": "sqllocks_spindle.domains.hr",
    "HRDomain": "sqllocks_spindle.domains.hr",  # alias → HrDomain
    "InsuranceDomain": "sqllocks_spindle.domains.insurance",
    "MarketingDomain": "sqllocks_spindle.domains.marketing",
    "EducationDomain": "sqllocks_spindle.domains.education",
    "RealEstateDomain": "sqllocks_spindle.domains.real_estate",
    "ManufacturingDomain": "sqllocks_spindle.domains.manufacturing",
    "TelecomDomain": "sqllocks_spindle.domains.telecom",
}


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        module = _importlib.import_module(_LAZY_IMPORTS[name])
        # HRDomain is an alias for HrDomain
        attr = "HrDomain" if name == "HRDomain" else name
        cls = getattr(module, attr)
        globals()[name] = cls
        return cls
    raise AttributeError(f"module 'sqllocks_spindle' has no attribute {name}")


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
    from sqllocks_spindle.inference import DataMasker, DataProfiler, ExportedProfile, LakehouseProfiler, MaskConfig, ProfileIO, SchemaBuilder
except ImportError:
    pass

# Presets
from sqllocks_spindle.presets import get_preset, list_presets

__all__ = [
    # Core
    "Spindle",
    "ChunkedSpindle",
    "ChunkedGenerationResult",
    # Multi-store
    "MultiWriter",
    "MultiWriteResult",
    "LakehouseWriteResult",
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
    "LakehouseProfiler",
    "MaskConfig",
    "ProfileIO",
    "SchemaBuilder",
    # Presets
    "get_preset",
    "list_presets",
]
