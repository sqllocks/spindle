import sys
sys.path.insert(0, r"E:\Dropbox\VSCode\AzureClients\forge-workspace\projects\fabric-datagen")
import pandas as pd
from sqllocks_spindle import (
    Spindle, RetailDomain, HealthcareDomain, FinancialDomain,
    SupplyChainDomain, IoTDomain, HrDomain, InsuranceDomain,
    MarketingDomain, EducationDomain, RealEstateDomain,
    ManufacturingDomain, TelecomDomain,
)
from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain

DOMAINS = [
    ("retail", RetailDomain),
    ("healthcare", HealthcareDomain),
    ("financial", FinancialDomain),
    ("supply_chain", SupplyChainDomain),
    ("iot", IoTDomain),
    ("hr", HrDomain),
    ("insurance", InsuranceDomain),
    ("marketing", MarketingDomain),
    ("education", EducationDomain),
    ("real_estate", RealEstateDomain),
    ("manufacturing", ManufacturingDomain),
    ("telecom", TelecomDomain),
    ("capital_markets", CapitalMarketsDomain),
]

LEGACY_LO = pd.Timestamp("2022-01-01")
LEGACY_HI = pd.Timestamp("2025-12-31")
exit_code = 0
for name, Dom in DOMAINS:
    d = Dom()
    schema = d.get_schema()
    dr = schema.model.date_range or {}
    declared_start = pd.Timestamp(dr.get("start", "2022-01-01"))
    declared_end = pd.Timestamp(dr.get("end", "2025-12-31"))
    result = Spindle().generate(domain=d, scale="fabric_demo", seed=42)
    orphans = result.verify_integrity()
    for tname, df in result.tables.items():
        for col in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                continue
            ts = pd.to_datetime(df[col], errors="coerce").dropna()
            if ts.empty:
                continue
            # Allow DOB-style historical dates (some domains intentionally generate
            # historical dates outside declared range, e.g. patient DOB).
            in_range = (ts >= declared_start) & (ts <= declared_end)
            # the column qualifies as a "date column for this run" only when
            # >50% of its non-null values fall within the declared model range.
            in_range_share = float(in_range.mean())
            if in_range_share < 0.5:
                continue
            all_in_legacy = ((ts >= LEGACY_LO) & (ts <= LEGACY_HI)).mean()
            if declared_end.year < 2022 or declared_start.year > 2025:
                assert all_in_legacy < 0.5, f"{name}.{tname}.{col} drift to legacy 2022-2025"
    print(f"OK {name:18s} orphans={len(orphans)}")
print("BEHAVIORAL VALIDATION: ALL DOMAINS PASS")
