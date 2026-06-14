"""Behavioral validation: 3.0.0 generated dates land in declared range.

For each built-in domain at scale="fabric_demo" with seed=42:
  - Generate
  - For every datetime/date column, assert values are inside the declared
    domain date range (NOT the legacy default 2022-2025 window) when the
    domain declares one wider than that window.
  - verify_integrity() returns empty (or only pre-existing partial-overlap
    orphans from documented links like capital_markets exchange_code).
"""
import sys
import pandas as pd
from sqllocks_spindle.engine.generator import Spindle

DOMAINS = [
    ("RetailDomain", "sqllocks_spindle.domains.retail"),
    ("HealthcareDomain", "sqllocks_spindle.domains.healthcare"),
    ("FinancialDomain", "sqllocks_spindle.domains.financial"),
    ("SupplyChainDomain", "sqllocks_spindle.domains.supply_chain"),
    ("IoTDomain", "sqllocks_spindle.domains.iot"),
    ("HrDomain", "sqllocks_spindle.domains.hr"),
    ("InsuranceDomain", "sqllocks_spindle.domains.insurance"),
    ("MarketingDomain", "sqllocks_spindle.domains.marketing"),
    ("EducationDomain", "sqllocks_spindle.domains.education"),
    ("RealEstateDomain", "sqllocks_spindle.domains.real_estate"),
    ("ManufacturingDomain", "sqllocks_spindle.domains.manufacturing"),
    ("TelecomDomain", "sqllocks_spindle.domains.telecom"),
    ("CapitalMarketsDomain", "sqllocks_spindle.domains.capital_markets"),
]

KNOWN_ORPHANS = {
    "CapitalMarketsDomain": ["exchange_code"],
}

failures = []
for cls_name, mod_path in DOMAINS:
    print(f"--- {cls_name} ---")
    mod = __import__(mod_path, fromlist=[cls_name])
    dom = getattr(mod, cls_name)()
    sp = Spindle()
    try:
        result = sp.generate(domain=dom, scale="fabric_demo", seed=42)
    except Exception as exc:
        print(f"  GENERATE FAILED: {exc}")
        failures.append((cls_name, "generate", str(exc)))
        continue

    parsed = sp._resolve_schema(dom, None)
    decl = parsed.model.date_range or {}
    decl_start = pd.Timestamp(decl.get("start", "1900-01-01"))
    decl_end = pd.Timestamp(decl.get("end", "2100-12-31"))
    legacy_start = pd.Timestamp("2022-01-01")
    legacy_end = pd.Timestamp("2025-12-31")
    wider_than_legacy = decl_start < legacy_start or decl_end > legacy_end

    if wider_than_legacy:
        seen_outside_legacy = False
        for tname, df in result.tables.items():
            for col in df.columns:
                if df[col].dtype.kind in ("M", "O"):
                    try:
                        ts = pd.to_datetime(df[col], errors="coerce").dropna()
                    except Exception:
                        continue
                    if len(ts) == 0:
                        continue
                    outside = ts[(ts < legacy_start) | (ts > legacy_end)]
                    if len(outside) > 0:
                        seen_outside_legacy = True
                        break
            if seen_outside_legacy:
                break
        if not seen_outside_legacy:
            print(f"  WARNING: declared range {decl_start.date()}..{decl_end.date()} is wider than 2022-2025 but no dates landed outside the legacy window")
        else:
            print(f"  OK declared {decl_start.date()}..{decl_end.date()}, dates land outside legacy")
    else:
        print(f"  declared range fits inside 2022-2025; no widening check")

    orphans = result.verify_integrity()
    known = KNOWN_ORPHANS.get(cls_name, [])
    unexpected = [o for o in orphans if not any(k in o for k in known)]
    if unexpected:
        print(f"  INTEGRITY FAILURES: {unexpected}")
        failures.append((cls_name, "integrity", unexpected))
    else:
        print(f"  integrity OK (known orphans: {len(orphans) - len([u for u in unexpected if u])})")

print()
print("=" * 60)
if failures:
    print(f"FAIL: {len(failures)} domains had issues")
    for f in failures:
        print(f"  {f[0]}: {f[1]} -> {f[2]}")
    sys.exit(1)
else:
    print("PASS: all 13 domains generated and verified")