"""
Scenario 07 -- CDM Export
=========================
Spindle can export any dataset as a Microsoft Common Data Model (CDM) folder.

CDM folders are compatible with:
  - Microsoft Fabric CDM connectors
  - Azure Data Lake Storage CDM folders
  - Dataverse / Power Platform
  - Power BI dataflows (CDM source)

Output structure:
  output/
    model.json              ← CDM manifest (entity definitions, partition paths, dtypes)
    Customer/
      Customer.csv          ← entity data file
    SalesOrder/
      SalesOrder.csv
    ...

Run:
    python examples/scenarios/07_cdm_export.py
"""

import json
import tempfile
from pathlib import Path

from sqllocks_spindle import Spindle, RetailDomain, HealthcareDomain
from sqllocks_spindle.transform import CdmMapper, CdmEntityMap

spindle = Spindle()
mapper  = CdmMapper()

# ------------------------------------------------------------------
# 1. Basic CDM export -- default PascalCase entity names
# ------------------------------------------------------------------
retail = spindle.generate(domain=RetailDomain(), scale="fabric_demo", seed=42)

with tempfile.TemporaryDirectory() as tmp:
    files = mapper.write_cdm_folder(
        retail.tables,
        output_dir=tmp,
        domain_name="SpindleRetail",
    )

    # Show what was written
    model_path = Path(tmp) / "model.json"
    model = json.loads(model_path.read_text())

    print(f"=== CDM folder written ({len(files)} files) ===")
    print(f"Domain name:  {model['name']}")
    print(f"Entities:     {[e['name'] for e in model['entities']]}")
    print(f"Format:       CSV (default)")

    # Verify every partition file actually exists
    missing = []
    for entity in model["entities"]:
        loc = entity["partitions"][0]["location"]
        data_path = Path(tmp) / loc
        if not data_path.exists():
            missing.append(loc)
    assert not missing, f"Missing partition files: {missing}"
    print("Partition files: all present -- PASS")

# ------------------------------------------------------------------
# 2. Map to standard CDM entity names (Contact, SalesOrder, etc.)
# ------------------------------------------------------------------
retail_entity_map = RetailDomain().cdm_map()
print(f"\n=== RetailDomain.cdm_map() entity names ===")
for src_table in ["customer", "order", "order_line", "product", "return"]:
    print(f"  {src_table:<15} -> {retail_entity_map.entity_name(src_table)}")

with tempfile.TemporaryDirectory() as tmp:
    files = mapper.write_cdm_folder(
        retail.tables,
        output_dir=tmp,
        domain_name="SpindleRetailCDM",
        entity_map=retail_entity_map,
    )
    model = json.loads((Path(tmp) / "model.json").read_text())
    entity_names = [e["name"] for e in model["entities"]]
    print(f"\nEntities with CDM mapping: {entity_names}")

# ------------------------------------------------------------------
# 3. Parquet format
# ------------------------------------------------------------------
with tempfile.TemporaryDirectory() as tmp:
    try:
        files = mapper.write_cdm_folder(
            retail.tables,
            output_dir=tmp,
            domain_name="SpindleRetailParquet",
            fmt="parquet",
        )
        parquet_files = [f for f in files if f.suffix == ".parquet"]
        print(f"\n=== Parquet CDM folder: {len(parquet_files)} Parquet entity files ===")
        model = json.loads((Path(tmp) / "model.json").read_text())
        fmt_type = model["entities"][0]["partitions"][0]["fileFormatSettings"]["$type"]
        print(f"fileFormatSettings.$type = {fmt_type}")
    except ImportError:
        print("\nParquet CDM: skipped (pip install pyarrow)")

# ------------------------------------------------------------------
# 4. In-memory model.json -- no files written
# ------------------------------------------------------------------
model = mapper.to_model_json(retail.tables, domain_name="InMemoryTest")
print(f"\n=== In-memory model.json ===")
print(f"name:         {model['name']}")
print(f"version:      {model['version']}")
print(f"culture:      {model['culture']}")
print(f"entities:     {len(model['entities'])}")
print(f"modifiedTime: {model['modifiedTime']}")

# Inspect one entity's attributes
cust_entity = next(e for e in model["entities"] if e["name"] == "Customer")
print(f"\nCustomer entity attributes ({len(cust_entity['attributes'])}):")
for attr in cust_entity["attributes"]:
    print(f"  {attr['name']:<25} {attr['dataType']}")

# ------------------------------------------------------------------
# 5. Custom entity map -- define your own CDM names
# ------------------------------------------------------------------
custom_map = CdmEntityMap({
    "customer":    "Account",
    "order":       "SalesInvoice",
    "order_line":  "SalesInvoiceLine",
    "product":     "Item",
    "store":       "Site",
})

model = mapper.to_model_json(retail.tables, entity_map=custom_map)
entity_names = [e["name"] for e in model["entities"]]
print(f"\n=== Custom entity map ===")
print(f"Entity names: {entity_names}")

# ------------------------------------------------------------------
# 6. Healthcare CDM export
# ------------------------------------------------------------------
print("\n=== Healthcare CDM export ===")
hc = spindle.generate(domain=HealthcareDomain(), scale="fabric_demo", seed=42)
hc_map = HealthcareDomain().cdm_map()

for src_table in ["patient", "provider", "encounter", "claim"]:
    print(f"  {src_table:<15} -> {hc_map.entity_name(src_table)}")
