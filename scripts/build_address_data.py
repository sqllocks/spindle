"""Build script: download GeoNames US postal code data into Spindle's reference format.

Source:  GeoNames Geographical Database
URL:     https://download.geonames.org/export/zip/US.zip
License: Creative Commons Attribution 4.0 International (CC-BY-4.0)
         See: https://creativecommons.org/licenses/by/4.0/
Attribution required in distributed products — see LICENSE-NOTICES.md.

Output:
    sqllocks_spindle/domains/retail/reference_data/us_zip_locations.json

Run once from the project root whenever you want to refresh the bundled data:
    python scripts/build_address_data.py
"""

from __future__ import annotations

import io
import json
import sys
import urllib.request
import zipfile
from pathlib import Path

SOURCE_URL = "https://download.geonames.org/export/zip/US.zip"
INNER_FILENAME = "US.txt"
OUTPUT_PATH = (
    Path(__file__).parent.parent
    / "sqllocks_spindle"
    / "domains"
    / "retail"
    / "reference_data"
    / "us_zip_locations.json"
)

# GeoNames US.txt column indices (tab-delimited)
COL_ZIP = 1
COL_CITY = 2
COL_STATE = 4    # admin_code1 — 2-letter state abbreviation
COL_LAT = 9
COL_LNG = 10

# These are US territories, not states. Include or exclude as desired.
# Excluded here to keep data focused on the 50 states + DC.
EXCLUDED_STATES = {"PR", "VI", "GU", "AS", "MP", "UM", "FM", "MH", "PW"}


def download_and_build() -> None:
    print(f"Downloading GeoNames US postal code data...")
    print(f"  Source: {SOURCE_URL}")
    print(f"  License: Creative Commons Attribution 4.0 International")

    with urllib.request.urlopen(SOURCE_URL, timeout=60) as response:
        zip_bytes = response.read()

    print(f"  Downloaded {len(zip_bytes) / 1024:.0f} KB")

    # Extract the TSV from inside the ZIP
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        with zf.open(INNER_FILENAME) as tsv_file:
            lines = tsv_file.read().decode("utf-8").splitlines()

    print(f"  Parsing {len(lines):,} lines...")

    entries: list[dict] = []
    skipped = 0

    for line in lines:
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 11:
            skipped += 1
            continue

        state = parts[COL_STATE].strip()
        if state in EXCLUDED_STATES:
            skipped += 1
            continue

        zip_code = parts[COL_ZIP].strip()
        city = parts[COL_CITY].strip()
        lat_str = parts[COL_LAT].strip()
        lng_str = parts[COL_LNG].strip()

        if not zip_code or not city or not state or not lat_str or not lng_str:
            skipped += 1
            continue

        try:
            lat = round(float(lat_str), 6)
            lng = round(float(lng_str), 6)
        except ValueError:
            skipped += 1
            continue

        entries.append({"zip": zip_code, "city": city, "state": state, "lat": lat, "lng": lng})

    print(f"  Valid entries: {len(entries):,} ({skipped} skipped)")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(entries, f, separators=(",", ":"))

    size_kb = OUTPUT_PATH.stat().st_size / 1024
    states = len({e["state"] for e in entries})
    print(f"\n  Written: {OUTPUT_PATH.relative_to(Path(__file__).parent.parent)}")
    print(f"  File size: {size_kb:.1f} KB")
    print(f"  States/territories covered: {states}")
    print(f"  Sample entries:")
    for entry in entries[:3]:
        print(f"    {entry}")


if __name__ == "__main__":
    try:
        download_and_build()
        print("\nDone. Remember: redistribution requires attribution per CC-BY-4.0.")
        print("See LICENSE-NOTICES.md for the required attribution notice.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
