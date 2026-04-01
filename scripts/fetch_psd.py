#!/usr/bin/env python3
"""
Fetch USDA FAS PSD data via bulk CSV download from PSD Online.
The API endpoints are currently broken (500/403), so this uses
the publicly accessible CSV zip files instead.
 
Saves as data/psd_data.json matching terminal _PSD_RAW format.
"""
import os, sys, json, urllib.request, csv, io, zipfile
from datetime import datetime
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "psd_data.json")
 
ATTR_MAP = {
    "Area Harvested": "ah", "Beginning Stocks": "bs",
    "Domestic Consumption": "dc", "Ending Stocks": "es",
    "Exports": "ex", "Feed Dom. Consumption": "fd",
    "FSI Consumption": "fi", "Imports": "im",
    "Production": "pr", "Yield": "yl",
    "TY Exports": "te", "TY Imports": "ti",
    "Total Distribution": "td", "Total Supply": "ts",
    "Feed Domestic Consumption": "fd",
}
 
# Terminal commodity names we want
WANT_COMMS = {
    "Wheat", "Corn", "Soybeans", "Soybean Meal", "Soybean Oil",
    "Rapeseed", "Cotton", "Beef and Veal",
}
 
# Map CSV commodity descriptions to terminal names
COMM_MAP = {
    "Wheat": "Wheat", "Corn": "Corn",
    "Soybeans": "Soybeans", "Soybean Oilseed": "Soybeans",
    "Soybean Meal": "Soybean Meal", "Soybean Oil": "Soybean Oil",
    "Rapeseed": "Rapeseed/Canola", "Rapeseed Meal": None,
    "Rapeseed Oil": None, "Cotton": "Cotton",
    "Beef and Veal": "Beef and Veal",
}
 
COUNTRIES = {
    "Argentina", "Australia", "Brazil", "Canada", "China",
    "Egypt", "European Union", "India", "Indonesia", "Japan",
    "Kazakhstan", "Mexico", "Pakistan", "Russia", "South Africa",
    "Thailand", "Turkey", "Ukraine", "United Kingdom",
    "United States", "Vietnam", "World",
}
 
COUNTRY_MAP = {
    "European Union (EU-27)": "European Union",
    "European Union-27": "European Union",
    "EU-27": "European Union", "EU27": "European Union",
}
 
MIN_YEAR = 2015
BASE = "https://apps.fas.usda.gov/psdonline/downloads"
 
 
def download_zip(filename):
    """Download a zip file from PSD Online downloads."""
    url = f"{BASE}/{filename}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=180) as resp:
        return resp.read()
 
 
def parse_csv_zip(zipdata, result):
    """Parse a PSD CSV zip file and add records to result dict."""
    count = 0
    zf = zipfile.ZipFile(io.BytesIO(zipdata))
    for fname in zf.namelist():
        if not fname.lower().endswith('.csv'):
            continue
        print(f"    Parsing {fname}...", end=" ", flush=True)
        fc = 0
        with zf.open(fname) as f:
            text = io.TextIOWrapper(f, encoding='utf-8-sig')
            reader = csv.DictReader(text)
            for row in reader:
                comm = row.get("Commodity_Description", "")
                comm_name = COMM_MAP.get(comm)
                if not comm_name:
                    continue
 
                country = row.get("Country_Name", "")
                country = COUNTRY_MAP.get(country, country)
                if country not in COUNTRIES:
                    continue
 
                attr = row.get("Attribute_Description", "")
                short = ATTR_MAP.get(attr)
                if not short:
                    continue
 
                year = row.get("Market_Year", "")
                try:
                    if int(year) < MIN_YEAR:
                        continue
                except (ValueError, TypeError):
                    continue
 
                value = row.get("Value", "0")
                if value:
                    value = value.replace(",", "").strip()
 
                result.setdefault(comm_name, {}).setdefault(country, {}).setdefault(year, {})
                try:
                    v = float(value) if value else 0
                    result[comm_name][country][year][short] = round(v, 2) if short == "yl" else int(round(v))
                    fc += 1
                except (ValueError, TypeError):
                    pass
        print(f"{fc} records")
        count += fc
    return count
 
 
def main():
    print("Fetching FAS PSD data via bulk CSV download...")
 
    # Try multiple possible filenames — the correct ones vary
    candidates = [
        "psd_alldata_csv.zip",
        "psd_grains_csv.zip",
        "psd_grains_pulses_csv.zip",
        "psd_grain_csv.zip",
        "psd_oilseeds_csv.zip",
        "psd_cotton_csv.zip",
        "psd_livestock_csv.zip",
        "psd_sugar_csv.zip",
        "psd_coffee_csv.zip",
        "psd_dairy_csv.zip",
    ]
 
    result = {}
    downloaded = []
 
    for filename in candidates:
        try:
            print(f"  {filename}...", end=" ", flush=True)
            data = download_zip(filename)
            print(f"OK ({len(data):,} bytes)")
            count = parse_csv_zip(data, result)
            if count > 0:
                downloaded.append(filename)
        except urllib.error.HTTPError as e:
            print(f"{e.code} {e.reason}")
        except Exception as e:
            print(f"error: {e}")
 
    if not result:
        print("\nERROR: No data fetched from any source.", file=sys.stderr)
        sys.exit(1)
 
    # Save
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, separators=(",", ":"))
 
    size = os.path.getsize(OUT)
    comms = sorted(result.keys())
    print(f"\nDownloaded from: {', '.join(downloaded)}")
    print(f"Commodities: {', '.join(comms)}")
    for c in comms:
        print(f"  {c}: {len(result[c])} countries")
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
