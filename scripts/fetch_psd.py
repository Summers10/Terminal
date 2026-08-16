#!/usr/bin/env python3
"""
Basic USDA FAS PSD fetcher — grains only (Wheat, Corn, Soybeans, Canola).
Bulk CSV download from PSD Online, no API key required.
Saves data/psd_data.json.
"""
import os, sys, json, urllib.request, urllib.error, csv, io, zipfile
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
 
SUM_ATTRS = {"ah", "bs", "dc", "es", "ex", "fd", "fi", "im", "pr", "te", "ti", "td", "ts"}
 
COMM_MAP = {
    "Wheat": "Wheat",
    "Corn": "Corn",
    "Soybeans": "Soybeans", "Soybean Oilseed": "Soybeans", "Oilseed, Soybean": "Soybeans",
    "Rapeseed": "Rapeseed/Canola", "Oilseed, Rapeseed": "Rapeseed/Canola", "Canola": "Rapeseed/Canola",
    "Oats": "Oats",
}
 
COUNTRIES = {
    "Algeria", "Argentina", "Australia", "Bangladesh", "Brazil", "Canada",
    "China", "Colombia", "Egypt", "European Union", "India", "Indonesia",
    "Iran", "Japan", "Kazakhstan", "Malaysia", "Mexico", "Morocco",
    "Pakistan", "Philippines", "Russia", "South Korea", "Taiwan",
    "Thailand", "Turkey", "Ukraine", "United States", "Vietnam",
}
 
COUNTRY_MAP = {
    "European Union (EU-27)": "European Union",
    "European Union-27": "European Union",
    "EU-27": "European Union", "EU27": "European Union",
    "Korea, South": "South Korea", "Korea, Republic of": "South Korea",
}
 
MIN_YEAR = 2015
BASE = "https://apps.fas.usda.gov/psdonline/downloads"
 
 
def download_zip(filename):
    url = f"{BASE}/{filename}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=180) as resp:
        return resp.read()
 
 
def parse_csv_zip(zipdata, result, world_by_country):
    count = 0
    unseen = set()
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
                if comm not in COMM_MAP:
                    unseen.add(comm)
                    continue
                comm_name = COMM_MAP[comm]
 
                country = row.get("Country_Name", "")
                country = COUNTRY_MAP.get(country, country)
 
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
                try:
                    v = float(value) if value else 0
                except (ValueError, TypeError):
                    continue
 
                val = round(v, 2) if short == "yl" else int(round(v))
 
                if short in SUM_ATTRS and country != "World":
                    world_by_country.setdefault(comm_name, {}).setdefault(year, {}).setdefault(short, {})[country] = val
 
                if country in COUNTRIES:
                    result.setdefault(comm_name, {}).setdefault(country, {}).setdefault(year, {})
                    result[comm_name][country][year][short] = val
                    fc += 1
 
        print(f"{fc} records")
        count += fc
 
    if unseen:
        print(f"    Unmapped (ignored, not in scope): {sorted(unseen)}")
    return count
 
 
def main():
    print("Fetching FAS PSD data (grains-only, bulk CSV)...")
 
    files_to_try = ["psd_alldata_csv.zip", "psd_grains_csv.zip", "psd_oilseeds_csv.zip"]
 
    result = {}
    world_by_country = {}
    downloaded = []
 
    for filename in files_to_try:
        try:
            print(f"  {filename}...", end=" ", flush=True)
            data = download_zip(filename)
            print(f"OK ({len(data):,} bytes)")
            count = parse_csv_zip(data, result, world_by_country)
            if count > 0:
                downloaded.append(filename)
            if filename == "psd_alldata_csv.zip" and count > 0:
                break  # alldata covers everything we need; skip redundant category files
        except urllib.error.HTTPError as e:
            print(f"{e.code} {e.reason}")
        except Exception as e:
            print(f"error: {e}")
 
    if not result:
        print("\nERROR: No data fetched from any source.", file=sys.stderr)
        sys.exit(1)
 
    # World totals (deduped by country so overlapping files can't double-count)
    print("\nComputing World totals...")
    for comm, years in world_by_country.items():
        result.setdefault(comm, {})["World"] = {}
        for year, attrs in years.items():
            result[comm]["World"][year] = {
                attr: sum(countries.values())
                for attr, countries in attrs.items()
            }
 
    # Sanity check against known plausible production ranges (1000 MT, latest complete MY)
    SANITY_RANGES = {
        "Wheat":            (700_000, 900_000),
        "Corn":             (1_100_000, 1_400_000),
        "Soybeans":         (350_000, 500_000),
        "Rapeseed/Canola":  (70_000, 110_000),
        "Oats":             (18_000, 30_000),
    }
    print("\nSanity checking World production...")
    warnings = []
    for comm, (lo, hi) in SANITY_RANGES.items():
        world = result.get(comm, {}).get("World", {})
        if not world:
            warnings.append(f"  {comm}: NO World rollup (missing data)")
            continue
        latest_yr, latest_pr = None, 0
        for yr in sorted(world.keys(), reverse=True):
            pr = world[yr].get("pr", 0) or 0
            if pr > 0:
                latest_yr, latest_pr = yr, pr
                break
        if latest_pr == 0:
            warnings.append(f"  {comm}: production = 0 (unexpected)")
        elif not (lo <= latest_pr <= hi):
            warnings.append(f"  {comm} {latest_yr}: production = {latest_pr:,} out of expected range [{lo:,}, {hi:,}] — POSSIBLE BUG")
        else:
            print(f"  OK {comm} {latest_yr}: {latest_pr:,} kMT (within [{lo:,}, {hi:,}])")
 
    if warnings:
        print("\nSANITY CHECK WARNINGS:")
        for w in warnings:
            print(w)
        # Fail loud: do not silently ship data that fails sanity checks
        sys.exit(1)
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        result["_meta"] = {"fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
        json.dump(result, f, indent=2)
 
    size = os.path.getsize(OUT)
    comms = sorted(k for k in result.keys() if not k.startswith("_"))
    print(f"\nDownloaded from: {', '.join(downloaded)}")
    print(f"Commodities ({len(comms)}): {', '.join(comms)}")
    for c in comms:
        print(f"  {c}: {len(result[c])} entries {'(+World)' if 'World' in result[c] else ''}")
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
