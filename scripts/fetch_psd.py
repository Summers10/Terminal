#!/usr/bin/env python3
"""
Fetch USDA FAS PSD data via bulk CSV download from PSD Online.
Computes World totals by summing all countries (deduped by country, so running
through multiple overlapping ZIP files doesn't double-count).
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
 
# Attributes summed for World totals (no yield - meaningless as a global average)
SUM_ATTRS = {"ah", "bs", "dc", "es", "ex", "fd", "fi", "im", "pr", "te", "ti", "td", "ts"}
 
COMM_MAP = {
    "Wheat": "Wheat", "Corn": "Corn",
    "Soybeans": "Soybeans", "Soybean Oilseed": "Soybeans",
    "Oilseed, Soybean": "Soybeans",
    "Soybean Meal": "Soybean Meal", "Meal, Soybean": "Soybean Meal",
    "Soybean Oil": "Soybean Oil", "Oil, Soybean": "Soybean Oil",
    "Rapeseed": "Rapeseed/Canola", "Oilseed, Rapeseed": "Rapeseed/Canola",
    "Canola": "Rapeseed/Canola",
    "Cotton": "Cotton",
    "Beef and Veal": "Beef and Veal", "Beef": "Beef and Veal",
    # Oilseeds
    "Sunflowerseed": "Sunflowerseed", "Oilseed, Sunflowerseed": "Sunflowerseed",
    "Sunflowerseed Oil": "Sunflowerseed Oil", "Oil, Sunflowerseed": "Sunflowerseed Oil",
    "Sunflowerseed Meal": "Sunflowerseed Meal", "Meal, Sunflowerseed": "Sunflowerseed Meal",
    "Rapeseed Meal": "Rapeseed Meal", "Meal, Rapeseed": "Rapeseed Meal",
    "Rapeseed Oil": "Rapeseed Oil", "Oil, Rapeseed": "Rapeseed Oil",
    "Palm Oil": "Palm Oil", "Oil, Palm": "Palm Oil",
    # Grains (discard)
    "Barley": None, "Sorghum": None, "Oats": None, "Rye": None,
    "Millet": None, "Mixed Grain": None, "Rice, Milled": None,
    # Remaining oilseeds/oils (discard)
    "Peanut": None, "Palm Kernel": None, "Copra": None, "Cottonseed": None,
    "Oilseed, Peanut": None, "Oilseed, Cottonseed": None,
    "Oilseed, Copra": None, "Oilseed, Palm Kernel": None,
    "Meal, Peanut": None, "Meal, Fish": None, "Meal, Cottonseed": None,
    "Meal, Copra": None, "Meal, Palm Kernel": None,
    "Oil, Peanut": None, "Oil, Cottonseed": None,
    "Oil, Copra": None, "Oil, Palm Kernel": None, "Oil, Olive": None,
    "Cottonseed Meal": None, "Cottonseed Oil": None,
    "Fish Meal": None, "Coconut Oil": None,
    "Palm Kernel Meal": None, "Palm Kernel Oil": None,
    "Peanut Meal": None, "Peanut Oil": None, "Olive Oil": None,
    "Pork": None, "Broiler Meat": None, "Poultry, Meat, Broiler": None,
    "Turkey Meat": None, "Poultry, Meat, Turkey": None,
    "Lamb": None, "Sheep Meat": None,
    "Butter": None, "Cheese": None, "Milk, Nonfat Dry": None,
    "Milk, Whole Dry": None, "Whey, Dry": None, "Fluid Milk": None,
}
 
COUNTRIES = {
    "Argentina", "Australia", "Brazil", "Canada", "China",
    "Egypt", "European Union", "India", "Indonesia", "Japan",
    "Kazakhstan", "Mexico", "Pakistan", "Russia", "South Africa",
    "Thailand", "Turkey", "Ukraine", "United Kingdom",
    "United States", "Vietnam",
}
 
COUNTRY_MAP = {
    "European Union (EU-27)": "European Union",
    "European Union-27": "European Union",
    "EU-27": "European Union", "EU27": "European Union",
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
    """
    result: {comm: {country: {year: {attr: val}}}} — tracked countries only
    world_by_country: {comm: {year: {attr: {country: val}}}} — ALL countries, deduped by assignment
    """
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
                if not comm_name:
                    continue
 
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
 
                # World rollup: store per-country so duplicate passes don't double-count
                if short in SUM_ATTRS and country != "World":
                    world_by_country.setdefault(comm_name, {}).setdefault(year, {}).setdefault(short, {})[country] = val
 
                # Store individual country data for tracked countries only
                if country in COUNTRIES:
                    result.setdefault(comm_name, {}).setdefault(country, {}).setdefault(year, {})
                    result[comm_name][country][year][short] = val
                    fc += 1
 
        print(f"{fc} records")
        count += fc
 
    if unseen:
        print(f"    Unmapped: {sorted(unseen)}")
    return count
 
 
def main():
    print("Fetching FAS PSD data via bulk CSV download...")
 
    # psd_alldata_csv.zip contains everything. Category-specific files are
    # fallbacks in case the master file is unavailable.
    candidates = [
        "psd_alldata_csv.zip",
        "psd_grains_csv.zip",
        "psd_grains_pulses_csv.zip",
        "psd_grain_csv.zip",
        "psd_oilseeds_csv.zip",
        "psd_oilseed_csv.zip",
        "psd_cotton_csv.zip",
        "psd_livestock_csv.zip",
        "psd_livestock_poultry_csv.zip",
        "psd_dairy_csv.zip",
    ]
 
    result = {}
    world_by_country = {}
    downloaded = []
    master_ok = False
 
    for filename in candidates:
        # If the master all-data file already succeeded, skip category files
        # (they contain the same records and just waste network+CPU)
        if master_ok:
            break
        try:
            print(f"  {filename}...", end=" ", flush=True)
            data = download_zip(filename)
            print(f"OK ({len(data):,} bytes)")
            count = parse_csv_zip(data, result, world_by_country)
            if count > 0:
                downloaded.append(filename)
                if filename == "psd_alldata_csv.zip":
                    master_ok = True
                    print("  (psd_alldata contains all commodities — skipping category files)")
        except urllib.error.HTTPError as e:
            print(f"{e.code} {e.reason}")
        except Exception as e:
            print(f"error: {e}")
 
    if not result:
        print("\nERROR: No data fetched.", file=sys.stderr)
        sys.exit(1)
 
    # Aggregate World totals by summing per-country values (deduped)
    print("\nComputing World totals...")
    for comm, years in world_by_country.items():
        result.setdefault(comm, {})["World"] = {}
        for year, attrs in years.items():
            result[comm]["World"][year] = {
                attr: sum(countries.values())
                for attr, countries in attrs.items()
            }
        n_countries_sample = len(next(iter(next(iter(years.values())).values()))) if years else 0
        print(f"  {comm}: World totals for {len(years)} years (~{n_countries_sample} countries)")
 
    # Snapshot previous WASDE values before overwriting
    prev_wasde = {}
    if os.path.exists(OUT):
        try:
            with open(OUT) as pf:
                old_data = json.load(pf)
            for comm in ["Corn", "Soybeans", "Wheat"]:
                if comm not in old_data or not isinstance(old_data[comm], dict):
                    continue
                for country in old_data[comm]:
                    if country.startswith("_"):
                        continue
                    yrs = sorted(old_data[comm][country].keys())
                    if not yrs:
                        continue
                    latest_yr = yrs[-1]
                    vals = old_data[comm][country][latest_yr]
                    for attr in ["Ending Stocks", "Production"]:
                        if attr in vals and vals[attr]:
                            key = f"{comm}|{country}|{latest_yr}|{attr}"
                            prev_wasde[key] = vals[attr]
            if prev_wasde:
                print(f"  Snapshot: {len(prev_wasde)} previous WASDE values preserved")
        except Exception as e:
            print(f"  Warning: Could not snapshot previous WASDE: {e}")
 
    if prev_wasde:
        result["_prev_wasde"] = prev_wasde
 
    # Save
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        result["_meta"] = {"fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
        json.dump(result, f, separators=(",", ":"))
 
    size = os.path.getsize(OUT)
    comms = sorted(k for k in result.keys() if not k.startswith("_"))
    print(f"\nDownloaded from: {', '.join(downloaded)}")
    print(f"Commodities ({len(comms)}): {', '.join(comms)}")
    for c in comms:
        countries = len(result[c])
        has_world = "World" in result[c]
        print(f"  {c}: {countries} entries {'(+World)' if has_world else ''}")
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
