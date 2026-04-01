#!/usr/bin/env python3
"""
Fetch USDA FAS PSD data via bulk CSV download from PSD Online.
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
 
# Comprehensive CSV commodity name → terminal name mapping
# FAS CSV files use various naming conventions
COMM_MAP = {
    # Grains
    "Wheat": "Wheat",
    "Corn": "Corn",
    "Barley": None, "Sorghum": None, "Oats": None, "Rye": None,
    "Millet": None, "Mixed Grain": None, "Rice, Milled": None,
    # Oilseeds - try many variants
    "Soybeans": "Soybeans",
    "Soybean Oilseed": "Soybeans",
    "Oilseed, Soybean": "Soybeans",
    "Soybean Meal": "Soybean Meal",
    "Meal, Soybean": "Soybean Meal",
    "Soybean Oil": "Soybean Oil",
    "Oil, Soybean": "Soybean Oil",
    "Rapeseed": "Rapeseed/Canola",
    "Oilseed, Rapeseed": "Rapeseed/Canola",
    "Canola": "Rapeseed/Canola",
    "Rapeseed Meal": None, "Rapeseed Oil": None,
    "Meal, Rapeseed": None, "Oil, Rapeseed": None,
    "Sunflowerseed": None, "Peanut": None, "Palm Kernel": None,
    "Copra": None, "Cottonseed": None, "Palm Oil": None,
    "Oilseed, Sunflowerseed": None, "Oilseed, Peanut": None,
    "Oilseed, Cottonseed": None, "Oilseed, Copra": None,
    "Oilseed, Palm Kernel": None,
    "Meal, Sunflowerseed": None, "Meal, Peanut": None,
    "Meal, Fish": None, "Meal, Cottonseed": None,
    "Meal, Copra": None, "Meal, Palm Kernel": None,
    "Oil, Sunflowerseed": None, "Oil, Peanut": None,
    "Oil, Cottonseed": None, "Oil, Copra": None,
    "Oil, Palm Kernel": None, "Oil, Olive": None, "Oil, Palm": None,
    "Sunflowerseed Meal": None, "Sunflowerseed Oil": None,
    "Cottonseed Meal": None, "Cottonseed Oil": None,
    "Fish Meal": None, "Coconut Oil": None,
    "Palm Kernel Meal": None, "Palm Kernel Oil": None,
    "Peanut Meal": None, "Peanut Oil": None,
    "Olive Oil": None,
    # Cotton
    "Cotton": "Cotton",
    # Livestock
    "Beef and Veal": "Beef and Veal",
    "Beef": "Beef and Veal",
    "Pork": None, "Broiler Meat": None, "Poultry, Meat, Broiler": None,
    "Turkey Meat": None, "Poultry, Meat, Turkey": None,
    "Lamb": None, "Sheep Meat": None,
    # Dairy
    "Butter": None, "Cheese": None, "Milk, Nonfat Dry": None,
    "Milk, Whole Dry": None, "Whey, Dry": None,
    "Fluid Milk": None,
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
    url = f"{BASE}/{filename}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=180) as resp:
        return resp.read()
 
 
def parse_csv_zip(zipdata, result):
    count = 0
    unseen_comms = set()
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
                    unseen_comms.add(comm)
                    continue
 
                comm_name = COMM_MAP[comm]
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
 
    if unseen_comms:
        print(f"    Unmapped commodities: {sorted(unseen_comms)}")
 
    return count
 
 
def main():
    print("Fetching FAS PSD data via bulk CSV download...")
 
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
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, separators=(",", ":"))
 
    size = os.path.getsize(OUT)
    comms = sorted(result.keys())
    print(f"\nDownloaded from: {', '.join(downloaded)}")
    print(f"Commodities ({len(comms)}): {', '.join(comms)}")
    for c in comms:
        print(f"  {c}: {len(result[c])} countries")
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
