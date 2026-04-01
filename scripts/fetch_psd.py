#!/usr/bin/env python3
"""
Fetch USDA FAS PSD data via the OpenData API.
Correct endpoint: /api/psd/commodity/{code}/country/all/year/{year}
Saves as data/psd_data.json matching terminal _PSD_RAW format.
Required env var: USDA_API_KEY
"""
import os, sys, json, urllib.request, time
from datetime import datetime
 
API_KEY = os.environ.get("USDA_API_KEY", "")
BASE = "https://apps.fas.usda.gov/OpenData/api/psd"
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "psd_data.json")
 
COMMODITIES = {
    "0410000": "Wheat",
    "0440000": "Corn",
    "2222000": "Soybeans",
    "2223100": "Soybean Meal",
    "2224000": "Soybean Oil",
    "2226000": "Rapeseed/Canola",
    "2631000": "Cotton",
    "0111000": "Beef and Veal",
}
 
ATTR_MAP = {
    "Area Harvested": "ah", "Beginning Stocks": "bs",
    "Domestic Consumption": "dc", "Ending Stocks": "es",
    "Exports": "ex", "Feed Dom. Consumption": "fd",
    "FSI Consumption": "fi", "Imports": "im",
    "Production": "pr", "Yield": "yl",
    "TY Exports": "te", "TY Imports": "ti",
    "Total Distribution": "td", "Total Supply": "ts",
    "Feed Domestic Consumption": "fd",
    "Food Seed and Industrial Dom. Cons.": "fi",
    "Total Dom. Cons.": "dc",
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
    "EU-27": "European Union",
}
 
YEARS = list(range(2015, datetime.now().year + 2))
 
 
def api_get(path):
    url = f"{BASE}/{path}"
    req = urllib.request.Request(url, headers={
        "API_KEY": API_KEY,
        "Accept": "application/json",
        "User-Agent": "SUMCO-Terminal/1.0",
    })
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode())
 
 
def fetch_commodity(code, name):
    print(f"  {name} ({code})...")
    data = {}
    errors = 0
 
    for year in YEARS:
        try:
            records = api_get(f"commodity/{code}/country/all/year/{year}")
            if not records:
                continue
 
            for r in records:
                country = r.get("countryDescription", r.get("country_Name", ""))
                country = COUNTRY_MAP.get(country, country)
                attr = r.get("attributeDescription", r.get("attribute_Description", ""))
                value = r.get("value", r.get("Value", 0))
 
                if country not in COUNTRIES:
                    continue
                short = ATTR_MAP.get(attr)
                if not short:
                    continue
 
                yr = str(year)
                data.setdefault(country, {}).setdefault(yr, {})
                try:
                    v = float(value) if value else 0
                    data[country][yr][short] = round(v, 2) if short == "yl" else int(round(v))
                except (ValueError, TypeError):
                    pass
 
        except Exception as e:
            errors += 1
            if errors <= 2:
                print(f"    Year {year}: {e}")
            elif errors == 3:
                print(f"    (suppressing further errors...)")
 
        time.sleep(0.3)
 
    print(f"    → {len(data)} countries, {errors} errors")
    return data
 
 
def main():
    if not API_KEY:
        print("ERROR: USDA_API_KEY not set", file=sys.stderr)
        sys.exit(1)
 
    print(f"Fetching FAS PSD data ({len(YEARS)} years × {len(COMMODITIES)} commodities)...")
    result = {}
 
    for code, name in COMMODITIES.items():
        try:
            data = fetch_commodity(code, name)
            if data:
                result[name] = data
        except Exception as e:
            print(f"    FAILED: {e}")
        time.sleep(1)
 
    if not result:
        print("ERROR: No data fetched. Check USDA_API_KEY.", file=sys.stderr)
        sys.exit(1)
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, separators=(",", ":"))
 
    print(f"\nSaved {OUT} ({os.path.getsize(OUT):,} bytes) — {len(result)} commodities")
 
 
if __name__ == "__main__":
    main()
