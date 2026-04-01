#!/usr/bin/env python3
"""
Fetch USDA FAS PSD data — tries multiple API endpoints.
Saves as data/psd_data.json matching terminal _PSD_RAW format.
Required env var: USDA_API_KEY
 
Tries in order:
  1. PSDOnlineDataServices API (newer)
  2. OpenData API (older, may be deprecated)
  3. Bulk CSV download (no API key needed)
"""
import os, sys, json, urllib.request, urllib.parse, time, csv, io, zipfile
from datetime import datetime
 
API_KEY = os.environ.get("USDA_API_KEY", "")
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
    "EU27": "European Union",
    "Korea, South": "South Korea",
}
 
MIN_YEAR = 2015
YEARS = list(range(MIN_YEAR, datetime.now().year + 2))
 
 
def http_get(url, headers=None):
    hdrs = {"Accept": "application/json", "User-Agent": "SUMCO-Terminal/1.0"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, headers=hdrs)
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read()
 
 
def process_records(records, result):
    """Process API response records into result dict."""
    count = 0
    for r in records:
        comm_code = r.get("commodityCode", r.get("Commodity_Code", ""))
        comm_name = COMMODITIES.get(comm_code)
        if not comm_name:
            continue
 
        country = r.get("countryDescription", r.get("Country_Name", r.get("countryName", "")))
        country = COUNTRY_MAP.get(country, country)
        if country not in COUNTRIES:
            continue
 
        attr = r.get("attributeDescription", r.get("Attribute_Description", r.get("attributeName", "")))
        short = ATTR_MAP.get(attr)
        if not short:
            continue
 
        year = str(r.get("marketYear", r.get("Market_Year", r.get("year", ""))))
        try:
            if int(year) < MIN_YEAR:
                continue
        except ValueError:
            continue
 
        value = r.get("value", r.get("Value", 0))
 
        result.setdefault(comm_name, {}).setdefault(country, {}).setdefault(year, {})
        try:
            v = float(value) if value else 0
            result[comm_name][country][year][short] = round(v, 2) if short == "yl" else int(round(v))
            count += 1
        except (ValueError, TypeError):
            pass
    return count
 
 
def try_psd_dataservices():
    """Try PSDOnlineDataServices API."""
    print("\n[1] Trying PSDOnlineDataServices API...")
    base = "https://apps.fas.usda.gov/PSDOnlineDataServices/api/CommodityData"
    result = {}
 
    for code, name in COMMODITIES.items():
        print(f"  {name}...", end=" ", flush=True)
        total = 0
        errors = 0
        for year in YEARS:
            try:
                url = f"{base}/GetCommodityDataByYear?commodityCode={code}&marketYear={year}"
                data = json.loads(http_get(url, {"API_KEY": API_KEY}))
                if data:
                    total += process_records(data, result)
                time.sleep(0.3)
            except Exception as e:
                errors += 1
                if errors == 1:
                    print(f"({e})", end=" ", flush=True)
        print(f"{total} records, {errors} errors")
        time.sleep(0.5)
 
    return result
 
 
def try_opendata_api():
    """Try OpenData API."""
    print("\n[2] Trying OpenData API...")
    base = "https://apps.fas.usda.gov/OpenData/api/psd"
    result = {}
 
    for code, name in COMMODITIES.items():
        print(f"  {name}...", end=" ", flush=True)
        total = 0
        errors = 0
        for year in YEARS:
            try:
                url = f"{base}/commodity/{code}/country/all/year/{year}"
                data = json.loads(http_get(url, {"API_KEY": API_KEY}))
                if data:
                    total += process_records(data, result)
                time.sleep(0.3)
            except Exception as e:
                errors += 1
                if errors == 1:
                    print(f"({e})", end=" ", flush=True)
        print(f"{total} records, {errors} errors")
        time.sleep(0.5)
 
    return result
 
 
def try_bulk_csv():
    """Try downloading bulk CSV from PSD Online."""
    print("\n[3] Trying bulk CSV download...")
    # PSD Online provides CSV downloads for commodity groups
    csv_urls = [
        "https://apps.fas.usda.gov/psdonline/downloads/psd_grains_csv.zip",
        "https://apps.fas.usda.gov/psdonline/downloads/psd_oilseeds_csv.zip",
        "https://apps.fas.usda.gov/psdonline/downloads/psd_cotton_csv.zip",
        "https://apps.fas.usda.gov/psdonline/downloads/psd_meats_csv.zip",
    ]
 
    result = {}
    # Map CSV commodity names to our names
    csv_comm_map = {
        "Wheat": "Wheat", "Corn": "Corn", "Soybeans": "Soybeans",
        "Soybean Meal": "Soybean Meal", "Soybean Oil": "Soybean Oil",
        "Rapeseed": "Rapeseed/Canola", "Cotton": "Cotton",
        "Beef and Veal": "Beef and Veal",
        "Soybean Oilseed": "Soybeans",
    }
 
    for url in csv_urls:
        try:
            print(f"  Downloading {url.split('/')[-1]}...", end=" ", flush=True)
            data = http_get(url)
            zf = zipfile.ZipFile(io.BytesIO(data))
            for fname in zf.namelist():
                if not fname.endswith('.csv'):
                    continue
                with zf.open(fname) as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))
                    count = 0
                    for row in reader:
                        comm = row.get("Commodity_Description", "")
                        comm_name = csv_comm_map.get(comm)
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
                        except ValueError:
                            continue
 
                        value = row.get("Value", "0").replace(",", "")
 
                        result.setdefault(comm_name, {}).setdefault(country, {}).setdefault(year, {})
                        try:
                            v = float(value) if value else 0
                            result[comm_name][country][year][short] = round(v, 2) if short == "yl" else int(round(v))
                            count += 1
                        except (ValueError, TypeError):
                            pass
                    print(f"{count} records from {fname}")
        except Exception as e:
            print(f"failed ({e})")
 
    return result
 
 
def main():
    print(f"Fetching FAS PSD data...")
    print(f"API key: {'set (' + API_KEY[:4] + '...)' if API_KEY else 'NOT SET'}")
 
    result = {}
 
    # Try approach 1: PSDOnlineDataServices
    if API_KEY:
        try:
            result = try_psd_dataservices()
        except Exception as e:
            print(f"  Failed: {e}")
 
    # Try approach 2: OpenData API
    if not result and API_KEY:
        try:
            result = try_opendata_api()
        except Exception as e:
            print(f"  Failed: {e}")
 
    # Try approach 3: Bulk CSV download (no API key needed)
    if not result:
        try:
            result = try_bulk_csv()
        except Exception as e:
            print(f"  Failed: {e}")
 
    if not result:
        print("\nERROR: All approaches failed. No data fetched.", file=sys.stderr)
        sys.exit(1)
 
    # Save
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, separators=(",", ":"))
 
    size = os.path.getsize(OUT)
    comms = len(result)
    countries = sum(len(v) for v in result.values())
    print(f"\nSaved {OUT} ({size:,} bytes) — {comms} commodities, {countries} total country entries")
 
 
if __name__ == "__main__":
    main()
