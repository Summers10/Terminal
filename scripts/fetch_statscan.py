#!/usr/bin/env python3
"""Fetch Statistics Canada supply and disposition data (table 32-10-0013-01).
Downloads CSV zip, parses Canadian grain S&D, saves as data/statscan_sd.json.
This provides the historical actuals that feed the AAFC tab.
AAFC forecasts must be manually updated in data/aafc_forecasts.json."""
import os, sys, json, urllib.request, csv, io, zipfile
from datetime import datetime
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "statscan_sd.json")
URL = "https://www150.statcan.gc.ca/n1/tbl/csv/32100013-eng.zip"
 
# Commodities we care about
COMMODITIES = {
    "Wheat, excluding durum": "Wheat (ex Durum)",
    "Durum wheat": "Durum",
    "Barley": "Barley",
    "Corn for grain": "Corn",
    "Oats": "Oats",
    "Canola (rapeseed)": "Canola",
    "Soybeans": "Soybeans",
    "Dry peas": "Dry Peas",
    "Lentils": "Lentils",
    "Flaxseed": "Flaxseed",
    "Rye": "Rye",
}
 
# S&D items mapping
SD_MAP = {
    "Production": "prod",
    "Imports": "imports",
    "Total supply": "supply",
    "Exports, total, grains and products (wheat equivalent)": "exports",
    "Exports": "exports",
    "Total exports": "exports",
    "Food and industrial use": "food",
    "Feed, waste and dockage": "feed",
    "Total domestic use": "dom",
    "Seed use": "seed",
    "Ending stocks, total": "stocks",
    "Total ending stocks": "stocks",
    "Carry-over stocks, total": "stocks",
    "Beginning stocks, total": "beg_stocks",
    "Total beginning stocks": "beg_stocks",
    "Seeded area": "area_s",
    "Harvested area": "area_h",
    "Average yield": "yield",
    "Loss in handling": "loss",
}
 
 
def download_and_parse():
    print(f"Downloading StatsCan table 32-10-0013-01...")
    req = urllib.request.Request(URL, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=180) as resp:
        zipdata = resp.read()
    print(f"  Downloaded: {len(zipdata):,} bytes")
 
    zf = zipfile.ZipFile(io.BytesIO(zipdata))
    csv_name = None
    for name in zf.namelist():
        if name.endswith('.csv') and 'metadata' not in name.lower():
            csv_name = name
            break
    if not csv_name:
        csv_name = zf.namelist()[0]
 
    print(f"  Parsing {csv_name}...")
 
    result = {}
    count = 0
 
    with zf.open(csv_name) as f:
        text = io.TextIOWrapper(f, encoding='utf-8-sig')
        reader = csv.DictReader(text)
 
        for row in reader:
            # Get commodity
            grain = row.get("Type of grain", row.get("Grains", "")).strip()
            comm = None
            for pattern, key in COMMODITIES.items():
                if pattern.lower() == grain.lower():
                    comm = key
                    break
            if not comm:
                continue
 
            # Get S&D item
            sd_item = row.get("Supply and disposition", row.get("Supply and Disposition", "")).strip()
            item_key = None
            for pattern, key in SD_MAP.items():
                if pattern.lower() in sd_item.lower() or sd_item.lower() in pattern.lower():
                    item_key = key
                    break
            if not item_key:
                continue
 
            # Get reference date (crop year like "2024-2025")
            ref_date = row.get("REF_DATE", "").strip()
            if not ref_date:
                continue
 
            # Only keep recent crop years (2018+)
            try:
                start_year = int(ref_date.split("-")[0])
                if start_year < 2018:
                    continue
            except (ValueError, IndexError):
                continue
 
            # Get value
            val_str = row.get("VALUE", "").strip().replace(",", "")
            if not val_str:
                continue
            try:
                value = float(val_str)
            except ValueError:
                continue
 
            # Get unit
            uom = row.get("UOM", "").strip().lower()
 
            # Store
            result.setdefault(comm, {}).setdefault(ref_date, {})
            result[comm][ref_date][item_key] = value
            count += 1
 
    print(f"  Parsed {count} data points")
    return result
 
 
def format_for_terminal(data):
    """Format StatsCan data into AAFC_DATA compatible structure."""
    formatted = {"report_date": datetime.now().strftime("%B %d, %Y"), "crops": {}}
 
    for comm in sorted(data.keys()):
        years = sorted(data[comm].keys())
        if not years:
            continue
 
        rows = []
        for yr in years[-4:]:  # Last 4 crop years
            d = data[comm][yr]
            rows.append({
                "year": yr,
                "area_s": d.get("area_s"),
                "area_h": d.get("area_h"),
                "yield": round(d["yield"], 2) if d.get("yield") else None,
                "prod": d.get("prod"),
                "imports": d.get("imports"),
                "supply": d.get("supply"),
                "exports": d.get("exports"),
                "food": d.get("food"),
                "feed": d.get("feed"),
                "dom": d.get("dom"),
                "stocks": d.get("stocks"),
                "price": None,  # StatsCan doesn't have prices
            })
 
        formatted["crops"][comm] = {"price_label": "", "rows": rows}
 
    return formatted
 
 
def main():
    data = download_and_parse()
    if not data:
        print("ERROR: No StatsCan data parsed.", file=sys.stderr)
        sys.exit(1)
 
    formatted = format_for_terminal(data)
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(formatted, f, separators=(",", ":"), default=str)
 
    size = os.path.getsize(OUT)
    comms = sorted(formatted["crops"].keys())
    print(f"\nCommodities ({len(comms)}): {', '.join(comms)}")
    for c in comms:
        years = [r["year"] for r in formatted["crops"][c]["rows"]]
        print(f"  {c}: {', '.join(years)}")
    print(f"Saved {OUT} ({size:,} bytes)")
    print(f"\nNOTE: This contains HISTORICAL actuals only.")
    print(f"AAFC forecasts must be updated manually in data/aafc_forecasts.json")
 
 
if __name__ == "__main__":
    main()
 
