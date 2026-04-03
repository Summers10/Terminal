#!/usr/bin/env python3
"""Fetch Statistics Canada supply and disposition data (table 32-10-0013-01).
Downloads CSV zip, parses Canadian grain S&D, saves as data/statscan_sd.json.
This provides the historical actuals that feed the AAFC tab.
 
Handles both old (crop year: 2024-2025) and new (monthly: 2025-03) REF_DATE formats.
Canadian crop year runs August 1 to July 31.
"""
import os, sys, json, urllib.request, csv, io, zipfile, ssl
from datetime import datetime
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "statscan_sd.json")
URL = "https://www150.statcan.gc.ca/n1/tbl/csv/32100013-eng.zip"
 
# Commodities we care about — multiple aliases for each
COMMODITIES = {
    "Wheat, excluding durum": "Wheat (ex Durum)",
    "Wheat excluding durum": "Wheat (ex Durum)",
    "Durum wheat": "Durum",
    "Barley": "Barley",
    "Corn for grain": "Corn",
    "Oats": "Oats",
    "Canola (rapeseed)": "Canola",
    "Canola": "Canola",
    "Rapeseed": "Canola",
    "Soybeans": "Soybeans",
    "Dry peas": "Dry Peas",
    "Lentils": "Lentils",
    "Flaxseed": "Flaxseed",
    "Rye": "Rye",
}
 
# S&D items mapping — patterns matched via substring
SD_MAP = {
    "Production": "prod",
    "Imports": "imports",
    "Total supply": "supply",
    "Total supplies": "supply",
    "Exports, total": "exports",
    "Exports": "exports",
    "Total exports": "exports",
    "Food and industrial use": "food",
    "Feed, waste and dockage": "feed",
    "Total domestic use": "dom",
    "Total domestic disappearance": "dom",
    "Seed use": "seed",
    "Ending stocks": "stocks",
    "Carry-over stocks": "stocks",
    "Beginning stocks": "beg_stocks",
    "Opening stocks": "beg_stocks",
    "Seeded area": "area_s",
    "Harvested area": "area_h",
    "Average yield": "yield",
    "Loss in handling": "loss",
}
 
 
def ref_date_to_crop_year(ref_date):
    """Convert REF_DATE to crop year string.
    
    Handles:
    - Crop year format: '2024-2025' or '2024/2025' -> '2024-2025'
    - Monthly format: '2025-03' -> crop year based on Aug-Jul cycle
    - Calendar year: '2025' -> '2024-2025' (assume mid-year)
    """
    ref_date = ref_date.strip().replace("/", "-")
    parts = ref_date.split("-")
    
    if len(parts) == 2:
        try:
            p1 = int(parts[0])
            p2 = int(parts[1])
        except ValueError:
            return None
        
        if p2 > 12:
            # Crop year format: 2024-2025
            return f"{p1}-{p2}"
        else:
            # Monthly format: 2025-03 (year-month)
            year, month = p1, p2
            if month >= 8:  # Aug-Dec: crop year starts this year
                return f"{year}-{year+1}"
            else:  # Jan-Jul: crop year started previous year
                return f"{year-1}-{year}"
    
    elif len(parts) == 1:
        try:
            year = int(parts[0])
            return f"{year-1}-{year}"
        except ValueError:
            return None
    
    return None
 
 
def download_and_parse():
    print(f"Downloading StatsCan table 32-10-0013-01...")
    
    ctx = ssl.create_default_context()
    zipdata = None
    
    for attempt in range(3):
        try:
            req = urllib.request.Request(URL, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "*/*",
            })
            with urllib.request.urlopen(req, timeout=300, context=ctx) as resp:
                zipdata = resp.read()
            print(f"  Downloaded: {len(zipdata):,} bytes (attempt {attempt+1})")
            break
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt < 2:
                import time; time.sleep(10)
    
    if not zipdata:
        return {}
 
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
    unmatched_grains = set()
    unmatched_sd = set()
    ref_dates_seen = set()
 
    with zf.open(csv_name) as f:
        text = io.TextIOWrapper(f, encoding='utf-8-sig')
        reader = csv.DictReader(text)
        
        # Print column names for debugging
        first_row = None
        rows_iter = iter(reader)
        try:
            first_row = next(rows_iter)
        except StopIteration:
            print("  ERROR: CSV is empty")
            return {}
        
        print(f"  CSV columns: {list(first_row.keys())}")
        print(f"  Sample row: {dict(list(first_row.items())[:8])}")
        
        import itertools
        all_rows = itertools.chain([first_row], rows_iter)
 
        for row in all_rows:
            # Get commodity — try multiple column names
            grain = ""
            for col_name in ["Type of crop", "Type of grain", "Grains", "Commodity"]:
                grain = row.get(col_name, "").strip()
                if grain:
                    break
            
            comm = None
            # Try exact match first
            for pattern, key in COMMODITIES.items():
                if pattern.lower() == grain.lower():
                    comm = key
                    break
            # Try substring match
            if not comm:
                for pattern, key in COMMODITIES.items():
                    if pattern.lower() in grain.lower() or grain.lower() in pattern.lower():
                        comm = key
                        break
            if not comm:
                if grain and grain.lower() not in ('all wheat', 'total grains', 'total'):
                    unmatched_grains.add(grain[:40])
                continue
 
            # Get S&D item — try multiple column names
            sd_item = ""
            for col_name in ["Supply and disposition of grains", "Supply and disposition", 
                            "Supply and Disposition", "Supply and disposition of principal field crops"]:
                sd_item = row.get(col_name, "").strip()
                if sd_item:
                    break
            
            item_key = None
            for pattern, key in SD_MAP.items():
                if pattern.lower() in sd_item.lower() or sd_item.lower() in pattern.lower():
                    item_key = key
                    break
            if not item_key:
                if sd_item:
                    unmatched_sd.add(sd_item[:50])
                continue
 
            # Get reference date and convert to crop year
            ref_date = row.get("REF_DATE", "").strip()
            if not ref_date:
                continue
            
            ref_dates_seen.add(ref_date)
            crop_year = ref_date_to_crop_year(ref_date)
            if not crop_year:
                continue
 
            # Only keep recent crop years (2018+)
            try:
                start_year = int(crop_year.split("-")[0])
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
 
            # Store — later dates overwrite earlier ones (latest estimate wins)
            result.setdefault(comm, {}).setdefault(crop_year, {})
            result[comm][crop_year][item_key] = value
            count += 1
 
    print(f"  Parsed {count} data points")
    
    # Diagnostics
    sample_dates = sorted(ref_dates_seen)
    print(f"  REF_DATE range: {sample_dates[0] if sample_dates else '?'} to {sample_dates[-1] if sample_dates else '?'}")
    print(f"  REF_DATE format samples: {sample_dates[:5]}")
    
    if unmatched_grains:
        print(f"  Unmatched grains: {sorted(unmatched_grains)[:10]}")
    if unmatched_sd:
        print(f"  Unmatched S&D items: {sorted(unmatched_sd)[:10]}")
    
    # Show what we got for Canola specifically
    if "Canola" in result:
        print(f"  Canola years: {sorted(result['Canola'].keys())}")
        for yr in sorted(result['Canola'].keys())[-2:]:
            items = sorted(result['Canola'][yr].keys())
            print(f"    {yr}: {items}")
    else:
        print(f"  WARNING: No Canola data found!")
    
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
                "seed": d.get("seed"),
                "stocks": d.get("stocks"),
                "beg_stocks": d.get("beg_stocks"),
                "price": None,
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
    formatted["_meta"] = {"fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
    with open(OUT, "w") as f:
        json.dump(formatted, f, separators=(",", ":"), default=str)
 
    size = os.path.getsize(OUT)
    comms = sorted(formatted["crops"].keys())
    print(f"\nCommodities ({len(comms)}): {', '.join(comms)}")
    for c in comms:
        years = [r["year"] for r in formatted["crops"][c]["rows"]]
        items_sample = [k for k, v in formatted["crops"][c]["rows"][-1].items() if v is not None and k != "year"]
        print(f"  {c}: {', '.join(years)} — fields: {', '.join(items_sample[:6])}")
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
