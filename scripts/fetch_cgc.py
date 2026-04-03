#!/usr/bin/env python3
"""Fetch Canadian Grain Commission Grain Statistics Weekly CSV data.
Downloads cumulative crop-year CSVs, parses into terminal _GSW_RAW format.
Saves as data/gsw_data.json."""
import os, sys, json, urllib.request, csv, io, time, ssl
from datetime import datetime
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "gsw_data.json")
 
# Crop years to fetch (current + 5 previous)
CURRENT_CY = "2025-26"
CROP_YEARS = ["2020-21", "2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]
 
# URL patterns (note: some years use /csv/ subdirectory, some don't)
BASE = "https://www.grainscanada.gc.ca/en/grain-research/statistics/grain-statistics-weekly"
 
# Commodities we care about
COMMODITIES = {"Wheat": "Wheat", "Barley": "Barley", "Canola": "Canola",
               "Durum": "Durum", "Oats": "Oats", "Flaxseed": "Flaxseed",
               "All Wheat": "Wheat", "Canola (Rapeseed)": "Canola"}
 
# Series mapping from CGC activity names to our internal keys
SERIES_MAP = {
    "Producer Deliveries": "producer_deliveries",
    "Domestic Use": "crush",
    "Exports": "exports",
    "Commercial Stocks": "commercial_stocks",
    "Primary Elevator Receipts": "primary_receipts",
    "Terminal Receipts": "terminal_receipts",
}
 
 
def download_csv(crop_year):
    """Try multiple URL patterns for the crop year CSV with retries."""
    urls = [
        f"{BASE}/{crop_year}/gsw-shg-en.csv",
        f"{BASE}/{crop_year}/csv/gsw-shg-en.csv",
    ]
    ctx = ssl.create_default_context()
    for url in urls:
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/csv,text/plain,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                })
                with urllib.request.urlopen(req, timeout=300, context=ctx) as resp:
                    data = resp.read()
                    print(f"  {crop_year}: {len(data):,} bytes from {url}")
                    return data.decode("utf-8-sig", errors="replace")
            except Exception as e:
                print(f"  {crop_year}: attempt {attempt+1} failed for {url}: {type(e).__name__}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
                continue
    print(f"  {crop_year}: FAILED all URLs")
    return None
 
 
def parse_csv(text, crop_year):
    """Parse the CGC CSV and extract weekly data by commodity and series."""
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return {}
 
    # Find header row (look for a row containing 'Week' or 'Commodity')
    header = None
    header_idx = 0
    for i, row in enumerate(rows):
        row_lower = [c.strip().lower() for c in row]
        if 'week' in row_lower or 'commodity' in row_lower or 'grain' in row_lower:
            header = [c.strip() for c in row]
            header_idx = i
            break
 
    if not header:
        # Try first row as header
        header = [c.strip() for c in rows[0]]
        header_idx = 0
 
    print(f"    Header ({len(header)} cols): {header}")
 
    # Map column indices
    col_map = {}
    for i, h in enumerate(header):
        hl = h.lower()
        if ('grain_week' == hl or hl == 'grain week') and 'week' not in col_map:
            col_map['week'] = i
        elif 'week' in hl and 'end' not in hl and 'number' not in hl and 'grain' not in hl and 'crop' not in hl:
            col_map['week'] = i
        elif 'week' in hl and ('end' in hl or 'date' in hl):
            col_map['date'] = i
        elif hl in ('commodity', 'grain', 'crop'):
            col_map['commodity'] = i
        elif 'commodity' in hl and 'grain' not in hl:
            col_map['commodity'] = i
        elif hl == 'grain' or (('grain' in hl or 'crop' in hl) and 'week' not in hl and 'year' not in hl):
            col_map['commodity'] = i
        elif 'activity' in hl or 'worksheet' in hl or 'category' in hl:
            col_map['activity'] = i
        elif 'value' in hl or 'quantity' in hl or '000' in hl or 'tonnes' in hl:
            col_map['value'] = i
 
    # If we can't find named columns, try positional
    if 'commodity' not in col_map:
        print(f"    WARNING: Could not find commodity column. Available: {header}")
        # Log first 3 data rows for debugging
        for row in rows[header_idx+1:header_idx+4]:
            print(f"    Sample: {row[:8]}")
        return {}
 
    print(f"    Column map: {col_map}")
    for row in rows[header_idx+1:header_idx+4]:
        print(f"    Sample: {row}")
 
    # Parse data rows
    result = {}
    skipped = 0
    parsed = 0
    skip_reasons = {}
 
    for row in rows[header_idx + 1:]:
        if len(row) <= max(col_map.values()):
            continue
 
        commodity = row[col_map.get('commodity', 0)].strip()
        comm_key = None
        for pattern, key in COMMODITIES.items():
            if pattern.lower() in commodity.lower() or commodity.lower() in pattern.lower():
                comm_key = key
                break
        if not comm_key:
            skipped += 1
            skip_reasons[f"comm:{commodity[:20]}"] = skip_reasons.get(f"comm:{commodity[:20]}", 0) + 1
            continue
 
        activity = row[col_map.get('activity', 1)].strip() if 'activity' in col_map else ""
        act_clean = activity.lower().replace("_", " ").replace("-", " ").strip()
        series_key = None
        for pattern, key in SERIES_MAP.items():
            if pattern.lower() in act_clean or act_clean in pattern.lower():
                series_key = key
                break
        if not series_key:
            skipped += 1
            skip_reasons[f"act:{activity[:30]}"] = skip_reasons.get(f"act:{activity[:30]}", 0) + 1
            continue
 
        try:
            week = int(row[col_map.get('week', 0)]) if 'week' in col_map else 0
        except (ValueError, IndexError):
            week = 0
 
        date_str = row[col_map.get('date', 1)].strip() if 'date' in col_map else ""
 
        try:
            val_str = row[col_map.get('value', -1)].strip().replace(",", "")
            value = round(float(val_str), 1) if val_str else None
        except (ValueError, IndexError):
            value = None
 
        if value is None:
            continue
 
        cy_label = crop_year.replace("-20", "-").replace("20", "", 1) if len(crop_year) > 5 else crop_year
        # Normalize to format like "2025-2026"
        parts = crop_year.split("-")
        if len(parts) == 2:
            y1 = int("20" + parts[0]) if len(parts[0]) == 2 else int(parts[0])
            y2 = int("20" + parts[1]) if len(parts[1]) == 2 else int(parts[1])
            cy_label = f"{y1}-{y2}"
 
        result.setdefault(comm_key, {}).setdefault(series_key, {}).setdefault(cy_label, [])
        result[comm_key][series_key][cy_label].append([week, date_str, value])
        parsed += 1
 
    print(f"    Parsed: {parsed} rows, skipped: {skipped}")
    if skip_reasons and parsed == 0:
        top = sorted(skip_reasons.items(), key=lambda x: -x[1])[:10]
        print(f"    Top skip reasons: {top}")
    return result
 
 
def merge_results(all_results):
    """Merge results from multiple crop years."""
    merged = {}
    for result in all_results:
        for comm, series in result.items():
            for sname, seasons in series.items():
                for cy, rows in seasons.items():
                    merged.setdefault(comm, {}).setdefault(sname, {}).setdefault(cy, [])
                    merged[comm][sname][cy] = rows
    return merged
 
 
def add_weekly_series(data):
    """Compute weekly (non-cumulative) series from cumulative data."""
    for comm in data:
        for sname in list(data[comm].keys()):
            if sname in ("commercial_stocks",):  # stocks are not cumulative
                continue
            weekly_key = sname + "_weekly"
            if weekly_key in data[comm]:
                continue
            data[comm][weekly_key] = {}
            for cy, rows in data[comm][sname].items():
                sorted_rows = sorted(rows, key=lambda r: r[0])
                weekly = []
                prev = 0
                for r in sorted_rows:
                    wk_val = round(r[2] - prev, 1)
                    weekly.append([r[0], r[1], wk_val])
                    prev = r[2]
                data[comm][weekly_key][cy] = weekly
 
 
def main():
    print("Fetching CGC Grain Statistics Weekly data...")
    all_results = []
 
    for cy in CROP_YEARS:
        text = download_csv(cy)
        if text:
            result = parse_csv(text, cy)
            if result:
                all_results.append(result)
 
    if not all_results:
        print("ERROR: No CGC data fetched.", file=sys.stderr)
        sys.exit(1)
 
    merged = merge_results(all_results)
    add_weekly_series(merged)
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    merged["_meta"] = {"fetched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")}
    with open(OUT, "w") as f:
        json.dump(merged, f, separators=(",", ":"))
 
    size = os.path.getsize(OUT)
    print(f"\nCommodities: {', '.join(sorted(merged.keys()))}")
    for c in sorted(merged.keys()):
        series = list(merged[c].keys())
        print(f"  {c}: {', '.join(series)}")
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
