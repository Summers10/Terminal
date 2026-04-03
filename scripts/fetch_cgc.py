#!/usr/bin/env python3
"""Fetch Canadian Grain Commission Grain Statistics Weekly CSV data.
Downloads cumulative crop-year CSVs, parses into terminal _GSW_RAW format.
Saves as data/gsw_data.json.
 
CSV columns: grain_week, crop_year, week_ending_date, worksheet, metric, period, grain, grade, region, Ktonnes
Key worksheets: Feed Grains, Process, Terminal Exports, Terminal Receipts, Terminal Stocks, PPShipDist, Primary
"""
import os, sys, json, urllib.request, csv, io, ssl
from datetime import datetime
from collections import Counter
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "gsw_data.json")
 
CURRENT_CY = "2025-26"
CROP_YEARS = ["2020-21", "2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]
 
BASE = "https://www.grainscanada.gc.ca/en/grain-research/statistics/grain-statistics-weekly"
 
# Commodities we care about
COMMODITIES = {"Wheat": "Wheat", "Barley": "Barley", "Canola": "Canola",
               "Durum": "Durum", "Oats": "Oats", "Flaxseed": "Flaxseed",
               "All Wheat": "Wheat", "Canola (Rapeseed)": "Canola",
               "Amber Durum": "Durum", "Oat": "Oats"}
 
# Series mapping — matches against metric column value
# Order matters: more specific patterns checked first in match_series()
SERIES_MAP = {
    "Milled/MFG Grain": "crush",
    "Milled/MFG": "crush",
    "Milled": "crush",
    "MFG Grain": "crush",
    "Domestic Disappearance": "crush",
    "Domestic Use": "crush",
    "Producer Deliveries": "producer_deliveries",
    "Primary Deliveries": "producer_deliveries",
    "Deliveries": "producer_deliveries",
    "Terminal Exports": "exports",
    "Exports": "exports",
    "Terminal Stocks": "commercial_stocks",
    "Visible Supply": "commercial_stocks",
    "Commercial Stocks": "commercial_stocks",
    "Stocks": "commercial_stocks",
    "Primary Elevator Receipts": "primary_receipts",
    "Primary Receipts": "primary_receipts",
    "Receipts": "terminal_receipts",
    "Terminal Receipts": "terminal_receipts",
}
 
# Map worksheet+metric combos to series (overrides SERIES_MAP when both columns present)
COMBO_MAP = {
    ("feed grains", "deliveries"): "producer_deliveries",
    ("primary", "deliveries"): "producer_deliveries",
    ("primary", "receipts"): "primary_receipts",
    ("process", "milled"): "crush",
    ("process", "mfg"): "crush",
    ("process", "domestic"): "crush",
    ("process", "producer deliveries"): None,  # skip — this is deliveries TO process, not crush
    ("terminal exports", ""): "exports",
    ("terminal receipts", ""): "terminal_receipts",
    ("terminal stocks", ""): "commercial_stocks",
    ("ppshipdist", "canadian domestic"): None,  # skip for now — partial domestic only
}
 
# Period rules: cumulative vs point-in-time
CUMULATIVE_SERIES = {"producer_deliveries", "exports", "crush", "primary_receipts", "terminal_receipts"}
POINT_IN_TIME_SERIES = {"commercial_stocks"}
 
 
def download_csv(crop_year):
    """Try multiple URL patterns for the crop year CSV."""
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
                    "Accept": "text/csv,*/*",
                })
                with urllib.request.urlopen(req, timeout=300, context=ctx) as resp:
                    data = resp.read()
                    print(f"  {crop_year}: {len(data):,} bytes from {url}")
                    return data.decode("utf-8-sig", errors="replace")
            except Exception as e:
                if attempt < 2:
                    import time; time.sleep(5)
                continue
    print(f"  {crop_year}: FAILED all URLs")
    return None
 
 
def match_series(worksheet, metric):
    """Match worksheet+metric to a series key. Returns series_key or None."""
    ws = worksheet.lower().strip()
    met = metric.lower().strip()
 
    # 1. Try COMBO_MAP first (most specific)
    for (ws_pat, met_pat), key in COMBO_MAP.items():
        if ws_pat in ws and (not met_pat or met_pat in met):
            return key  # Can be None to explicitly skip
 
    # 2. Try metric match (column 4 in CSV)
    for pattern, key in SERIES_MAP.items():
        pl = pattern.lower()
        if pl == met or pl in met:
            return key
 
    # 3. Try worksheet match (column 3 in CSV)
    for pattern, key in SERIES_MAP.items():
        pl = pattern.lower()
        if pl == ws or pl in ws:
            return key
 
    # 4. Reverse substring checks
    for pattern, key in SERIES_MAP.items():
        pl = pattern.lower()
        if met in pl and len(met) > 3:
            return key
    for pattern, key in SERIES_MAP.items():
        pl = pattern.lower()
        if ws in pl and len(ws) > 3:
            return key
 
    return None
 
 
def parse_csv(text, crop_year):
    """Parse the CGC CSV and extract weekly data by commodity and series."""
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return {}
 
    # Find header row
    header = None
    header_idx = 0
    for i, row in enumerate(rows):
        row_lower = [c.strip().lower() for c in row]
        if any(x in row_lower for x in ('grain_week', 'grain week')):
            header = [c.strip() for c in row]
            header_idx = i
            break
        if 'week' in row_lower and ('grain' in row_lower or 'commodity' in row_lower):
            header = [c.strip() for c in row]
            header_idx = i
            break
    if not header:
        header = [c.strip() for c in rows[0]]
        header_idx = 0
 
    print(f"    Header ({len(header)} cols): {header}")
 
    # Map column indices
    col_map = {}
    for i, h in enumerate(header):
        hl = h.lower().strip()
        if hl in ('grain_week', 'grain week'):
            col_map['week'] = i
        elif 'week' in hl and 'end' not in hl and 'grain' not in hl and 'crop' not in hl and 'week' not in col_map:
            col_map['week'] = i
        elif 'week' in hl and ('end' in hl or 'date' in hl):
            col_map['date'] = i
        elif hl in ('grain', 'commodity'):
            col_map['commodity'] = i
        elif hl == 'worksheet':
            col_map['worksheet'] = i
        elif hl == 'metric':
            col_map['metric'] = i
        elif hl == 'period':
            col_map['period'] = i
        elif hl in ('region', 'province'):
            col_map['region'] = i
        elif 'tonnes' in hl or hl == 'ktonnes':
            col_map['value'] = i
        elif ('value' in hl or 'quantity' in hl) and 'value' not in col_map:
            col_map['value'] = i
 
    # Fallback for commodity column
    if 'commodity' not in col_map:
        for i, h in enumerate(header):
            hl = h.lower().strip()
            if 'grain' in hl and 'week' not in hl and 'year' not in hl:
                col_map['commodity'] = i
                break
 
    if 'commodity' not in col_map or 'value' not in col_map:
        print(f"    WARNING: Missing columns. commodity={'commodity' in col_map} value={'value' in col_map}")
        for row in rows[header_idx+1:header_idx+4]:
            print(f"    Sample: {row}")
        return {}
 
    print(f"    Column map: {col_map}")
    for row in rows[header_idx+1:header_idx+4]:
        print(f"    Sample: {row}")
 
    # Diagnostic: collect all unique (worksheet, metric, period) combos for Canola
    canola_combos = Counter()
 
    # Parse data rows — accumulate by (comm, series, year, week)
    # Use MAX value per key to avoid double-counting from sub-items + Total rows
    temp = {}
    skipped = 0
    parsed = 0
    skip_reasons = Counter()
 
    max_col = max(col_map.values())
 
    for row in rows[header_idx + 1:]:
        if len(row) <= max_col:
            continue
 
        # Match commodity
        commodity = row[col_map['commodity']].strip()
        comm_key = None
        for pattern, key in COMMODITIES.items():
            if pattern.lower() == commodity.lower():
                comm_key = key
                break
        if not comm_key:
            for pattern, key in COMMODITIES.items():
                if pattern.lower() in commodity.lower() or commodity.lower() in pattern.lower():
                    comm_key = key
                    break
        if not comm_key:
            skipped += 1
            skip_reasons[f"comm:{commodity[:20]}"] += 1
            continue
 
        # Get worksheet, metric, period, region
        worksheet = row[col_map['worksheet']].strip() if 'worksheet' in col_map else ""
        metric = row[col_map['metric']].strip() if 'metric' in col_map else ""
        period = row[col_map['period']].strip().lower() if 'period' in col_map else ""
        region = row[col_map['region']].strip() if 'region' in col_map else ""
 
        # Diagnostic for Canola
        if comm_key == "Canola":
            canola_combos[(worksheet, metric, period)] += 1
 
        # Match series
        series_key = match_series(worksheet, metric)
        if series_key is None:
            skipped += 1
            skip_reasons[f"ws:{worksheet[:15]}|{metric[:15]}"] += 1
            continue
 
        # Period filter
        if series_key in CUMULATIVE_SERIES and period == "current week":
            continue
        if series_key in POINT_IN_TIME_SERIES and period != "current week":
            continue
 
        # Parse week and value
        try:
            week = int(row[col_map['week']])
        except (ValueError, IndexError):
            continue
 
        date_str = row[col_map['date']].strip() if 'date' in col_map else ""
 
        try:
            val_str = row[col_map['value']].strip().replace(",", "")
            value = float(val_str) if val_str else None
        except (ValueError, IndexError):
            value = None
 
        if value is None:
            continue
 
        # Crop year label
        parts = crop_year.split("-")
        if len(parts) == 2:
            y1 = int("20" + parts[0]) if len(parts[0]) == 2 else int(parts[0])
            y2 = int("20" + parts[1]) if len(parts[1]) == 2 else int(parts[1])
            cy_label = f"{y1}-{y2}"
        else:
            cy_label = crop_year
 
        # Aggregate: for each (comm, series, year, week), keep MAX value
        # This handles: Summary "Total" vs sub-item rows, multiple worksheets
        agg_key = (comm_key, series_key, cy_label, week)
        existing = temp.get(agg_key)
        if existing is None or value > existing[1]:
            temp[agg_key] = (date_str, round(value, 1))
        parsed += 1
 
    # Convert to result format
    result = {}
    for (comm_key, series_key, cy_label, week), (date_str, value) in temp.items():
        result.setdefault(comm_key, {}).setdefault(series_key, {}).setdefault(cy_label, [])
        result[comm_key][series_key][cy_label].append([week, date_str, value])
 
    # Sort by week
    for comm in result:
        for sname in result[comm]:
            for cy in result[comm][sname]:
                result[comm][sname][cy].sort(key=lambda r: r[0])
 
    final_count = sum(len(rows) for comm in result for sname in result[comm] for rows in result[comm][sname].values())
    print(f"    Raw parsed: {parsed}, aggregated: {final_count} points, skipped: {skipped}")
 
    # Print Canola diagnostics
    if canola_combos:
        print(f"    Canola combos (ws|metric|period):")
        for (ws, met, per), cnt in sorted(canola_combos.items(), key=lambda x: -x[1])[:20]:
            print(f"      ws={ws:25s} met={met:25s} per={per:15s} rows={cnt}")
 
    if skip_reasons:
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
        if comm == "_meta":
            continue
        for sname in list(data[comm].keys()):
            if sname in ("commercial_stocks",):
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
    merged["_meta"] = {"fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
    with open(OUT, "w") as f:
        json.dump(merged, f, separators=(",", ":"))
 
    size = os.path.getsize(OUT)
    print(f"\nCommodities: {', '.join(k for k in sorted(merged.keys()) if k != '_meta')}")
    for c in sorted(merged.keys()):
        if c == "_meta":
            continue
        series = [s for s in sorted(merged[c].keys()) if not s.endswith("_weekly")]
        for s in series:
            years = sorted(merged[c][s].keys())
            counts = [f"{y}:{len(merged[c][s][y])}" for y in years[-2:]]
            print(f"  {c}.{s}: {', '.join(counts)}")
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
