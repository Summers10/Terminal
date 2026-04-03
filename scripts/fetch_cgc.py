#!/usr/bin/env python3
"""Fetch Canadian Grain Commission Grain Statistics Weekly CSV data.
Downloads cumulative crop-year CSVs, parses into terminal _GSW_RAW format.
Saves as data/gsw_data.json.
 
CSV columns: grain_week, crop_year, week_ending_date, worksheet, metric, period, grain, grade, region, Ktonnes
 
The CGC Summary sheet aggregates across facility types (Primary + Process + Terminal + Producer Cars).
This script replicates that aggregation:
  - Producer Deliveries = Feed Grains deliveries + Process deliveries + Producer Cars
  - Exports = Terminal Exports + Primary exports + Producer Cars exports
  - Domestic Disappearance = Process Milled/MFG + Primary Canadian Domestic + Terminal Domestic
  - Commercial Stocks = Primary + Process + Terminal stocks
  - Terminal Receipts = all terminal location receipts
"""
import os, sys, json, urllib.request, csv, io, ssl
from datetime import datetime
from collections import Counter, defaultdict
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "gsw_data.json")
 
CURRENT_CY = "2025-26"
CROP_YEARS = ["2020-21", "2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]
 
BASE = "https://www.grainscanada.gc.ca/en/grain-research/statistics/grain-statistics-weekly"
 
# Commodities we care about
COMMODITIES = {
    "Wheat": "Wheat", "Barley": "Barley", "Canola": "Canola",
    "Durum": "Durum", "Oats": "Oats", "Oat": "Oats",
    "Flaxseed": "Flaxseed", "All Wheat": "Wheat",
    "Canola (Rapeseed)": "Canola", "Amber Durum": "Durum",
}
 
# ── Series routing ──────────────────────────────────────────────────
# Each (worksheet_pattern, metric_pattern) → series_key
# Empty string = match anything. None = explicitly skip row.
# These replicate the CGC Summary sheet aggregation.
ROUTE = [
    # ── Producer Deliveries (all channels) ──
    ("feed grains",       "deliveries",            "producer_deliveries"),
    ("primary",           "deliveries",            "producer_deliveries"),
    ("process",           "producer deliveries",   "producer_deliveries"),
    ("producer cars",     "shipments",             "producer_deliveries"),
    ("producer cars",     "deliveries",            "producer_deliveries"),
 
    # ── Domestic Disappearance / Crush ──
    ("process",           "milled",                "crush"),
    ("process",           "mfg",                   "crush"),
    ("ppshipdist",        "canadian domestic",      "crush"),
    ("terminal disposition", "canadian domestic",   "crush"),
    ("feed grains",       "commercial disappearance", "crush"),
    ("feed grains",       "canadian domestic",      "crush"),
 
    # ── Exports (all channels) ──
    ("terminal exports",  "",                      "exports"),
    ("ppshipdist",        "export",                "exports"),
    ("producer cars",     "export",                "exports"),
 
    # ── Terminal Receipts ──
    ("terminal receipts", "",                      "terminal_receipts"),
 
    # ── Commercial Stocks (all facility types) ──
    ("terminal stocks",   "",                      "commercial_stocks"),
    ("process",           "stocks",                "commercial_stocks"),
    ("feed grains",       "visible supply",        "commercial_stocks"),
    ("feed grains",       "stocks",                "commercial_stocks"),
    ("primary",           "stocks",                "commercial_stocks"),
 
    # ── Explicit skips ──
    ("process",           "other deliveries",      None),
    ("process",           "shipments",             None),
    ("imported",          "",                      None),
]
 
# Period rules
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
 
 
def route_series(worksheet, metric):
    """Route a (worksheet, metric) pair to a series key using ROUTE table."""
    ws = worksheet.lower().strip()
    met = metric.lower().strip()
 
    for ws_pat, met_pat, series_key in ROUTE:
        ws_match = ws_pat in ws or ws in ws_pat
        met_match = (not met_pat) or (met_pat in met) or (met in met_pat and len(met) > 3)
        if ws_match and met_match:
            return series_key  # Can be None = explicit skip
 
    return "UNMATCHED"
 
 
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
        if 'worksheet' in row_lower or 'metric' in row_lower:
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
 
    # Fallback for commodity
    if 'commodity' not in col_map:
        for i, h in enumerate(header):
            if 'grain' in h.lower() and 'week' not in h.lower() and 'year' not in h.lower():
                col_map['commodity'] = i
                break
 
    if 'commodity' not in col_map or 'value' not in col_map:
        print(f"    WARNING: Missing columns. Header: {header}")
        return {}
 
    print(f"    Column map: {col_map}")
    for row in rows[header_idx+1:header_idx+4]:
        print(f"    Sample: {row}")
 
    # Diagnostic counters
    canola_combos = Counter()
    skip_reasons = Counter()
 
    # ── Two-level aggregation ──
    # Level 1: Collect values grouped by (comm, series, year, week, worksheet, region)
    # Level 2: Within each worksheet: use "Total" if present, else SUM regions
    # Level 3: SUM across worksheets for same (comm, series, year, week)
 
    # raw[comm][series][cy][week] = {worksheet: {region: value}}
    raw = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(float))))))
    dates = {}  # (cy, week) -> date_str
    skipped = 0
    parsed = 0
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
 
        worksheet = row[col_map['worksheet']].strip() if 'worksheet' in col_map else ""
        metric = row[col_map['metric']].strip() if 'metric' in col_map else ""
        period = row[col_map['period']].strip().lower() if 'period' in col_map else ""
        region = row[col_map['region']].strip() if 'region' in col_map else ""
 
        # Diagnostic for Canola
        if comm_key == "Canola":
            canola_combos[(worksheet, metric, period)] += 1
 
        # Route to series
        series_key = route_series(worksheet, metric)
        if series_key is None:
            continue  # Explicit skip
        if series_key == "UNMATCHED":
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
 
        # Store raw value
        raw[comm_key][series_key][cy_label][week][worksheet][region] += value
        dates[(cy_label, week)] = date_str
        parsed += 1
 
    # ── Aggregate ──
    result = {}
 
    for comm_key in raw:
        for series_key in raw[comm_key]:
            for cy_label in raw[comm_key][series_key]:
                for week in raw[comm_key][series_key][cy_label]:
                    ws_data = raw[comm_key][series_key][cy_label][week]
                    date_str = dates.get((cy_label, week), "")
 
                    # For each worksheet: prefer "Total" row, else SUM regions
                    ws_total = 0.0
                    for ws_name, regions in ws_data.items():
                        # Check for "Total" region (case-insensitive)
                        total_val = None
                        for reg, val in regions.items():
                            if reg.lower() in ("total", "grand total", ""):
                                total_val = val
                                break
                        if total_val is not None and len(regions) > 1:
                            # "Total" row exists alongside sub-items — use Total only
                            ws_total += total_val
                        else:
                            # No "Total" row or only one region — SUM all
                            ws_total += sum(regions.values())
 
                    final_value = round(ws_total, 1)
 
                    result.setdefault(comm_key, {}).setdefault(series_key, {}).setdefault(cy_label, [])
                    result[comm_key][series_key][cy_label].append([week, date_str, final_value])
 
    # Sort by week
    for comm in result:
        for sname in result[comm]:
            for cy in result[comm][sname]:
                result[comm][sname][cy].sort(key=lambda r: r[0])
 
    final_count = sum(len(rows) for comm in result for sname in result[comm] for rows in result[comm][sname].values())
    print(f"    Raw parsed: {parsed}, aggregated: {final_count} points, skipped: {skipped}")
 
    # Canola diagnostics
    if canola_combos:
        print(f"    Canola combos (ws|metric|period):")
        for (ws, met, per), cnt in sorted(canola_combos.items(), key=lambda x: -x[1])[:20]:
            series = route_series(ws, met)
            tag = f"→{series}" if series and series != "UNMATCHED" else "SKIP" if series is None else "?"
            print(f"      ws={ws:25s} met={met:25s} per={per:15s} rows={cnt:5d} {tag}")
 
    # Validation: print Canola week 34 values for 2025-2026 if available
    if "Canola" in result:
        for cy in sorted(result["Canola"].get("producer_deliveries", {}).keys())[-1:]:
            for series_name in ["producer_deliveries", "exports", "crush", "commercial_stocks", "terminal_receipts"]:
                if series_name in result["Canola"] and cy in result["Canola"][series_name]:
                    last = result["Canola"][series_name][cy][-1]
                    print(f"    CHECK Canola.{series_name} {cy} wk{last[0]}: {last[2]}")
 
    if skip_reasons:
        top = sorted(skip_reasons.items(), key=lambda x: -x[1])[:10]
        print(f"    Top skip reasons: {top}")
 
    return result
 
 
def merge_results(all_results):
    merged = {}
    for result in all_results:
        for comm, series in result.items():
            for sname, seasons in series.items():
                for cy, rows in seasons.items():
                    merged.setdefault(comm, {}).setdefault(sname, {}).setdefault(cy, [])
                    merged[comm][sname][cy] = rows
    return merged
 
 
def add_weekly_series(data):
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
 
