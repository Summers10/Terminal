#!/usr/bin/env python3
"""Fetch Statistics Canada supply and disposition data (table 32-10-0013-01).
Annual crop-year table — REF_DATE format is YYYY-YYYY (e.g. 2023-2024).
 
This version prints ALL column headers and sample data to stdout so failures
in GitHub Actions can be diagnosed immediately from the log output.
"""
import os, sys, json, urllib.request, csv, io, zipfile
from datetime import datetime
 
OUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "statscan_sd.json"
)
URL = "https://www150.statcan.gc.ca/n1/tbl/csv/32100013-eng.zip"
 
COMMODITIES = {
    "wheat, excluding durum":  "Wheat (ex Durum)",
    "durum wheat":             "Durum",
    "barley":                  "Barley",
    "corn for grain":          "Corn",
    "oats":                    "Oats",
    "canola (rapeseed)":       "Canola",
    "soybeans":                "Soybeans",
    "dry peas":                "Dry Peas",
    "lentils":                 "Lentils",
    "flaxseed":                "Flaxseed",
    "rye":                     "Rye",
}
 
SD_MAP = {
    "production":              "prod",
    "imports":                 "imports",
    "total supply":            "supply",
    "exports, total":          "exports",
    "total exports":           "exports",
    "exports":                 "exports",
    "food and industrial use": "food",
    "food, industrial":        "food",
    "feed, waste and dockage": "feed",
    "feed, waste":             "feed",
    "total domestic use":      "dom",
    "domestic use":            "dom",
    "seed use":                "seed",
    "ending stocks, total":    "stocks",
    "total ending stocks":     "stocks",
    "carry-over stocks, total":"stocks",
    "ending stocks":           "stocks",
    "beginning stocks, total": "beg_stocks",
    "total beginning stocks":  "beg_stocks",
    "beginning stocks":        "beg_stocks",
    "seeded area":             "area_s",
    "harvested area":          "area_h",
    "average yield":           "yield",
    "loss in handling":        "loss",
}
 
STANDARD_COLS = {
    "ref_date","geo","dguid","uom","uom_id","scalar_factor",
    "scalar_id","vector","coordinate","value","status",
    "symbol","terminated","decimals",
}
 
 
def find_col(headers_lower, candidates):
    for cand in candidates:
        for raw, low in headers_lower.items():
            if cand in low:
                return raw
    return None
 
 
def parse_crop_year(s):
    """Return (start_year:int, label:str) from any StatsCan REF_DATE format."""
    s = str(s).strip()
 
    # YYYY-MM-DD or YYYY-MM  → derive crop year from calendar month
    for fmt, ln in [("%Y-%m-%d", 10), ("%Y-%m", 7)]:
        try:
            d = datetime.strptime(s[:ln], fmt)
            start = d.year if d.month >= 8 else d.year - 1
            return start, f"{start}-{start+1}"
        except (ValueError, TypeError):
            pass
 
    # YYYY-YYYY, YYYY-YY, YYYY/YYYY, YYYY/YY
    for sep in ("-", "/"):
        if sep in s:
            parts = s.split(sep)
            if len(parts) == 2:
                try:
                    y1 = int(parts[0])
                    raw2 = parts[1].strip()
                    y2 = (y1 // 100) * 100 + int(raw2) if len(raw2) <= 2 else int(raw2)
                    if 2000 <= y1 <= 2040 and y2 in (y1, y1+1):
                        return y1, f"{y1}-{y1+1}"
                except (ValueError, TypeError):
                    pass
 
    # Plain YYYY
    try:
        y = int(s[:4])
        if 2000 <= y <= 2040:
            return y, f"{y}-{y+1}"
    except (ValueError, TypeError):
        pass
 
    return None, None
 
 
def download_and_parse():
    print("Downloading StatsCan table 32-10-0013-01...")
    req = urllib.request.Request(URL, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=180) as resp:
        zipdata = resp.read()
    print(f"  Downloaded: {len(zipdata):,} bytes")
 
    zf = zipfile.ZipFile(io.BytesIO(zipdata))
    csv_name = next(
        (n for n in zf.namelist() if n.endswith(".csv") and "metadata" not in n.lower()),
        zf.namelist()[0]
    )
    print(f"  Parsing: {csv_name}")
 
    with zf.open(csv_name) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        headers = list(reader.fieldnames or [])
 
    # ── Print EVERY header — the most important diagnostic ──────────────────
    print(f"  All {len(headers)} CSV columns:")
    for h in headers:
        print(f"    '{h}'")
 
    headers_lower = {h: h.lower() for h in headers}
 
    col_grain = find_col(headers_lower, ["type of grain","grain","commodity","product"])
    col_sd    = find_col(headers_lower, ["supply and disposition","supply and disp","disposition"])
    col_ref   = find_col(headers_lower, ["ref_date","refdate","ref date","date"])
    col_val   = find_col(headers_lower, ["value"])
 
    print(f"  Column mapping:")
    print(f"    grain = {repr(col_grain)}")
    print(f"    sd    = {repr(col_sd)}")
    print(f"    ref   = {repr(col_ref)}")
    print(f"    value = {repr(col_val)}")
 
    if not col_grain or not col_sd:
        extras = [h for h in headers if h.lower() not in STANDARD_COLS]
        print(f"  Non-standard columns (fallback candidates): {extras}")
        if not col_grain and len(extras) >= 1:
            col_grain = extras[0]
            print(f"  → Using '{col_grain}' as grain column")
        if not col_sd and len(extras) >= 2:
            col_sd = extras[1]
            print(f"  → Using '{col_sd}' as SD column")
    if not col_ref: col_ref = "REF_DATE"
    if not col_val: col_val = "VALUE"
 
    # ── Second pass: parse data ─────────────────────────────────────────────
    result = {}
    count  = 0
    sample_grains = set()
    sample_sds    = set()
    sample_refs   = set()
 
    with zf.open(csv_name) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for i, row in enumerate(reader):
            grain_raw = (row.get(col_grain) or "").strip()
            sd_raw    = (row.get(col_sd)    or "").strip()
            ref_raw   = (row.get(col_ref)   or "").strip()
            val_raw   = (row.get(col_val)   or "").strip().replace(",","")
 
            if i < 50:
                if grain_raw: sample_grains.add(grain_raw)
                if sd_raw:    sample_sds.add(sd_raw)
                if ref_raw:   sample_refs.add(ref_raw)
 
            # Commodity match (exact lower)
            comm = None
            for k, v in COMMODITIES.items():
                if k == grain_raw.lower():
                    comm = v
                    break
            if not comm:
                continue
 
            # SD item match (substring)
            item_key = None
            sd_l = sd_raw.lower()
            for k, v in SD_MAP.items():
                if k in sd_l or sd_l in k:
                    item_key = v
                    break
            if not item_key:
                continue
 
            start_yr, cy_label = parse_crop_year(ref_raw)
            if start_yr is None or start_yr < 2015:
                continue
 
            try:
                value = float(val_raw)
            except (ValueError, TypeError):
                continue
 
            result.setdefault(comm, {}).setdefault(cy_label, {})[item_key] = value
            count += 1
 
    print(f"  Parsed {count} data points across {len(result)} commodities")
 
    if count == 0:
        print("  ── SAMPLE DATA (first 50 rows) ──────────────────────────────────")
        print(f"  Grain values seen:  {sorted(sample_grains)[:15]}")
        print(f"  SD values seen:     {sorted(sample_sds)[:10]}")
        print(f"  REF_DATE values:    {sorted(sample_refs)[:10]}")
        print("  ─────────────────────────────────────────────────────────────────")
 
    return result
 
 
def _current_crop_year():
    now = datetime.now()
    start = now.year if now.month >= 8 else now.year - 1
    return f"{start}-{start+1}"
 
 
def format_for_terminal(raw):
    now = datetime.now()
    formatted = {
        "report_date":        now.strftime("%B %d, %Y"),
        "current_crop_year":  _current_crop_year(),
        "current_quarter":    "Q4",
        "current_quarter_label": "Full year (Aug–Jul)",
        "crops":              {},
        "_meta":              {"fetched_at": now.strftime("%Y-%m-%dT%H:%M:%SZ")},
    }
 
    for comm in sorted(raw.keys()):
        years_data = raw[comm]
        sorted_years = sorted(years_data.keys())
 
        def make_rec(cy, d):
            return {
                "year":      cy,
                "area_s":    d.get("area_s"),
                "area_h":    d.get("area_h"),
                "yield":     round(d["yield"], 2) if d.get("yield") else None,
                "prod":      d.get("prod"),
                "imports":   d.get("imports"),
                "supply":    d.get("supply"),
                "exports":   d.get("exports"),
                "food":      d.get("food"),
                "feed":      d.get("feed"),
                "dom":       d.get("dom"),
                "stocks":    d.get("stocks"),
                "beg_stocks":d.get("beg_stocks"),
                "price":     None,
                "is_full_year": True,
            }
 
        rows = [make_rec(cy, years_data[cy]) for cy in sorted_years[-6:]]
 
        # Quarterly list: annual data only, each marked Q4 (full year)
        quarterly = []
        for cy in sorted(sorted_years[-3:], reverse=True):
            d = years_data[cy]
            quarterly.append({
                "crop_year":     cy,
                "quarter":       "Q4",
                "quarter_label": "Full year (Aug–Jul)",
                "ref_date":      cy,
                "prod":    d.get("prod"),
                "imports": d.get("imports"),
                "supply":  d.get("supply"),
                "exports": d.get("exports"),
                "food":    d.get("food"),
                "feed":    d.get("feed"),
                "dom":     d.get("dom"),
                "stocks":  d.get("stocks"),
                "area_s":  d.get("area_s"),
                "area_h":  d.get("area_h"),
                "yield":   round(d["yield"], 2) if d.get("yield") else None,
            })
 
        formatted["crops"][comm] = {
            "price_label": "",
            "rows":        rows,
            "quarterly":   quarterly,
            "full_year":   rows,
            "notes": [
                "StatsCan table 32-10-0013-01 — annual crop-year data (Aug 1–Jul 31).",
                "StatsCan does not publish mid-year estimates; current year shown as YTD until Q4 is released.",
            ],
        }
        print(f"  {comm}: {', '.join(r['year'] for r in rows)}")
 
    return formatted
 
 
def main():
    data = download_and_parse()
    if not data:
        print("ERROR: No StatsCan data parsed.", file=sys.stderr)
        print("See DIAGNOSTIC output above for actual column names/values.", file=sys.stderr)
        sys.exit(1)
 
    formatted = format_for_terminal(data)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(formatted, f, separators=(",", ":"), default=str)
 
    size = os.path.getsize(OUT)
    print(f"\nSaved {OUT} ({size:,} bytes) — {len(formatted['crops'])} commodities")
 
 
if __name__ == "__main__":
    main()
 
