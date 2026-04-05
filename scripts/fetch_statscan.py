#!/usr/bin/env python3
"""Fetch Statistics Canada supply and disposition data (table 32-10-0013-01).
Quarterly table — REF_DATE is quarterly (YYYY-MM-DD or YYYY-MM).
Stores:
  • rows        — full-year actuals per crop year (Q4 = full year; earlier Qs if Q4 not yet available)
  • quarterly   — per-quarter YTD snapshots for the last 3 crop years (for comparison view)
  • current_*   — metadata about what quarter/year is current
 
Canadian crop year: Aug 1 – Jul 31
  Q1 = Aug–Oct  (ref month 8)
  Q2 = Nov–Jan  (ref month 11)
  Q3 = Feb–Apr  (ref month 2)
  Q4 = May–Jul  (ref month 5) → full-year actual when available
"""
import os, sys, json, urllib.request, csv, io, zipfile
from datetime import datetime
 
OUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "statscan_sd.json"
)
URL = "https://www150.statcan.gc.ca/n1/tbl/csv/32100013-eng.zip"
 
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
 
SD_MAP = {
    "Production": "prod",
    "Imports": "imports",
    "Total supply": "supply",
    "Exports, total, grains and products (wheat equivalent)": "exports",
    "Total exports": "exports",
    "Exports": "exports",
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
 
# Maps the reference month to (quarter_code, human_label) within the Aug-Jul crop year
MONTH_TO_Q = {
    8:  ("Q1", "Aug–Oct"),  9:  ("Q1", "Aug–Oct"),  10: ("Q1", "Aug–Oct"),
    11: ("Q2", "Nov–Jan"),  12: ("Q2", "Nov–Jan"),   1:  ("Q2", "Nov–Jan"),
    2:  ("Q3", "Feb–Apr"),  3:  ("Q3", "Feb–Apr"),   4:  ("Q3", "Feb–Apr"),
    5:  ("Q4", "May–Jul"),  6:  ("Q4", "May–Jul"),   7:  ("Q4", "May–Jul"),
}
Q_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
 
def parse_ref_date(s):
    """
    Parse a REF_DATE string into (crop_year, quarter, q_label, ref_date_iso).
    Handles:
      YYYY-MM-DD  — quarterly date (StatsCan standard)
      YYYY-MM     — year-month
      YYYY-YYYY   — annual crop year string (treated as Q4 = full year)
    Returns None tuple on failure.
    """
    s = s.strip()
    # ── Try YYYY-MM-DD or YYYY-MM ────────────────────────────────────────────
    for fmt, slen in [("%Y-%m-%d", 10), ("%Y-%m", 7)]:
        try:
            d = datetime.strptime(s[:slen], fmt)
            m, y = d.month, d.year
            quarter, q_label = MONTH_TO_Q.get(m, ("Q1", "Aug–Oct"))
            crop_start = y if m >= 8 else y - 1
            cy = f"{crop_start}-{crop_start + 1}"
            return cy, quarter, q_label, d.strftime("%Y-%m-%d")
        except ValueError:
            pass
 
    # ── Try YYYY-YYYY annual crop year ────────────────────────────────────────
    parts = s.split("-")
    if len(parts) == 2:
        try:
            y1, y2 = int(parts[0]), int(parts[1])
            if 2000 <= y1 <= 2040 and y2 == y1 + 1:
                return f"{y1}-{y2}", "Q4", "May–Jul", f"{y1}-05-01"
        except ValueError:
            pass
 
    return None, None, None, None
 
 
def download_and_parse():
    print("Downloading StatsCan table 32-10-0013-01 (quarterly)…")
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
    print(f"  Parsing {csv_name}…")
 
    # Structure:  raw[comm][crop_year][quarter] = {item_key: value, …, '_q_label': …}
    raw = {}
    count = 0
    fmt_sample = None
 
    with zf.open(csv_name) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            grain = row.get("Type of grain", row.get("Grains", "")).strip()
            comm = next(
                (v for k, v in COMMODITIES.items() if k.lower() == grain.lower()), None
            )
            if not comm:
                continue
 
            sd_item = row.get("Supply and disposition",
                               row.get("Supply and Disposition", "")).strip()
            item_key = next(
                (v for k, v in SD_MAP.items()
                 if k.lower() in sd_item.lower() or sd_item.lower() in k.lower()), None
            )
            if not item_key:
                continue
 
            ref_str = row.get("REF_DATE", "").strip()
            cy, quarter, q_label, ref_iso = parse_ref_date(ref_str)
            if not cy:
                continue
 
            # Log the format we're seeing
            if fmt_sample is None:
                fmt_sample = ref_str
                print(f"  REF_DATE format sample: '{ref_str}' → crop_year={cy}, {quarter}")
 
            # Only keep 2015+ crop years to keep file small
            try:
                if int(cy.split("-")[0]) < 2015:
                    continue
            except (ValueError, IndexError):
                continue
 
            val_str = row.get("VALUE", "").strip().replace(",", "")
            if not val_str:
                continue
            try:
                value = float(val_str)
            except ValueError:
                continue
 
            bucket = raw.setdefault(comm, {}).setdefault(cy, {}).setdefault(
                quarter, {"_q_label": q_label, "_ref_date": ref_iso}
            )
            bucket[item_key] = value
            count += 1
 
    print(f"  Parsed {count} data points across {len(raw)} commodities")
    return raw
 
 
def make_sd_record(d):
    """Extract standard S&D fields from a quarter bucket dict."""
    return {
        "prod":     d.get("prod"),
        "imports":  d.get("imports"),
        "supply":   d.get("supply"),
        "exports":  d.get("exports"),
        "food":     d.get("food"),
        "feed":     d.get("feed"),
        "dom":      d.get("dom"),
        "stocks":   d.get("stocks"),
        "beg_stocks": d.get("beg_stocks"),
        "area_s":   d.get("area_s"),
        "area_h":   d.get("area_h"),
        "yield":    round(d["yield"], 2) if d.get("yield") else None,
        "price":    None,
    }
 
 
def format_for_terminal(raw):
    now = datetime.now()
    m, y = now.month, now.year
    curr_q, curr_q_label = MONTH_TO_Q.get(m, ("Q1", "Aug–Oct"))
    crop_start = y if m >= 8 else y - 1
    curr_crop_year = f"{crop_start}-{crop_start + 1}"
 
    out = {
        "report_date": now.strftime("%B %d, %Y"),
        "current_crop_year": curr_crop_year,
        "current_quarter": curr_q,
        "current_quarter_label": curr_q_label,
        "crops": {},
        "_meta": {"fetched_at": now.strftime("%Y-%m-%dT%H:%M:%SZ")},
    }
 
    for comm in sorted(raw.keys()):
        comm_data = raw[comm]
        sorted_years = sorted(comm_data.keys())   # oldest → newest
 
        # ── Annual rows ────────────────────────────────────────────────────────
        # Per crop year: use the highest-available quarter (Q4 = full year).
        rows = []
        for cy in sorted_years[-6:]:
            qdata = comm_data[cy]
            best_q = next((q for q in ["Q4", "Q3", "Q2", "Q1"] if q in qdata), None)
            if not best_q:
                continue
            d = qdata[best_q]
            rec = make_sd_record(d)
            rec["year"] = cy
            rec["quarter"] = best_q
            rec["is_full_year"] = (best_q == "Q4")
            rows.append(rec)
 
        # ── Quarterly snapshots ────────────────────────────────────────────────
        # Flat list of every quarter for the last 3 crop years, newest-first.
        # The terminal JS picks the relevant ones for comparison.
        quarterly = []
        for cy in sorted_years[-3:]:
            qdata = comm_data[cy]
            for q in ["Q4", "Q3", "Q2", "Q1"]:
                if q not in qdata:
                    continue
                d = qdata[q]
                rec = make_sd_record(d)
                rec["crop_year"] = cy
                rec["quarter"] = q
                rec["quarter_label"] = d.get("_q_label", q)
                rec["ref_date"] = d.get("_ref_date", "")
                quarterly.append(rec)
 
        # Sort newest first: by crop_year desc, then quarter desc
        quarterly.sort(
            key=lambda x: (x["crop_year"], Q_ORDER.get(x["quarter"], 0)),
            reverse=True
        )
 
        # ── Full-year snapshots ────────────────────────────────────────────────
        # For each completed crop year where Q4 exists — these are the full-year actuals.
        full_year = []
        for cy in sorted_years:
            qdata = comm_data[cy]
            if "Q4" in qdata:
                d = qdata["Q4"]
                rec = make_sd_record(d)
                rec["year"] = cy
                full_year.append(rec)
 
        out["crops"][comm] = {
            "price_label": "",
            "rows": rows,
            "quarterly": quarterly,
            "full_year": full_year,
            "notes": [],
        }
 
        years_avail = [r["year"] for r in rows]
        qtrs_avail  = [f"{r['crop_year']} {r['quarter']}" for r in quarterly[:4]]
        print(f"  {comm}: annual={', '.join(years_avail)} | quarterly={', '.join(qtrs_avail)}")
 
    return out
 
 
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
    print(f"\nSaved {OUT} ({size:,} bytes)")
    print(f"Crops: {', '.join(sorted(formatted['crops'].keys()))}")
    print(f"Current: {formatted['current_crop_year']} {formatted['current_quarter']} ({formatted['current_quarter_label']})")
 
 
if __name__ == "__main__":
    main()
