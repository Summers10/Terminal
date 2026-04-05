#!/usr/bin/env python3
"""Fetch Statistics Canada supply and disposition data (table 32-10-0013-01).
 
CONFIRMED CSV STRUCTURE (from GH Actions log 2025-04-05):
  Grain column : 'Type of crop'
  SD column    : 'Supply and disposition of grains'
  REF_DATE     : YYYY-MM  (monthly/quarterly, e.g. '1996-12')
  VALUE        : 'VALUE'
 
Canadian crop year: Aug 1 – Jul 31
Quarter mapping (month → quarter within crop year):
  Aug(8)  Sep(9)  Oct(10) → Q1  Aug–Oct
  Nov(11) Dec(12) Jan(1)  → Q2  Nov–Jan
  Feb(2)  Mar(3)  Apr(4)  → Q3  Feb–Apr
  May(5)  Jun(6)  Jul(7)  → Q4  May–Jul  (= full-year cumulative)
 
Quarterly output allows comparison of:
  • Current quarter (e.g. Q2 2024-25)
  • Prev quarter    (e.g. Q1 2024-25)  → QoQ
  • Same Q prev yr  (e.g. Q2 2023-24)  → YoY
  • Same Q 2yrs ago (e.g. Q2 2022-23)  → YoY-2
"""
import os, sys, json, urllib.request, csv, io, zipfile
from datetime import datetime
 
OUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "statscan_sd.json"
)
URL = "https://www150.statcan.gc.ca/n1/tbl/csv/32100013-eng.zip"
 
# ── Commodity name mapping (lower-case key → display name) ───────────────────
# Keys must match exactly what StatsCan puts in 'Type of crop', lowercased.
# Add variants if StatsCan uses different phrasing.
COMMODITIES = {
    "wheat, excluding durum":        "Wheat (ex Durum)",
    "wheat (excluding durum)":       "Wheat (ex Durum)",
    "wheat excluding durum":         "Wheat (ex Durum)",
    "spring wheat":                  "Wheat (ex Durum)",
    "durum wheat":                   "Durum",
    "barley":                        "Barley",
    "corn for grain":                "Corn",
    "grain corn":                    "Corn",
    "corn":                          "Corn",
    "oats":                          "Oats",
    "canola (rapeseed)":             "Canola",
    "canola":                        "Canola",
    "rapeseed":                      "Canola",
    "soybeans":                      "Soybeans",
    "soybean":                       "Soybeans",
    "dry peas":                      "Dry Peas",
    "field peas":                    "Dry Peas",
    "lentils":                       "Lentils",
    "flaxseed":                      "Flaxseed",
    "flax":                          "Flaxseed",
    "rye":                           "Rye",
    "mixed grains":                  "Mixed Grains",
}
 
# ── SD item mapping (CONFIRMED actual values, lowercased → field key) ─────────
SD_MAP = {
    "production":                              "prod",
    "imports":                                 "imports",
    "total supply":                            "supply",
    # Exports — two types; we sum them into 'exports'
    "grain exports":                           "exports_grain",
    "product exports":                         "exports_prod",
    "total exports":                           "exports",
    "exports":                                 "exports",
    # Domestic use breakdown
    "human food":                              "food",
    "food":                                    "food",
    "industrial use":                          "industrial",
    "animal feed, waste and dockage":          "feed",
    "feed, waste and dockage":                 "feed",
    "animal feed":                             "feed",
    "feed":                                    "feed",
    "seed requirements":                       "seed",
    "seed use":                                "seed",
    "loss in handling":                        "loss",
    "total disposition":                       "dom",
    "total domestic use":                      "dom",
    "domestic use":                            "dom",
    # Stocks — two components; we sum into 'stocks'
    "ending stocks in commercial positions":   "stocks_comm",
    "ending stocks on farms":                  "stocks_farm",
    "total ending stocks":                     "stocks",
    "ending stocks, total":                    "stocks",
    "carry-over stocks, total":                "stocks",
    # Beginning stocks — for supply cross-check
    "beginning stocks in commercial positions":"beg_comm",
    "beginning stocks on farms":               "beg_farm",
    "total beginning stocks":                  "beg_stocks",
}
 
# Month → (quarter_code, label) within the Aug-Jul crop year
MONTH_Q = {
    8: ("Q1","Aug–Oct"), 9: ("Q1","Aug–Oct"), 10: ("Q1","Aug–Oct"),
    11: ("Q2","Nov–Jan"), 12: ("Q2","Nov–Jan"), 1: ("Q2","Nov–Jan"),
    2: ("Q3","Feb–Apr"), 3: ("Q3","Feb–Apr"), 4: ("Q3","Feb–Apr"),
    5: ("Q4","May–Jul"), 6: ("Q4","May–Jul"), 7: ("Q4","May–Jul"),
}
Q_ORDER = {"Q1":1,"Q2":2,"Q3":3,"Q4":4}
 
 
def parse_ref_date(s):
    """Return (crop_year_str, quarter_code, q_label, cal_year, cal_month) or None×5."""
    s = str(s).strip()
    # YYYY-MM
    try:
        d = datetime.strptime(s[:7], "%Y-%m")
        m, y = d.month, d.year
        q, ql = MONTH_Q.get(m, ("Q1","Aug–Oct"))
        start = y if m >= 8 else y - 1
        cy = f"{start}-{start+1}"
        return cy, q, ql, y, m
    except (ValueError, TypeError):
        pass
    # YYYY-MM-DD
    try:
        d = datetime.strptime(s[:10], "%Y-%m-%d")
        m, y = d.month, d.year
        q, ql = MONTH_Q.get(m, ("Q1","Aug–Oct"))
        start = y if m >= 8 else y - 1
        cy = f"{start}-{start+1}"
        return cy, q, ql, y, m
    except (ValueError, TypeError):
        pass
    return None, None, None, None, None
 
 
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
 
    # Confirmed column names
    COL_GRAIN = "Type of crop"
    COL_SD    = "Supply and disposition of grains"
    COL_REF   = "REF_DATE"
    COL_VAL   = "VALUE"
 
    # result[comm][crop_year][quarter] = {field: value, _month: int}
    # We keep the LATEST month within each quarter (highest month number
    # within the quarter = most complete YTD snapshot for that quarter)
    result = {}
    count = 0
    unknown_crops = set()
    unknown_sds   = set()
 
    with zf.open(csv_name) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
 
        # Verify columns present
        headers = list(reader.fieldnames or [])
        missing = [c for c in [COL_GRAIN, COL_SD, COL_REF, COL_VAL] if c not in headers]
        if missing:
            print(f"  WARNING: expected columns not found: {missing}")
            print(f"  Available: {headers}")
 
        for row in reader:
            grain_raw = (row.get(COL_GRAIN) or "").strip()
            sd_raw    = (row.get(COL_SD)    or "").strip()
            ref_raw   = (row.get(COL_REF)   or "").strip()
            val_raw   = (row.get(COL_VAL)   or "").strip().replace(",","")
 
            # Commodity match (case-insensitive exact)
            comm = COMMODITIES.get(grain_raw.lower())
            if not comm:
                if grain_raw:
                    unknown_crops.add(grain_raw)
                continue
 
            # SD item match (substring, longest match first to avoid 'exports' swallowing 'grain exports')
            item_key = None
            sd_l = sd_raw.lower()
            # Try longest key first for specificity
            for k in sorted(SD_MAP.keys(), key=len, reverse=True):
                if k in sd_l:
                    item_key = SD_MAP[k]
                    break
            if not item_key:
                if sd_raw:
                    unknown_sds.add(sd_raw)
                continue
 
            cy, quarter, q_label, cal_yr, cal_m = parse_ref_date(ref_raw)
            if cy is None:
                continue
            # Only keep 2018+ crop years
            try:
                if int(cy.split("-")[0]) < 2018:
                    continue
            except (ValueError, IndexError):
                continue
 
            try:
                value = float(val_raw)
            except (ValueError, TypeError):
                continue
 
            bucket = result.setdefault(comm, {}).setdefault(cy, {}).setdefault(quarter, {
                "_q_label": q_label,
                "_months_seen": [],
            })
            # Keep data from the latest month within this quarter
            seen = bucket["_months_seen"]
            if not seen or cal_m >= max(seen):
                bucket[item_key] = value
                if cal_m not in seen:
                    seen.append(cal_m)
            count += 1
 
    print(f"  Parsed {count} data points across {len(result)} commodities")
 
    if unknown_crops:
        print(f"  Unmatched crop names ({len(unknown_crops)}): {sorted(unknown_crops)[:10]}")
    if unknown_sds:
        print(f"  Unmatched SD items  ({len(unknown_sds)}): {sorted(unknown_sds)[:10]}")
 
    return result
 
 
def derive_fields(d):
    """Compute derived fields: combined exports, combined stocks, supply, dom."""
    out = dict(d)
    # Total exports = grain exports + product exports (if no total given)
    if out.get("exports") is None:
        eg = out.get("exports_grain") or 0
        ep = out.get("exports_prod")  or 0
        if eg or ep:
            out["exports"] = eg + ep
    # Total ending stocks = commercial + farm (if no total given)
    if out.get("stocks") is None:
        sc = out.get("stocks_comm") or 0
        sf = out.get("stocks_farm") or 0
        if sc or sf:
            out["stocks"] = sc + sf
    # Beginning stocks
    if out.get("beg_stocks") is None:
        bc = out.get("beg_comm") or 0
        bf = out.get("beg_farm") or 0
        if bc or bf:
            out["beg_stocks"] = bc + bf
    # Total supply (if not provided)
    if out.get("supply") is None:
        bs = out.get("beg_stocks") or 0
        pr = out.get("prod")    or 0
        im = out.get("imports") or 0
        if pr:
            out["supply"] = bs + pr + im
    # Food+Industrial → food field
    if out.get("food") is None and out.get("industrial") is not None:
        out["food"] = out["industrial"]
    elif out.get("food") is not None and out.get("industrial") is not None:
        out["food"] = (out["food"] or 0) + (out["industrial"] or 0)
    return out
 
 
def make_sd_rec(d):
    d2 = derive_fields(d)
    return {
        "prod":      d2.get("prod"),
        "imports":   d2.get("imports"),
        "supply":    d2.get("supply"),
        "exports":   d2.get("exports"),
        "food":      d2.get("food"),
        "feed":      d2.get("feed"),
        "dom":       d2.get("dom"),
        "stocks":    d2.get("stocks"),
        "beg_stocks":d2.get("beg_stocks"),
        "area_s":    d2.get("area_s"),
        "area_h":    d2.get("area_h"),
        "yield":     d2.get("yield"),
        "price":     None,
    }
 
 
def _current_crop_year():
    now = datetime.now()
    start = now.year if now.month >= 8 else now.year - 1
    return f"{start}-{start+1}"
 
 
def _current_quarter():
    now = datetime.now()
    return MONTH_Q.get(now.month, ("Q1","Aug–Oct"))
 
 
def format_for_terminal(raw):
    now = datetime.now()
    cur_cy = _current_crop_year()
    cur_q, cur_ql = _current_quarter()
 
    out = {
        "report_date":           now.strftime("%B %d, %Y"),
        "current_crop_year":     cur_cy,
        "current_quarter":       cur_q,
        "current_quarter_label": cur_ql,
        "crops": {},
        "_meta": {"fetched_at": now.strftime("%Y-%m-%dT%H:%M:%SZ")},
    }
 
    for comm in sorted(raw.keys()):
        comm_data = raw[comm]
        sorted_years = sorted(comm_data.keys())
 
        # ── Annual rows: best quarter per year (Q4 = full year; else latest) ─
        rows = []
        for cy in sorted_years[-6:]:
            qdata = comm_data[cy]
            best_q = next((q for q in ["Q4","Q3","Q2","Q1"] if q in qdata), None)
            if not best_q:
                continue
            rec = make_sd_rec(qdata[best_q])
            rec["year"]         = cy
            rec["quarter"]      = best_q
            rec["is_full_year"] = (best_q == "Q4")
            rows.append(rec)
 
        # ── Quarterly: all (cy, quarter) pairs for last 3 crop years ─────────
        # newest first; within a crop year, highest quarter first
        quarterly = []
        for cy in sorted(sorted_years[-3:], reverse=True):
            qdata = comm_data[cy]
            for q in sorted(qdata.keys(), key=lambda x: Q_ORDER.get(x,0), reverse=True):
                d = qdata[q]
                rec = make_sd_rec(d)
                rec["crop_year"]     = cy
                rec["quarter"]       = q
                rec["quarter_label"] = d.get("_q_label", q)
                rec["ref_date"]      = cy + " " + q
                quarterly.append(rec)
 
        out["crops"][comm] = {
            "price_label": "",
            "rows":        rows,
            "quarterly":   quarterly,
            "full_year":   [r for r in rows if r.get("is_full_year")],
            "notes": [
                "StatsCan table 32-10-0013-01 — cumulative YTD quarterly data (Aug 1–Jul 31).",
                "Q4 (May–Jul) represents the full crop year actual.",
                "StatsCan does not publish full-year estimates mid-year.",
            ],
        }
 
        yr_qtrs = [(r["year"], r["quarter"]) for r in rows]
        print(f"  {comm}: {', '.join(y+' '+q for y,q in yr_qtrs)}")
 
    return out
 
 
def main():
    data = download_and_parse()
    if not data:
        print("ERROR: No StatsCan data parsed.", file=sys.stderr)
        print("Check diagnostic output above.", file=sys.stderr)
        sys.exit(1)
 
    formatted = format_for_terminal(data)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(formatted, f, separators=(",", ":"), default=str)
 
    size = os.path.getsize(OUT)
    print(f"\nSaved {OUT} ({size:,} bytes) — {len(formatted['crops'])} commodities")
 
 
if __name__ == "__main__":
    main()
 
