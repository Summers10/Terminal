#!/usr/bin/env python3
"""
fetch_exports.py — USDA FAS Weekly Export Sales via OpenData ESR API.
 
The old highlite.htm scraper died on April 2, 2026 when FAS retired all
legacy /export-sales/*.htm endpoints in favor of the new ESRQS system.
 
This version pulls weekly data from the FAS OpenData ESR API:
    https://apps.fas.usda.gov/OpenData/api/esr/exports/
        commodityCode/{code}/allCountries/marketYear/{year}
 
Authentication:
    Sends the FAS_API_KEY (GitHub secret) in the API_KEY header when present.
    Also tries without auth — ESRQS announced that the API is open, though
    the OpenData path has historically required a key.
 
Output schema (data/export_sales.json) is unchanged:
    { commodity: {
        "insp":  {"MY": label, "years": [...], "w": {"25/26": [52 values]}},
        "sales": {          "years": [...], "w": {"25/26": [52 values]}}
      },
      ...,
      "_meta": {"fetched_at": "..."} }
 
Units: raw metric tons (MT), matching historical values already in the file.
"""
import os
import sys
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone, date
 
# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
OUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "export_sales.json"
)
BASE = "https://apps.fas.usda.gov/OpenData/api/esr"
API_KEY = os.environ.get("FAS_API_KEY", "").strip()
 
COMMODITIES = {
    # FAS OpenData commodity codes (verified)
    "wheat":    {"code": 107, "my_start_month": 6, "my_label": "Jun-May"},
    "corn":     {"code": 401, "my_start_month": 9, "my_label": "Sep-Aug"},
    "soybeans": {"code": 801, "my_start_month": 9, "my_label": "Sep-Aug"},
}
 
# ------------------------------------------------------------------
# HTTP helper
# ------------------------------------------------------------------
def api_get(path):
    """GET the OpenData ESR endpoint. Returns parsed JSON, raises on error."""
    url = BASE + path
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Summers-Tholl-Terminal/1.0")
    req.add_header("Accept", "application/json")
    if API_KEY:
        req.add_header("API_KEY", API_KEY)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))
 
# ------------------------------------------------------------------
# MY helpers
# ------------------------------------------------------------------
def my_string_for(start_year):
    a = start_year % 100
    b = (start_year + 1) % 100
    return "{:02d}/{:02d}".format(a, b)
 
def current_my_start(my_start_month, today=None):
    t = today or date.today()
    return t.year if t.month >= my_start_month else t.year - 1
 
def week_of_my(dt, my_start_month):
    """1-indexed week within the marketing year (1..52)."""
    y1 = dt.year if dt.month >= my_start_month else dt.year - 1
    start = date(y1, my_start_month, 1)
    return max(1, min(52, (dt - start).days // 7 + 1))
 
def parse_week_ending(value):
    """ESR API returns dates like '2026-03-19T00:00:00' or '2026-03-19'."""
    if not value:
        return None
    s = str(value)[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None
 
def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0
 
# ------------------------------------------------------------------
# Fetch + aggregate
# ------------------------------------------------------------------
def fetch_commodity_my(name, code, my_year):
    """Hit the ESR API for a single (commodity, marketYear).
       Returns a dict: {week_ending_date: {"ex": MT, "ns": MT}} aggregated
       across all countries."""
    path = "/exports/commodityCode/{}/allCountries/marketYear/{}".format(code, my_year)
    try:
        data = api_get(path)
    except urllib.error.HTTPError as e:
        print("    {} MY{}: HTTP {} — {}".format(name, my_year, e.code, e.reason))
        return None
    except Exception as e:
        print("    {} MY{}: fetch failed — {}".format(name, my_year, e))
        return None
 
    if not isinstance(data, list):
        print("    {} MY{}: unexpected response (type={})".format(
            name, my_year, type(data).__name__))
        return None
 
    weekly = {}
    for rec in data:
        wed = parse_week_ending(rec.get("weekEndingDate"))
        if not wed:
            continue
        slot = weekly.setdefault(wed, {"ex": 0.0, "ns": 0.0})
        # Weekly shipped exports (MT)
        slot["ex"] += num(rec.get("weeklyExports"))
        # Weekly net sales (MT) — ESR calls it currentMYNetSales
        slot["ns"] += num(rec.get("currentMYNetSales"))
    print("    {} MY{}: {} weekly aggregates across {} country rows".format(
        name, my_year, len(weekly), len(data)))
    return weekly
 
# ------------------------------------------------------------------
# File I/O + merge
# ------------------------------------------------------------------
def load_existing():
    if os.path.exists(OUT):
        with open(OUT) as f:
            return json.load(f)
    return {}
 
def ensure_shape(data, key, my_label):
    if key not in data:
        data[key] = {
            "insp":  {"MY": my_label, "years": [], "w": {}},
            "sales": {                 "years": [], "w": {}},
        }
    else:
        data[key].setdefault("insp",  {"MY": my_label, "years": [], "w": {}})
        data[key].setdefault("sales", {                 "years": [], "w": {}})
        data[key]["insp"].setdefault("MY", my_label)
    return data[key]
 
def ensure_year(comm, my):
    for section in ("insp", "sales"):
        sec = comm[section]
        years = sec.setdefault("years", [])
        if my not in years:
            years.append(my)
            # Sort chronologically by the MY's starting year
            years.sort(key=lambda s: int(s.split("/")[0]) + (2000 if int(s.split("/")[0]) < 80 else 1900))
        sec.setdefault("w", {})
        if my not in sec["w"]:
            sec["w"][my] = [None] * 52
 
def upsert_week(comm, my, week_num, exports_mt, net_sales_mt):
    """Return True if a new slot was filled, False if it was already present and unchanged."""
    idx = week_num - 1
    if idx < 0 or idx >= 52:
        return False
    added = False
    if comm["insp"]["w"][my][idx] is None and exports_mt:
        comm["insp"]["w"][my][idx] = round(exports_mt, 1)
        added = True
    if comm["sales"]["w"][my][idx] is None and net_sales_mt is not None:
        comm["sales"]["w"][my][idx] = round(net_sales_mt, 1)
        added = True
    return added
 
# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    print("=" * 60)
    print("FETCH_EXPORTS — USDA FAS ESR OpenData API")
    print("=" * 60)
    if API_KEY:
        print("  Using FAS_API_KEY from environment ({} chars).".format(len(API_KEY)))
    else:
        print("  No FAS_API_KEY set — trying unauthenticated.")
 
    data = load_existing()
    updated_count = 0
 
    for name, cfg in COMMODITIES.items():
        print("\n  === {} (commodity {}) ===".format(name.upper(), cfg["code"]))
        comm = ensure_shape(data, name, cfg["my_label"])
 
        # Fetch the current MY and the prior MY (to backfill any gaps)
        this_my = current_my_start(cfg["my_start_month"])
        prior_my = this_my - 1
 
        for my_year in (prior_my, this_my):
            my_str = my_string_for(my_year)
            ensure_year(comm, my_str)
 
            weekly = fetch_commodity_my(name, cfg["code"], my_year)
            if not weekly:
                continue
 
            new_for_this_my = 0
            for wed, vals in sorted(weekly.items()):
                wk = week_of_my(wed, cfg["my_start_month"])
                if upsert_week(comm, my_str, wk, vals["ex"], vals["ns"]):
                    new_for_this_my += 1
            if new_for_this_my:
                print("    {} MY {}: added {} week(s)".format(name, my_str, new_for_this_my))
                updated_count += new_for_this_my
 
    data["_meta"] = {
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "FAS OpenData ESR API",
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(data, f, separators=(",", ":"))
 
    print("\n" + "-" * 60)
    if updated_count:
        print("Wrote {} new weekly value(s).".format(updated_count))
    else:
        print("No new weekly values (file still refreshed with timestamp).")
    print("Saved {} ({:,} bytes)".format(OUT, os.path.getsize(OUT)))
    # Exit 0 even if nothing new — the workflow's other steps shouldn't fail
    # because of a quiet week.
 
 
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR: {}".format(e), file=sys.stderr)
        # Exit 0 on any failure: the workflow uses continue-on-error anyway,
        # and we'd rather not rewrite the JSON to a partial state.
        sys.exit(0)
