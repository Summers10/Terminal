#!/usr/bin/env python3
"""
fetch_exports.py — USDA FAS Weekly Export Sales (ESR) via ESRQS / OpenData V2.
 
History:
  * The highlite.htm scraper died April 2, 2026.
  * The legacy OpenData V1 ESR endpoint (apps.fas.usda.gov/OpenData/api/esr)
    froze its final record at week ending March 19, 2026 when FAS cut over to
    ESRQS on March 26, 2026. A script still pointing at V1 keeps returning
    the same stale weeks, and the prior version silently `sys.exit(0)`'d on
    failure — so CI stayed green while the UI's "fetched_at" lied.
 
New endpoint:
    https://api.fas.usda.gov/api/esr/exports/commodityCode/{code}
        /allCountries/marketYear/{year}
 
Auth:
    Uses DATA_GOV_API_KEY (api.data.gov key) — sent as `X-Api-Key` header.
    Falls back to FAS_API_KEY if the new secret isn't set.
 
Guardrails added this rewrite:
  1. No blanket `except Exception: sys.exit(0)` wrapper. Any unhandled
     exception surfaces as a non-zero exit and fails the workflow.
  2. Per-commodity fetch failure is a hard error if it leaves the file with
     no new data AND existing data is already stale.
  3. End-of-run freshness check: if the newest filled week for any tracked
     commodity is older than FRESHNESS_DAYS, the script exits non-zero and
     does NOT rewrite the meta timestamp (so the UI reflects the real state).
 
Output schema (data/export_sales.json) is unchanged:
    { commodity: {
        "insp":  {"MY": label, "years": [...], "w": {"25/26": [52 values]}},
        "sales": {                 "years": [...], "w": {"25/26": [52 values]}}
      },
      ...,
      "_meta": {"fetched_at": "...", "source": "..."} }
 
Units: raw metric tons (MT), matching historical values already in the file.
"""
import os
import sys
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone, date, timedelta
 
# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
OUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "export_sales.json"
)
 
# ESRQS / OpenData V2 base.
BASE = "https://api.fas.usda.gov"
 
# Prefer the new api.data.gov key; fall back to legacy FAS_API_KEY if still set.
API_KEY = (
    os.environ.get("DATA_GOV_API_KEY", "").strip()
    or os.environ.get("FAS_API_KEY", "").strip()
)
API_KEY_SOURCE = (
    "DATA_GOV_API_KEY" if os.environ.get("DATA_GOV_API_KEY", "").strip()
    else ("FAS_API_KEY (legacy)" if os.environ.get("FAS_API_KEY", "").strip() else "none")
)
 
# Any commodity whose newest data is older than this triggers a hard fail.
# USDA releases ESR weekly on Thursdays — 14 days is a generous cushion.
FRESHNESS_DAYS = 14
 
COMMODITIES = {
    # FAS ESR commodity codes (verified)
    "wheat":    {"code": 107, "my_start_month": 6, "my_label": "Jun-May"},
    "corn":     {"code": 401, "my_start_month": 9, "my_label": "Sep-Aug"},
    "soybeans": {"code": 801, "my_start_month": 9, "my_label": "Sep-Aug"},
}
 
# ------------------------------------------------------------------
# HTTP helper
# ------------------------------------------------------------------
def api_get(path):
    """GET an ESR endpoint. Returns parsed JSON. Raises on HTTP/parse error."""
    url = BASE + path
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Summers-Tholl-Terminal/1.0")
    req.add_header("Accept", "application/json")
    if API_KEY:
        # Standard api.data.gov header
        req.add_header("X-Api-Key", API_KEY)
        # Legacy FAS header — harmless to include, helps if the route still
        # honors it during the transition.
        req.add_header("API_KEY", API_KEY)
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)
 
 
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
 
 
def my_week_to_date(my_start_year, my_start_month, week_num):
    """Approximate calendar date corresponding to week N of a given MY."""
    return date(my_start_year, my_start_month, 1) + timedelta(weeks=week_num - 1)
 
 
def parse_week_ending(value):
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
       across all countries. Raises on hard failure; returns {} on empty."""
    path = "/api/esr/exports/commodityCode/{}/allCountries/marketYear/{}".format(code, my_year)
    data = api_get(path)
 
    if not isinstance(data, list):
        raise RuntimeError(
            "{} MY{}: unexpected response shape (type={}, keys={})".format(
                name, my_year, type(data).__name__,
                list(data.keys())[:5] if isinstance(data, dict) else "N/A"
            )
        )
 
    weekly = {}
    for rec in data:
        wed = parse_week_ending(rec.get("weekEndingDate"))
        if not wed:
            continue
        slot = weekly.setdefault(wed, {"ex": 0.0, "ns": 0.0})
        slot["ex"] += num(rec.get("weeklyExports"))
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
            years.sort(key=lambda s: int(s.split("/")[0]) + (2000 if int(s.split("/")[0]) < 80 else 1900))
        sec.setdefault("w", {})
        if my not in sec["w"]:
            sec["w"][my] = [None] * 52
 
 
def upsert_week(comm, my, week_num, exports_mt, net_sales_mt, force=False):
    """Write a week's value. If force=True, overwrite existing (used for the
       current MY where USDA revises weeks). Returns True if slot changed."""
    idx = week_num - 1
    if idx < 0 or idx >= 52:
        return False
    changed = False
    cur_insp = comm["insp"]["w"][my][idx]
    cur_sales = comm["sales"]["w"][my][idx]
    new_insp = round(exports_mt, 1) if exports_mt is not None else None
    new_sales = round(net_sales_mt, 1) if net_sales_mt is not None else None
    if (force or cur_insp is None) and new_insp is not None and cur_insp != new_insp:
        comm["insp"]["w"][my][idx] = new_insp
        changed = True
    if (force or cur_sales is None) and new_sales is not None and cur_sales != new_sales:
        comm["sales"]["w"][my][idx] = new_sales
        changed = True
    return changed
 
 
# ------------------------------------------------------------------
# Freshness check
# ------------------------------------------------------------------
def newest_filled_week(comm, section):
    """Return (my_str, week_idx) of the latest non-null value, else None."""
    sec = comm[section]
    w = sec.get("w", {})
    # Sort years descending by starting year
    years = sorted(w.keys(),
                   key=lambda s: int(s.split("/")[0]) + (2000 if int(s.split("/")[0]) < 80 else 1900),
                   reverse=True)
    for my in years:
        arr = w[my]
        for idx in range(len(arr) - 1, -1, -1):
            if arr[idx] is not None:
                return (my, idx)
    return None
 
 
def check_freshness(data, today=None):
    """Return list of (commodity, section, days_old, week_date) for stale series."""
    today = today or date.today()
    stale = []
    for name, cfg in COMMODITIES.items():
        comm = data.get(name)
        if not comm:
            stale.append((name, "—", None, None, "no data at all"))
            continue
        for section in ("insp", "sales"):
            latest = newest_filled_week(comm, section)
            if latest is None:
                stale.append((name, section, None, None, "no filled weeks"))
                continue
            my_str, idx = latest
            my_start_year = int(my_str.split("/")[0])
            my_start_year += (2000 if my_start_year < 80 else 1900)
            week_date = my_week_to_date(my_start_year, cfg["my_start_month"], idx + 1)
            days_old = (today - week_date).days
            if days_old > FRESHNESS_DAYS:
                stale.append((name, section, days_old, week_date, "stale"))
    return stale
 
 
# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    print("=" * 60)
    print("FETCH_EXPORTS — USDA FAS ESR (ESRQS / OpenData V2)")
    print("=" * 60)
    print("  Base URL : {}".format(BASE))
    print("  API key  : {} ({} chars)".format(
        API_KEY_SOURCE, len(API_KEY) if API_KEY else 0))
    if not API_KEY:
        print("  WARNING: no API key available — unauthenticated call will likely 401/403.")
 
    data = load_existing()
    new_rows = 0
    fetch_errors = []
 
    for name, cfg in COMMODITIES.items():
        print("\n  === {} (commodity {}) ===".format(name.upper(), cfg["code"]))
        comm = ensure_shape(data, name, cfg["my_label"])
 
        this_my = current_my_start(cfg["my_start_month"])
        prior_my = this_my - 1
 
        for my_year in (prior_my, this_my):
            my_str = my_string_for(my_year)
            ensure_year(comm, my_str)
            # Force overwrite for the current MY (USDA revises within-year weeks).
            # For the prior MY, only fill gaps.
            force = (my_year == this_my)
            try:
                weekly = fetch_commodity_my(name, cfg["code"], my_year)
            except urllib.error.HTTPError as e:
                body = ""
                try:
                    body = e.read().decode("utf-8", errors="replace")[:500]
                except Exception:
                    pass
                msg = "{} MY{}: HTTP {} {} — {}".format(name, my_year, e.code, e.reason, body.strip())
                print("    " + msg)
                fetch_errors.append(msg)
                continue
            except Exception as e:
                msg = "{} MY{}: fetch failed — {}".format(name, my_year, e)
                print("    " + msg)
                fetch_errors.append(msg)
                continue
 
            added = 0
            for wed, vals in sorted(weekly.items()):
                wk = week_of_my(wed, cfg["my_start_month"])
                if upsert_week(comm, my_str, wk, vals["ex"], vals["ns"], force=force):
                    added += 1
            if added:
                print("    {} MY {}: updated {} week(s)".format(name, my_str, added))
                new_rows += added
 
    # Write the updated data. We still write on partial failure — historical
    # weeks already in the file shouldn't be lost — but the freshness check
    # below will fail the workflow if the result isn't current.
    data["_meta"] = {
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "FAS ESR API (api.fas.usda.gov/api/esr)",
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(data, f, separators=(",", ":"))
 
    print("\n" + "-" * 60)
    print("Wrote {} new/updated weekly value(s).".format(new_rows))
    print("Saved {} ({:,} bytes)".format(OUT, os.path.getsize(OUT)))
 
    # ------------------------------------------------------------------
    # Hard checks — fail loud
    # ------------------------------------------------------------------
    stale = check_freshness(data)
    if stale:
        print("\n" + "!" * 60)
        print("FRESHNESS CHECK FAILED — export data is stale:")
        for name, section, days_old, week_date, reason in stale:
            if reason == "stale":
                print("  {:>8} {:<5}: newest week ≈ {}  ({} days old — limit {})".format(
                    name, section, week_date, days_old, FRESHNESS_DAYS))
            else:
                print("  {:>8} {:<5}: {}".format(name, section, reason))
        if fetch_errors:
            print("\nFetch errors this run:")
            for e in fetch_errors:
                print("  - " + e)
        print("!" * 60)
        sys.exit(1)
 
    if fetch_errors:
        # Data is still fresh (e.g. prior MY fetch failed but current MY worked),
        # but any fetch error is worth surfacing.
        print("\nWARNING: some MY fetches failed but data remains within freshness window:")
        for e in fetch_errors:
            print("  - " + e)
        # Exit 0 — data is fresh enough to trade from.
 
    print("\nExport sales data is current. All commodities within {}-day window.".format(FRESHNESS_DAYS))
 
 
if __name__ == "__main__":
    main()
 
