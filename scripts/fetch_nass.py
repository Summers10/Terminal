#!/usr/bin/env python3
"""
Fetch USDA NASS QuickStats data and save as compact JSON.
Designed for GitHub Actions cron (daily at 4:30pm CT).
 
Required env var: NASS_API_KEY
Output: data/nass_reports.json
"""
 
import os, sys, json, urllib.request, urllib.parse
from datetime import datetime, timedelta
 
API_KEY = os.environ.get("NASS_API_KEY", "")
BASE = "https://quickstats.nass.usda.gov/api/api_GET/"
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "nass_reports.json")
 
CROPS = ["CORN", "SOYBEANS", "WHEAT"]
CURRENT_YEAR = datetime.now().year
# Fetch current year + 1 prior year for comparisons
YEARS = [CURRENT_YEAR - 1, CURRENT_YEAR]
# For 5yr avg, we need 5 prior years for stocks/acreage
HIST_YEARS = list(range(CURRENT_YEAR - 5, CURRENT_YEAR + 1))
 
 
def api_get(params):
    """Make a NASS QuickStats API call, return list of records."""
    params["key"] = API_KEY
    params["format"] = "JSON"
    url = BASE + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "SUMCO-Terminal/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            return data.get("data", [])
    except Exception as e:
        print(f"  API error: {e}", file=sys.stderr)
        return []
 
 
def fetch_conditions():
    """Crop conditions: % in each category by week, national level."""
    print("Fetching crop conditions...")
    out = {}
    for crop in CROPS:
        # For wheat, get both winter and spring
        if crop == "WHEAT":
            variants = [
                ("WINTER WHEAT", {"commodity_desc": "WHEAT", "prodn_practice_desc": "WINTER"}),
                ("SPRING WHEAT", {"commodity_desc": "WHEAT", "prodn_practice_desc": "SPRING, EXCL DURUM"}),
            ]
        else:
            variants = [(crop, {"commodity_desc": crop})]
 
        for label, qp in variants:
            records = api_get({
                **qp,
                "source_desc": "SURVEY",
                "statisticcat_desc": "CONDITION",
                "agg_level_desc": "NATIONAL",
                "year__GE": str(CURRENT_YEAR - 1),
                "freq_desc": "WEEKLY",
            })
            # Group by year + week_ending, then by unit_desc (PCT VERY POOR, PCT POOR, etc.)
            weeks = {}
            for r in records:
                we = r.get("week_ending", "")
                yr = r.get("year", 0)
                unit = r.get("unit_desc", "")
                val = r.get("Value", "")
                if not we or val in ("", "(D)", "(NA)"):
                    continue
                key = f"{yr}_{we}"
                if key not in weeks:
                    weeks[key] = {"week_ending": we, "year": int(yr)}
                cat_map = {
                    "PCT VERY POOR": "vp", "PCT POOR": "p",
                    "PCT FAIR": "f", "PCT GOOD": "g", "PCT EXCELLENT": "e"
                }
                if unit in cat_map:
                    try:
                        weeks[key][cat_map[unit]] = int(float(val))
                    except ValueError:
                        pass
 
            # Sort by week_ending and split by year
            sorted_weeks = sorted(weeks.values(), key=lambda x: x["week_ending"])
            out[label] = sorted_weeks
    return out
 
 
def fetch_progress():
    """Crop progress: % planted, emerged, harvested by week."""
    print("Fetching crop progress...")
    out = {}
    for crop in CROPS:
        if crop == "WHEAT":
            variants = [
                ("WINTER WHEAT", {"commodity_desc": "WHEAT", "prodn_practice_desc": "WINTER"}),
                ("SPRING WHEAT", {"commodity_desc": "WHEAT", "prodn_practice_desc": "SPRING, EXCL DURUM"}),
            ]
        else:
            variants = [(crop, {"commodity_desc": crop})]
 
        for label, qp in variants:
            records = api_get({
                **qp,
                "source_desc": "SURVEY",
                "statisticcat_desc": "PROGRESS",
                "agg_level_desc": "NATIONAL",
                "year__GE": str(CURRENT_YEAR - 1),
                "freq_desc": "WEEKLY",
            })
            stages = {}
            for r in records:
                we = r.get("week_ending", "")
                yr = r.get("year", 0)
                desc = r.get("short_desc", "")
                val = r.get("Value", "")
                if not we or val in ("", "(D)", "(NA)"):
                    continue
                # Extract stage from short_desc (e.g., "CORN - PROGRESS, MEASURED IN PCT PLANTED")
                stage = "OTHER"
                for s in ["PLANTED", "EMERGED", "SILKING", "DOUGH", "DENTED", "MATURE", "HARVESTED",
                           "HEADED", "TURNING", "COLORING", "DROPPING LEAVES", "SETTING PODS",
                           "BLOOMING", "JOINTED"]:
                    if s in desc.upper():
                        stage = s
                        break
                key = f"{label}|{stage}"
                if key not in stages:
                    stages[key] = []
                try:
                    stages[key].append({
                        "week_ending": we, "year": int(yr), "value": int(float(val))
                    })
                except ValueError:
                    pass
 
            crop_progress = {}
            for k, v in stages.items():
                _, stage = k.split("|")
                crop_progress[stage] = sorted(v, key=lambda x: x["week_ending"])
            out[label] = crop_progress
    return out
 
 
def fetch_grain_stocks():
    """Quarterly grain stocks, national level."""
    print("Fetching grain stocks...")
    out = {}
    for crop in CROPS:
        records = api_get({
            "commodity_desc": crop,
            "source_desc": "SURVEY",
            "statisticcat_desc": "STOCKS",
            "agg_level_desc": "NATIONAL",
            "year__GE": str(CURRENT_YEAR - 3),
            "freq_desc": "POINT IN TIME",
            "unit_desc": "BU",
            "domain_desc": "TOTAL",
        })
        stocks = []
        for r in records:
            val = r.get("Value", "")
            yr = r.get("year", 0)
            ref = r.get("reference_period_desc", "")
            if val in ("", "(D)", "(NA)"):
                continue
            try:
                # Value is in bushels, convert to million bushels
                v = float(val.replace(",", "")) / 1_000_000
                stocks.append({
                    "year": int(yr),
                    "ref_period": ref,
                    "mil_bu": round(v, 1)
                })
            except ValueError:
                pass
        out[crop] = sorted(stocks, key=lambda x: (x["year"], x["ref_period"]))
    return out
 
 
def fetch_acreage():
    """Prospective plantings + acreage, national level."""
    print("Fetching acreage...")
    out = {}
    for crop in CROPS:
        records = api_get({
            "commodity_desc": crop,
            "source_desc": "SURVEY",
            "statisticcat_desc": "AREA PLANTED",
            "agg_level_desc": "NATIONAL",
            "year__GE": str(CURRENT_YEAR - 3),
            "unit_desc": "ACRES",
            "domain_desc": "TOTAL",
            "prodn_practice_desc": "ALL PRODUCTION PRACTICES",
        })
        entries = []
        seen = set()
        for r in records:
            val = r.get("Value", "")
            yr = r.get("year", 0)
            ref = r.get("reference_period_desc", "")
            if val in ("", "(D)", "(NA)"):
                continue
            key = f"{yr}_{ref}"
            if key in seen:
                continue
            seen.add(key)
            try:
                v = float(val.replace(",", "")) / 1_000_000
                entries.append({
                    "year": int(yr),
                    "ref_period": ref,
                    "mil_acres": round(v, 2)
                })
            except ValueError:
                pass
        out[crop] = sorted(entries, key=lambda x: (x["year"], x["ref_period"]))
    return out
 
 
def fetch_cattle():
    """Cattle on feed: on feed, placements, marketings."""
    print("Fetching cattle data...")
    records = api_get({
        "commodity_desc": "CATTLE, ON FEED",
        "source_desc": "SURVEY",
        "agg_level_desc": "NATIONAL",
        "year__GE": str(CURRENT_YEAR - 2),
        "domain_desc": "TOTAL",
        "freq_desc": "MONTHLY",
    })
    entries = []
    for r in records:
        val = r.get("Value", "")
        yr = r.get("year", 0)
        ref = r.get("reference_period_desc", "")
        desc = r.get("short_desc", "")
        if val in ("", "(D)", "(NA)"):
            continue
        # Determine category from short_desc
        cat = "OTHER"
        if "INVENTORY" in desc.upper():
            cat = "ON_FEED"
        elif "PLACED" in desc.upper():
            cat = "PLACEMENTS"
        elif "MARKETED" in desc.upper() or "SOLD" in desc.upper():
            cat = "MARKETINGS"
        try:
            v = float(val.replace(",", "")) / 1000  # to thousands head
            entries.append({
                "year": int(yr), "ref_period": ref,
                "category": cat, "thou_head": round(v, 0)
            })
        except ValueError:
            pass
    return sorted(entries, key=lambda x: (x["year"], x["ref_period"], x["category"]))
 
 
def fetch_hogs():
    """Hogs & pigs inventory."""
    print("Fetching hogs data...")
    records = api_get({
        "commodity_desc": "HOGS",
        "source_desc": "SURVEY",
        "statisticcat_desc": "INVENTORY",
        "agg_level_desc": "NATIONAL",
        "year__GE": str(CURRENT_YEAR - 2),
        "domain_desc": "TOTAL",
    })
    entries = []
    for r in records:
        val = r.get("Value", "")
        yr = r.get("year", 0)
        ref = r.get("reference_period_desc", "")
        desc = r.get("short_desc", "")
        cl = r.get("class_desc", "")
        if val in ("", "(D)", "(NA)"):
            continue
        try:
            v = float(val.replace(",", "")) / 1000
            entries.append({
                "year": int(yr), "ref_period": ref,
                "class": cl, "thou_head": round(v, 0)
            })
        except ValueError:
            pass
    return sorted(entries, key=lambda x: (x["year"], x["ref_period"]))
 
 
def main():
    if not API_KEY:
        print("ERROR: NASS_API_KEY environment variable not set", file=sys.stderr)
        sys.exit(1)
 
    print(f"Fetching NASS data at {datetime.now().isoformat()}")
 
    result = {
        "fetched": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "crop_conditions": fetch_conditions(),
        "crop_progress": fetch_progress(),
        "grain_stocks": fetch_grain_stocks(),
        "acreage": fetch_acreage(),
        "cattle": fetch_cattle(),
        "hogs": fetch_hogs(),
    }
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, separators=(",", ":"))
 
    size = os.path.getsize(OUT)
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
