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
YEARS = [CURRENT_YEAR - 2, CURRENT_YEAR - 1, CURRENT_YEAR]
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
    cat_map = {"PCT VERY POOR":"vp","PCT POOR":"p","PCT FAIR":"f","PCT GOOD":"g","PCT EXCELLENT":"e"}
 
    for crop in ["CORN","SOYBEANS"]:
        records = api_get({"commodity_desc":crop,"source_desc":"SURVEY",
            "statisticcat_desc":"CONDITION","agg_level_desc":"NATIONAL",
            "year__GE":str(CURRENT_YEAR-2),"freq_desc":"WEEKLY"})
        weeks = {}
        for r in records:
            we,yr,unit,val = r.get("week_ending",""),r.get("year",0),r.get("unit_desc",""),r.get("Value","")
            if not we or val in ("","(D)","(NA)"): continue
            key = f"{yr}_{we}"
            if key not in weeks: weeks[key] = {"week_ending":we,"year":int(yr)}
            if unit in cat_map:
                try: weeks[key][cat_map[unit]] = int(float(val))
                except ValueError: pass
        out[crop] = sorted(weeks.values(), key=lambda x: x["week_ending"])
        print(f"  {crop}: {len(out[crop])} weeks")
 
    # Wheat: fetch all, split by short_desc (avoids brittle prodn_practice_desc filter)
    wheat_recs = api_get({"commodity_desc":"WHEAT","source_desc":"SURVEY",
        "statisticcat_desc":"CONDITION","agg_level_desc":"NATIONAL",
        "year__GE":str(CURRENT_YEAR-2),"freq_desc":"WEEKLY"})
    print(f"  WHEAT raw records: {len(wheat_recs)}")
    by_type = {"WINTER WHEAT":{},"SPRING WHEAT":{}}
    for r in wheat_recs:
        we,yr,unit,val = r.get("week_ending",""),r.get("year",0),r.get("unit_desc",""),r.get("Value","")
        short = r.get("short_desc","").upper()
        if not we or val in ("","(D)","(NA)"): continue
        if "WINTER" in short: label = "WINTER WHEAT"
        elif "SPRING" in short and "DURUM" not in short: label = "SPRING WHEAT"
        else: continue
        key = f"{yr}_{we}"
        if key not in by_type[label]: by_type[label][key] = {"week_ending":we,"year":int(yr)}
        if unit in cat_map:
            try: by_type[label][key][cat_map[unit]] = int(float(val))
            except ValueError: pass
    for label,weeks in by_type.items():
        out[label] = sorted(weeks.values(), key=lambda x: x["week_ending"])
        print(f"  {label}: {len(out[label])} weeks")
    return out
 
def fetch_progress():
    """Crop progress: % planted, emerged, harvested by week."""
    print("Fetching crop progress...")
    out = {}
    STAGES = ["PLANTED","EMERGED","SILKING","DOUGH","DENTED","MATURE","HARVESTED","HEADED","TURNING","COLORING","DROPPING LEAVES","SETTING PODS","BLOOMING","JOINTED"]
 
    def parse_stages(records, label):
        stages = {}
        for r in records:
            we,yr = r.get("week_ending",""),r.get("year",0)
            desc,val = r.get("short_desc","").upper(),r.get("Value","")
            if not we or val in ("","(D)","(NA)"): continue
            stage = "OTHER"
            for s in STAGES:
                if s in desc: stage = s; break
            key = f"{label}|{stage}"
            if key not in stages: stages[key] = []
            try: stages[key].append({"week_ending":we,"year":int(yr),"value":int(float(val))})
            except ValueError: pass
        cp = {}
        for k,v in stages.items():
            _,s = k.split("|")
            cp[s] = sorted(v, key=lambda x: x["week_ending"])
        return cp
 
    for crop in ["CORN","SOYBEANS"]:
        records = api_get({"commodity_desc":crop,"source_desc":"SURVEY",
            "statisticcat_desc":"PROGRESS","agg_level_desc":"NATIONAL",
            "year__GE":str(CURRENT_YEAR-2),"freq_desc":"WEEKLY"})
        out[crop] = parse_stages(records, crop)
 
    # Wheat: fetch all, split by short_desc
    wheat_recs = api_get({"commodity_desc":"WHEAT","source_desc":"SURVEY",
        "statisticcat_desc":"PROGRESS","agg_level_desc":"NATIONAL",
        "year__GE":str(CURRENT_YEAR-2),"freq_desc":"WEEKLY"})
    print(f"  WHEAT progress raw records: {len(wheat_recs)}")
    for wlabel,wtype in [("WINTER WHEAT","WINTER"),("SPRING WHEAT","SPRING")]:
        filtered = []
        for r in wheat_recs:
            short = r.get("short_desc","").upper()
            if wtype not in short: continue
            if wtype == "SPRING" and "DURUM" in short: continue
            filtered.append(r)
        out[wlabel] = parse_stages(filtered, wlabel)
        stages_found = [k for k in out[wlabel] if k != "OTHER"]
        print(f"  {wlabel}: {len(filtered)} records, stages: {stages_found}")
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
 
 
