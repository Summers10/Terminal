#!/usr/bin/env python3
"""
fetch_exports.py - USDA FAS Weekly Export Sales via OpenData API.
Uses the ESR API at apps.fas.usda.gov/OpenData/api/esr/
Fetches weekly exports + net sales for wheat, corn, soybeans.
Builds full marketing-year weekly arrays for the dashboard.
 
API docs: https://apps.fas.usda.gov/opendataweb/home
Commodity codes: 107=All Wheat, 401=Corn, 801=Soybeans
"""
import os, sys, json, urllib.request
from datetime import datetime, timezone
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "export_sales.json")
API_BASE = "https://apps.fas.usda.gov/OpenData/api/esr"
 
COMMODITIES = {
    "wheat":    {"code": 107, "my_start": 6, "my_label": "Jun-May"},
    "corn":     {"code": 401, "my_start": 9, "my_label": "Sep-Aug"},
    "soybeans": {"code": 801, "my_start": 9, "my_label": "Sep-Aug"},
}
 
def get_my_years(my_start):
    now = datetime.now(timezone.utc)
    cur = now.year if now.month >= my_start else now.year - 1
    return cur, cur - 1
 
def my_label(year):
    return f"{year % 100:02d}/{(year + 1) % 100:02d}"
 
def week_of_my(date_str, my_start, my_year):
    dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
    start = datetime(my_year, my_start, 1)
    return max(1, min(52, (dt - start).days // 7 + 1))
 
def fetch_json(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (USDA-Terminal)",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))
 
def fetch_commodity(key, cfg):
    code, my_start = cfg["code"], cfg["my_start"]
    cur_year, prev_year = get_my_years(my_start)
    all_results = {}
 
    for year in [prev_year, cur_year]:
        my = my_label(year)
        url = f"{API_BASE}/exports/commodityCode/{code}/allCountries/marketYear/{year}"
        print(f"    {key} MY {my}: {url}")
 
        try:
            data = fetch_json(url)
        except urllib.error.HTTPError as e:
            print(f"      HTTP {e.code} — skipping")
            continue
        except Exception as e:
            print(f"      Error: {e}")
            continue
 
        # Handle both list and dict responses
        if isinstance(data, dict):
            for k in ["data", "records", "results", "exports"]:
                if k in data and isinstance(data[k], list):
                    data = data[k]; break
        if not isinstance(data, list):
            print(f"      Unexpected response type: {type(data)}")
            if isinstance(data, dict):
                print(f"      Keys: {list(data.keys())[:10]}")
            continue
 
        print(f"      {len(data)} records", end="")
        if data:
            print(f" — sample keys: {list(data[0].keys())[:6]}")
        else:
            print()
            continue
 
        # Aggregate by week — try multiple field name patterns
        weekly = {}
        for rec in data:
            we = None
            for wk in ["weekEndingDate","WeekEndingDate","weekEnding","week_ending","reportingPeriodEndDate"]:
                if wk in rec and rec[wk]:
                    we = str(rec[wk])[:10]; break
            if not we:
                continue
 
            exp_val, ns_val = 0.0, 0.0
            for ek in ["currentMyExports","CurrentMyExports","exports","Exports","weeklyExports","currentExports"]:
                if ek in rec and rec[ek] is not None:
                    try: exp_val += float(rec[ek])
                    except: pass
                    break
            for nk in ["currentMyNetSales","CurrentMyNetSales","netSales","NetSales","currentNetSales"]:
                if nk in rec and rec[nk] is not None:
                    try: ns_val += float(rec[nk])
                    except: pass
                    break
 
            weekly.setdefault(we, {"exports":0,"net_sales":0})
            weekly[we]["exports"] += exp_val
            weekly[we]["net_sales"] += ns_val
 
        if not weekly:
            print(f"      No weekly data extracted")
            continue
 
        insp, sales = [None]*52, [None]*52
        for we_date, wv in sorted(weekly.items()):
            idx = week_of_my(we_date, my_start, year) - 1
            if 0 <= idx < 52:
                insp[idx] = round(wv["exports"], 1)
                sales[idx] = round(wv["net_sales"], 1)
 
        filled = sum(1 for v in insp if v is not None)
        print(f"      {filled} weeks with data")
        all_results[my] = {"insp": insp, "sales": sales}
 
    if not all_results:
        return None
 
    years = sorted(all_results.keys(),
                   key=lambda s: int(s.split("/")[0]) + (2000 if int(s.split("/")[0]) < 80 else 1900))
    return {
        "insp": {"MY": cfg["my_label"], "years": years,
                 "w": {yr: all_results[yr]["insp"] for yr in years}},
        "sales": {"years": years,
                  "w": {yr: all_results[yr]["sales"] for yr in years}},
    }
 
def main():
    print("=" * 60)
    print("FETCH_EXPORTS — USDA FAS OpenData API")
    print("=" * 60)
 
    # Test API
    print("\nTesting API...")
    try:
        test = fetch_json(f"{API_BASE}/commodities")
        n = len(test) if isinstance(test, list) else "?"
        print(f"  API OK — {n} commodities")
    except Exception as e:
        print(f"  API FAILED: {e}")
        print(f"  Check: https://apps.fas.usda.gov/opendatawebv2/")
        sys.exit(1)
 
    existing = {}
    if os.path.exists(OUT):
        try:
            with open(OUT) as f: existing = json.load(f)
        except: pass
 
    results = dict(existing)
    for key, cfg in COMMODITIES.items():
        print(f"\n--- {key.upper()} ---")
        try:
            data = fetch_commodity(key, cfg)
            if data:
                results[key] = data
        except Exception as e:
            print(f"  ERROR: {e}")
 
    results["_meta"] = {"fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(results, f, separators=(",",":"))
    print(f"\nSaved {OUT} ({os.path.getsize(OUT):,} bytes)")
 
if __name__ == "__main__":
    main()
 
