#!/usr/bin/env python3
from datetime import datetime, timezone
"""Fetch FRED economic data server-side to avoid CORS issues.
Saves as data/fred_data.json for the terminal to load."""
import os, sys, json, urllib.request, time
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "fred_data.json")
API_KEY = os.environ.get("FRED_API_KEY", "db2f5ad351573c6f6e34c1e1ec667feb")
 
SERIES = [
    ("DGS3MO", 252), ("DGS1", 252), ("DGS2", 252), ("DGS5", 252),
    ("DGS7", 252), ("DGS10", 252), ("DGS30", 252),
    ("T10Y2Y", 252), ("T10Y3M", 252),
    ("T5YIE", 252), ("T10YIE", 252), ("DFII10", 252),
    ("DFF", 252), ("SOFR", 252),
    ("BAMLH0A0HYM2", 252), ("BAMLC0A4CBBB", 252),
    ("UNRATE", 60), ("ICSA", 104),
    ("CPALTT01USM657N", 60),
    ("M2SL", 60), ("WALCL", 104),
    ("INDPRO", 60),
    # FX rates
    ("DEXCAUS", 252), ("DEXUSEU", 252), ("DEXJPUS", 252),
    ("DEXUSAL", 252), ("DEXUSUK", 252), ("DEXBZUS", 252),
    # Foreign 10Y yields for rate differentials
    ("IRLTLT01CAM156N", 60), ("IRLTLT01EZM156N", 60),
    ("IRLTLT01JPM156N", 60), ("IRLTLT01GBM156N", 60),
    ("IRLTLT01AUM156N", 60), ("IRLTLT01BRM156N", 60),
]
 
# PCE needs units=pc1 for YoY% change
SPECIAL = {"PCEPI": {"lim": 60, "extra": "&units=pc1"}}
 
 
def fetch_series(sid, limit, extra=""):
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={sid}&api_key={API_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}{extra}")
    req = urllib.request.Request(url, headers={"User-Agent": "SUMCO-Terminal/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    if "observations" not in data:
        return []
    return [{"d": o["date"], "v": float(o["value"])}
            for o in data["observations"] if o["value"] != "."]
 
 
def main():
    print("Fetching FRED data...")
    result = {}
    ok = 0
    fail = 0
 
    for sid, lim in SERIES:
        try:
            pts = fetch_series(sid, lim)
            if pts:
                result[sid] = pts
                print(f"  {sid}: {len(pts)} obs")
                ok += 1
            else:
                print(f"  {sid}: no data")
                fail += 1
        except Exception as e:
            print(f"  {sid}: error ({e})")
            fail += 1
        time.sleep(0.15)  # Rate limit: ~5 req/sec
 
    # PCE with units=pc1
    for sid, cfg in SPECIAL.items():
        try:
            pts = fetch_series(sid, cfg["lim"], cfg.get("extra", ""))
            if pts:
                result[sid] = pts
                print(f"  {sid}: {len(pts)} obs")
                ok += 1
            else:
                print(f"  {sid}: no data")
                fail += 1
        except Exception as e:
            print(f"  {sid}: error ({e})")
            fail += 1
        time.sleep(0.15)
 
    if not result:
        print("ERROR: No FRED data fetched.", file=sys.stderr)
        sys.exit(1)
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, separators=(",", ":"))
 
    size = os.path.getsize(OUT)
    print(f"\n{ok} series fetched, {fail} failed")
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
