#!/usr/bin/env python3
from datetime import datetime, timezone
"""Fetch FRED economic data server-side to avoid CORS issues.
Saves as data/fred_data.json for the terminal to load.
 
Series groups:
  UST yields / spreads  — daily, 252 days
  Policy rates          — daily, 252 days
  Credit spreads        — daily, 252 days
  Labor / macro         — monthly/weekly, 60–104 obs
  FX rates              — daily, 252 days  (NEW — required by currency dashboard)
  Foreign 10Y yields    — monthly, 60 obs  (NEW — required by rate differential table)
"""
import os, sys, json, urllib.request, time
 
OUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "fred_data.json"
)
API_KEY = os.environ.get("FRED_API_KEY", "db2f5ad351573c6f6e34c1e1ec667feb")
 
# (series_id, obs_limit)
SERIES = [
    # ── US Treasury yields ────────────────────────────────────────────────────
    ("DGS3MO", 252), ("DGS1", 252), ("DGS2", 252), ("DGS5", 252),
    ("DGS7", 252), ("DGS10", 252), ("DGS30", 252),
    # ── Spreads ───────────────────────────────────────────────────────────────
    ("T10Y2Y", 252), ("T10Y3M", 252),
    ("T5YIE", 252), ("T10YIE", 252), ("DFII10", 252),
    # ── Policy rates ─────────────────────────────────────────────────────────
    ("DFF", 252), ("SOFR", 252),
    # ── Credit spreads ────────────────────────────────────────────────────────
    ("BAMLH0A0HYM2", 252), ("BAMLC0A4CBBB", 252),
    # ── Labour / real economy ─────────────────────────────────────────────────
    ("UNRATE", 60), ("ICSA", 104),
    ("M2SL", 60), ("WALCL", 104),
    ("INDPRO", 60),
    # ── FX rates (daily) — required by Currencies dashboard ───────────────────
    ("DEXCAUS", 252),   # USD/CAD  Canadian dollar
    ("DEXUSEU", 252),   # EUR/USD  Euro (inverted pair — USD per EUR)
    ("DEXUSUK", 252),   # GBP/USD  British pound (inverted — USD per GBP)
    ("DEXJPUS", 252),   # USD/JPY  Japanese yen
    ("DEXUSAL", 252),   # AUD/USD  Australian dollar (inverted — USD per AUD)
    ("DEXBZUS", 252),   # USD/BRL  Brazilian real
    # ── Foreign 10Y government yields (monthly) — rate differential table ────
    ("IRLTLT01CAM156N", 60),   # Canada
    ("IRLTLT01EZM156N", 60),   # Euro area
    ("IRLTLT01GBM156N", 60),   # United Kingdom
    ("IRLTLT01JPM156N", 60),   # Japan
    ("IRLTLT01AUM156N", 60),   # Australia
    ("IRLTLT01BRM156N", 60),   # Brazil
]
 
# Series that need a non-default units transform
SPECIAL = {
    "CPIAUCSL": {"lim": 60, "extra": "&units=pc1"},  # CPI YoY %
    "PCEPI":    {"lim": 60, "extra": "&units=pc1"},  # PCE YoY %
}
 
 
def fetch_series(sid, limit, extra=""):
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={sid}&api_key={API_KEY}&file_type=json"
        f"&sort_order=desc&limit={limit}{extra}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "SUMCO-Terminal/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    if "observations" not in data:
        return []
    return [
        {"d": o["date"], "v": float(o["value"])}
        for o in data["observations"]
        if o["value"] != "."
    ]
 
 
def main():
    print("Fetching FRED data…")
    result = {}
    ok = fail = 0
 
    all_series = list(SERIES)
 
    for sid, lim in all_series:
        try:
            pts = fetch_series(sid, lim)
            if pts:
                result[sid] = pts
                print(f"  {sid}: {len(pts)} obs  (latest {pts[0]['d']} = {pts[0]['v']})")
                ok += 1
            else:
                print(f"  {sid}: no data")
                fail += 1
        except Exception as e:
            print(f"  {sid}: ERROR — {e}")
            fail += 1
        time.sleep(0.15)   # ~5 req/sec — stay under FRED rate limit
 
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
            print(f"  {sid}: ERROR — {e}")
            fail += 1
        time.sleep(0.15)
 
    if not result:
        print("ERROR: No FRED data fetched.", file=sys.stderr)
        sys.exit(1)
 
    result["_meta"] = {"fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, separators=(",", ":"))
 
    size = os.path.getsize(OUT)
    fx_ok  = [s for s in ["DEXCAUS","DEXUSEU","DEXUSUK","DEXJPUS","DEXUSAL","DEXBZUS"] if s in result]
    irt_ok = [s for s in result if s.startswith("IRLTLT01")]
    print(f"\n{ok} series fetched, {fail} failed")
    print(f"FX series:            {len(fx_ok)}/6  — {', '.join(fx_ok) if fx_ok else 'NONE'}")
    print(f"Foreign 10Y yields:   {len(irt_ok)}/6  — {', '.join(irt_ok) if irt_ok else 'NONE'}")
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
