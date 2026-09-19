#!/usr/bin/env python3
"""
CFTC Commitments of Traders fetcher — Disaggregated Futures-and-Options Combined report.
Pulls Managed Money ("Fund") positioning for a fixed set of contracts via
CFTC's public Socrata Open Data API (no API key required).
Saves data/cftc_data.json.
 
Source: https://publicreporting.cftc.gov/resource/kh3c-gbw2.json
Dataset: Disaggregated Commitments of Traders — Futures and Options Combined
 
NOTE: this was originally built against the Futures-Only dataset (72hh-3qpy).
Cross-checked against two independent professional sources (both showing
noticeably higher Open Interest and different weekly-change figures than
what Futures-Only produced), confirmed via CFTC's own API documentation that
those sources use the Combined report — switched to match, since that's the
convention most trade desks reference as "the" COT numbers.
"""
import os, sys, json, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timedelta
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "cftc_data.json")
BASE = "https://publicreporting.cftc.gov/resource/kh3c-gbw2.json"
 
# CFTC contract_market_code -> display label. Codes are the stable identifier;
# names on the report can vary in formatting, codes do not.
CONTRACTS = {
    "002602": "Corn",
    "001602": "Wheat (Chicago SRW)",
    "001612": "Wheat (KC HRW)",
    "001626": "Wheat (Mpls Spring)",
    "135731": "Canola",
    "005602": "Soybeans",
    "007601": "Soybean Oil",
    "026603": "Soybean Meal",
    "061641": "Feeder Cattle",
    "057642": "Live Cattle",
    "054642": "Lean Hogs",
    "073732": "Cocoa",
    "083731": "Coffee",
    "033661": "Cotton",
    "080732": "Sugar",
    "067651": "Crude Oil (WTI)",
    "022651": "Heating Oil",
    "111659": "RBOB Gasoline",
    "023651": "Natural Gas",
    "088691": "Gold",
    "084691": "Silver",
    "085692": "Copper",
}
 
YEARS_HISTORY = 10
FIELDS = [
    "report_date_as_yyyy_mm_dd", "cftc_contract_market_code", "contract_market_name",
    "open_interest_all", "change_in_open_interest_all",
    "m_money_positions_long_all", "m_money_positions_short_all",
]
 
 
def fetch_page(where_clause, offset, limit=50000):
    params = {
        "$select": ",".join(FIELDS),
        "$where": where_clause,
        "$order": "report_date_as_yyyy_mm_dd ASC",
        "$limit": str(limit),
        "$offset": str(offset),
    }
    url = BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))
 
 
def main():
    print("Fetching CFTC Disaggregated COT data (Managed Money positioning)...")
 
    cutoff = (datetime.utcnow() - timedelta(days=365 * YEARS_HISTORY)).strftime("%Y-%m-%dT00:00:00.000")
    codes_list = "', '".join(CONTRACTS.keys())
    where = f"cftc_contract_market_code in ('{codes_list}') AND report_date_as_yyyy_mm_dd > '{cutoff}'"
 
    all_rows = []
    offset = 0
    while True:
        print(f"  Fetching offset {offset}...", end=" ", flush=True)
        try:
            rows = fetch_page(where, offset)
        except urllib.error.HTTPError as e:
            print(f"HTTP error: {e.code} {e.reason}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"error: {e}", file=sys.stderr)
            sys.exit(1)
        print(f"{len(rows)} rows")
        all_rows.extend(rows)
        if len(rows) < 50000:
            break
        offset += 50000
 
    if not all_rows:
        print("ERROR: No rows returned from CFTC API.", file=sys.stderr)
        sys.exit(1)
 
    # Group by contract code, build sorted weekly history.
    by_code = {}
    for row in all_rows:
        code = row.get("cftc_contract_market_code")
        if code not in CONTRACTS:
            continue
        date = row.get("report_date_as_yyyy_mm_dd", "")[:10]
 
        def to_num(key):
            v = row.get(key)
            if v is None or v == "":
                return None
            try:
                return float(v)
            except (ValueError, TypeError):
                return None
 
        entry = {
            "date": date,
            "oi": to_num("open_interest_all"),
            "oi_chg": to_num("change_in_open_interest_all"),
            "mm_long": to_num("m_money_positions_long_all"),
            "mm_short": to_num("m_money_positions_short_all"),
        }
        by_code.setdefault(code, {}).setdefault(date, entry)  # dedupe by date, first write wins
 
    # Fail loud: verify every expected contract actually returned data.
    missing = [f"{code} ({label})" for code, label in CONTRACTS.items() if code not in by_code or not by_code[code]]
    if missing:
        print("\nERROR: No data returned for the following contracts (check contract codes):", file=sys.stderr)
        for m in missing:
            print(f"  - {m}", file=sys.stderr)
        sys.exit(1)
 
    result = {"codes": {}}
    for code, label in CONTRACTS.items():
        dates_sorted = sorted(by_code[code].keys())
        history = [by_code[code][d] for d in dates_sorted]
        # Sanity: open interest should never be zero/negative for an active contract's latest row.
        latest = history[-1]
        if latest["oi"] is None or latest["oi"] <= 0:
            print(f"ERROR: {label} ({code}) latest open interest is missing or non-positive: {latest}", file=sys.stderr)
            sys.exit(1)
        result["codes"][code] = {"label": label, "history": history}
 
    result["_meta"] = {
        "fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "years_history": YEARS_HISTORY,
    }
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, indent=2)
 
    size = os.path.getsize(OUT)
    print(f"\nContracts fetched ({len(result['codes'])}):")
    for code, label in CONTRACTS.items():
        h = result["codes"][code]["history"]
        latest = h[-1]
        print(f"  {label} ({code}): {len(h)} weeks, latest OI={latest['oi']:,.0f} as of {latest['date']}")
    print(f"\nSaved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
