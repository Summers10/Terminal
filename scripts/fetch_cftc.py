#!/usr/bin/env python3
"""
CFTC Commitments of Traders fetcher — Disaggregated Futures-and-Options Combined report.
Pulls Managed Money ("Fund") positioning for a fixed set of contracts via
CFTC's public Socrata Open Data API (no API key required).
Saves data/cftc_data.json.
 
Source: https://publicreporting.cftc.gov/resource/kh3c-gbw2.json
Dataset: Disaggregated Commitments of Traders — Futures and Options Combined
 
Financial futures (currencies, S&P 500, Treasuries) are NOT in the Disaggregated report - they are in the
Traders in Financial Futures (TFF) report, a separate dataset on the same API:
    TFF Futures and Options Combined: https://publicreporting.cftc.gov/resource/yw9f-hn96.json
TFF has no "Managed Money" category; its closest equivalent is "Leveraged Funds" (hedge funds / CTAs), so that is
what is pulled for the financial contracts. The values are stored under the same mm_long / mm_short keys so the
page treats every contract identically, and each financial entry carries category = "Leveraged Funds".
The TFF step is deliberately non-fatal: if anything about it is wrong the commodity data still publishes, the
problem is printed loudly (and raised as a GitHub Actions warning), and affected rows show "No data" on the page.
 
NOTE: this was originally built against the Futures-Only dataset (72hh-3qpy).
Cross-checked against two independent professional sources (both showing
noticeably higher Open Interest and different weekly-change figures than
what Futures-Only produced), confirmed via CFTC's own API documentation that
those sources use the Combined report — switched to match, since that's the
convention most trade desks reference as "the" COT numbers.
"""
import os, sys, json, re, time, urllib.request, urllib.parse, urllib.error
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
 
# --- Financial futures (TFF report, Leveraged Funds) -------------------------------------------------------------
TFF_BASE = "https://publicreporting.cftc.gov/resource/yw9f-hn96.json"
# (CFTC contract_market_code, label, regex the contract name CFTC returns must match). The name check means a wrong code
# can never silently show a different instrument: the contract is excluded and reported instead.
FINANCIAL_CONTRACTS = [
    ("098662", "US Dollar Index",     r"DOLLAR INDEX|USD INDEX"),
    ("099741", "Euro",                r"EURO FX"),
    ("097741", "Japanese Yen",        r"JAPANESE YEN"),
    ("096742", "British Pound",       r"BRITISH POUND"),
    ("090741", "Canadian Dollar",     r"CANADIAN DOLLAR"),
    ("232741", "Australian Dollar",   r"AUSTRALIAN DOLLAR"),
    ("092741", "Swiss Franc",         r"SWISS FRANC"),
    ("13874A", "S&P 500 (E-mini)",    r"^(?!.*CONSOLIDATED).*E-MINI S&P 500"),
    ("042601", "2-Yr T-Note",         r"\b2[- ]?(Y\b|YR|YEAR)"),
    ("044601", "5-Yr T-Note",         r"\b5[- ]?(Y\b|YR|YEAR)"),
    ("043602", "10-Yr T-Note",        r"^(?!.*ULTRA).*\b10[- ]?(Y\b|YR|YEAR)"),
    ("043607", "Ultra 10-Yr T-Note",  r"ULTRA.*\b10[- ]?(Y\b|YR|YEAR)"),
    ("020601", "T-Bond (30-Yr)",      r"^(?!.*ULTRA).*(UST BOND|TREASURY BOND|T-BOND)"),
    ("020604", "Ultra T-Bond",        r"ULTRA.*(UST BOND|TREASURY BOND|T-BOND|BOND)"),
]
FINANCIAL_CATEGORY = "Leveraged Funds"
# logical field -> candidate API column names (first one present in the dataset wins; discovered by a probe request)
TFF_FIELD_CANDIDATES = {
    "date":   ["report_date_as_yyyy_mm_dd"],
    "code":   ["cftc_contract_market_code"],
    "oi":     ["open_interest_all"],
    "oi_chg": ["change_in_open_interest_all"],
    "long":   ["lev_money_positions_long", "lev_money_positions_long_all"],
    "short":  ["lev_money_positions_short", "lev_money_positions_short_all"],
}
TFF_NAME_CANDIDATES = ["contract_market_name", "market_and_exchange_names"]
TFF_REQUIRED = ("date", "code", "oi", "long", "short")
 
YEARS_HISTORY = 10
FIELDS = [
    "report_date_as_yyyy_mm_dd", "cftc_contract_market_code", "contract_market_name",
    "open_interest_all", "change_in_open_interest_all",
    "m_money_positions_long_all", "m_money_positions_short_all",
]
 
 
def fetch_page(where_clause, offset, limit=50000, base=None, fields=None):
    params = {
        "$where": where_clause,
        "$order": "report_date_as_yyyy_mm_dd ASC",
        "$limit": str(limit),
        "$offset": str(offset),
    }
    # fields=None -> the commodity column list; fields=[] -> no $select at all (used to probe a dataset's real columns)
    sel = FIELDS if fields is None else fields
    if sel:
        params["$select"] = ",".join(sel)
    url = (base or BASE) + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
    })
 
    # Retry on transient server-side errors (503 Service Unavailable, 429 Too Many
    # Requests) with backoff — these are temporary CFTC-side hiccups, not a real
    # problem with the request. Any other HTTP error (404, 400 — an actually wrong
    # URL or query) is NOT retried and fails loud immediately, same as before.
    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (503, 429) and attempt < max_attempts:
                wait = 10 * attempt
                print(f"HTTP {e.code} (transient) — retrying in {wait}s (attempt {attempt}/{max_attempts})...")
                time.sleep(wait)
                continue
            raise
 
 
def _num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None
 
 
def resolve_tff_fields(keys):
    """Pick the dataset's real column for each logical field from a probe row's keys."""
    resolved = {}
    for logical, cands in TFF_FIELD_CANDIDATES.items():
        hit = next((c for c in cands if c in keys), None)
        if hit:
            resolved[logical] = hit
    missing = [f for f in TFF_REQUIRED if f not in resolved]
    if missing:
        lev = sorted(k for k in keys if "lev" in k.lower())
        raise ValueError(f"TFF columns not found for {missing}; columns containing 'lev': {lev}")
    names = [c for c in TFF_NAME_CANDIDATES if c in keys]
    return resolved, names
 
 
def fetch_financials():
    """Leveraged Funds positioning for the financial contracts from the TFF report. NEVER raises: returns
    (codes_dict, problems_list) so a problem here can't block the commodity data."""
    problems, out = [], {}
    expected = {c: (label, pat) for c, label, pat in FINANCIAL_CONTRACTS}
    codes_in = "', '".join(expected.keys())
    try:
        probe = fetch_page(f"cftc_contract_market_code in ('{codes_in}')", 0, limit=1, base=TFF_BASE, fields=[])
        if not probe:
            raise ValueError("TFF dataset returned no rows for any requested contract code")
        resolved, name_cols = resolve_tff_fields(set(probe[0].keys()))
        select = sorted(set(resolved.values()) | set(name_cols))
        cutoff = (datetime.utcnow() - timedelta(days=365 * YEARS_HISTORY)).strftime("%Y-%m-%dT00:00:00.000")
        where = f"cftc_contract_market_code in ('{codes_in}') AND report_date_as_yyyy_mm_dd > '{cutoff}'"
        rows, offset = [], 0
        while True:
            page = fetch_page(where, offset, base=TFF_BASE, fields=select)
            rows.extend(page)
            if len(page) < 50000:
                break
            offset += 50000
    except urllib.error.HTTPError as e:
        try:
            body = e.read(300).decode("utf-8", errors="replace")
        except Exception:
            body = ""
        return {}, [f"TFF dataset: HTTP {e.code} {e.reason} {body}".strip()]
    except Exception as e:
        return {}, [f"TFF dataset: {type(e).__name__}: {e}"]
 
    by_code, names = {}, {}
    for row in rows:
        code = row.get(resolved["code"])
        if code not in expected:
            continue
        date = (row.get(resolved["date"]) or "")[:10]
        by_code.setdefault(code, {}).setdefault(date, {
            "date": date,
            "oi": _num(row.get(resolved["oi"])),
            "oi_chg": _num(row.get(resolved["oi_chg"])) if "oi_chg" in resolved else None,
            "mm_long": _num(row.get(resolved["long"])),      # Leveraged Funds, stored under the shared key
            "mm_short": _num(row.get(resolved["short"])),
        })
        names[code] = " | ".join(dict.fromkeys(str(row.get(c)) for c in name_cols if row.get(c)))   # rows are oldest->newest, so latest wins
 
    for code, (label, pattern) in expected.items():
        by_date = by_code.get(code)
        if not by_date:
            problems.append(f"{label} ({code}): no rows returned - check the contract code")
            continue
        nm = names.get(code, "")
        if not re.search(pattern, nm.upper()):
            problems.append(f"{label} ({code}): CFTC calls this contract '{nm}', which doesn't look like {label} - excluded")
            continue
        history = [by_date[d] for d in sorted(by_date)]
        latest = history[-1]
        if latest["oi"] is None or latest["oi"] <= 0:
            problems.append(f"{label} ({code}): latest open interest missing or non-positive ({latest}) - excluded")
            continue
        if latest["mm_long"] is None or latest["mm_short"] is None:
            problems.append(f"{label} ({code}): Leveraged Funds long/short missing on the latest row - excluded")
            continue
        out[code] = {"label": label, "category": FINANCIAL_CATEGORY, "report": "TFF", "name": nm, "history": history}
    return out, problems
 
 
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
 
    # Financial futures (currencies / S&P / Treasuries) - non-fatal by design, see module docstring.
    print("\nFetching CFTC TFF (financial futures, Leveraged Funds)...")
    fin, fin_problems = fetch_financials()
    result["codes"].update(fin)
    for code, label, _ in FINANCIAL_CONTRACTS:
        if code in fin:
            h = fin[code]["history"]
            print(f"  {label} ({code}) = CFTC '{fin[code]['name']}': {len(h)} weeks, latest OI={h[-1]['oi']:,.0f} as of {h[-1]['date']}")
    if fin_problems:
        print("WARNING: some financial contracts could not be loaded (the commodity data is unaffected):", file=sys.stderr)
        for pb in fin_problems:
            print(f"  - {pb}", file=sys.stderr)
            print(f"::warning title=CFTC financial futures::{pb}")
 
    result["_meta"] = {
        "fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "years_history": YEARS_HISTORY,
        "financials": {"dataset": "yw9f-hn96 (TFF Futures and Options Combined)", "category": FINANCIAL_CATEGORY,
                       "contracts_loaded": len(fin), "contracts_expected": len(FINANCIAL_CONTRACTS)},
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
 
