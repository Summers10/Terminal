#!/usr/bin/env python3
"""
fetch_exports.py - USDA FAS Weekly Export Sales (highlite.htm scraper).
Parses the weekly highlights narrative at apps.fas.usda.gov/export-sales/highlite.htm
Extracts net sales and exports for wheat, corn, soybeans from the text.
Appends new weeks to existing export_sales.json incrementally.
 
No API key required — direct HTML page scrape like the other fetchers.
"""
import os, sys, json, re, urllib.request
from datetime import datetime, timezone
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "export_sales.json")
URL = "https://apps.fas.usda.gov/export-sales/highlite.htm"
 
COMMODITIES = {
    "wheat":    {"pattern": r"\*\*Wheat:\*\*|<b>Wheat:</b>|Wheat:\s*Net sales",
                 "my_start": 6, "my_label": "Jun-May"},
    "corn":     {"pattern": r"\*\*Corn:\*\*|<b>Corn:</b>|Corn:\s*\n?\s*Net sales",
                 "my_start": 9, "my_label": "Sep-Aug"},
    "soybeans": {"pattern": r"\*\*Soybeans:\*\*|<b>Soybeans:</b>|Soybeans:\s*\n?\s*Net sales",
                 "my_start": 9, "my_label": "Sep-Aug"},
}
 
MONTHS = {"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
          "july":7,"august":8,"september":9,"october":10,"november":11,"december":12}
 
 
def get_marketing_year(dt, my_start):
    y1 = dt.year if dt.month >= my_start else dt.year - 1
    return f"{y1 % 100:02d}/{(y1 + 1) % 100:02d}"
 
 
def week_of_my(dt, my_start):
    y1 = dt.year if dt.month >= my_start else dt.year - 1
    start = datetime(y1, my_start, 1)
    return max(1, min(52, (dt - start).days // 7 + 1))
 
 
def parse_number(text):
    """Extract number from text like '397,200' or '1,217,800'."""
    m = re.search(r'([\d,]+(?:\.\d+)?)', text.replace(",", ","))
    if m:
        return float(m.group(1).replace(",", ""))
    return None
 
 
def fetch_highlights():
    """Fetch and parse highlite.htm."""
    print(f"  Fetching {URL}...")
    req = urllib.request.Request(URL, headers={
        "User-Agent": "Mozilla/5.0 (USDA-Terminal-Fetcher)"
    })
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
 
    # Clean HTML tags but preserve bold markers
    text = raw.replace("<b>", "**").replace("</b>", "**")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"\s+", " ", text)
 
    # Extract report period date
    # "reports from exporters for the period March 13-19, 2026"
    period_match = re.search(
        r"period\s+(\w+)\s+\d+[-–]\s*(\d+),?\s*(\d{4})", text, re.IGNORECASE
    )
    if not period_match:
        print("    Could not find report period date")
        return None
 
    month_name = period_match.group(1).lower()
    end_day = int(period_match.group(2))
    year = int(period_match.group(3))
    month_num = MONTHS.get(month_name)
    if not month_num:
        print(f"    Unknown month: {month_name}")
        return None
 
    report_date = datetime(year, month_num, end_day)
    print(f"    Report date: {report_date.strftime('%Y-%m-%d')}")
 
    results = {}
    for key, cfg in COMMODITIES.items():
        # Find the commodity section
        # Look for "**Wheat:** Net sales of X metric tons"
        # and "Exports of X MT"
        pat_start = re.search(cfg["pattern"], text, re.IGNORECASE)
        if not pat_start:
            print(f"    {key}: section not found")
            continue
 
        # Get text from this commodity to the next bold commodity header
        section_start = pat_start.start()
        # Find next commodity header or end
        next_headers = list(re.finditer(r'\*\*\w+(?:\s+\w+)?:\*\*', text[section_start + 10:]))
        if next_headers:
            section_end = section_start + 10 + next_headers[0].start()
        else:
            section_end = min(section_start + 2000, len(text))
        section = text[section_start:section_end]
 
        # Extract net sales: "Net sales of 397,200 metric tons"
        ns_match = re.search(r'Net sales of ([\d,]+(?:\.\d+)?)\s*(?:metric tons|MT)', section, re.IGNORECASE)
        net_sales = parse_number(ns_match.group(1)) if ns_match else None
 
        # Extract exports: "Exports of 383,500 MT"
        exp_match = re.search(r'Exports of ([\d,]+(?:\.\d+)?)\s*(?:MT|metric tons)', section, re.IGNORECASE)
        exports = parse_number(exp_match.group(1)) if exp_match else None
 
        if net_sales is None and exports is None:
            print(f"    {key}: no numbers found in section")
            continue
 
        my = get_marketing_year(report_date, cfg["my_start"])
        wk = week_of_my(report_date, cfg["my_start"])
 
        # Convert from MT to 1,000 MT (the format used in the JSON)
        results[key] = {
            "date": report_date,
            "my": my,
            "week": wk,
            "exports": round(exports / 1000, 1) if exports else 0,
            "net_sales": round(net_sales / 1000, 1) if net_sales else 0,
            "my_label": cfg["my_label"],
        }
        print(f"    {key}: MY {my} Wk{wk} — Exports={results[key]['exports']}k, NetSales={results[key]['net_sales']}k")
 
    return results
 
 
def load_existing():
    if os.path.exists(OUT):
        with open(OUT) as f:
            return json.load(f)
    return {}
 
 
def ensure_commodity(data, key, my_label):
    if key not in data:
        data[key] = {"insp": {"MY": my_label, "years": [], "w": {}},
                     "sales": {"years": [], "w": {}}}
    return data[key]
 
 
def append_week(data, key, parsed):
    comm = ensure_commodity(data, key, parsed["my_label"])
    my, wk, idx = parsed["my"], parsed["week"], parsed["week"] - 1
 
    for section in ["insp", "sales"]:
        sec = comm[section]
        if my not in sec.get("years", []):
            sec.setdefault("years", []).append(my)
            sec["years"].sort(key=lambda s: int(s.split("/")[0]) + (2000 if int(s.split("/")[0]) < 80 else 1900))
        if my not in sec.get("w", {}):
            sec["w"][my] = [None] * 52
 
    if idx < 52:
        if comm["insp"]["w"][my][idx] is None:
            comm["insp"]["w"][my][idx] = parsed["exports"]
            comm["sales"]["w"][my][idx] = parsed["net_sales"]
            print(f"    {key}: Added week {wk} ({my})")
            return True
        else:
            print(f"    {key}: Week {wk} ({my}) exists, skipping")
    return False
 
 
def main():
    print("=" * 60)
    print("FETCH_EXPORTS — USDA FAS highlite.htm scraper")
    print("=" * 60)
 
    data = load_existing()
    try:
        parsed = fetch_highlights()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
 
    if not parsed:
        print("ERROR: No data parsed.", file=sys.stderr)
        sys.exit(1)
 
    updated = False
    for key, p in parsed.items():
        if append_week(data, key, p):
            updated = True
 
    data["_meta"] = {"fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(data, f, separators=(",", ":"))
 
    print(f"\n{'Updated' if updated else 'No new data.'}")
    print(f"Saved {OUT} ({os.path.getsize(OUT):,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
