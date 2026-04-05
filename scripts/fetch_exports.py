#!/usr/bin/env python3
"""
fetch_exports.py — USDA FAS Weekly Export Sales updater.
Parses weeksumm.htm for latest weekly data (exports + net sales).
Appends new weeks to existing export_sales.json (incremental update).
Runs in GitHub Actions daily at 4:30pm CT (Mon-Fri).
 
NOTE: This is an append-only updater. The existing export_sales.json
contains historical data from prior runs. This script adds new weeks
as they become available. If export_sales.json is missing, it creates
a minimal skeleton and starts accumulating from scratch.
"""
import os, sys, json, re, urllib.request
from datetime import datetime, timezone
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "export_sales.json")
 
WEEKSUM_URL = "https://apps.fas.usda.gov/export-sales/weeksumm.htm"
 
# Commodity patterns to extract from weeksumm.htm
# Format: regex pattern → (key, my_start_month, MY label)
COMMODITIES = {
    "wheat":    {"pattern": r"ALL WHEAT\s+", "my_start": 6, "my_label": "Jun-May"},
    "corn":     {"pattern": r"CORN\s+",      "my_start": 9, "my_label": "Sep-Aug"},
    "soybeans": {"pattern": r"SOYBEANS\s+",  "my_start": 9, "my_label": "Sep-Aug"},
}
 
 
def get_marketing_year(dt, my_start):
    """Return MY string like '25/26' from a date and MY start month."""
    if dt.month >= my_start:
        y1 = dt.year
    else:
        y1 = dt.year - 1
    y2 = y1 + 1
    return f"{y1 % 100:02d}/{y2 % 100:02d}"
 
 
def week_of_my(dt, my_start):
    """Return week number (1-52) within the marketing year."""
    if dt.month >= my_start:
        my_start_date = datetime(dt.year, my_start, 1)
    else:
        my_start_date = datetime(dt.year - 1, my_start, 1)
    delta = (dt - my_start_date).days
    return max(1, min(52, delta // 7 + 1))
 
 
def fetch_weeksumm():
    """Fetch and parse weeksumm.htm, return dict of latest data per commodity."""
    print(f"  Fetching {WEEKSUM_URL}...")
    req = urllib.request.Request(WEEKSUM_URL, headers={
        "User-Agent": "Mozilla/5.0 (USDA-Export-Sales-Fetcher)"
    })
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
 
    # Extract text between <pre> tags or just use the whole thing
    pre_match = re.search(r"<pre[^>]*>(.*?)</pre>", raw, re.DOTALL | re.IGNORECASE)
    text = pre_match.group(1) if pre_match else raw
 
    # Clean HTML entities
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
 
    results = {}
 
    for key, cfg in COMMODITIES.items():
        pat = cfg["pattern"]
        # Find all lines matching this commodity
        # Format: "ALL WHEAT      : 01/29       399.6  ...  403.8    5301.6"
        #         "               : 02/05       498.0  ...  580.0    5209.6"
        # We want: week_ending, new_sales, exports, outstanding_sales
 
        # Find the commodity section
        matches = re.finditer(
            pat + r":\s+(\d{2}/\d{2})\s+([\d,.*]+(?:\s+[\d,.*]+){4})",
            text
        )
        # Also find continuation lines (start with spaces + colon)
        # The commodity name line has the name, continuation has spaces
        section_start = re.search(pat, text)
        if not section_start:
            print(f"    {key}: pattern not found in weeksumm")
            continue
 
        # Get all data lines for this commodity
        # Pattern: optional commodity name, colon, date, then 5 numbers
        pos = section_start.start()
        lines = []
 
        # Look for lines with ": MM/DD" pattern after the commodity name
        remaining = text[pos:]
        for line_match in re.finditer(
            r":\s+(\d{2}/\d{2})\s+([\d,.*\s]+?)(?=\n|$)", remaining
        ):
            date_str = line_match.group(1)
            nums_str = line_match.group(2).strip()
            # Parse numbers (space-separated, may have commas)
            nums = []
            for n in nums_str.split():
                n = n.replace(",", "").strip()
                if n == "*" or not n:
                    nums.append(0.0)
                else:
                    try:
                        nums.append(float(n))
                    except ValueError:
                        nums.append(0.0)
 
            if len(nums) >= 5:
                lines.append({
                    "date_str": date_str,
                    "new_sales": nums[0],    # NEW SALES
                    "from_foreign": nums[1],  # PURCHASES FROM FOREIGN
                    "buybacks": nums[2],      # BUY-BACKS & CANCELLATIONS
                    "exports": nums[3],       # EXPORTS
                    "outstanding": nums[4],   # OUTSTANDING SALES
                })
 
            # Stop after 2 data lines (current + previous week)
            if len(lines) >= 2:
                break
 
        if not lines:
            print(f"    {key}: no data lines found")
            continue
 
        # Use the LATEST line (last in the pair)
        latest = lines[-1]
 
        # Resolve full date (assume current year context)
        now = datetime.now(timezone.utc)
        mm, dd = latest["date_str"].split("/")
        # Try current year first, then previous
        try_year = now.year
        dt = datetime(try_year, int(mm), int(dd))
        # If date is in the future by more than 30 days, use previous year
        if (dt - now.replace(tzinfo=None)).days > 30:
            dt = datetime(try_year - 1, int(mm), int(dd))
 
        my = get_marketing_year(dt, cfg["my_start"])
        wk = week_of_my(dt, cfg["my_start"])
 
        results[key] = {
            "date": dt,
            "my": my,
            "week": wk,
            "exports": latest["exports"],
            "net_sales": latest["new_sales"] - latest["from_foreign"] - latest["buybacks"],
            "outstanding": latest["outstanding"],
            "my_label": cfg["my_label"],
        }
        print(f"    {key}: MY {my}, Week {wk}, Exports={latest['exports']:.1f}, "
              f"NetSales={results[key]['net_sales']:.1f}, Outstanding={latest['outstanding']:.1f}")
 
    return results
 
 
def load_existing():
    """Load existing export_sales.json or create skeleton."""
    if os.path.exists(OUT):
        with open(OUT) as f:
            return json.load(f)
 
    print("  No existing export_sales.json — creating skeleton")
    return {}
 
 
def ensure_commodity(data, key, my_label):
    """Ensure commodity structure exists in data."""
    if key not in data:
        data[key] = {
            "insp": {"MY": my_label, "years": [], "w": {}},
            "sales": {"years": [], "w": {}},
        }
    return data[key]
 
 
def append_week(data, key, parsed):
    """Append a new week's data to the commodity arrays."""
    comm = ensure_commodity(data, key, parsed["my_label"])
    my = parsed["my"]
    wk = parsed["week"]
    idx = wk - 1  # 0-indexed
 
    # Ensure MY exists in years list
    for section in ["insp", "sales"]:
        sec = comm[section]
        if my not in sec.get("years", []):
            sec.setdefault("years", []).append(my)
            sec["years"].sort(key=lambda s: int(s.split("/")[0]) + (2000 if int(s.split("/")[0]) < 80 else 1900))
        if my not in sec.get("w", {}):
            sec["w"][my] = [None] * 52
 
    # Set values (only if not already set for this week)
    insp_arr = comm["insp"]["w"][my]
    sales_arr = comm["sales"]["w"][my]
 
    if idx < 52:
        if insp_arr[idx] is None:
            insp_arr[idx] = round(parsed["exports"], 1)
            sales_arr[idx] = round(parsed["net_sales"], 1)
            print(f"    {key}: Added week {wk} ({my})")
            return True
        else:
            print(f"    {key}: Week {wk} ({my}) already exists, skipping")
            return False
 
    return False
 
 
def main():
    print("=" * 60)
    print("FETCH_EXPORTS — USDA FAS Weekly Export Sales (incremental)")
    print("=" * 60)
 
    # Load existing data
    data = load_existing()
 
    # Fetch latest from weeksumm.htm
    try:
        parsed = fetch_weeksumm()
    except Exception as e:
        print(f"ERROR fetching weeksumm: {e}", file=sys.stderr)
        sys.exit(1)
 
    if not parsed:
        print("ERROR: No commodity data parsed.", file=sys.stderr)
        sys.exit(1)
 
    # Append new weeks
    updated = False
    for key, p in parsed.items():
        if append_week(data, key, p):
            updated = True
 
    # Update meta
    data["_meta"] = {"fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
 
    # Save
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(data, f, separators=(",", ":"))
 
    sz = os.path.getsize(OUT)
    print(f"\n{'Updated' if updated else 'No new data.'}")
    print(f"Saved {OUT} ({sz:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
