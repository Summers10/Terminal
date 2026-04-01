#!/usr/bin/env python3
"""Fetch USDA FAS Weekly Export Sales historical data.
Parses HTML tables from apps.fas.usda.gov/export-sales/
Saves as data/export_sales.json matching terminal EXP_INSP/EXP_SALES format."""
import os, sys, json, urllib.request, re
from datetime import datetime, timedelta
from html.parser import HTMLParser
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "export_sales.json")
 
COMMODITIES = {
    "wheat":    {"url": "https://apps.fas.usda.gov/export-sales/h107.htm", "my_start": 6},  # Jun-May
    "corn":     {"url": "https://apps.fas.usda.gov/export-sales/h401.htm", "my_start": 9},  # Sep-Aug
    "soybeans": {"url": "https://apps.fas.usda.gov/export-sales/h801.htm", "my_start": 9},  # Sep-Aug
}
 
MY_LABELS = {6: "Jun-May", 9: "Sep-Aug"}
 
 
class TableParser(HTMLParser):
    """Extract rows from HTML tables."""
    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.rows = []
        self.current_row = []
        self.current_cell = ""
 
    def handle_starttag(self, tag, attrs):
        if tag == "table": self.in_table = True
        elif tag == "tr" and self.in_table: self.in_row = True; self.current_row = []
        elif tag == "td" and self.in_row: self.in_cell = True; self.current_cell = ""
 
    def handle_endtag(self, tag):
        if tag == "td" and self.in_cell:
            self.in_cell = False
            self.current_row.append(self.current_cell.strip())
        elif tag == "tr" and self.in_row:
            self.in_row = False
            if self.current_row:
                self.rows.append(self.current_row)
        elif tag == "table":
            self.in_table = False
 
    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data
 
 
def parse_number(s):
    """Parse number from FAS format: '1,234,567' or '(1,234)' for negatives."""
    s = s.strip().replace(",", "").replace("\xa0", "")
    if not s or s == "*" or s == "N/A":
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    try:
        v = float(s)
        return -v if neg else v
    except ValueError:
        return None
 
 
def parse_date(s):
    """Parse date from MM/DD/YYYY format."""
    s = s.strip()
    for fmt in ["%m/%d/%Y", "%m/%d/%y"]:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None
 
 
def get_marketing_year(dt, my_start_month):
    """Return marketing year string like '24/25' given a date and MY start month."""
    if dt.month >= my_start_month:
        y1 = dt.year
    else:
        y1 = dt.year - 1
    y2 = y1 + 1
    return f"{y1 % 100:02d}/{y2 % 100:02d}"
 
 
def fetch_commodity(key, cfg):
    """Fetch and parse one commodity's historical data."""
    url = cfg["url"]
    my_start = cfg["my_start"]
 
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "text/html,*/*",
    })
 
    print(f"  Fetching {key} from {url}...", end=" ", flush=True)
    with urllib.request.urlopen(req, timeout=60) as resp:
        html = resp.read().decode("utf-8", errors="replace")
    print(f"{len(html):,} bytes")
 
    parser = TableParser()
    parser.feed(html)
 
    # Parse rows: [week_ending, weekly_exports, accum_exports, net_sales, outstanding_sales, ...]
    data_rows = []
    for row in parser.rows:
        if len(row) < 5:
            continue
        dt = parse_date(row[0])
        if dt is None:
            continue
        weekly_exports = parse_number(row[1])
        net_sales = parse_number(row[3])
        if weekly_exports is None and net_sales is None:
            continue
        my = get_marketing_year(dt, my_start)
        data_rows.append({
            "date": dt,
            "my": my,
            "weekly_exports": weekly_exports,
            "net_sales": net_sales,
        })
 
    if not data_rows:
        print(f"    WARNING: No data parsed for {key}")
        return None
 
    # Group by marketing year, keep last 3 years
    by_my = {}
    for r in data_rows:
        by_my.setdefault(r["my"], []).append(r)
 
    all_mys = sorted(by_my.keys())
    recent_mys = all_mys[-3:] if len(all_mys) >= 3 else all_mys
 
    # Build weekly arrays (convert MT to 1000 MT, round to 1 decimal)
    insp_years = {}
    sales_years = {}
 
    for my in recent_mys:
        rows = sorted(by_my[my], key=lambda r: r["date"])
        insp_years[my] = [round(r["weekly_exports"] / 1000, 1) if r["weekly_exports"] is not None else None for r in rows]
        sales_years[my] = [round(r["net_sales"] / 1000, 1) if r["net_sales"] is not None else None for r in rows]
 
    year_labels = recent_mys[-2:]  # Current and previous MY
    my_label = MY_LABELS.get(my_start, f"Month{my_start}")
 
    result = {
        "insp": {"MY": my_label, "years": year_labels, "w": {}},
        "sales": {"years": year_labels, "w": {}},
    }
 
    for yr in year_labels:
        # Pad to 52 weeks
        iw = insp_years.get(yr, [])
        sw = sales_years.get(yr, [])
        while len(iw) < 52: iw.append(None)
        while len(sw) < 52: sw.append(None)
        result["insp"]["w"][yr] = iw[:52]
        result["sales"]["w"][yr] = sw[:52]
 
    print(f"    {key}: {len(data_rows)} total rows, MYs: {', '.join(recent_mys)}")
    return result
 
 
def main():
    print("Fetching USDA FAS Export Sales data...")
    results = {}
 
    for key, cfg in COMMODITIES.items():
        try:
            data = fetch_commodity(key, cfg)
            if data:
                results[key] = data
        except Exception as e:
            print(f"    ERROR {key}: {e}")
 
    if not results:
        print("ERROR: No export data fetched.", file=sys.stderr)
        sys.exit(1)
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(results, f, separators=(",", ":"))
 
    size = os.path.getsize(OUT)
    print(f"\nCommodities: {', '.join(results.keys())}")
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
