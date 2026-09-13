#!/usr/bin/env python3
"""
USDA FAS Export Sales Reporting (ESR) fetcher.
Pulls country-level weekly export sales via ESRQS's static XML report —
no API key or registration required (confirmed by USDA-FAS for ESRQS).
 
Source: https://apps.fas.usda.gov/esrqs/StaticReports/CWRCountryCommoditySummary.xml
This report auto-updates each week with the current and prior report week,
and includes built-in prior-marketing-year comparison fields, so no
historical accumulation is needed for a YoY view.
 
Note: the exact attribute names on this endpoint have not been directly
confirmed (only the sibling CWRCommoditySummary.xml — national totals,
no country breakdown — has been verified). This script is written
defensively: if the expected attributes aren't found, it prints every
attribute actually present on a sample record and exits non-zero, so a
mismatch is caught and fixed in one pass rather than shipped silently.
"""
import os, sys, json, urllib.request, urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "export_sales.json")
URL = "https://apps.fas.usda.gov/esrqs/StaticReports/CWRCountryCommoditySummary.xml"
 
# Only keep commodities whose name contains one of these keywords.
# Matching by keyword (not hardcoded numeric code) since exact codes for
# every class weren't independently confirmed this session.
COMMODITY_KEYWORDS = ["WHEAT", "CORN", "SOYBEAN", "OATS"]
 
# Candidate attribute names per logical field — the country-level report's
# exact attribute spelling wasn't directly confirmed, so try several.
FIELD_CANDIDATES = {
    "country_code": ["CountryCode", "Countrycode", "CtryCode"],
    "country_name": ["CountryName", "Countryname", "CtryName"],
    "commodity_code": ["CommodityCode"],
    "commodity_name": ["CommodityName"],
    "period_ending": ["PeriodEndingDate"],
    "mkt_year": ["MarketingYear"],
    "mkt_year_week": ["MarketingYearWeekNumber"],
    "beginning_balance": ["BeginningBalance"],
    "new_sales": ["NewSales"],
    "net_sales": ["NetSales"],
    "outstanding_sales": ["OutstandingSales"],
    "weekly_exports": ["WeeklyExports"],
    "accumulated_exports": ["AccumulatedExports"],
    "total_commitment": ["TotalCommitment"],
    "prev_accumulated_exports": ["PreviousMKTYearAccumulatedExports"],
    "prev_outstanding_sales": ["PreviousMKTYearOutstandingSales"],
}
 
 
def fetch_xml(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://apps.fas.usda.gov/esrqs/",
    })
    with urllib.request.urlopen(req, timeout=90) as resp:
        return resp.read()
 
 
def local_tag(elem):
    """Strip XML namespace prefix, e.g. '{ns}Details' -> 'Details'."""
    return elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
 
 
def get_field(attrib, candidates):
    for name in candidates:
        if name in attrib:
            return attrib[name]
    return None
 
 
def to_num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None
 
 
def yoy_pct(cur, prev):
    if cur is None or prev is None or prev == 0:
        return None
    return (cur - prev) / abs(prev) * 100
 
 
def main():
    print(f"Fetching USDA FAS Export Sales (country-level) from {URL} ...")
    try:
        raw = fetch_xml(URL)
    except urllib.error.HTTPError as e:
        print(f"ERROR: HTTP {e.code} {e.reason}", file=sys.stderr)
        try:
            body = e.read(2000).decode("utf-8", errors="replace")
            print("Response body (first 2000 chars):", file=sys.stderr)
            print(body, file=sys.stderr)
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
 
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        print(f"ERROR: Could not parse XML: {e}", file=sys.stderr)
        sys.exit(1)
 
    details = [el for el in root.iter() if local_tag(el) == "Details"]
    if not details:
        print("ERROR: No <Details> records found in the XML — report structure may have changed.", file=sys.stderr)
        sys.exit(1)
 
    # Fail loud with full diagnostics if the expected attributes aren't present,
    # rather than silently mapping to the wrong field or defaulting to None everywhere.
    sample_attrib = details[0].attrib
    required_min = ["commodity_name", "net_sales", "outstanding_sales", "accumulated_exports"]
    missing = [f for f in required_min if get_field(sample_attrib, FIELD_CANDIDATES[f]) is None]
    if missing:
        print("ERROR: Expected attributes not found on sample record. Missing logical fields:", file=sys.stderr)
        for f in missing:
            print(f"  - {f} (tried: {FIELD_CANDIDATES[f]})", file=sys.stderr)
        print("\nAttributes actually present on the first <Details> record:", file=sys.stderr)
        for k, v in sample_attrib.items():
            print(f"  {k} = {v!r}", file=sys.stderr)
        sys.exit(1)
 
    if get_field(sample_attrib, FIELD_CANDIDATES["country_name"]) is None:
        print("ERROR: No country-level field found — this may be the national-totals report, not the country-level one.", file=sys.stderr)
        print("Attributes present on the first <Details> record:", file=sys.stderr)
        for k, v in sample_attrib.items():
            print(f"  {k} = {v!r}", file=sys.stderr)
        sys.exit(1)
 
    result = {"commodities": {}}
    matched_commodities = set()
    skipped_no_country = 0
 
    for el in details:
        attrib = el.attrib
        commodity_name = get_field(attrib, FIELD_CANDIDATES["commodity_name"])
        if not commodity_name or not any(kw in commodity_name.upper() for kw in COMMODITY_KEYWORDS):
            continue
 
        country_name = get_field(attrib, FIELD_CANDIDATES["country_name"])
        if not country_name:
            skipped_no_country += 1
            continue
 
        matched_commodities.add(commodity_name)
 
        period = get_field(attrib, FIELD_CANDIDATES["period_ending"])
        week_num = to_num(get_field(attrib, FIELD_CANDIDATES["mkt_year_week"]))
 
        net_sales = to_num(get_field(attrib, FIELD_CANDIDATES["net_sales"]))
        outstanding = to_num(get_field(attrib, FIELD_CANDIDATES["outstanding_sales"]))
        accum_exports = to_num(get_field(attrib, FIELD_CANDIDATES["accumulated_exports"]))
        total_commit = to_num(get_field(attrib, FIELD_CANDIDATES["total_commitment"]))
        prev_accum = to_num(get_field(attrib, FIELD_CANDIDATES["prev_accumulated_exports"]))
        prev_outstanding = to_num(get_field(attrib, FIELD_CANDIDATES["prev_outstanding_sales"]))
 
        comm = result["commodities"].setdefault(commodity_name, {
            "code": get_field(attrib, FIELD_CANDIDATES["commodity_code"]),
            "countries": {},
            "total": None,
        })
 
        record = {
            "period_ending": period,
            "mkt_year": get_field(attrib, FIELD_CANDIDATES["mkt_year"]),
            "mkt_year_week": week_num,
            "net_sales": net_sales,
            "outstanding_sales": outstanding,
            "accumulated_exports": accum_exports,
            "total_commitment": total_commit,
            "prev_yr_accumulated_exports": prev_accum,
            "prev_yr_outstanding_sales": prev_outstanding,
            "yoy_accum_exports_pct": yoy_pct(accum_exports, prev_accum),
            "yoy_outstanding_sales_pct": yoy_pct(outstanding, prev_outstanding),
        }
 
        # "Total Known and Unknown" is the authoritative commodity-wide total (all destinations).
        # Capture it separately as the summary total; keep it out of the per-country list.
        if country_name.strip().upper() == "TOTAL KNOWN AND UNKNOWN":
            existing_total = comm.get("total")
            if existing_total is None or (week_num is not None and week_num > (existing_total.get("mkt_year_week") or -1)):
                comm["total"] = record
            continue
 
        # Skip other aggregate/rollup rows so per-country data stays clean.
        if country_name.strip().upper() in ("TOTAL KNOWN", "TOTAL UNKNOWN",
                                             "OPTIONAL ORIGIN", "EXPORTS FOR OWN ACCT", "UNKNOWN"):
            continue
 
        existing = comm["countries"].get(country_name)
        # Keep only the latest period per country (report includes 2 weeks; take the newer).
        if existing is None or (week_num is not None and week_num > (existing.get("mkt_year_week") or -1)):
            comm["countries"][country_name] = record
 
    if not matched_commodities:
        print("ERROR: No commodities matched the expected keywords (WHEAT, CORN, SOYBEAN, OATS).", file=sys.stderr)
        print("Sample commodity names found in the file:", file=sys.stderr)
        sample_names = sorted({get_field(d.attrib, FIELD_CANDIDATES["commodity_name"]) for d in details[:200]
                                if get_field(d.attrib, FIELD_CANDIDATES["commodity_name"])})
        for n in sample_names[:20]:
            print(f"  - {n}", file=sys.stderr)
        sys.exit(1)
 
    if skipped_no_country > 0 and skipped_no_country == len(details):
        print("ERROR: Every record was missing a country name — check FIELD_CANDIDATES['country_name'].", file=sys.stderr)
        sys.exit(1)
 
    missing_totals = [name for name, data in result["commodities"].items() if data["total"] is None]
    if missing_totals:
        print("ERROR: No 'Total Known and Unknown' row found for these matched commodities "
              "(needed for the summary table) — the aggregate row's country-name text may have "
              "changed on USDA's end:", file=sys.stderr)
        for n in missing_totals:
            print(f"  - {n}", file=sys.stderr)
        sys.exit(1)
 
    result["_meta"] = {
        "fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": URL,
    }
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, indent=2)
 
    size = os.path.getsize(OUT)
    print(f"\nCommodities matched ({len(result['commodities'])}):")
    for name, data in result["commodities"].items():
        print(f"  {name}: {len(data['countries'])} countries, YTD accum exports={data['total']['accumulated_exports']}")
    print(f"\nSaved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
