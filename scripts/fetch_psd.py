#!/usr/bin/env python3
"""
Basic USDA FAS PSD fetcher — grains only (Wheat, Corn, Soybeans, Canola).
Bulk CSV download from PSD Online, no API key required.
Saves data/psd_data.json.
"""
import os, sys, json, urllib.request, urllib.error, csv, io, zipfile, re, subprocess, html as html_lib
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "psd_data.json")
 
ATTR_MAP = {
    "Area Harvested": "ah", "Beginning Stocks": "bs",
    "Domestic Consumption": "dc", "Ending Stocks": "es",
    "Exports": "ex", "Feed Dom. Consumption": "fd",
    "FSI Consumption": "fi", "Imports": "im",
    "Production": "pr", "Yield": "yl",
    "TY Exports": "te", "TY Imports": "ti",
    "Total Distribution": "td", "Total Supply": "ts",
    "Feed Domestic Consumption": "fd",
    "Total Dom. Consumption": "dc",
    "Domestic Use": "dc",  # confirmed: this is what Cotton actually uses (not "Domestic Consumption")
    "Human Dom. Consumption": "dc",  # confirmed: this is what Sugar actually uses
}
 
SUM_ATTRS = {"ah", "bs", "dc", "es", "ex", "fd", "fi", "im", "pr", "te", "ti", "td", "ts"}
 
COMM_MAP = {
    "Wheat": "Wheat",
    "Corn": "Corn",
    "Soybeans": "Soybeans", "Soybean Oilseed": "Soybeans", "Oilseed, Soybean": "Soybeans",
    "Rapeseed": "Rapeseed/Canola", "Oilseed, Rapeseed": "Rapeseed/Canola", "Canola": "Rapeseed/Canola",
    "Oats": "Oats",
    "Oil, Palm": "Palm Oil",
    "Oil, Soybean": "Soybean Oil",
    "Oil, Rapeseed": "Rapeseed Oil",
    "Oil, Sunflowerseed": "Sunflower Oil",
    "Meal, Soybean": "Soybean Meal",
    "Meal, Rapeseed": "Rapeseed Meal",
    "Meal, Sunflowerseed": "Sunflower Meal",
    "Cotton": "Cotton",
    "Coffee, Green": "Coffee",        # confirmed exact string from a prior fetch log
    "Sugar, Centrifugal": "Sugar",    # confirmed exact string from a prior fetch log
    # Note: Cocoa is NOT tracked by USDA PSD at all (confirmed — no cocoa-related string
    # showed up anywhere in the "Unmapped" commodity list either). That's ICCO's domain,
    # not USDA's. Don't re-add a Cocoa entry here.
}
 
COUNTRIES = {
    "Algeria", "Argentina", "Australia", "Bangladesh", "Brazil", "Cameroon", "Canada",
    "China", "Colombia", "Cote d'Ivoire", "Ecuador", "Egypt", "Ethiopia", "European Union",
    "Ghana", "Honduras", "India", "Indonesia",
    "Iran", "Japan", "Kazakhstan", "Malaysia", "Mexico", "Morocco", "Nigeria",
    "Pakistan", "Paraguay", "Philippines", "Russia", "South Korea", "Taiwan",
    "Thailand", "Turkey", "Ukraine", "United States", "Vietnam",
}
 
COUNTRY_MAP = {
    "European Union (EU-27)": "European Union",
    "European Union-27": "European Union",
    "EU-27": "European Union", "EU27": "European Union",
    "Korea, South": "South Korea", "Korea, Republic of": "South Korea",
}
 
MIN_YEAR = 2015
BASE = "https://apps.fas.usda.gov/psdonline/downloads"
 
 
def download_zip(filename):
    url = f"{BASE}/{filename}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=180) as resp:
        return resp.read()
 
 
# --- "Data as of" for the USDA numbers: the latest WASDE release this pull can reflect -------------
# The PSD bulk file doesn't say which release its numbers come from (its Calendar_Year/Month columns
# look like marketing-year fields, not release dates), so the release is inferred from USDA's published
# WASDE schedule: the latest release that was out (noon ET + ~1h for PSD to update) when we pulled.
# Entirely non-fatal: any problem here just means the page shows no WASDE date, plus a loud warning.
WASDE_URLS = [
    "https://www.usda.gov/about-usda/general-information/staff-offices/office-chief-economist/commodity-markets/wasde-report",
    "https://www.usda.gov/oce/commodity/wasde",
]
# Used only if the page can't be read. Both years confirmed against usda.gov's own schedule (12:00 ET).
FALLBACK_WASDE_SCHEDULE = [
    "2026-01-12", "2026-02-10", "2026-03-10", "2026-04-09", "2026-05-12", "2026-06-11",
    "2026-07-10", "2026-08-12", "2026-09-11", "2026-10-09", "2026-11-10", "2026-12-10",
    "2027-01-12", "2027-02-10", "2027-03-10", "2027-04-09", "2027-05-12", "2027-06-11",
    "2027-07-09", "2027-08-12", "2027-09-10", "2027-10-08", "2027-11-10", "2027-12-10",
]
WASDE_PSD_LATENCY = timedelta(hours=1)
_MONTHS = {m: i for i, m in enumerate(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
 
 
def parse_wasde_schedule(page_html):
    """Pull every 'In YYYY the WASDE report will be released on Jan. 12, Feb. 10, ...' sentence."""
    text = html_lib.unescape(re.sub(r"<[^>]+>", " ", page_html))
    text = re.sub(r"\s+", " ", text)
    found = set()
    for m in re.finditer(r"In (\d{4}) the WASDE report will be released on", text):
        year = int(m.group(1))
        window = text[m.end(): m.end() + 260]
        got, last_month = [], 0
        for tok in re.finditer(r"([A-Z][a-z]{2})[a-z]*\.?\s+(\d{1,2})(?!\d)", window):
            mon = _MONTHS.get(tok.group(1).lower())
            if mon is None or mon <= last_month:   # months must keep increasing; stops at the next sentence
                break
            last_month = mon
            try:
                got.append(date(year, mon, int(tok.group(2))))
            except ValueError:
                break
        if len(got) >= 6:                           # a real full-year list, not a stray fragment
            found.update(got)
    return sorted(found)
 
 
def fetch_wasde_schedule():
    last_err = None
    for url in WASDE_URLS:
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                sched = parse_wasde_schedule(resp.read().decode("utf-8", errors="replace"))
            if len(sched) >= 12:
                return sched
            last_err = f"{url}: only {len(sched)} release dates found"
        except Exception as e:
            last_err = f"{url}: {type(e).__name__}: {e}"
    raise ValueError(last_err or "no schedule found")
 
 
def resolve_wasde_release(schedule, now_utc):
    """Latest scheduled release (noon ET) that was already out, plus PSD update latency, at now_utc."""
    et = ZoneInfo("America/New_York")
    out = [d for d in schedule if datetime(d.year, d.month, d.day, 12, 0, tzinfo=et) + WASDE_PSD_LATENCY <= now_utc]
    return max(out) if out else None
 
 
def get_wasde_meta(now_utc):
    """Never raises. Returns (meta_fields, schedule, release_date) describing which WASDE the pulled data reflects."""
    problem = None
    try:
        schedule, source = fetch_wasde_schedule(), "usda.gov WASDE page"
    except Exception as e:
        schedule, source = [date.fromisoformat(d) for d in FALLBACK_WASDE_SCHEDULE], "built-in schedule"
        problem = f"could not read the WASDE schedule from usda.gov ({e}); using the built-in 2026-2027 schedule"
    release = resolve_wasde_release(schedule, now_utc)
    if release is None:
        problem = (problem + "; " if problem else "") + "no WASDE release found on or before today"
    elif now_utc.date() - release > timedelta(days=45):
        problem = (problem + "; " if problem else "") + f"latest known WASDE ({release}) is over 45 days old - schedule may be out of date"
    if problem:
        print(f"WARNING: WASDE date: {problem}", file=sys.stderr)
        print(f"::warning title=USDA data-as-of date::{problem}")
    print(f"WASDE release this data reflects: {release} (source: {source})")
    return {"wasde_release": release.isoformat() if release else None, "wasde_source": source}, schedule, release
 
 
# --- Prior WASDE snapshot -------------------------------------------------------------------------
# PSD only publishes the latest estimates, so "what did the previous WASDE say?" comes from this repo's own
# history: the workflow commits data/psd_data.json every day, so the version committed just BEFORE the current
# WASDE came out holds the previous release's numbers. Only the latest market year per commodity is kept (that is
# the column the tables compare). Non-fatal: if git history isn't available the page just shows no prior column.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PSD_REPO_PATH = "data/psd_data.json"
PRIOR_KEYS = ("ah", "bs", "pr", "im", "dc", "ex", "es")
 
 
def _git(*args):
    return subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, timeout=90)
 
 
def get_prior_snapshot(result, current_release, schedule):
    """Never raises. Returns the _prior block, or None (after a loud warning) if it can't be built."""
    problem = None
    try:
        if current_release is None:
            raise ValueError("current WASDE release unknown")
        cutoff = datetime(current_release.year, current_release.month, current_release.day, 12, 0,
                          tzinfo=ZoneInfo("America/New_York"))
        log = _git("log", f"--until={cutoff.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
                   "-n", "1", "--format=%H", "--", PSD_REPO_PATH)
        commit = log.stdout.strip()
        if log.returncode != 0 or not commit:
            raise ValueError("no earlier commit of psd_data.json found - the workflow checkout must use fetch-depth: 0 "
                             f"(git said: {log.stderr.strip() or 'nothing'})")
        shown = _git("show", f"{commit}:{PSD_REPO_PATH}")
        if shown.returncode != 0:
            raise ValueError(f"could not read {PSD_REPO_PATH} at {commit[:10]}: {shown.stderr.strip()}")
        old = json.loads(shown.stdout)
        fetched = (old.get("_meta") or {}).get("fetched_at")
        if not fetched:
            raise ValueError(f"commit {commit[:10]} has no fetched_at - can't tell which release it holds")
        fetched_dt = datetime.strptime(fetched, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        prior_release = resolve_wasde_release(schedule, fetched_dt)
        if prior_release is None or prior_release >= current_release:
            raise ValueError(f"commit {commit[:10]} (pulled {fetched}) does not hold an earlier release than {current_release}")
 
        data, n_rows = {}, 0
        for comm, countries in result.items():
            if comm.startswith("_") or comm not in old:
                continue
            years = {y for yrs in countries.values() for y in yrs}
            if not years:
                continue
            latest = max(years)
            for country, yrs in countries.items():
                rec = ((old.get(comm) or {}).get(country) or {}).get(latest)
                if not rec:
                    continue
                slim = {k: rec[k] for k in PRIOR_KEYS if k in rec}
                if slim:
                    data.setdefault(comm, {}).setdefault(country, {})[latest] = slim
                    n_rows += 1
        if not data:
            raise ValueError(f"commit {commit[:10]} shares no commodity/country rows with the current data")
        print(f"Prior WASDE snapshot: release {prior_release} from commit {commit[:10]} (pulled {fetched}) - "
              f"{len(data)} commodities, {n_rows} country rows")
        return {"release": prior_release.isoformat(), "commit": commit[:10], "fetched_at": fetched, "data": data}
    except Exception as e:
        problem = f"{type(e).__name__}: {e}"
    print(f"WARNING: prior-WASDE comparison unavailable - {problem}", file=sys.stderr)
    print(f"::warning title=Prior WASDE comparison unavailable::{problem}")
    return None
 
 
def parse_csv_zip(zipdata, result, world_by_country, unmapped_attrs):
    count = 0
    unseen = set()
    zf = zipfile.ZipFile(io.BytesIO(zipdata))
    for fname in zf.namelist():
        if not fname.lower().endswith('.csv'):
            continue
        print(f"    Parsing {fname}...", end=" ", flush=True)
        fc = 0
        with zf.open(fname) as f:
            text = io.TextIOWrapper(f, encoding='utf-8-sig')
            reader = csv.DictReader(text)
            for row in reader:
                comm = row.get("Commodity_Description", "")
                if comm not in COMM_MAP:
                    unseen.add(comm)
                    continue
                comm_name = COMM_MAP[comm]
 
                country = row.get("Country_Name", "")
                country = COUNTRY_MAP.get(country, country)
 
                attr = row.get("Attribute_Description", "")
                short = ATTR_MAP.get(attr)
                if not short:
                    # Track attributes we're dropping, per mapped commodity, so a commodity
                    # reporting a field under an unexpected name (like Cotton's consumption
                    # figure) is actually visible instead of silently discarded.
                    unmapped_attrs.setdefault(comm_name, set()).add(attr)
                    continue
 
                year = row.get("Market_Year", "")
                try:
                    if int(year) < MIN_YEAR:
                        continue
                except (ValueError, TypeError):
                    continue
 
                value = row.get("Value", "0")
                if value:
                    value = value.replace(",", "").strip()
                try:
                    v = float(value) if value else 0
                except (ValueError, TypeError):
                    continue
 
                val = round(v, 2) if short == "yl" else int(round(v))
 
                if short in SUM_ATTRS and country != "World":
                    world_by_country.setdefault(comm_name, {}).setdefault(year, {}).setdefault(short, {})[country] = val
 
                if country in COUNTRIES:
                    result.setdefault(comm_name, {}).setdefault(country, {}).setdefault(year, {})
                    result[comm_name][country][year][short] = val
                    fc += 1
 
        print(f"{fc} records")
        count += fc
 
    if unseen:
        print(f"    Unmapped (ignored, not in scope): {sorted(unseen)}")
    return count
 
 
def main():
    print("Fetching FAS PSD data (grains-only, bulk CSV)...")
 
    files_to_try = ["psd_alldata_csv.zip", "psd_grains_csv.zip", "psd_oilseeds_csv.zip"]
 
    result = {}
    world_by_country = {}
    unmapped_attrs = {}
    downloaded = []
 
    for filename in files_to_try:
        try:
            print(f"  {filename}...", end=" ", flush=True)
            data = download_zip(filename)
            print(f"OK ({len(data):,} bytes)")
            count = parse_csv_zip(data, result, world_by_country, unmapped_attrs)
            if count > 0:
                downloaded.append(filename)
            if filename == "psd_alldata_csv.zip" and count > 0:
                break  # alldata covers everything we need; skip redundant category files
        except urllib.error.HTTPError as e:
            print(f"{e.code} {e.reason}")
        except Exception as e:
            print(f"error: {e}")
 
    if unmapped_attrs:
        print("\nUnmapped attributes per commodity (dropped — not in ATTR_MAP):")
        for comm_name, attrs in sorted(unmapped_attrs.items()):
            print(f"  {comm_name}: {sorted(attrs)}")
 
    if not result:
        print("\nERROR: No data fetched from any source.", file=sys.stderr)
        sys.exit(1)
 
    # World totals (deduped by country so overlapping files can't double-count)
    print("\nComputing World totals...")
    for comm, years in world_by_country.items():
        result.setdefault(comm, {})["World"] = {}
        for year, attrs in years.items():
            result[comm]["World"][year] = {
                attr: sum(countries.values())
                for attr, countries in attrs.items()
            }
 
    # Sanity check against known plausible production ranges (latest complete MY).
    # Units are commodity-specific: 1000 MT for grains/oils/meals, 1000 bales for cotton
    # (kept in cotton's native USDA unit rather than force-converting to MT).
    SANITY_RANGES = {
        "Wheat":            (700_000, 900_000),
        "Corn":             (1_100_000, 1_400_000),
        "Soybeans":         (350_000, 500_000),
        "Rapeseed/Canola":  (70_000, 110_000),
        "Oats":             (18_000, 30_000),
        "Palm Oil":         (70_000, 92_000),
        "Soybean Oil":      (60_000, 82_000),
        "Rapeseed Oil":     (28_000, 42_000),
        "Sunflower Oil":    (18_000, 28_000),
        "Soybean Meal":     (220_000, 320_000),
        "Rapeseed Meal":    (36_000, 58_000),
        "Sunflower Meal":   (18_000, 34_000),
        "Cotton":           (100_000, 145_000),
        "Coffee":           (150_000, 200_000),  # confirmed: reported in thousands of 60kg bags, not MT
        "Sugar":            (165_000, 200_000),
    }
    SANITY_UNITS = {"Cotton": "k bales", "Coffee": "k bags (60kg)"}
    print("\nSanity checking World production...")
    warnings = []
    for comm, (lo, hi) in SANITY_RANGES.items():
        unit = SANITY_UNITS.get(comm, "kMT")
        world = result.get(comm, {}).get("World", {})
        if not world:
            warnings.append(f"  {comm}: NO World rollup (missing data)")
            continue
        latest_yr, latest_pr = None, 0
        for yr in sorted(world.keys(), reverse=True):
            pr = world[yr].get("pr", 0) or 0
            if pr > 0:
                latest_yr, latest_pr = yr, pr
                break
        if latest_pr == 0:
            warnings.append(f"  {comm}: production = 0 (unexpected)")
        elif not (lo <= latest_pr <= hi):
            warnings.append(f"  {comm} {latest_yr}: production = {latest_pr:,} out of expected range [{lo:,}, {hi:,}] — POSSIBLE BUG")
        else:
            print(f"  OK {comm} {latest_yr}: {latest_pr:,} {unit} (within [{lo:,}, {hi:,}])")
 
    # Domestic consumption is required to compute Stocks/Use % — check it isn't entirely
    # missing (e.g. a commodity reporting its consumption under an attribute name not yet
    # in ATTR_MAP), separately from the production-range check above.
    print("\nSanity checking World consumption (dc) is populated...")
    for comm in COMM_MAP.values():
        world = result.get(comm, {}).get("World", {})
        if not world:
            continue
        recent_years = sorted(world.keys(), reverse=True)[:3]
        dc_values = [world[yr].get("dc") for yr in recent_years]
        if all(v is None or v == 0 for v in dc_values):
            warnings.append(f"  {comm}: World 'Domestic Consumption' (dc) is 0/missing for all of the last "
                             f"{len(recent_years)} years — Stocks/Use % will be blank. This commodity likely "
                             f"reports consumption under an attribute name not in ATTR_MAP.")
 
    if warnings:
        print("\nSANITY CHECK WARNINGS:")
        for w in warnings:
            print(w)
        # Fail loud: do not silently ship data that fails sanity checks
        sys.exit(1)
 
    meta = {"fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
    wasde_meta, schedule, release = get_wasde_meta(datetime.now(timezone.utc))
    meta.update(wasde_meta)
    prior = get_prior_snapshot(result, release, schedule)
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        result["_meta"] = meta
        if prior:
            result["_prior"] = prior
        json.dump(result, f, indent=2)
 
    size = os.path.getsize(OUT)
    comms = sorted(k for k in result.keys() if not k.startswith("_"))
    print(f"\nDownloaded from: {', '.join(downloaded)}")
    print(f"Commodities ({len(comms)}): {', '.join(comms)}")
    for c in comms:
        print(f"  {c}: {len(result[c])} entries {'(+World)' if 'World' in result[c] else ''}")
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
