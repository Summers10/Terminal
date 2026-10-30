#!/usr/bin/env python3
"""
Weather fetcher for major grain-growing regions — Open-Meteo (no API key required).
 
Calls (each batched across ALL regions in a single request):
  1. Forecast API with past_days + forecast_days: ~92 days of recent actuals plus a
     16-day forward forecast in one continuous daily series.
  2. Archive API, one request per past year (10 total): only the ~104-day window around
     today's calendar date that the "normal" calculations actually use (89 days back to
     14 days forward). Averaged across the 10 years, this is the baseline everything is
     compared against. Pulling just these windows instead of one continuous 10-year
     block is roughly 4x cheaper in Open-Meteo's weighted API-call accounting (calls are
     weighted by locations x days requested), which matters now that there are 17 regions.
 
3. Seasonal Forecast API (ECMWF SEAS5, monthly): a 3-month "outlook" per region — the ensemble-mean
   temperature anomaly, and precipitation as % of the model's own climate, over the next 3 calendar
   months. This section is deliberately NON-FATAL: if it fails, the rest of the weather data still
   publishes, the failure is printed loudly (and raised as a GitHub Actions warning annotation), and
   the page shows the outlook as unavailable. The exact monthly variable names weren't fully confirmed,
   so each variable is requested separately with candidate names — one run reveals which ones work.
 
Soil moisture is intentionally NOT included — the exact Open-Meteo daily variable name
for a root-zone aggregate wasn't confirmed, and a guessed field risked a silently-wrong column.
"""
import os, sys, json, time, urllib.request, urllib.error
from datetime import date, datetime, timedelta
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "weather_data.json")
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
 
# (division, region name, lat, lon) — this order is the order shown on the Summary page.
REGIONS = [
    ("Canada", "Alberta (Calgary)", 51.0, -114.1),
    ("Canada", "Saskatchewan (Regina)", 50.5, -104.6),
    ("Canada", "Manitoba", 49.8, -99.9),
    ("United States", "US Midwest (Iowa)", 42.0, -93.5),
    ("United States", "US Southern Plains (Kansas)", 38.5, -98.0),
    ("United States", "US Cotton Belt (Texas)", 33.5, -101.5),
    ("South America", "Brazil (Mato Grosso)", -13.0, -56.0),
    ("South America", "Brazil (Paraná)", -24.9, -53.5),
    ("South America", "Argentina (Rosario)", -33.0, -61.0),
    ("South America", "Argentina (S. Buenos Aires)", -38.0, -60.3),
    ("Europe", "France", 48.5, 2.5),
    ("Europe", "Ukraine", 49.0, 32.0),
    ("Europe", "Southern Russia", 45.0, 40.0),
    ("Asia", "China (Henan)", 34.5, 114.0),
    ("Asia", "China (Heilongjiang)", 46.0, 126.5),
    ("Asia", "Western Australia", -31.5, 117.0),
    ("Asia", "New South Wales", -32.5, 148.5),
]
 
SEASONAL_URL = "https://seasonal-api.open-meteo.com/v1/seasonal"
OUTLOOK_MONTHS = 3
# variable -> candidate API tokens, tried in order (one request per variable so one bad name can't sink the rest)
OUTLOOK_VARS = {
    "temp_anom":   ["temperature_2m_anomaly"],
    "precip_anom": ["precipitation_anomaly"],
    "precip_mean": ["precipitation_mean", "precipitation_sum"],
}
 
FRESH_HOURS = 20   # skip the fetch if the saved file is newer than this (the workflow runs several times a day)
 
HISTORY_YEARS = 10
PAST_DAYS = 92       # Open-Meteo's documented max for this parameter; still covers the 90-day trailing window
FORECAST_DAYS = 16   # Open-Meteo's max; we only use the first 14
NORMAL_LOOKBACK_DAYS = 89   # longest trailing window used (90-day, inclusive of the anchor day)
NORMAL_LOOKAHEAD_DAYS = 14  # longest forward window used (14-day forecast)
 
 
def http_get_json(url, params, max_attempts=4):
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    full_url = f"{url}?{qs}"
    req = urllib.request.Request(full_url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
    })
    # Retry transient problems with backoff:
    #   - HTTP 429 / 5xx (rate limiting, temporary server trouble)
    #   - connection-level failures (TLS handshake timeouts, resets, read timeouts) — these are
    #     network blips between the GitHub runner and Open-Meteo, not problems with the request.
    # Anything else (400 bad parameter, 404, ...) is a real problem: fail loud immediately.
    # If retries are exhausted the error is re-raised, so the run still fails loudly.
    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                wait = 15 * attempt
                print(f"\n    HTTP {e.code} (transient) — retrying in {wait}s (attempt {attempt}/{max_attempts})...", flush=True)
                time.sleep(wait)
                continue
            raise
        except OSError as e:  # URLError, ssl.SSLError, socket timeouts, ConnectionError
            if attempt < max_attempts:
                wait = 10 * attempt
                print(f"\n    network error ({e}) — retrying in {wait}s (attempt {attempt}/{max_attempts})...", flush=True)
                time.sleep(wait)
                continue
            raise
 
 
def as_location_list(data, expected, label):
    # Multi-location requests return a JSON array, one object per location, in request order.
    if isinstance(data, dict):
        data = [data]
    if len(data) != expected:
        print(f"ERROR: {label} returned {len(data)} locations, expected {expected}.", file=sys.stderr)
        sys.exit(1)
    return data
 
 
def fetch_forecast_block(n, lats, lons):
    params = {
        "latitude": ",".join(str(x) for x in lats),
        "longitude": ",".join(str(x) for x in lons),
        "daily": "precipitation_sum,temperature_2m_max,temperature_2m_min",
        "past_days": PAST_DAYS,
        "forecast_days": FORECAST_DAYS,
        "timezone": "UTC",
    }
    return as_location_list(http_get_json(FORECAST_URL, params), n, "Forecast API")
 
 
def fetch_archive_block(n, lats, lons, start_date, end_date):
    params = {
        "latitude": ",".join(str(x) for x in lats),
        "longitude": ",".join(str(x) for x in lons),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "precipitation_sum,temperature_2m_max,temperature_2m_min",
        "timezone": "UTC",
    }
    return as_location_list(http_get_json(ARCHIVE_URL, params), n, "Archive API")
 
 
def anchor_for_year(today, year):
    try:
        return today.replace(year=year)
    except ValueError:
        return today.replace(year=year, day=28)  # Feb 29 in a non-leap year
 
 
def series_to_dict(block):
    """Turn one location's {daily: {time:[], precipitation_sum:[], ...}} into date->values."""
    daily = block.get("daily")
    if not daily or "time" not in daily:
        return {}
    n = len(daily["time"])
    out = {}
    for i, d in enumerate(daily["time"]):
        out[d] = {
            "precip": daily.get("precipitation_sum", [None] * n)[i],
            "tmax": daily.get("temperature_2m_max", [None] * n)[i],
            "tmin": daily.get("temperature_2m_min", [None] * n)[i],
        }
    return out
 
 
def fetch_history(n, lats, lons, today):
    """One archive request per past year covering only the window the normals use.
    Returns one merged date->values dict per region."""
    merged = [dict() for _ in range(n)]
    for yr_offset in range(1, HISTORY_YEARS + 1):
        anchor = anchor_for_year(today, today.year - yr_offset)
        blocks = fetch_archive_block(
            n, lats, lons,
            anchor - timedelta(days=NORMAL_LOOKBACK_DAYS),
            anchor + timedelta(days=NORMAL_LOOKAHEAD_DAYS),
        )
        for i, block in enumerate(blocks):
            merged[i].update(series_to_dict(block))
        if yr_offset < HISTORY_YEARS:
            time.sleep(2)  # small pause between requests — gentler on Open-Meteo's per-minute limits
    return merged
 
 
def next_months(today, k):
    """The k calendar months AFTER the current one, as 'YYYY-MM' keys."""
    y, m = today.year, today.month
    out = []
    for _ in range(k):
        m += 1
        if m == 13:
            y, m = y + 1, 1
        out.append(f"{y:04d}-{m:02d}")
    return out
 
 
MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
 
 
def month_label(months):
    first, last = months[0], months[-1]
    return f"{MONTH_ABBR[int(first[5:7]) - 1]}\u2013{MONTH_ABBR[int(last[5:7]) - 1]}"
 
 
def fetch_seasonal_var(n, lats, lons, token):
    """One batched Seasonal API request for a single monthly variable.
    Returns (per-location list of {'YYYY-MM': value}, units string or None)."""
    params = {
        "latitude": ",".join(str(x) for x in lats),
        "longitude": ",".join(str(x) for x in lons),
        "monthly": token,
    }
    blocks = as_location_list(http_get_json(SEASONAL_URL, params), n, f"Seasonal API ({token})")
    out, units = [], None
    for block in blocks:
        monthly = block.get("monthly") or {}
        times = monthly.get("time")
        # tolerate a model-name suffix on the key (e.g. temperature_2m_anomaly_ecmwf_seas5)
        key = next((k for k in monthly if k == token or k.startswith(token + "_")), None)
        if not times or key is None:
            raise ValueError(f"response has no monthly '{token}' series (keys: {sorted(monthly.keys())})")
        out.append({t[:7]: v for t, v in zip(times, monthly[key])})
        if units is None:
            units = (block.get("monthly_units") or {}).get(key)
    return out, units
 
 
def fetch_outlook(n, lats, lons, today):
    """Seasonal outlook per region. Never raises: returns (results, label, error_or_None)."""
    months = next_months(today, OUTLOOK_MONTHS)
    label = month_label(months)
    empty = [{"outlook_temp_dep_c": None, "outlook_precip_pct_normal": None} for _ in range(n)]
    series, problems = {}, []
    try:
        for var, candidates in OUTLOOK_VARS.items():
            got = None
            for token in candidates:
                try:
                    got, units = fetch_seasonal_var(n, lats, lons, token)
                    print(f"    outlook {var}: using '{token}' (units: {units})")
                    break
                except urllib.error.HTTPError as e:
                    try:
                        body = e.read(400).decode("utf-8", errors="replace")
                    except Exception:
                        body = ""
                    print(f"    outlook {var}: '{token}' rejected, HTTP {e.code} {body}")
                except Exception as e:
                    print(f"    outlook {var}: '{token}' failed: {e}")
            series[var] = got
            if got is None:
                problems.append(var)
        results = []
        for i in range(n):
            temp_dep = precip_pct = None
            t = series.get("temp_anom")
            if t:
                vals = [t[i].get(m) for m in months]
                if all(v is not None for v in vals):
                    temp_dep = sum(vals) / len(vals)
            pa, pm = series.get("precip_anom"), series.get("precip_mean")
            if pa and pm:
                anoms = [pa[i].get(m) for m in months]
                means = [pm[i].get(m) for m in months]
                if all(v is not None for v in anoms + means):
                    normal = sum(means) - sum(anoms)   # the model's own climatology = forecast - anomaly
                    if normal > 0:
                        precip_pct = sum(means) / normal * 100
            results.append({"outlook_temp_dep_c": temp_dep, "outlook_precip_pct_normal": precip_pct})
        error = None
        if problems:
            error = "could not get: " + ", ".join(problems)
        elif all(r["outlook_temp_dep_c"] is None and r["outlook_precip_pct_normal"] is None for r in results):
            error = f"no data for months {months[0]}..{months[-1]} in the response"
        return results, label, error
    except Exception as e:
        return empty, label, f"{type(e).__name__}: {e}"
 
 
def window_sum(series, start, end, key):
    """Sum `key` over [start, end] inclusive, skipping missing days. Returns None if nothing found."""
    vals = []
    d = start
    while d <= end:
        rec = series.get(d.isoformat())
        if rec and rec.get(key) is not None:
            vals.append(rec[key])
        d += timedelta(days=1)
    return sum(vals) if vals else None
 
 
def window_avg_temp(series, start, end):
    vals = []
    d = start
    while d <= end:
        rec = series.get(d.isoformat())
        if rec and rec.get("tmax") is not None and rec.get("tmin") is not None:
            vals.append((rec["tmax"] + rec["tmin"]) / 2)
        d += timedelta(days=1)
    return (sum(vals) / len(vals)) if vals else None
 
 
def window_extreme_temps(series, start, end):
    """Highest daily max and lowest daily min actually seen/forecast over the window."""
    tmaxes, tmins = [], []
    d = start
    while d <= end:
        rec = series.get(d.isoformat())
        if rec:
            if rec.get("tmax") is not None: tmaxes.append(rec["tmax"])
            if rec.get("tmin") is not None: tmins.append(rec["tmin"])
        d += timedelta(days=1)
    return (max(tmaxes) if tmaxes else None, min(tmins) if tmins else None)
 
 
def pct_of_normal(actual, normal):
    if actual is None or normal is None or normal == 0:
        return None
    return (actual / normal) * 100
 
 
def avg(lst):
    return (sum(lst) / len(lst)) if lst else None
 
 
def saved_data_is_fresh(region_names):
    """True if data/weather_data.json is recent enough that re-fetching would only burn Open-Meteo's free allowance.
    Always refetches when forced (WEATHER_FORCE=1, set for manual workflow runs), when the region list or file shape
    has changed, or when the last run's outlook failed (so a failed outlook is retried on the next run)."""
    if os.environ.get("WEATHER_FORCE"):
        return False, "forced (WEATHER_FORCE set)"
    try:
        with open(OUT) as f:
            d = json.load(f)
        fetched = datetime.strptime(d["_meta"]["fetched_at"], "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return False, "no usable saved file"
    regions = d.get("regions") or {}
    if set(regions) != set(region_names):
        return False, "region list changed"
    if any("outlook_temp_dep_c" not in r for r in regions.values()):
        return False, "saved file predates the outlook fields"
    if (d["_meta"].get("outlook_error")):
        return False, "last run's outlook failed - retrying"
    age_h = (datetime.utcnow() - fetched).total_seconds() / 3600
    if age_h < FRESH_HOURS:
        return True, f"saved data is {age_h:.1f}h old (< {FRESH_HOURS}h)"
    return False, f"saved data is {age_h:.1f}h old"
 
 
def main():
    print("Fetching weather data (Open-Meteo, no API key) for major grain-growing regions...")
    n = len(REGIONS)
    fresh, why = saved_data_is_fresh([r[1] for r in REGIONS])
    if fresh:
        print(f"Skipping weather fetch - {why}. (Set WEATHER_FORCE=1 to refresh anyway.)")
        return
    print(f"Fetching because: {why}")
    lats = [r[2] for r in REGIONS]
    lons = [r[3] for r in REGIONS]
    today = date.today()
 
    try:
        print(f"  Forecast API (past {PAST_DAYS}d + next {FORECAST_DAYS}d, {n} regions)...", end=" ", flush=True)
        fc_blocks = fetch_forecast_block(n, lats, lons)
        print("OK")
        print(f"  Archive API ({HISTORY_YEARS} yearly windows, {n} regions)...", end=" ", flush=True)
        hist_series_list = fetch_history(n, lats, lons, today)
        print("OK")
    except urllib.error.HTTPError as e:
        print(f"\nHTTP {e.code} {e.reason}", file=sys.stderr)
        try:
            print("Response body:", e.read(2000).decode("utf-8", errors="replace"), file=sys.stderr)
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        print(f"\nerror: {e}", file=sys.stderr)
        sys.exit(1)
 
    print(f"  Seasonal outlook (SEAS5 monthly, next {OUTLOOK_MONTHS} months, {n} regions):")
    outlook, outlook_label, outlook_error = fetch_outlook(n, lats, lons, today)
    if outlook_error:
        # Non-fatal by design: the rest of the weather data still publishes. Make it impossible to miss.
        print(f"WARNING: seasonal outlook unavailable — {outlook_error}", file=sys.stderr)
        print(f"::warning title=Weather outlook unavailable::{outlook_error}")
 
    result = {"regions": {}}
    missing_regions = []
 
    for i, (division, name, _lat, _lon) in enumerate(REGIONS):
        fc_series = series_to_dict(fc_blocks[i])
        hist_series = hist_series_list[i]
        if not fc_series or not hist_series:
            missing_regions.append(name)
            continue
 
        # --- Actuals (trailing windows ending today) ---
        precip_30d = window_sum(fc_series, today - timedelta(days=29), today, "precip")
        precip_90d = window_sum(fc_series, today - timedelta(days=89), today, "precip")
        temp_7d_actual = window_avg_temp(fc_series, today - timedelta(days=6), today)
 
        # --- Forecast (forward windows starting tomorrow) ---
        temp_14d_forecast = window_avg_temp(fc_series, today + timedelta(days=1), today + timedelta(days=14))
        precip_14d_forecast = window_sum(fc_series, today + timedelta(days=1), today + timedelta(days=14), "precip")
        temp_14d_hi, temp_14d_lo = window_extreme_temps(fc_series, today + timedelta(days=1), today + timedelta(days=14))
 
        # --- Historical normals: same calendar window, averaged across each of the past N years ---
        precip_30d_normals, precip_90d_normals, temp_7d_normals = [], [], []
        temp_14d_fwd_normals, precip_14d_fwd_normals = [], []
        for yr_offset in range(1, HISTORY_YEARS + 1):
            anchor = anchor_for_year(today, today.year - yr_offset)
 
            v = window_sum(hist_series, anchor - timedelta(days=29), anchor, "precip")
            if v is not None: precip_30d_normals.append(v)
            v = window_sum(hist_series, anchor - timedelta(days=89), anchor, "precip")
            if v is not None: precip_90d_normals.append(v)
            v = window_avg_temp(hist_series, anchor - timedelta(days=6), anchor)
            if v is not None: temp_7d_normals.append(v)
            v = window_avg_temp(hist_series, anchor + timedelta(days=1), anchor + timedelta(days=14))
            if v is not None: temp_14d_fwd_normals.append(v)
            v = window_sum(hist_series, anchor + timedelta(days=1), anchor + timedelta(days=14), "precip")
            if v is not None: precip_14d_fwd_normals.append(v)
 
        precip_30d_normal = avg(precip_30d_normals)
        precip_90d_normal = avg(precip_90d_normals)
        temp_7d_normal = avg(temp_7d_normals)
        temp_14d_fwd_normal = avg(temp_14d_fwd_normals)
        precip_14d_fwd_normal = avg(precip_14d_fwd_normals)
 
        result["regions"][name] = {
            "division": division,
            "precip_30d_pct_normal": pct_of_normal(precip_30d, precip_30d_normal),
            "precip_30d_mm": precip_30d,
            "precip_90d_pct_normal": pct_of_normal(precip_90d, precip_90d_normal),
            "precip_90d_mm": precip_90d,
            "temp_dep_7d_c": (temp_7d_actual - temp_7d_normal) if (temp_7d_actual is not None and temp_7d_normal is not None) else None,
            "temp_dep_14d_forecast_c": (temp_14d_forecast - temp_14d_fwd_normal) if (temp_14d_forecast is not None and temp_14d_fwd_normal is not None) else None,
            "temp_14d_forecast_hi_c": temp_14d_hi,
            "temp_14d_forecast_lo_c": temp_14d_lo,
            "precip_14d_forecast_pct_normal": pct_of_normal(precip_14d_forecast, precip_14d_fwd_normal),
            "precip_14d_forecast_mm": precip_14d_forecast,
            "outlook_temp_dep_c": outlook[i]["outlook_temp_dep_c"],
            "outlook_precip_pct_normal": outlook[i]["outlook_precip_pct_normal"],
        }
 
    if missing_regions:
        print(f"ERROR: No usable weather series for: {missing_regions}", file=sys.stderr)
        sys.exit(1)
 
    # Fail loud: every region should have produced at least the two trailing precip-vs-normal figures.
    broken = [nm for nm, d in result["regions"].items()
              if d["precip_30d_pct_normal"] is None and d["precip_90d_pct_normal"] is None]
    if broken:
        print(f"ERROR: All precip metrics came back empty for: {broken} — check API response shape.", file=sys.stderr)
        sys.exit(1)
 
    result["_meta"] = {
        "fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "Open-Meteo (no API key)",
        "history_years": HISTORY_YEARS,
        "outlook_label": outlook_label,
        "outlook_source": "ECMWF SEAS5 via Open-Meteo Seasonal API",
        "outlook_error": outlook_error,
    }
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, indent=2)
 
    size = os.path.getsize(OUT)
    print(f"\nRegions fetched ({len(result['regions'])}):")
    cur_div = None
    for nm, d in result["regions"].items():
        if d["division"] != cur_div:
            cur_div = d["division"]
            print(f"  [{cur_div}]")
        p30 = d["precip_30d_pct_normal"]
        ot, op = d["outlook_temp_dep_c"], d["outlook_precip_pct_normal"]
        out_txt = (f"outlook {outlook_label}: temp {ot:+.1f}C, precip {op:.0f}%" if (ot is not None and op is not None)
                   else f"outlook {outlook_label}: n/a")
        print(f"    {nm}: 30D precip = {p30:.0f}% of normal | {out_txt}" if p30 is not None else f"    {nm}: 30D precip = n/a | {out_txt}")
    print(f"\nSaved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
