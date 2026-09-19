#!/usr/bin/env python3
"""
Weather fetcher for major grain-growing regions — Open-Meteo (no API key required).
 
Two API calls total, both batched across all 9 regions in one request each:
  1. Forecast API with past_days + forecast_days: gives ~95 days of recent
     actuals plus a 16-day forward forecast in one continuous daily series.
  2. Archive API: ~10 years of historical daily data, used to compute
     "normal" for the same calendar window in each of the past 10 years,
     then averaged — the baseline everything else is compared against.
 
Soil moisture is intentionally NOT included in this version — the exact
Open-Meteo daily variable name for a root-zone aggregate wasn't confirmed
this session, and shipping a guessed field risked a silently-wrong column.
"""
import os, sys, json, urllib.request, urllib.error
from datetime import date, timedelta
 
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "weather_data.json")
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
 
REGIONS = {
    "Alberta (Calgary area)": (51.0, -114.1),
    "Saskatchewan (Regina area)": (50.5, -104.6),
    "Manitoba": (49.8, -99.9),
    "US Midwest (Iowa)": (42.0, -93.5),
    "US Southern Plains (Kansas)": (38.5, -98.0),
    "US Cotton Belt (Texas)": (33.5, -101.5),
    "Brazil (Mato Grosso)": (-13.0, -56.0),
    "Argentina (Pampas)": (-33.0, -61.0),
    "Ukraine": (49.0, 32.0),
    "Southern Russia": (45.0, 40.0),
    "France": (48.5, 2.5),
    "Western Australia": (-31.5, 117.0),
}
 
HISTORY_YEARS = 10
PAST_DAYS = 92       # Open-Meteo's documented max for this parameter; still covers the 90-day trailing window
FORECAST_DAYS = 16   # Open-Meteo's max; we only use the first 14
 
 
def http_get_json(url, params):
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    full_url = f"{url}?{qs}"
    req = urllib.request.Request(full_url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=90) as resp:
        return json.loads(resp.read().decode("utf-8"))
 
 
def fetch_forecast_block(names, lats, lons):
    params = {
        "latitude": ",".join(str(x) for x in lats),
        "longitude": ",".join(str(x) for x in lons),
        "daily": "precipitation_sum,temperature_2m_max,temperature_2m_min",
        "past_days": PAST_DAYS,
        "forecast_days": FORECAST_DAYS,
        "timezone": "UTC",
    }
    data = http_get_json(FORECAST_URL, params)
    # Multi-location requests return a JSON array, one object per location, in the same order requested.
    if isinstance(data, dict):
        data = [data]
    if len(data) != len(names):
        print(f"ERROR: Forecast API returned {len(data)} locations, expected {len(names)}.", file=sys.stderr)
        sys.exit(1)
    return data
 
 
def fetch_archive_block(names, lats, lons, start_date, end_date):
    params = {
        "latitude": ",".join(str(x) for x in lats),
        "longitude": ",".join(str(x) for x in lons),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "precipitation_sum,temperature_2m_max,temperature_2m_min",
        "timezone": "UTC",
    }
    data = http_get_json(ARCHIVE_URL, params)
    if isinstance(data, dict):
        data = [data]
    if len(data) != len(names):
        print(f"ERROR: Archive API returned {len(data)} locations, expected {len(names)}.", file=sys.stderr)
        sys.exit(1)
    return data
 
 
def series_to_dict(block):
    """Turn one location's {daily: {time:[], precipitation_sum:[], ...}} into date->values."""
    daily = block.get("daily")
    if not daily or "time" not in daily:
        return {}
    out = {}
    for i, d in enumerate(daily["time"]):
        out[d] = {
            "precip": daily.get("precipitation_sum", [None] * len(daily["time"]))[i],
            "tmax": daily.get("temperature_2m_max", [None] * len(daily["time"]))[i],
            "tmin": daily.get("temperature_2m_min", [None] * len(daily["time"]))[i],
        }
    return out
 
 
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
 
 
def main():
    print("Fetching weather data (Open-Meteo, no API key) for major grain-growing regions...")
    names = list(REGIONS.keys())
    lats = [REGIONS[n][0] for n in names]
    lons = [REGIONS[n][1] for n in names]
 
    today = date.today()
 
    print(f"  Forecast API (past {PAST_DAYS}d + next {FORECAST_DAYS}d, {len(names)} regions)...", end=" ", flush=True)
    try:
        fc_blocks = fetch_forecast_block(names, lats, lons)
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} {e.reason}", file=sys.stderr)
        try:
            print("Response body:", e.read(2000).decode("utf-8", errors="replace"), file=sys.stderr)
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
    print("OK")
 
    archive_start = date(today.year - HISTORY_YEARS, 1, 1)
    archive_end = today - timedelta(days=1)  # archive doesn't include today
    print(f"  Archive API ({HISTORY_YEARS}yr history, {len(names)} regions)...", end=" ", flush=True)
    try:
        hist_blocks = fetch_archive_block(names, lats, lons, archive_start, archive_end)
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} {e.reason}", file=sys.stderr)
        try:
            print("Response body:", e.read(2000).decode("utf-8", errors="replace"), file=sys.stderr)
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
    print("OK")
 
    result = {"regions": {}}
    missing_regions = []
 
    for i, name in enumerate(names):
        fc_series = series_to_dict(fc_blocks[i])
        hist_series = series_to_dict(hist_blocks[i])
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
 
        # --- Historical normals: same calendar window, averaged across each of the past N years ---
        precip_30d_normals, precip_90d_normals, temp_7d_normals = [], [], []
        temp_14d_fwd_normals, precip_14d_fwd_normals = [], []
        for yr_offset in range(1, HISTORY_YEARS + 1):
            try:
                anchor = today.replace(year=today.year - yr_offset)
            except ValueError:
                anchor = today.replace(year=today.year - yr_offset, day=28)  # Feb 29 in a non-leap year
 
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
 
        def avg(lst):
            return (sum(lst) / len(lst)) if lst else None
 
        precip_30d_normal = avg(precip_30d_normals)
        precip_90d_normal = avg(precip_90d_normals)
        temp_7d_normal = avg(temp_7d_normals)
        temp_14d_fwd_normal = avg(temp_14d_fwd_normals)
        precip_14d_fwd_normal = avg(precip_14d_fwd_normals)
 
        # 14-day forecast temperature extremes (actual forecast hi/lo, not an average)
        temp_14d_hi, temp_14d_lo = window_extreme_temps(fc_series, today + timedelta(days=1), today + timedelta(days=14))
 
        result["regions"][name] = {
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
        }
 
    if missing_regions:
        print(f"ERROR: No usable weather series for: {missing_regions}", file=sys.stderr)
        sys.exit(1)
 
    # Fail loud: every region should have produced at least the two precip-vs-normal figures.
    broken = [n for n, d in result["regions"].items()
              if d["precip_30d_pct_normal"] is None and d["precip_90d_pct_normal"] is None]
    if broken:
        print(f"ERROR: All precip metrics came back empty for: {broken} — check API response shape.", file=sys.stderr)
        sys.exit(1)
 
    result["_meta"] = {
        "fetched_at": __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "Open-Meteo (no API key)",
        "history_years": HISTORY_YEARS,
    }
 
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, indent=2)
 
    size = os.path.getsize(OUT)
    print(f"\nRegions fetched ({len(result['regions'])}):")
    for name, d in result["regions"].items():
        p30 = d["precip_30d_pct_normal"]
        print(f"  {name}: 30D precip = {p30:.0f}% of normal" if p30 is not None else f"  {name}: 30D precip = n/a")
    print(f"\nSaved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
 
