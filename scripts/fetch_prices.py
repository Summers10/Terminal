#!/usr/bin/env python3
"""
fetch_prices.py — Daily commodity price updater
Fetches weekly Friday settlement prices from Yahoo Finance.
Outputs data/prices.json in the same format as _SEAS_ENC in index.html.
Also updates data/spreads_data.json with latest spread values.
Runs in GitHub Actions daily Mon-Fri.
"""
 
import json, os, sys, math
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
 
try:
    import yfinance as yf
except ImportError:
    os.system(f"{sys.executable} -m pip install yfinance --break-system-packages -q")
    import yfinance as yf
 
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
PRICES_FILE  = DATA_DIR / "prices.json"
SPREADS_FILE = DATA_DIR / "spreads_data.json"
 
# ---------------------------------------------------------------------------
# Commodity config: marketing year start month (1=Jan) and unit label
# Non-agricultural commodities (metals, softs, meats, energy) use Jan-Dec.
# Grains/oilseeds use their traditional marketing years.
# ---------------------------------------------------------------------------
SYMBOLS = {
    # Grains & Oilseeds
    'ZC': {'name':'Corn',          'unit':'¢/bu',     'exchange':'CBOT', 'my_start':9,  'my':'Sep–Aug', 'ticker':'ZC=F'},
    'ZW': {'name':'Wheat SRW',     'unit':'¢/bu',     'exchange':'CBOT', 'my_start':6,  'my':'Jun–May', 'ticker':'ZW=F'},
    'KE': {'name':'Wheat HRW',     'unit':'¢/bu',     'exchange':'KCBT', 'my_start':6,  'my':'Jun–May', 'ticker':'KE=F'},
    'MW': {'name':'Wheat HRS',     'unit':'¢/bu',     'exchange':'MGEX', 'my_start':6,  'my':'Jun–May', 'ticker':'MW=F'},
    'ZO': {'name':'Oats',          'unit':'¢/bu',     'exchange':'CBOT', 'my_start':7,  'my':'Jul–Jun', 'ticker':'ZO=F'},
    'ZS': {'name':'Soybeans',      'unit':'¢/bu',     'exchange':'CBOT', 'my_start':9,  'my':'Sep–Aug', 'ticker':'ZS=F'},
    'ZM': {'name':'Soybean Meal',  'unit':'$/ton',    'exchange':'CBOT', 'my_start':10, 'my':'Oct–Sep', 'ticker':'ZM=F'},
    'ZL': {'name':'Soybean Oil',   'unit':'¢/lb',     'exchange':'CBOT', 'my_start':10, 'my':'Oct–Sep', 'ticker':'ZL=F'},
    'RS': {'name':'Canola',        'unit':'C$/MT',    'exchange':'ICE',  'my_start':8,  'my':'Aug–Jul', 'ticker':'RS=F'},
 
    # Energy
    'CL': {'name':'WTI Crude',     'unit':'$/bbl',    'exchange':'NYMEX','my_start':1,  'my':'Jan–Dec', 'ticker':'CL=F'},
    'HO': {'name':'Heating Oil',   'unit':'¢/gal',    'exchange':'NYMEX','my_start':1,  'my':'Jan–Dec', 'ticker':'HO=F'},
    'RB': {'name':'RBOB Gasoline', 'unit':'¢/gal',    'exchange':'NYMEX','my_start':1,  'my':'Jan–Dec', 'ticker':'RB=F'},
    'NG': {'name':'Natural Gas',   'unit':'$/MMBtu',  'exchange':'NYMEX','my_start':4,  'my':'Apr–Mar', 'ticker':'NG=F'},
 
    # Softs
    'CT': {'name':'Cotton',        'unit':'¢/lb',     'exchange':'ICE',  'my_start':8,  'my':'Aug–Jul', 'ticker':'CT=F'},
    'CC': {'name':'Cocoa',         'unit':'$/MT',     'exchange':'ICE',  'my_start':10, 'my':'Oct–Sep', 'ticker':'CC=F'},
    'SB': {'name':'Sugar #11',     'unit':'¢/lb',     'exchange':'ICE',  'my_start':10, 'my':'Oct–Sep', 'ticker':'SB=F'},
    'KC': {'name':'Coffee',        'unit':'¢/lb',     'exchange':'ICE',  'my_start':10, 'my':'Oct–Sep', 'ticker':'KC=F'},
 
    # Livestock
    'LH': {'name':'Lean Hogs',     'unit':'¢/lb',     'exchange':'CME',  'my_start':1,  'my':'Jan–Dec', 'ticker':'HE=F'},
    'LC': {'name':'Live Cattle',   'unit':'¢/lb',     'exchange':'CME',  'my_start':1,  'my':'Jan–Dec', 'ticker':'LE=F'},
    'FC': {'name':'Feeder Cattle', 'unit':'¢/lb',     'exchange':'CME',  'my_start':1,  'my':'Jan–Dec', 'ticker':'GF=F'},
 
    # Metals
    'GC': {'name':'Gold',          'unit':'$/oz',     'exchange':'COMEX','my_start':1,  'my':'Jan–Dec', 'ticker':'GC=F'},
    'SI': {'name':'Silver',        'unit':'$/oz',     'exchange':'COMEX','my_start':1,  'my':'Jan–Dec', 'ticker':'SI=F'},
    'HG': {'name':'Copper',        'unit':'$/lb',     'exchange':'COMEX','my_start':1,  'my':'Jan–Dec', 'ticker':'HG=F'},
    'PL': {'name':'Platinum',      'unit':'$/oz',     'exchange':'NYMEX','my_start':1,  'my':'Jan–Dec', 'ticker':'PL=F'},
    'PA': {'name':'Palladium',     'unit':'$/oz',     'exchange':'NYMEX','my_start':1,  'my':'Jan–Dec', 'ticker':'PA=F'},
}
 
# Spread-only (not in seasonal charts but needed for spreads_data)
SPREAD_ONLY = {'CADUSD': 'CADUSD=X'}
 
def marketing_year_label(sym_key, dt):
    """Return 'YYYY-YY' marketing year label for a given date."""
    cfg = SYMBOLS[sym_key]
    ms = cfg['my_start']
    if dt.month >= ms:
        y1, y2 = dt.year, dt.year + 1
    else:
        y1, y2 = dt.year - 1, dt.year
    return f"{y1}-{str(y2)[-2:]}"
 
def day_of_my(sym_key, dt):
    """Return day number within the marketing year (used for x-axis in _SEAS_ENC delta encoding)."""
    cfg = SYMBOLS[sym_key]
    ms = cfg['my_start']
    if dt.month >= ms:
        my_start_dt = date(dt.year, ms, 1)
    else:
        my_start_dt = date(dt.year - 1, ms, 1)
    return (dt - my_start_dt).days
 
def fetch_all():
    """Fetch 6 years of daily history for all symbols."""
    all_tickers = [v['ticker'] for v in SYMBOLS.values()] + list(SPREAD_ONLY.values())
    print(f"Fetching {len(all_tickers)} tickers (6yr daily)...")
    raw = yf.download(all_tickers, period='6y', interval='1d', auto_adjust=True, progress=False)
    return raw
 
def build_prices_json(raw, existing=None):
    """
    Build prices.json: weekly Friday closes organized by marketing year.
    Format matches _SEAS_ENC structure expected by index.html:
    { SYM: { name, unit, exchange, my, data: { 'YYYY-YY': [[day, price], ...] } } }
 
    If `existing` is provided and a symbol returns fewer than MIN_POINTS_TO_OVERWRITE
    fresh points (flaky Yahoo feed, e.g. RS=F / MW=F occasional outages), the
    existing historical entry is preserved instead of being overwritten with
    empty data. This prevents silent wipe of years of accumulated history when
    Yahoo's feed breaks for a run.
    """
    MIN_POINTS_TO_OVERWRITE = 20
    out = {}
    stale_syms = []
    closes = raw['Close'] if 'Close' in raw.columns.get_level_values(0) else raw
 
    for sym, cfg in SYMBOLS.items():
        ticker = cfg['ticker']
        try:
            series = closes[ticker].dropna()
        except Exception as e:
            print(f"  {sym}: FAILED - {e}")
            series = None
 
        if series is not None:
            # Keep only Fridays (weekday==4)
            series = series[series.index.weekday == 4]
 
        # Flaky fetch -> preserve existing history rather than wiping
        if series is None or len(series) < MIN_POINTS_TO_OVERWRITE:
            prev = (existing or {}).get(sym)
            if prev and prev.get('data'):
                fresh = 0 if series is None else len(series)
                prev_pts = sum(len(v) for v in prev['data'].values())
                print(f"  {sym}: only {fresh} fresh pts - KEEPING existing ({prev_pts} pts across {len(prev['data'])} MYs)")
                out[sym] = prev
                stale_syms.append(sym)
                continue
            print(f"  {sym}: 0 marketing years, 0 weekly points (no existing history)")
            out[sym] = {
                'name': cfg['name'], 'unit': cfg['unit'], 'exchange': cfg['exchange'],
                'my': cfg['my'], 'data': {}
            }
            continue
 
        # Group by marketing year
        by_my = {}
        for dt, price in series.items():
            if hasattr(dt, 'date'):
                d = dt.date()
            else:
                d = dt
            label = marketing_year_label(sym, d)
            day   = day_of_my(sym, d)
            val   = round(float(price), 4)
            if label not in by_my:
                by_my[label] = []
            by_my[label].append([day, val])
 
        for label in by_my:
            by_my[label].sort(key=lambda x: x[0])
 
        out[sym] = {
            'name':     cfg['name'],
            'unit':     cfg['unit'],
            'exchange': cfg['exchange'],
            'my':       cfg['my'],
            'data':     by_my
        }
        n_pts = sum(len(v) for v in by_my.values())
        print(f"  {sym}: {len(by_my)} marketing years, {n_pts} weekly points")
 
    if stale_syms:
        print(f"\n  NOTE: preserved existing history for {len(stale_syms)} stale ticker(s): {', '.join(stale_syms)}")
 
    return out
 
def get_latest(raw, ticker):
    """Get the most recent close for a ticker."""
    closes = raw['Close'] if 'Close' in raw.columns.get_level_values(0) else raw
    try:
        s = closes[ticker].dropna()
        return float(s.iloc[-1]) if len(s) else None
    except:
        return None
 
def compute_spreads(px):
    sp = {}
    if 'ZS' in px and 'ZC' in px and px['ZC']:
        sp['bean_corn_ratio'] = round(px['ZS'] / px['ZC'], 4)
    if 'KE' in px and 'ZW' in px:
        sp['kc_cbot_spread'] = round(px['KE'] - px['ZW'], 4)
    if 'ZW' in px and 'ZC' in px:
        sp['wheat_corn_spread'] = round(px['ZW'] - px['ZC'], 4)
    if 'KE' in px and 'ZC' in px:
        sp['kc_corn_spread'] = round(px['KE'] - px['ZC'], 4)
    if 'RS' in px and 'ZS' in px and 'CADUSD' in px and px['ZS']:
        canola_usd = px['RS'] * px['CADUSD']
        beans_usd  = px['ZS'] / 100 * 36.744
        if beans_usd:
            sp['canola_bean_usd'] = round(canola_usd / beans_usd, 4)
    if 'ZM' in px and 'ZL' in px and 'ZS' in px:
        sp['soy_crush'] = round(px['ZM'] * 2.2 + px['ZL'] * 11 - px['ZS'], 2)
    if 'ZL' in px and 'ZM' in px and 'RS' in px and 'CADUSD' in px:
        sbo_mt = px['ZL'] * 22.0462 / 100
        sbm_mt = px['ZM'] * 1.10231
        canola_usd = px['RS'] * px['CADUSD']
        sp['canola_crush'] = round(sbo_mt * 0.43 + sbm_mt * 0.57 - canola_usd, 4)
    if 'ZL' in px and 'ZM' in px:
        oil_val = px['ZL'] * 11
        meal_val = px['ZM'] * 2.2
        total = oil_val + meal_val
        if total:
            sp['oil_share'] = round(oil_val / total * 100, 4)
    if 'ZL' in px and 'HO' in px and px['HO']:
        sp['soyoil_heat'] = round(px['ZL'] / (px['HO'] * 100), 4)
    if 'RB' in px and 'HO' in px and 'CL' in px:
        sp['crack_321'] = round((2 * px['RB'] * 42 + px['HO'] * 42 - 3 * px['CL']) / 3, 4)
    return sp
 
def update_spreads(raw, spread_data, date_str):
    """Update spreads_data.json with latest prices."""
    if not spread_data:
        return spread_data
 
    # Get latest prices
    px = {}
    for sym, cfg in SYMBOLS.items():
        v = get_latest(raw, cfg['ticker'])
        if v is not None:
            px[sym] = v
    for k, ticker in SPREAD_ONLY.items():
        v = get_latest(raw, ticker)
        if v is not None:
            px[k] = v
 
    new_sp = compute_spreads(px)
    today_dt = datetime.strptime(date_str[:10], '%Y-%m-%d')
    today_week = today_dt.isocalendar()[1]
 
    for key, val in new_sp.items():
        sp = spread_data.get('spreads', {}).get(key)
        if not sp:
            continue
        sp['latest'] = val
        sp['latest_date'] = date_str
 
        series = sp.get('series_w', [])
        cur_weekly = sp.get('cur_weekly', [])
        found = False
        for i, cw in enumerate(cur_weekly):
            if cw[0] == today_week:
                cur_weekly[i][1] = val; found = True; break
        if not found:
            cur_weekly.append([today_week, val])
            cur_weekly.sort(key=lambda x: x[0])
        sp['cur_weekly'] = cur_weekly
 
        if today_dt.weekday() == 4:  # Friday
            series.append(val)
            sp['series_w'] = series
 
        n = len(series)
        if n >= 2: sp['chg_1w'] = round(val - series[-2], 4)
        if n >= 5: sp['chg_4w'] = round(val - series[-5], 4)
 
        lookback = series[-260:] if len(series) > 260 else series
        if len(lookback) > 10:
            mean = sum(lookback) / len(lookback)
            sp['mean5'] = round(mean, 4)
            variance = sum((x - mean)**2 for x in lookback) / len(lookback)
            sp['std5'] = round(math.sqrt(variance), 4)
            sp['min5'] = round(min(lookback), 4)
            sp['max5'] = round(max(lookback), 4)
            below = sum(1 for x in lookback if x <= val)
            sp['pctile'] = round(below / len(lookback) * 100, 1)
            if sp['std5'] > 0:
                sp['zscore'] = round((val - mean) / sp['std5'], 2)
        print(f"  {key}: {val}")
 
    spread_data['_meta'] = {
        'fetched_at': datetime.now(timezone.utc).isoformat(),
        'prices': {k: round(v, 4) for k, v in px.items()},
        'date': date_str,
    }
    return spread_data
 
def main():
    print("=" * 60)
    print("FETCH_PRICES — Yahoo Finance → prices.json + spreads_data.json")
    print("=" * 60)
 
    raw = fetch_all()
    now_ct = datetime.now(timezone(timedelta(hours=-5)))
    date_str = now_ct.strftime('%Y-%m-%d')
 
    # 1. Load existing prices.json so we can preserve history on flaky fetches
    existing = {}
    if PRICES_FILE.exists():
        try:
            with open(PRICES_FILE) as f:
                existing = json.load(f)
        except Exception as e:
            print(f"  WARN: could not parse existing prices.json ({e}); starting fresh")
 
    # 2. Build and save prices.json
    prices = build_prices_json(raw, existing=existing)
    prices['_meta'] = {
        'fetched_at': datetime.now(timezone.utc).isoformat(),
        'symbols': list(SYMBOLS.keys()),
        'date': date_str,
    }
    with open(PRICES_FILE, 'w') as f:
        json.dump(prices, f, separators=(',', ':'))
    sz = os.path.getsize(PRICES_FILE)
    print(f"\nSaved prices.json ({sz:,} bytes)")
 
    # 2. Update spreads_data.json if it exists
    if SPREADS_FILE.exists():
        with open(SPREADS_FILE) as f:
            spread_data = json.load(f)
        spread_data = update_spreads(raw, spread_data, date_str)
        with open(SPREADS_FILE, 'w') as f:
            json.dump(spread_data, f, separators=(',', ':'))
        print(f"Updated spreads_data.json")
    else:
        print("spreads_data.json not found — skipping spreads update")
 
    print("\nDone.")
 
if __name__ == '__main__':
    main()
 
 
