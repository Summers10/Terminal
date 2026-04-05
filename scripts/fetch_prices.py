#!/usr/bin/env python3
"""
fetch_prices.py — Daily commodity price updater
Fetches settlements from Yahoo Finance, computes 10 spreads,
appends to historical spreads_data.json, recalculates rolling stats.
Runs in GitHub Actions daily at 4:30pm CT (Mon-Fri).
"""
 
import json, os, sys, math
from datetime import datetime, timezone, timedelta
from pathlib import Path
 
# ---------------------------------------------------------------------------
# Install yfinance if missing
# ---------------------------------------------------------------------------
try:
    import yfinance as yf
except ImportError:
    os.system(f"{sys.executable} -m pip install yfinance --break-system-packages -q")
    import yfinance as yf
 
DATA_DIR = Path(__file__).resolve().parent / "data"
SPREADS_FILE = DATA_DIR / "spreads_data.json"
 
# ---------------------------------------------------------------------------
# Yahoo Finance tickers
# ---------------------------------------------------------------------------
TICKERS = {
    'ZS': 'ZS=F',       # Soybeans ¢/bu
    'ZC': 'ZC=F',       # Corn ¢/bu
    'ZW': 'ZW=F',       # CBOT Wheat SRW ¢/bu
    'KE': 'KE=F',       # KC Wheat HRW ¢/bu
    'ZM': 'ZM=F',       # Soybean Meal $/short ton
    'ZL': 'ZL=F',       # Soybean Oil ¢/lb
    'RS': 'RS=F',       # ICE Canola CAD/MT
    'CL': 'CL=F',       # WTI Crude $/bbl
    'HO': 'HO=F',       # Heating Oil $/gal
    'RB': 'RB=F',       # RBOB Gasoline $/gal
    'CADUSD': 'CADUSD=X',
}
 
 
def fetch_prices():
    """Fetch latest settlement prices from Yahoo Finance."""
    symbols = list(TICKERS.values())
    print(f"Fetching {len(symbols)} symbols from Yahoo Finance...")
    data = yf.download(symbols, period='5d', auto_adjust=True, progress=False)
 
    prices = {}
    for key, ticker in TICKERS.items():
        try:
            col = data['Close'][ticker] if len(symbols) > 1 else data['Close']
            close = col.dropna()
            if len(close) > 0:
                prices[key] = float(close.iloc[-1])
                print(f"  {key:8s} ({ticker:12s}): {prices[key]:.4f}")
        except Exception as e:
            print(f"  {key:8s} ({ticker:12s}): FAILED - {e}")
 
    return prices
 
 
def compute_spreads(px):
    """Compute all 10 spread values from raw prices."""
    sp = {}
 
    # 1. Bean/Corn ratio
    if 'ZS' in px and 'ZC' in px and px['ZC'] > 0:
        sp['bean_corn_ratio'] = round(px['ZS'] / px['ZC'], 4)
 
    # 2. KC - CBOT Wheat (¢/bu)
    if 'KE' in px and 'ZW' in px:
        sp['kc_cbot_spread'] = round(px['KE'] - px['ZW'], 4)
 
    # 3. Wheat - Corn (¢/bu)
    if 'ZW' in px and 'ZC' in px:
        sp['wheat_corn_spread'] = round(px['ZW'] - px['ZC'], 4)
 
    # 4. KC Wheat - Corn (¢/bu)
    if 'KE' in px and 'ZC' in px:
        sp['kc_corn_spread'] = round(px['KE'] - px['ZC'], 4)
 
    # 5. Canola/Bean USD ratio
    if 'RS' in px and 'ZS' in px and 'CADUSD' in px and px['ZS'] > 0:
        canola_usd_mt = px['RS'] * px['CADUSD']
        beans_usd_mt = px['ZS'] / 100 * 36.744
        if beans_usd_mt > 0:
            sp['canola_bean_usd'] = round(canola_usd_mt / beans_usd_mt, 4)
 
    # 6. Soy Crush (¢/bu): Meal×2.2 + Oil×11 - Beans
    if 'ZM' in px and 'ZL' in px and 'ZS' in px:
        sp['soy_crush'] = round(px['ZM'] * 2.2 + px['ZL'] * 11 - px['ZS'], 2)
 
    # 7. Canola Crush proxy ($/MT)
    if 'ZL' in px and 'ZM' in px and 'RS' in px and 'CADUSD' in px:
        sbo_mt = px['ZL'] * 22.0462 / 100   # ¢/lb → $/MT
        sbm_mt = px['ZM'] * 1.10231          # $/short ton → $/MT
        canola_usd = px['RS'] * px['CADUSD']
        sp['canola_crush'] = round(sbo_mt * 0.43 + sbm_mt * 0.57 - canola_usd, 4)
 
    # 8. Oil Share (%)
    if 'ZL' in px and 'ZM' in px:
        oil_val = px['ZL'] * 11
        meal_val = px['ZM'] * 2.2
        total = oil_val + meal_val
        if total > 0:
            sp['oil_share'] = round(oil_val / total * 100, 4)
 
    # 9. SBO / Heating Oil ratio
    if 'ZL' in px and 'HO' in px and px['HO'] > 0:
        sp['soyoil_heat'] = round(px['ZL'] / (px['HO'] * 100), 4)
 
    # 10. 3:2:1 Crack ($/bbl)
    if 'RB' in px and 'HO' in px and 'CL' in px:
        sp['crack_321'] = round((2 * px['RB'] * 42 + px['HO'] * 42 - 3 * px['CL']) / 3, 4)
 
    return sp
 
 
def iso_week(date_str):
    """Get ISO week number from YYYY-MM-DD string."""
    d = datetime.strptime(date_str[:10], '%Y-%m-%d')
    return d.isocalendar()[1]
 
 
def update_spreads(data, new_values, date_str):
    """Update spreads_data.json with new values."""
    today_week = iso_week(date_str)
 
    for key, val in new_values.items():
        sp = data['spreads'].get(key)
        if not sp:
            continue
 
        prev_latest = sp.get('latest')
        sp['latest'] = val
        sp['latest_date'] = date_str
 
        # Update series_w (append on Fridays or if last entry is >6 days old)
        series = sp.get('series_w', [])
        step = sp.get('series_step', 5)
        last_gap = sp.get('series_last_gap', step)
 
        # Always update the most recent value if same week
        cur_weekly = sp.get('cur_weekly', [])
        found_week = False
        for i, cw in enumerate(cur_weekly):
            if cw[0] == today_week:
                cur_weekly[i][1] = val
                found_week = True
                break
        if not found_week:
            cur_weekly.append([today_week, val])
            cur_weekly.sort(key=lambda x: x[0])
        sp['cur_weekly'] = cur_weekly
 
        # Append to series on Fridays (weekday 4)
        today_dt = datetime.strptime(date_str[:10], '%Y-%m-%d')
        if today_dt.weekday() == 4:  # Friday
            series.append(val)
            sp['series_w'] = series
            sp['series_last_gap'] = step  # normal gap
 
        # Recalculate changes
        n = len(series)
        if n >= 2:
            sp['chg_1w'] = round(val - series[-2], 4)
        if n >= 5:
            sp['chg_4w'] = round(val - series[-5], 4)
 
        # Recalculate 5-year rolling stats from series
        # 5 years ≈ 260 weekly observations
        lookback = series[-260:] if len(series) > 260 else series
        if len(lookback) > 10:
            sp['mean5'] = round(sum(lookback) / len(lookback), 4)
            variance = sum((x - sp['mean5'])**2 for x in lookback) / len(lookback)
            sp['std5'] = round(math.sqrt(variance), 4)
            sp['min5'] = round(min(lookback), 4)
            sp['max5'] = round(max(lookback), 4)
 
            # Percentile
            below = sum(1 for x in lookback if x <= val)
            sp['pctile'] = round(below / len(lookback) * 100, 1)
 
            # Z-score
            if sp['std5'] > 0:
                sp['zscore'] = round((val - sp['mean5']) / sp['std5'], 2)
 
        print(f"  {key}: {val} (chg_1w: {sp.get('chg_1w','?')}, pctile: {sp.get('pctile','?')})")
 
    return data
 
 
def main():
    print("=" * 60)
    print("FETCH_PRICES — Yahoo Finance → Spreads Pipeline")
    print("=" * 60)
 
    # Load existing data
    if not SPREADS_FILE.exists():
        print(f"ERROR: {SPREADS_FILE} not found")
        sys.exit(1)
 
    with open(SPREADS_FILE) as f:
        data = json.load(f)
 
    print(f"Loaded {len(data['spreads'])} spreads, {len(data.get('correlations', {}))} correlations")
 
    # Fetch prices
    prices = fetch_prices()
    if len(prices) < 8:
        print(f"WARNING: Only got {len(prices)} prices, need at least 8. Skipping update.")
        sys.exit(0)
 
    # Compute spreads
    new_sp = compute_spreads(prices)
    print(f"\nComputed {len(new_sp)} spreads:")
 
    # Today's date
    now = datetime.now(timezone(timedelta(hours=-5)))  # CT
    date_str = now.strftime('%Y-%m-%d')
 
    # Update
    data = update_spreads(data, new_sp, date_str)
 
    # Add meta
    data['_meta'] = {
        'fetched_at': datetime.now(timezone.utc).isoformat(),
        'prices': {k: round(v, 4) for k, v in prices.items()},
        'date': date_str,
    }
 
    # Save
    with open(SPREADS_FILE, 'w') as f:
        json.dump(data, f, separators=(',', ':'))
 
    sz = os.path.getsize(SPREADS_FILE)
    print(f"\nSaved {SPREADS_FILE} ({sz:,} bytes)")
    print("Done.")
 
 
if __name__ == '__main__':
    main()
 
