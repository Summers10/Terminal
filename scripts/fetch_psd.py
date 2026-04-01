#!/usr/bin/env python3
"""
Fetch USDA FAS PSD (Production, Supply & Distribution) data.
Saves as data/psd_data.json in the same format as the embedded _PSD_RAW.
Required env var: USDA_API_KEY
"""
import os, sys, json, urllib.request, urllib.parse, time
 
API_KEY = os.environ.get("USDA_API_KEY", "")
BASE = "https://apps.fas.usda.gov/OpenData/api/psd"
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "psd_data.json")
 
# FAS commodity codes → display names (matching terminal _PSD_RAW keys)
COMMODITIES = {
    "0410000": "Wheat",
    "0440000": "Corn",
    "2222000": "Soybeans",
    "2223100": "Soybean Meal",
    "2224000": "Soybean Oil",
    "2226000": "Rapeseed/Canola",
    "2631000": "Cotton",
    "0111000": "Beef and Veal",
}
 
# Attribute description → short code
ATTR_MAP = {
    "Area Harvested": "ah",
    "Beginning Stocks": "bs",
    "Domestic Consumption": "dc",
    "Ending Stocks": "es",
    "Exports": "ex",
    "Feed Dom. Consumption": "fd",
    "FSI Consumption": "fi",
    "Imports": "im",
    "Production": "pr",
    "Yield": "yl",
    "TY Exports": "te",
    "TY Imports": "ti",
    "Total Distribution": "td",
    "Total Supply": "ts",
    # Alternative spellings the API might use
    "Feed Domestic Consumption": "fd",
    "Food, Seed, and Industrial": "fi",
    "Total Dom. Cons.": "dc",
    "Domestic Cons.": "dc",
}
 
# Countries to include (matching terminal display names)
COUNTRIES = {
    "Argentina", "Australia", "Brazil", "Canada", "China",
    "Egypt", "European Union", "India", "Indonesia", "Japan",
    "Kazakhstan", "Mexico", "Pakistan", "Russia", "South Africa",
    "Thailand", "Turkey", "Ukraine", "United Kingdom",
    "United States", "Vietnam", "World",
}
 
# Country name normalization (API name → terminal name)
COUNTRY_MAP = {
    "European Union (EU-27)": "European Union",
    "European Union-27": "European Union",
    "EU-27": "European Union",
    "Korea, South": "South Korea",
    "Congo (Kinshasa)": "Congo",
}
 
MIN_YEAR = 2015
 
 
def api_get(endpoint):
    """Make a FAS API call."""
    url = f"{BASE}/{endpoint}"
    headers = {
        "API_KEY": API_KEY,
        "Accept": "application/json",
        "User-Agent": "SUMCO-Terminal/1.0",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  API error for {endpoint}: {e}", file=sys.stderr)
        return []
 
 
def fetch_commodity(code, name):
    """Fetch all PSD data for a commodity and transform to _PSD_RAW format."""
    print(f"  Fetching {name} ({code})...")
    
    # Try different endpoint patterns
    records = api_get(f"commodity/{code}")
    if not records:
        # Try alternative endpoint
        records = api_get(f"commodityData?commodityCode={code}")
    
    if not records:
        print(f"    WARNING: No data returned for {name}")
        return {}
    
    print(f"    Got {len(records)} records")
    
    # Log first record to help debug field names
    if records and len(records) > 0:
        r = records[0]
        # Try to identify field names
        fields = list(r.keys()) if isinstance(r, dict) else []
        print(f"    Fields: {fields[:10]}...")
    
    data = {}
    for r in records:
        if not isinstance(r, dict):
            continue
            
        # Extract fields - try common FAS API field names
        country = (r.get("countryDescription") or r.get("country_Name") or 
                   r.get("Country_Name") or r.get("country_name") or "")
        year = (r.get("marketYear") or r.get("market_Year") or 
                r.get("Market_Year") or r.get("year") or 0)
        attr = (r.get("attributeDescription") or r.get("attribute_Description") or
                r.get("Attribute_Description") or r.get("attribute_name") or "")
        value = (r.get("value") or r.get("Value") or 0)
        
        # Normalize country name
        country = COUNTRY_MAP.get(country, country)
        
        if country not in COUNTRIES:
            continue
        
        try:
            year = int(year)
        except (ValueError, TypeError):
            continue
            
        if year < MIN_YEAR:
            continue
        
        short = ATTR_MAP.get(attr)
        if not short:
            continue
        
        yr_str = str(year)
        if country not in data:
            data[country] = {}
        if yr_str not in data[country]:
            data[country][yr_str] = {}
        
        try:
            v = float(value) if value else 0
            # Round to 2 decimal places for yield, integer for everything else
            if short == "yl":
                data[country][yr_str][short] = round(v, 2)
            else:
                data[country][yr_str][short] = int(round(v))
        except (ValueError, TypeError):
            pass
    
    return data
 
 
def main():
    if not API_KEY:
        print("ERROR: USDA_API_KEY not set", file=sys.stderr)
        sys.exit(1)
    
    print(f"Fetching FAS PSD data...")
    result = {}
    
    for code, name in COMMODITIES.items():
        data = fetch_commodity(code, name)
        if data:
            result[name] = data
            print(f"    {name}: {len(data)} countries, years {MIN_YEAR}+")
        time.sleep(1)  # Rate limiting
    
    if not result:
        print("ERROR: No data fetched. Check API key and endpoint.", file=sys.stderr)
        sys.exit(1)
    
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, separators=(",", ":"))
    
    size = os.path.getsize(OUT)
    print(f"Saved {OUT} ({size:,} bytes)")
 
 
if __name__ == "__main__":
    main()
