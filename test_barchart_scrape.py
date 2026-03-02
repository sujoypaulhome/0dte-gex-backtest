"""Test: Login to Barchart via requests POST, then call API with session cookies."""
import os
import re
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
USER = os.environ.get("BARCHART_USER", "")
PASS = os.environ.get("BARCHART_PASS", "")

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
})

# Step 1: GET login page to get CSRF token and cookies
print("Fetching login page...")
r = session.get("https://www.barchart.com/login")
print(f"Login page status: {r.status_code}")

# Extract _token from the form (Laravel CSRF)
token_match = re.search(r'name="_token"\s+value="([^"]+)"', r.text)
if token_match:
    csrf_token = token_match.group(1)
    print(f"Found CSRF _token: {csrf_token[:20]}...")
else:
    # Try alternate pattern
    token_match = re.search(r'"_token"\s*:\s*"([^"]+)"', r.text)
    if token_match:
        csrf_token = token_match.group(1)
        print(f"Found CSRF _token (alt): {csrf_token[:20]}...")
    else:
        print("WARNING: No _token found in login page")
        csrf_token = ""

print(f"Cookies after GET: {list(session.cookies.keys())}")

# Step 2: POST login
print(f"\nLogging in as {USER}...")
login_data = {
    "_token": csrf_token,
    "email": USER,
    "password": PASS,
    "remember": "on",
}

r = session.post(
    "https://www.barchart.com/login",
    data=login_data,
    headers={
        "Referer": "https://www.barchart.com/login",
        "Origin": "https://www.barchart.com",
        "Content-Type": "application/x-www-form-urlencoded",
    },
    allow_redirects=True,
)
print(f"Login POST status: {r.status_code}")
print(f"Redirected to: {r.url}")
print(f"Cookies after login: {list(session.cookies.keys())}")

# Check if login worked
if "login" in r.url.lower() and r.status_code == 200:
    # Might still be on login page with error
    if "incorrect" in r.text.lower():
        print("ERROR: Bad credentials")
    else:
        print("May have succeeded (still on login URL but no error)")

# Step 3: Refresh XSRF token by hitting the target page
print("\nFetching IV rank page to refresh XSRF...")
r = session.get("https://www.barchart.com/options/iv-rank-percentile/high")
print(f"Page status: {r.status_code}")

import urllib.parse
xsrf = session.cookies.get("XSRF-TOKEN")
if xsrf:
    xsrf_decoded = urllib.parse.unquote(xsrf)
    session.headers["X-XSRF-TOKEN"] = xsrf_decoded
    print(f"XSRF token set (decoded)")
    print(f"  raw:     {xsrf[:40]}...")
    print(f"  decoded: {xsrf_decoded[:40]}...")

# Step 4: Call API
print("\nCalling Barchart API...")
session.headers["Accept"] = "application/json"
session.headers["X-Requested-With"] = "XMLHttpRequest"
session.headers["Referer"] = "https://www.barchart.com/options/iv-rank-percentile/high"

api_url = "https://www.barchart.com/proxies/core-api/v1/options/get"
params = {
    "list": "ivRankHigh",
    "fields": "symbol,symbolName,optionsImpliedVolatilityRank1y,optionsImpliedVolatilityPercentile1y,ivHv30DayDiff,optionsTotalVolume,tradeTime",
    "orderBy": "optionsImpliedVolatilityRank1y",
    "orderDir": "desc",
    "meta": "field.shortName,field.type",
    "hasOptions": "true",
    "limit": "50",
}

r = session.get(api_url, params=params, timeout=30)
print(f"API status: {r.status_code}")

if r.status_code == 200:
    import json
    data = r.json()
    print(f"Response keys: {list(data.keys())}")
    print(f"Total count: {data.get('count', '?')}")

    rows = data.get("data", [])
    print(f"Got {len(rows)} tickers\n")

    if rows:
        # Print first row fully to see structure
        print("First row structure:")
        print(json.dumps(rows[0], indent=2)[:800])
        print("\n")

        print(f"{'Symbol':<8} {'Name':<25} {'IV Rank 1yr':>12} {'IV Pctl 1yr':>12} {'Volume':>10}")
        print("-" * 70)
        for row in rows[:25]:
            raw = row.get("raw", row)
            sym = raw.get("symbol", "?")
            name = raw.get("symbolName", "?")[:24]
            ivr = raw.get("optionsImpliedVolatilityRank1y", "?")
            ivp = raw.get("optionsImpliedVolatilityPercentile1y", "?")
            vol = raw.get("optionsTotalVolume", "?")
            print(f"{sym:<8} {name:<25} {str(ivr):>12} {str(ivp):>12} {str(vol):>10}")
    else:
        print("No data rows.")
        print(f"Full response: {json.dumps(data)[:1000]}")
else:
    print(f"Failed: {r.text[:500]}")
