import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo  # ✅ For local timezone
import os

# --- Config ---
API_TOKEN = os.environ["ESIOS_API_TOKEN"]
BASE_URL = "https://api.esios.ree.es/indicators/540"  # ✅ Wind indicator ID
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- Time setup: Spain local time → UTC for API ---
now_local = datetime.now(ZoneInfo("Europe/Madrid")).replace(second=0, microsecond=0)
start_local = now_local.replace(hour=0, minute=0)

# Convert both to UTC for the API
now_utc = now_local.astimezone(timezone.utc)
start_utc = start_local.astimezone(timezone.utc)

# File path by local date
today_str = start_local.strftime("%Y-%m-%d")
daily_file = f"data/{today_str}.csv"
os.makedirs("data", exist_ok=True)

# Load existing data if file exists
df_existing = pd.DataFrame()
if os.path.exists(daily_file):
    df_existing = pd.read_csv(daily_file, parse_dates=["datetime"])

# --- Fetch from REE API ---
params = {
    "start_date": start_utc.isoformat(),
    "end_date": now_utc.isoformat(),
    "time_trunc": "quarter-hour"
}

print(f"📡 Fetching wind data from {start_local} (Spain) to {now_local} (Spain)...")

res = requests.get(BASE_URL, headers=HEADERS, params=params)
res.raise_for_status()
data = res.json()["indicator"]["values"]

# --- Process new data ---
df_new = pd.DataFrame(data)
df_new["datetime"] = pd.to_datetime(df_new["datetime"])  # already UTC
df_new = df_new.sort_values("datetime")

# --- Combine and deduplicate ---
df_combined = pd.concat([df_existing, df_new])
df_combined = df_combined.drop_duplicates(subset=["datetime"]).sort_values("datetime")

# --- Save ---
df_combined.to_csv(daily_file, index=False)

print(f"✅ Synced {len(df_combined)} rows to {daily_file}")
