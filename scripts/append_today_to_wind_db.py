import requests
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os
import duckdb

# --- CONFIG ---
API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/540"  # Wind generation
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- Time Setup ---
now_local = datetime.now(ZoneInfo("Europe/Madrid")).replace(second=0, microsecond=0)
start_local = now_local.replace(hour=0, minute=0)
now_utc = now_local.astimezone(timezone.utc)
start_utc = start_local.astimezone(timezone.utc)

# --- File Paths ---
today_str = start_local.strftime("%Y-%m-%d")
daily_file = f"data/{today_str}.csv"
os.makedirs("data", exist_ok=True)
os.makedirs("database", exist_ok=True)

# --- Load Existing Daily Data ---
df_existing = pd.read_csv(daily_file, parse_dates=["datetime"]) if os.path.exists(daily_file) else pd.DataFrame()

# --- Fetch New Data ---
params = {
    "start_date": start_utc.isoformat(),
    "end_date": now_utc.isoformat(),
    "time_trunc": "quarter-hour",
}

print(f"📡 Fetching wind data from {start_local} to {now_local}...")
res = requests.get(BASE_URL, headers=HEADERS, params=params)
res.raise_for_status()
data = res.json()["indicator"]["values"]

# --- Create new DataFrame ---
df_new = pd.DataFrame(data)
if df_new.empty or "datetime" not in df_new:
    print("⚠️ No new data fetched.")
    exit()

df_new["datetime"] = pd.to_datetime(df_new["datetime"])
df_new = df_new.sort_values("datetime")

# --- Combine and Save Daily File ---
df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=["datetime"])
df_combined.to_csv(daily_file, index=False)
print(f"✅ Saved daily file: {daily_file} ({len(df_combined)} rows)")

# --- Load Existing Database ---
db_file = "database/full_wind_data.csv"
df_db = pd.read_csv(db_file, parse_dates=["datetime"]) if os.path.exists(db_file) else pd.DataFrame()

# --- Append if new data exists ---
df_to_add = df_new[~df_new["datetime"].isin(df_db["datetime"])]
if df_to_add.empty:
    print("ℹ️ No new data to append to database.")
    exit()

# --- Append and save updated database ---
df_full = pd.concat([df_db, df_to_add]).drop_duplicates(subset=["datetime"]).sort_values("datetime")
df_full.to_csv("database/full_wind_data.csv", index=False)
df_full.to_parquet("database/full_wind_data.parquet", index=False)

con = duckdb.connect("database/full_wind_data.duckdb")
con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_full")
con.close()

print(f"📦 Appended {len(df_to_add)} new rows to main database ✅")

