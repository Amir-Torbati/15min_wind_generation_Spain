import requests
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os
import duckdb

# --- CONFIG ---
API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/540"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- Time Setup ---
TZ = ZoneInfo("Europe/Madrid")
now_local = datetime.now(TZ).replace(second=0, microsecond=0)
start_local = now_local.replace(hour=0, minute=0)
start_utc = start_local.astimezone(timezone.utc)
end_utc = now_local.astimezone(timezone.utc)

# --- File Paths ---
today_str = start_local.strftime("%Y-%m-%d")
daily_file = f"data/{today_str}.csv"
os.makedirs("data", exist_ok=True)
os.makedirs("database", exist_ok=True)

# --- Fetch from API ---
params = {
    "start_date": start_utc.isoformat(),
    "end_date": end_utc.isoformat(),
    "time_trunc": "quarter-hour",
}

print(f"📡 Fetching wind data from {start_local} to {now_local}...")
res = requests.get(BASE_URL, headers=HEADERS, params=params)
res.raise_for_status()
data = res.json()["indicator"]["values"]

df_raw = pd.DataFrame(data)

# --- Skip if empty API response ---
if df_raw.empty or "datetime" not in df_raw:
    print("⚠️ No new data fetched.")
    df_raw = pd.DataFrame()  # Still proceed with safe structure

# --- Tidy the raw data if not empty ---
if not df_raw.empty:
    df_raw["datetime"] = pd.to_datetime(df_raw["datetime"], utc=True)
    df_raw["datetime_local"] = df_raw["datetime"].dt.tz_convert("Europe/Madrid")
    df_raw["date_local"] = df_raw["datetime_local"].dt.date
    df_raw["time_local"] = df_raw["datetime_local"].dt.strftime("%H:%M")
    df_raw["date_utc"] = df_raw["datetime"].dt.date
    df_raw["time_utc"] = df_raw["datetime"].dt.strftime("%H:%M")
    df_raw = df_raw.rename(columns={"value": "value_mw"})

    df_tidy = df_raw[[
        "date_local", "time_local", "date_utc", "time_utc", "value_mw"
    ]].sort_values(["date_utc", "time_utc"]).reset_index(drop=True)
else:
    df_tidy = pd.DataFrame(columns=["date_local", "time_local", "date_utc", "time_utc", "value_mw"])

# --- Load daily file and update it ---
df_existing = pd.read_csv(daily_file) if os.path.exists(daily_file) else pd.DataFrame()
df_daily = pd.concat([df_existing, df_tidy]).drop_duplicates(
    subset=["date_utc", "time_utc"]
).sort_values(["date_utc", "time_utc"]).reset_index(drop=True)
df_daily.insert(0, "row", df_daily.index + 1)
df_daily.to_csv(daily_file, index=False)
print(f"✅ Saved daily file: {daily_file} ({len(df_daily)} rows)")

# --- Load full tidy DB and append ---
db_file = "database/full_wind_data_tidy.csv"
df_db = pd.read_csv(db_file) if os.path.exists(db_file) else pd.DataFrame()

merge_keys = ["date_utc", "time_utc", "value_mw"]
df_to_add = df_tidy[~df_tidy[merge_keys].apply(tuple, axis=1).isin(
    df_db[merge_keys].apply(tuple, axis=1)
)]

df_full = pd.concat([df_db, df_to_add]).drop_duplicates(
    subset=["date_utc", "time_utc"]
).sort_values(["date_utc", "time_utc"]).reset_index(drop=True)
df_full.insert(0, "row", df_full.index + 1)

# --- Always Save All Formats ---
df_full.to_csv("database/full_wind_data_tidy.csv", index=False)
df_full.to_parquet("database/full_wind_data_tidy.parquet", index=False)

con = duckdb.connect("database/full_wind_data.duckdb")
con.execute("CREATE OR REPLACE TABLE wind_tidy AS SELECT * FROM df_full")
con.close()

print(f"📦 Full database updated: {len(df_to_add)} new rows added ✅")
