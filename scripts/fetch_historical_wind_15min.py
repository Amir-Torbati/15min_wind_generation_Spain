import requests
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os
from dateutil.relativedelta import relativedelta
import duckdb

# --- CONFIG ---
API_TOKEN = "YOUR_TOKEN_HERE"
BASE_URL = "https://api.esios.ree.es/indicators/540"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- TIME ZONES ---
TZ_LOCAL = ZoneInfo("Europe/Madrid")

# --- RANGE: Start to Now ---
start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ_LOCAL)
end_date_local = datetime.now(TZ_LOCAL).replace(minute=0, second=0, microsecond=0)

print(f"📡 Fetching historical wind data from {start_date_local.date()} to {end_date_local.date()}...")

# --- Output container ---
all_data = []

# --- Month-by-month loop ---
current_local = start_date_local
while current_local < end_date_local:
    next_month_local = current_local + relativedelta(months=1)
    period_end_local = min(next_month_local, end_date_local)

    start_utc = current_local.astimezone(timezone.utc).isoformat()
    end_utc = period_end_local.astimezone(timezone.utc).isoformat()

    params = {
        "start_date": start_utc,
        "end_date": end_utc,
        "time_trunc": "quarter-hour",
    }

    try:
        print(f"  ⏳ {current_local.date()} → {period_end_local.date()}")
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        values = res.json()["indicator"]["values"]
        df = pd.DataFrame(values)

        if not df.empty and "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(TZ_LOCAL)

            df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")
            df["time"] = df["datetime"].dt.strftime("%H:%M")
            df["tz_offset"] = df["datetime"].dt.strftime("%z").str.slice(0, 3) + ":00"
            df["value_mw"] = df["value"]

            df = df[["date", "time", "tz_offset", "value_mw"]]
            all_data.append(df)

    except Exception as e:
        print(f"  ❌ Error on {current_local.date()}: {e}")

    current_local = period_end_local

# --- Save ---
os.makedirs("database", exist_ok=True)

if all_data:
    print("📦 Saving tidy dataset...")
    df_all = pd.concat(all_data).drop_duplicates().sort_values(["date", "time"]).reset_index(drop=True)

    df_all.to_csv("database/full_wind_data_tidy.csv", index=False)
    df_all.to_parquet("database/full_wind_data_tidy.parquet", index=False)

    con = duckdb.connect("database/full_wind_data_tidy.duckdb")
    con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_all")
    con.close()

    print(f"✅ Done: {len(df_all)} rows saved in CSV, Parquet & DuckDB formats.")
else:
    print("⚠️ No data fetched.")
