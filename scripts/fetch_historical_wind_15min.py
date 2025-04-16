import requests
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
import duckdb
import os

# --- CONFIG ---
API_TOKEN = "YOUR_TOKEN_HERE"  # 🔐 Replace with your actual token
BASE_URL = "https://api.esios.ree.es/indicators/540"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- TIME ZONE CONFIG ---
TZ_LOCAL = ZoneInfo("Europe/Madrid")

# --- TIME RANGE: from Jan 1, 2023 to now ---
start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ_LOCAL)
end_date_local = datetime.now(TZ_LOCAL).replace(minute=0, second=0, microsecond=0)

print(f"📡 Collecting wind data from {start_date_local.date()} to {end_date_local.date()}")

# --- CONTAINER FOR ALL DATA ---
all_data = []

# --- FETCH DATA MONTH BY MONTH ---
current_local = start_date_local
while current_local < end_date_local:
    next_month_local = current_local + relativedelta(months=1)
    period_end_local = min(next_month_local, end_date_local)

    start_utc = current_local.astimezone(timezone.utc).isoformat()
    end_utc = period_end_local.astimezone(timezone.utc).isoformat()

    params = {
        "start_date": start_utc,
        "end_date": end_utc,
        "time_trunc": "quarter-hour"
    }

    try:
        print(f"  ⏳ {current_local.date()} → {period_end_local.date()}")
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        values = res.json()["indicator"]["values"]
        df = pd.DataFrame(values)

        if not df.empty and "datetime" in df.columns:
            df["datetime_utc"] = pd.to_datetime(df["datetime"], utc=True)
            df["datetime_local"] = df["datetime_utc"].dt.tz_convert(TZ_LOCAL)

            df["date"] = df["datetime_local"].dt.date
            df["time"] = df["datetime_local"].dt.strftime("%H:%M")
            df["tz_offset"] = df["datetime_local"].dt.strftime("%z").str.replace(r'(\d{2})(\d{2})', r'\1:\2', regex=True)
            df["value_mw"] = df["value"]

            df_tidy = df[["date", "time", "tz_offset", "value_mw"]]
            all_data.append(df_tidy)

    except Exception as e:
        print(f"  ❌ Error on {current_local.date()}: {e}")

    current_local = period_end_local

# --- SAVE DATA ---
if all_data:
    df_all = pd.concat(all_data).drop_duplicates().sort_values(["date", "time"]).reset_index(drop=True)
    print(f"📦 Total rows collected: {len(df_all)}")

    # Save to three formats
    df_all.to_csv("database/full_wind_data_tidy.csv", index=False)
    df_all.to_parquet("database/full_wind_data_tidy.parquet", index=False)

    con = duckdb.connect("database/full_wind_data_tidy.duckdb")
    con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_all")
    con.close()

    print("✅ Data saved in CSV, Parquet and DuckDB formats.")
else:
    print("⚠️ No data was fetched. Check your API token or network.")

