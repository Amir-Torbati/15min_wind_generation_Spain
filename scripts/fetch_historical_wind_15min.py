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

# --- TIME ZONE ---
TZ = ZoneInfo("Europe/Madrid")

# --- Date Range ---
start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ)
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

print(f"📡 Fetching wind data from {start_date_local.date()} to {end_date_local.date()}...")

# --- Output container ---
all_data = []

# --- Fetch data month-by-month ---
current_local = start_date_local
while current_local < end_date_local:
    next_month_local = current_local + relativedelta(months=1)
    period_end_local = min(next_month_local, end_date_local)

    start_utc = current_local.astimezone(timezone.utc)
    end_utc = period_end_local.astimezone(timezone.utc)

    params = {
        "start_date": start_utc.isoformat(),
        "end_date": end_utc.isoformat(),
        "time_trunc": "quarter-hour"
    }

    try:
        print(f"  ⏳ {current_local.date()} → {period_end_local.date()}")
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        values = res.json()["indicator"]["values"]
        df = pd.DataFrame(values)

        if not df.empty and "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            df["datetime_local"] = df["datetime"].dt.tz_convert(TZ)

            df["date"] = df["datetime_local"].dt.strftime("%Y-%m-%d")
            df["time"] = df["datetime_local"].dt.strftime("%H:%M")
            df["tz_offset"] = df["datetime_local"].dt.strftime("%z").str[:3] + ":" + df["datetime_local"].dt.strftime("%z").str[3:]
            df["value_mw"] = df["value"]

            tidy_df = df[["date", "time", "tz_offset", "value_mw"]]
            all_data.append(tidy_df)

    except Exception as e:
        print(f"  ❌ Error on {current_local.date()}: {e}")

    current_local = period_end_local

# --- Save everything to repo-root/database/ ---
database_folder = os.path.join(os.path.dirname(__file__), "database")
os.makedirs(database_folder, exist_ok=True)

if all_data:
    df_all = pd.concat(all_data).drop_duplicates().sort_values(["date", "time"]).reset_index(drop=True)

    csv_path = os.path.join(database_folder, "full_wind_data_tidy.csv")
    parquet_path = os.path.join(database_folder, "full_wind_data_tidy.parquet")
    duckdb_path = os.path.join(database_folder, "full_wind_data_tidy.duckdb")

    df_all.to_csv(csv_path, index=False)
    df_all.to_parquet(parquet_path, index=False)

    con = duckdb.connect(duckdb_path)
    con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_all")
    con.close()

    print(f"✅ Done: {len(df_all)} rows saved to 'database/' folder.")
else:
    print("⚠️ No data was fetched.")

