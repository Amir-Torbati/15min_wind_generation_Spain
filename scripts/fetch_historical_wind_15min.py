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

# --- TIME RANGE ---
start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ)
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

print(f"📡 Fetching wind data from {start_date_local.date()} to {end_date_local.date()}...")

# --- Prepare folder ---
OUTPUT_DIR = "main_database"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Fetch loop ---
all_data = []
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
        print(f"  ⏳ Fetching {current_local.date()} → {period_end_local.date()}")
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        values = res.json()["indicator"]["values"]
        df = pd.DataFrame(values)

        if not df.empty and "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(TZ)
            df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")
            df["time"] = df["datetime"].dt.strftime("%H:%M")
            df["tz_offset"] = df["datetime"].dt.strftime("%z").str.replace(r"(\d{2})(\d{2})", r"+\1:\2", regex=True)
            df["value_mw"] = df["value"]
            df_tidy = df[["date", "time", "tz_offset", "value_mw"]]
            all_data.append(df_tidy)

    except Exception as e:
        print(f"  ❌ Error on {current_local.date()}: {e}")

    current_local = period_end_local

# --- Save ---
if all_data:
    df_all = pd.concat(all_data).drop_duplicates().sort_values(["date", "time"]).reset_index(drop=True)

    csv_path = os.path.join(OUTPUT_DIR, "wind_15min.csv")
    parquet_path = os.path.join(OUTPUT_DIR, "wind_15min.parquet")
    duckdb_path = os.path.join(OUTPUT_DIR, "wind_15min.duckdb")

    df_all.to_csv(csv_path, index=False)
    df_all.to_parquet(parquet_path, index=False)

    con = duckdb.connect(duckdb_path)
    con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_all")
    con.close()

    print(f"✅ Saved {len(df_all)} rows to {OUTPUT_DIR}/ as CSV, Parquet, and DuckDB.")
else:
    print("⚠️ No data was fetched.")


