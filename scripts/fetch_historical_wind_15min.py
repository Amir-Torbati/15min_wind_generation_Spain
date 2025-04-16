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
            df["datetime_utc"] = pd.to_datetime(df["datetime"], utc=True)
            df["datetime"] = df["datetime_utc"].dt.tz_convert(TZ_LOCAL)

            df["date_local"] = df["datetime"].dt.date
            df["time_local"] = df["datetime"].dt.strftime("%H:%M")

            df["date_utc"] = df["datetime_utc"].dt.date
            df["time_utc"] = df["datetime_utc"].dt.strftime("%H:%M")

            df["value_mw"] = df["value"]
            df = df[["datetime", "date_local", "time_local", "date_utc", "time_utc", "value_mw"]]

            all_data.append(df)

    except Exception as e:
        print(f"  ❌ Error on {current_local.date()}: {e}")

    current_local = period_end_local

# --- Save ---
if all_data:
    print("📦 Concatenating and saving full historical data...")
    df_all = pd.concat(all_data).drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)

    os.makedirs("database", exist_ok=True)

    csv_path = "database/full_wind_data_tidy.csv"
    parquet_path = "database/full_wind_data_tidy.parquet"
    duckdb_path = "database/full_wind_data_tidy.duckdb"

    df_all.to_csv(csv_path, index=False)
    df_all.to_parquet(parquet_path, index=False)

    con = duckdb.connect(duckdb_path)
    con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_all")
    con.close()

    print(f"✅ Done: {len(df_all)} rows saved to CSV, Parquet & DuckDB.")
else:
    print("⚠️ No data was fetched.")

