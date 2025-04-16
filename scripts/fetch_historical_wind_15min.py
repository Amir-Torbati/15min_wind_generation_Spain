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

# --- DATE RANGE ---
start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ)
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

print(f"📡 Fetching 15-min wind data from {start_date_local.date()} to {end_date_local.date()}...")

# --- Output container ---
all_data = []

# --- Monthly fetch loop ---
current_local = start_date_local
while current_local < end_date_local:
    next_month_local = current_local + relativedelta(months=1)
    period_end_local = min(next_month_local, end_date_local)

    current_utc = current_local.astimezone(timezone.utc)
    period_end_utc = period_end_local.astimezone(timezone.utc)

    params = {
        "start_date": current_utc.isoformat(),
        "end_date": period_end_utc.isoformat(),
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
            df["datetime_local"] = df["datetime_utc"].dt.tz_convert(TZ)

            df["date"] = df["datetime_local"].dt.date
            df["time"] = df["datetime_local"].dt.strftime("%H:%M")
            df["tz_offset"] = df["datetime_local"].dt.strftime("%z").str.replace(r"(\d{2})(\d{2})", r"\1:\2", regex=True)
            df["value_mw"] = df["value"]

            df = df[["date", "time", "tz_offset", "value_mw"]]
            all_data.append(df)

    except Exception as e:
        print(f"  ❌ Error on {current_local.date()}: {e}")

    current_local = period_end_local

# --- Save outputs ---
os.makedirs("database", exist_ok=True)

if all_data:
    df_all = pd.concat(all_data).drop_duplicates(subset=["date", "time"]).sort_values(["date", "time"]).reset_index(drop=True)

    # Save in 3 formats
    df_all.to_csv("database/full_wind_data_tidy.csv", index=False)
    df_all.to_parquet("database/full_wind_data_tidy.parquet", index=False)

    con = duckdb.connect("database/full_wind_data_tidy.duckdb")
    con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_all")
    con.close()

    print(f"✅ Done! Saved {len(df_all)} rows in CSV, Parquet & DuckDB.")
else:
    print("⚠️ No data was fetched.")
