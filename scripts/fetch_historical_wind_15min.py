import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
from dateutil.relativedelta import relativedelta
import duckdb

# --- CONFIG ---
API_TOKEN = "YOUR_API_TOKEN_HERE"
BASE_URL = "https://api.esios.ree.es/indicators/540"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

TZ = ZoneInfo("Europe/Madrid")
start_date_local = datetime(2023, 1, 1, tzinfo=TZ)
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

os.makedirs("database", exist_ok=True)

all_data = []
print(f"📡 Fetching 15-min wind data from {start_date_local.date()} to {end_date_local.date()}...")

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
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            all_data.append(df)

    except Exception as e:
        print(f"  ❌ Error on {current_local.date()}: {e}")

    current_local = period_end_local

# --- Build tidy dataset ---
if all_data:
    df_all = pd.concat(all_data)
    df_all = df_all.drop_duplicates(subset=["datetime"]).sort_values("datetime")
    df_all["datetime_local"] = df_all["datetime"].dt.tz_convert(TZ)

    df_all["date_utc"] = df_all["datetime"].dt.strftime("%Y-%m-%d")
    df_all["time_utc"] = df_all["datetime"].dt.strftime("%H:%M")
    df_all["date_local"] = df_all["datetime_local"].dt.strftime("%Y-%m-%d")
    df_all["time_local"] = df_all["datetime_local"].dt.strftime("%H:%M")

    df_tidy = df_all.rename(columns={"value": "value_mw"})
    df_tidy = df_tidy[["date_local", "time_local", "date_utc", "time_utc", "value_mw", "datetime"]]
    df_tidy = df_tidy.reset_index(drop=True)
    df_tidy.insert(0, "row", df_tidy.index + 1)

    # --- Save to database folder ---
    df_tidy.to_csv("database/full_wind_data_tidy.csv", index=False)
    df_tidy.to_parquet("database/full_wind_data_tidy.parquet", index=False)

    con = duckdb.connect("database/full_wind_data_tidy.duckdb")
    con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_tidy")
    con.close()

    print(f"✅ Saved {len(df_tidy)} rows to tidy database.")
else:
    print("⚠️ No data fetched.")

