import requests
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os
from dateutil.relativedelta import relativedelta

# --- CONFIG ---
API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/540"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

TZ = ZoneInfo("Europe/Madrid")
UTC = timezone.utc

# --- Time Range ---
start_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ)
end_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

print(f"📡 Fetching historical wind data from {start_local.date()} to {end_local.date()}...")

# --- Monthly Fetch ---
current = start_local
all_data = []

while current < end_local:
    next_month = min(current + relativedelta(months=1), end_local)
    params = {
        "start_date": current.astimezone(UTC).isoformat(),
        "end_date": next_month.astimezone(UTC).isoformat(),
        "time_trunc": "quarter-hour",
    }

    try:
        print(f"  ⏳ {current.date()} → {next_month.date()}")
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        values = res.json()["indicator"]["values"]
        df = pd.DataFrame(values)

        if not df.empty and "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(TZ)
            all_data.append(df)

    except Exception as e:
        print(f"  ❌ Failed to fetch {current.date()}: {e}")

    current = next_month

# --- Save tidy ---
if not all_data:
    print("⚠️ No data fetched.")
    exit()

df_all = pd.concat(all_data).drop_duplicates(subset=["datetime"]).sort_values("datetime")

# --- Tidy columns ---
df_all["value_mw"] = df_all["value"]
df_all["date_local"] = df_all["datetime"].dt.strftime("%Y-%m-%d")
df_all["time_local"] = df_all["datetime"].dt.strftime("%H:%M")

datetime_utc = df_all["datetime"].dt.tz_convert("UTC")
df_all["date_utc"] = datetime_utc.dt.strftime("%Y-%m-%d")
df_all["time_utc"] = datetime_utc.dt.strftime("%H:%M")

df_all.insert(0, "row", range(1, len(df_all) + 1))

df_tidy = df_all[[
    "row", "date_local", "time_local", "date_utc", "time_utc", "value_mw", "datetime"
]]

# --- Save ---
os.makedirs("database", exist_ok=True)
df_tidy.to_csv("database/full_wind_data_tidy.csv", index=False)
df_tidy.to_parquet("database/full_wind_data_tidy.parquet", index=False)

import duckdb
con = duckdb.connect("database/full_wind_data_tidy.duckdb")
con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_tidy")
con.close()

print(f"✅ Saved tidy data: {len(df_tidy)} rows.")
