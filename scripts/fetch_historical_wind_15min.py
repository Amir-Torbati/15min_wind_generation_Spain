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

DB_PATH = "database/full_wind_data"
TZ = ZoneInfo("Europe/Madrid")

start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ)
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

# --- Output container ---
os.makedirs("database", exist_ok=True)
all_data = []
print(f"📱 Fetching 15-min wind data from {start_date_local.date()} to {end_date_local.date()}...")

# --- Month-by-month fetch loop ---
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
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["datetime"] = df["datetime"].dt.tz_convert("Europe/Madrid")
            df["date_local"] = df["datetime"].dt.date
            df["time_local"] = df["datetime"].dt.time
            df = df[["value", "datetime", "geo_id", "geo_name", "date_local", "time_local"]]
            all_data.append(df)

    except Exception as e:
        print(f"  ❌ Error on {current_local.date()}: {e}")

    current_local = period_end_local

# --- Save final dataset ---
if all_data:
    df_all = pd.concat(all_data).drop_duplicates(subset=["datetime"]).sort_values("datetime")
    df_all.to_csv(f"{DB_PATH}.csv", index=False)
    df_all.to_parquet(f"{DB_PATH}.parquet", index=False)

    con = duckdb.connect(f"{DB_PATH}.duckdb")
    con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_all")
    con.close()

    print(f"✅ Done! Saved {len(df_all)} rows to 'database/' folder.")
else:
    print("⚠️ No data was fetched.")
