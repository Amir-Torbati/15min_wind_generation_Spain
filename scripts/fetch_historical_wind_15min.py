import requests
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os
from dateutil.relativedelta import relativedelta
import duckdb

# --- CONFIG ---
API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/540"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- TIME RANGE CONFIG ---
TZ = ZoneInfo("Europe/Madrid")
start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ)

# Use actual current time (no rounding)
now = datetime.now(TZ)
end_date_local = now.replace(second=0, microsecond=0)

# --- OUTPUT DIRS ---
os.makedirs("database", exist_ok=True)

# --- FETCH LOOP ---
all_data = []
current_local = start_date_local

print(f"📡 Fetching wind data from {start_date_local} to {end_date_local}...")

while current_local < end_date_local:
    next_month_local = current_local + relativedelta(months=1)
    period_end_local = min(next_month_local, end_date_local)

    # Convert to UTC for API
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

if not all_data:
    print("⚠️ No data was fetched.")
    exit()

# --- CONCAT & CLEAN ---
df_all = pd.concat(all_data).drop_duplicates(subset=["datetime"]).sort_values("datetime")

df_all["datetime_utc"] = df_all["datetime"]
df_all["datetime_local"] = df_all["datetime"].dt.tz_convert("Europe/Madrid")
df_all["date_local"] = df_all["datetime_local"].dt.date
df_all["time_local"] = df_all["datetime_local"].dt.strftime("%H:%M")
df_all["date_utc"] = df_all["datetime_utc"].dt.date
df_all["time_utc"] = df_all["datetime_utc"].dt.strftime("%H:%M")
df_all = df_all.rename(columns={"value": "value_mw"})

# --- TIDY FORMAT ---
df_tidy = df_all[[
    "date_local", "time_local", "date_utc", "time_utc", "value_mw"
]].sort_values(["date_utc", "time_utc"]).reset_index(drop=True)

# Safe insert of row number
if "row" in df_tidy.columns:
    df_tidy.drop(columns="row", inplace=True)
df_tidy.insert(0, "row", df_tidy.index + 1)

# --- SAVE OUTPUTS ---
csv_path = "database/full_wind_data_tidy.csv"
parquet_path = "database/full_wind_data_tidy.parquet"
duckdb_path = "database/full_wind_data.duckdb"

df_tidy.to_csv(csv_path, index=False)
df_tidy.to_parquet(parquet_path, index=False)

con = duckdb.connect(duckdb_path)
con.execute("CREATE OR REPLACE TABLE wind_tidy AS SELECT * FROM df_tidy")
con.close()

print(f"\n✅ Done! Saved {len(df_tidy)} rows to:")
print(f"   • {csv_path}")
print(f"   • {parquet_path}")
print(f"   • {duckdb_path} (table: wind_tidy)")

