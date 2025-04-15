import requests
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os
from dateutil.relativedelta import relativedelta
import duckdb

# --- CONFIG ---
API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/540"  # Wind generation indicator
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- TIME RANGE CONFIG ---
TZ = ZoneInfo("Europe/Madrid")
start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ)
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

# --- OUTPUT DIRECTORIES ---
os.makedirs("database", exist_ok=True)

# --- DATA FETCH LOOP ---
all_data = []
current_local = start_date_local

print(f"📡 Fetching 15-min wind data from {start_date_local.date()} to {end_date_local.date()}...")

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
            # ✅ FIX: Ensure timezone-aware parsing
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            all_data.append(df)

    except Exception as e:
        print(f"  ❌ Error on {current_local.date()}: {e}")

    current_local = period_end_local

if not all_data:
    print("⚠️ No data was fetched.")
    exit()

# --- CONCAT AND CLEAN ---
df_all = pd.concat(all_data).drop_duplicates(subset=["datetime"]).sort_values("datetime")

# Add timezone-aware columns
df_all["datetime_utc"] = df_all["datetime"]  # already UTC
df_all["datetime_local"] = df_all["datetime"].dt.tz_convert("Europe/Madrid")

# Split into date and time columns
df_all["date_local"] = df_all["datetime_local"].dt.date
df_all["time_local"] = df_all["datetime_local"].dt.strftime("%H:%M")
df_all["date_utc"] = df_all["datetime_utc"].dt.date
df_all["time_utc"] = df_all["datetime_utc"].dt.strftime("%H:%M")

# Final tidy DataFrame
df_tidy = df_all[[
    "date_local", "time_local", "date_utc", "time_utc", "value"
]].rename(columns={"value": "value_mw"}).sort_values(["date_utc", "time_utc"]).reset_index(drop=True)

# Add row number
df_tidy.insert(0, "row", df_tidy.index + 1)

# --- SAVE OUTPUTS ---
csv_path = "database/full_wind_data_tidy.csv"
parquet_path = "database/full_wind_data_tidy.parquet"
duckdb_path = "database/full_wind_data.duckdb"

df_tidy.to_csv(csv_path, index=False)
df_tidy.to_parquet(parquet_path, index=False)

# Save to DuckDB
con = duckdb.connect(duckdb_path)
con.execute("CREATE OR REPLACE TABLE wind_tidy AS SELECT * FROM df_tidy")
con.close()

# --- DONE ---
print(f"\n✅ Done! Saved {len(df_tidy)} rows to:")
print(f"   • {csv_path}")
print(f"   • {parquet_path}")
print(f"   • {duckdb_path} (table: wind_tidy)")



