import pandas as pd
import duckdb
import os
from datetime import datetime
from zoneinfo import ZoneInfo

# --- File Paths ---
today_str = datetime.now(ZoneInfo("Europe/Madrid")).strftime("%Y-%m-%d")
daily_file = f"data/{today_str}.csv"
full_csv = "database/full_wind_data_tidy.csv"
full_parquet = "database/full_wind_data_tidy.parquet"
duckdb_file = "database/full_wind_data.duckdb"

# --- Check if daily file exists ---
if not os.path.exists(daily_file):
    print(f"❌ Daily file not found: {daily_file}")
    exit()

# --- Load daily data ---
df = pd.read_csv(daily_file)

if df.empty or "datetime" not in df.columns:
    print("⚠️ No data in daily file.")
    exit()

# --- Tidy the data ---
df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
df["datetime_local"] = df["datetime"].dt.tz_convert("Europe/Madrid")
df["date_local"] = df["datetime_local"].dt.date
df["time_local"] = df["datetime_local"].dt.strftime("%H:%M")
df["date_utc"] = df["datetime"].dt.date
df["time_utc"] = df["datetime"].dt.strftime("%H:%M")
df = df.rename(columns={"value": "value_mw"})

df_tidy = df[[
    "date_local", "time_local", "date_utc", "time_utc", "value_mw"
]].sort_values(["date_utc", "time_utc"]).reset_index(drop=True)

# --- Load full database (if exists) ---
df_db = pd.read_csv(full_csv) if os.path.exists(full_csv) else pd.DataFrame(columns=df_tidy.columns)

# --- Compare using date + time (not value) ---
merge_keys = ["date_utc", "time_utc"]
df_to_add = df_tidy[~df_tidy[merge_keys].apply(tuple, axis=1).isin(
    df_db[merge_keys].apply(tuple, axis=1)
)]

if df_to_add.empty:
    print("ℹ️ No new data to append.")
    exit()

# --- Append and sort ---
df_full = pd.concat([df_db, df_to_add], ignore_index=True).drop_duplicates(
    subset=["date_utc", "time_utc"]
).sort_values(["date_utc", "time_utc"]).reset_index(drop=True)

# --- Add row number safely ---
if "row" in df_full.columns:
    df_full.drop(columns="row", inplace=True)
df_full.insert(0, "row", df_full.index + 1)

# --- Save to all formats ---
df_full.to_csv(full_csv, index=False)
df_full.to_parquet(full_parquet, index=False)

con = duckdb.connect(duckdb_file)
con.execute("CREATE OR REPLACE TABLE wind_tidy AS SELECT * FROM df_full")
con.close()

print(f"📦 Appended {len(df_to_add)} new rows to database ✅")


