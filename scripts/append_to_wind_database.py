import pandas as pd
import duckdb
import os
from datetime import datetime
from zoneinfo import ZoneInfo

# --- File Paths ---
TZ = ZoneInfo("Europe/Madrid")
today_str = datetime.now(TZ).strftime("%Y-%m-%d")

daily_file = f"data/{today_str}.csv"
output_dir = "main database"
os.makedirs(output_dir, exist_ok=True)

full_csv = f"{output_dir}/full_wind_data_tidy.csv"
full_parquet = f"{output_dir}/full_wind_data_tidy.parquet"
duckdb_file = f"{output_dir}/full_wind_data.duckdb"

# --- Check for daily file ---
if not os.path.exists(daily_file):
    print(f"❌ Daily file not found: {daily_file}")
    exit()

# --- Load daily data ---
df = pd.read_csv(daily_file)
if df.empty or "datetime" not in df.columns:
    print("⚠️ No data or missing datetime column.")
    exit()

# --- Tidy the daily data ---
df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
df["datetime_local"] = df["datetime"].dt.tz_convert(TZ)
df["date_local"] = df["datetime_local"].dt.date
df["time_local"] = df["datetime_local"].dt.strftime("%H:%M")
df["date_utc"] = df["datetime"].dt.date
df["time_utc"] = df["datetime"].dt.strftime("%H:%M")
df = df.rename(columns={"value": "value_mw"})

df_tidy = df[["date_local", "time_local", "date_utc", "time_utc", "value_mw"]]\
    .sort_values(["date_utc", "time_utc"])\
    .reset_index(drop=True)

# --- Load existing DB if it exists ---
if os.path.exists(full_csv):
    df_db = pd.read_csv(full_csv)
else:
    df_db = pd.DataFrame(columns=df_tidy.columns)

# --- Compare and get new rows to add ---
merge_keys = ["date_utc", "time_utc"]
df_to_add = df_tidy[~df_tidy[merge_keys].apply(tuple, axis=1).isin(
    df_db[merge_keys].apply(tuple, axis=1)
)]

if df_to_add.empty:
    print("ℹ️ No new data to append.")
    exit()

# --- Append and tidy master DB ---
df_full = pd.concat([df_db, df_to_add], ignore_index=True)\
    .drop_duplicates(subset=merge_keys)\
    .sort_values(merge_keys)\
    .reset_index(drop=True)

# Add row number column
df_full.insert(0, "row", df_full.index + 1)

# --- Ensure types before saving ---
df_full["value_mw"] = pd.to_numeric(df_full["value_mw"], errors="coerce")
df_full["time_local"] = df_full["time_local"].astype(str)
df_full["time_utc"] = df_full["time_utc"].astype(str)
df_full["date_local"] = pd.to_datetime(df_full["date_local"]).dt.date
df_full["date_utc"] = pd.to_datetime(df_full["date_utc"]).dt.date

# --- Save to all formats ---
df_full.to_csv(full_csv, index=False)
df_full.to_parquet(full_parquet, index=False)

con = duckdb.connect(duckdb_file)
con.execute("CREATE OR REPLACE TABLE wind_tidy AS SELECT * FROM df_full")
con.close()

print(f"📦 Appended {len(df_to_add)} new rows to full tidy wind database ✅")



