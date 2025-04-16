import pandas as pd
import duckdb
import os
from datetime import datetime
from zoneinfo import ZoneInfo

# --- Config ---
TZ = ZoneInfo("Europe/Madrid")
today_str = datetime.now(TZ).strftime("%Y-%m-%d")
input_file = f"data/{today_str}.csv"

output_dir = "main database"
os.makedirs(output_dir, exist_ok=True)

csv_file = f"{output_dir}/wind_local.csv"
parquet_file = f"{output_dir}/wind_local.parquet"
duckdb_file = f"{output_dir}/wind_local.duckdb"

# --- Load raw daily data from REE structure ---
if not os.path.exists(input_file):
    print(f"❌ File not found: {input_file}")
    exit()

df_raw = pd.read_csv(input_file)

if df_raw.empty or "datetime" not in df_raw.columns or "value" not in df_raw.columns:
    print("⚠️ Input file is empty or missing required columns.")
    exit()

# --- Convert to tidy format ---
df_raw["datetime"] = pd.to_datetime(df_raw["datetime"], utc=True)
df_raw["datetime_local"] = df_raw["datetime"].dt.tz_convert(TZ)

df_raw["date"] = df_raw["datetime_local"].dt.date.astype(str)
df_raw["time"] = df_raw["datetime_local"].dt.strftime("%H:%M")
df_raw["offset"] = df_raw["datetime_local"].dt.strftime("%z").str[:3] + ":" + df_raw["datetime_local"].dt.strftime("%z").str[3:]
df_raw["value"] = pd.to_numeric(df_raw["value"], errors="coerce")

df_tidy = df_raw[["date", "time", "offset", "value"]].copy()

# --- Load existing tidy DB (if exists) ---
if os.path.exists(csv_file):
    df_existing = pd.read_csv(csv_file)
else:
    df_existing = pd.DataFrame(columns=df_tidy.columns)

# --- Compare by date + time to find new rows ---
merge_keys = ["date", "time"]
df_to_add = df_tidy[~df_tidy[merge_keys].apply(tuple, axis=1).isin(
    df_existing[merge_keys].apply(tuple, axis=1)
)]

if df_to_add.empty:
    print("ℹ️ No new rows to append.")
    exit()

# --- Append and save updated tidy DB ---
df_full = pd.concat([df_existing, df_to_add], ignore_index=True)
df_full = df_full.drop_duplicates(subset=merge_keys).sort_values(merge_keys).reset_index(drop=True)

df_full.to_csv(csv_file, index=False)
df_full.to_parquet(parquet_file, index=False)

con = duckdb.connect(duckdb_file)
con.execute("CREATE OR REPLACE TABLE wind_local AS SELECT * FROM df_full")
con.close()

print(f"✅ Appended {len(df_to_add)} new rows to wind_local in '{output_dir}' ✅")
