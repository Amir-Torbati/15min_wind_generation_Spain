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
csv_file = f"{output_dir}/wind_local.csv"
parquet_file = f"{output_dir}/wind_local.parquet"
duckdb_file = f"{output_dir}/wind_local.duckdb"

# --- Ensure output directory exists ---
os.makedirs(output_dir, exist_ok=True)

# --- Load today's raw wind data ---
if not os.path.exists(input_file):
    print(f"❌ File not found: {input_file}")
    exit()

df_new = pd.read_csv(input_file)

# --- Basic validation ---
expected_columns = ["date", "time", "offset", "value"]
if df_new.empty or not set(expected_columns).issubset(df_new.columns):
    print("⚠️ Invalid or empty input file.")
    exit()

# --- Reorder and sanitize types ---
df_new = df_new[expected_columns]
df_new["value"] = pd.to_numeric(df_new["value"], errors="coerce")

# --- Load existing DB if it exists ---
if os.path.exists(csv_file):
    df_existing = pd.read_csv(csv_file)
else:
    df_existing = pd.DataFrame(columns=expected_columns)

# --- Compare and find new rows ---
merge_keys = ["date", "time"]
df_to_add = df_new[~df_new[merge_keys].apply(tuple, axis=1).isin(
    df_existing[merge_keys].apply(tuple, axis=1)
)]

if df_to_add.empty:
    print("ℹ️ No new rows to append.")
    exit()

# --- Append and sort full DB ---
df_full = pd.concat([df_existing, df_to_add], ignore_index=True)
df_full = df_full.drop_duplicates(subset=merge_keys).sort_values(merge_keys).reset_index(drop=True)

# --- Save to all formats ---
df_full.to_csv(csv_file, index=False)
df_full.to_parquet(parquet_file, index=False)

con = duckdb.connect(duckdb_file)
con.execute("CREATE OR REPLACE TABLE wind_local AS SELECT * FROM df_full")
con.close()

print(f"✅ Appended {len(df_to_add)} new rows to wind_local in {output_dir} ✅")



