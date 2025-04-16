import pandas as pd
import os
import duckdb
from datetime import datetime

# --- Paths ---
today_str = datetime.now().strftime("%Y-%m-%d")
daily_file = f"data/{today_str}.csv"
full_csv = "database/full_wind_data_tidy.csv"
full_parquet = "database/full_wind_data_tidy.parquet"
duckdb_file = "database/full_wind_data.duckdb"

# --- Ensure required files exist ---
if not os.path.exists(daily_file):
    print(f"❌ Daily file not found: {daily_file}")
    exit()

df_new = pd.read_csv(daily_file)

# --- Minimal tidy check (assume tidy format exists) ---
expected_columns = {"date_local", "time_local", "date_utc", "time_utc", "value_mw"}
if not expected_columns.issubset(set(df_new.columns)):
    print("❌ Daily file is not in tidy format. Please tidy it before appending.")
    exit()

# --- Load existing full tidy database ---
df_db = pd.read_csv(full_csv) if os.path.exists(full_csv) else pd.DataFrame(columns=df_new.columns)

# --- Identify and append only new rows ---
merge_keys = ["date_utc", "time_utc", "value_mw"]
df_to_add = df_new[~df_new[merge_keys].apply(tuple, axis=1).isin(
    df_db[merge_keys].apply(tuple, axis=1)
)]

if df_to_add.empty:
    print("ℹ️ No new data to append.")
else:
    # --- Combine and clean ---
    df_full = pd.concat([df_db, df_to_add]).drop_duplicates(
        subset=["date_utc", "time_utc"]
    ).sort_values(["date_utc", "time_utc"]).reset_index(drop=True)

    # --- Safely insert row column ---
    if "row" in df_full.columns:
        df_full.drop(columns="row", inplace=True)
    df_full.insert(0, "row", df_full.index + 1)

    # --- Save all formats ---
    df_full.to_csv(full_csv, index=False)
    df_full.to_parquet(full_parquet, index=False)

    con = duckdb.connect(duckdb_file)
    con.execute("CREATE OR REPLACE TABLE wind_tidy AS SELECT * FROM df_full")
    con.close()

    print(f"📦 Appended {len(df_to_add)} new rows to tidy database.")

