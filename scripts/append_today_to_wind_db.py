import pandas as pd
import duckdb
import os
from datetime import datetime
from zoneinfo import ZoneInfo

# --- Paths ---
os.makedirs("database", exist_ok=True)

today_local = datetime.now(ZoneInfo("Europe/Madrid"))
today_str = today_local.strftime("%Y-%m-%d")

daily_path = f"data/{today_str}.csv"
csv_db_path = "database/full_wind_data.csv"
parquet_db_path = "database/full_wind_data.parquet"
duckdb_path = "database/full_wind_data.duckdb"

# --- Load today's data ---
if not os.path.exists(daily_path):
    print(f"⚠️ No file found for today: {daily_path}")
    exit()

df_today = pd.read_csv(daily_path, parse_dates=["datetime"])
df_today = df_today.drop_duplicates(subset=["datetime"]).sort_values("datetime")

# --- Load existing full database (if any) ---
if os.path.exists(csv_db_path):
    df_full = pd.read_csv(csv_db_path, parse_dates=["datetime"])
    df_combined = pd.concat([df_full, df_today])
    df_combined = df_combined.drop_duplicates(subset=["datetime"]).sort_values("datetime")
else:
    df_combined = df_today

# --- Save all formats ---
df_combined.to_csv(csv_db_path, index=False)
df_combined.to_parquet(parquet_db_path, index=False)

con = duckdb.connect(duckdb_path)
con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_combined")
con.close()

print(f"✅ Appended {len(df_today)} rows to database (now {len(df_combined)} rows total).")
