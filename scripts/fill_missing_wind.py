import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dateutil import tz
import duckdb

# --- CONFIG ---
API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/540"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}
DB_PATH = "database/full_wind_data"
REPORTS_DIR = "reports"

os.makedirs("database", exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# --- Load existing data ---
df = pd.read_csv(f"{DB_PATH}.csv", parse_dates=["datetime"])
df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

# --- Generate expected timestamps ---
start = df["datetime"].min()
end = df["datetime"].max()
expected = pd.date_range(start=start, end=end, freq="15min", tz="UTC")

# --- Find missing timestamps ---
existing = pd.Series(df["datetime"].unique())
missing = expected.difference(existing)

log_lines = []
if missing.empty:
    log_lines.append("✅ No missing values found in wind database.")
else:
    log_lines.append(f"❗ Missing timestamps found: {len(missing)}")
    log_lines += [f"  - {ts}" for ts in missing[:10]]
    if len(missing) > 10:
        log_lines.append("  ...")

    # --- Download missing data in chunks ---
    filled_rows = []

    for ts in missing:
        ts_end = ts + timedelta(minutes=15)
        params = {
            "start_date": ts.isoformat(),
            "end_date": ts_end.isoformat(),
            "time_trunc": "quarter-hour",
        }
        try:
            res = requests.get(BASE_URL, headers=HEADERS, params=params)
            if res.status_code == 403:
                log_lines.append("🚫 Token expired or unauthorized (403)")
                break
            res.raise_for_status()
            data = res.json()["indicator"]["values"]
            if data:
                df_new = pd.DataFrame(data)
                df_new["datetime"] = pd.to_datetime(df_new["datetime"], utc=True)
                filled_rows.append(df_new)
        except Exception as e:
            log_lines.append(f"❌ Failed to fetch {ts}: {e}")

    if filled_rows:
        df_missing = pd.concat(filled_rows)
        df_combined = pd.concat([df, df_missing]).drop_duplicates(subset=["datetime"])
        df_combined = df_combined.sort_values("datetime")

        # --- Save to all formats ---
        df_combined.to_csv(f"{DB_PATH}.csv", index=False)
        df_combined.to_parquet(f"{DB_PATH}.parquet", index=False)
        con = duckdb.connect(f"{DB_PATH}.duckdb")
        con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_combined")
        con.close()

        log_lines.append(f"✅ Filled and updated {len(df_missing)} missing records.")
    else:
        log_lines.append("⚠️ No data retrieved to fill missing timestamps.")

# --- Save report ---
report_name = f"{datetime.now().date()}_missing_report.txt"
with open(os.path.join(REPORTS_DIR, report_name), "w") as f:
    f.write("\n".join(log_lines))

print("\n".join(log_lines))

