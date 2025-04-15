import os
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from dateutil import tz
from zoneinfo import ZoneInfo
import requests

# --- CONFIG ---
DATA_DIR = "data"
DB_DIR = "database"
REPORTS_DIR = "reports"
DB_PATH = os.path.join(DB_DIR, "full_wind_data")
API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/540"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- Ensure directories exist ---
os.makedirs(DB_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# --- Load existing database ---
all_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
all_data = []

for f in sorted(all_files):
    df = pd.read_csv(os.path.join(DATA_DIR, f), parse_dates=["datetime"])
    all_data.append(df)

df_all = pd.concat(all_data).drop_duplicates(subset="datetime").sort_values("datetime")

# --- Identify expected timestamps ---
start = df_all["datetime"].min()
end = df_all["datetime"].max()
expected = pd.date_range(start=start, end=end, freq="15min", tz="UTC")

# --- Find missing timestamps ---
missing_times = expected.difference(df_all["datetime"])
report_lines = []

if not missing_times.empty:
    report_lines.append(f"⛔ Missing timestamps found: {len(missing_times)}")
    report_lines.append(f"Date range: {missing_times.min()} to {missing_times.max()}")
else:
    report_lines.append("✅ No missing timestamps found.")

# --- Attempt to fetch missing data if any ---
new_rows = []
if not missing_times.empty:
    for ts in missing_times:
        start_utc = ts.isoformat()
        end_utc = (ts + timedelta(minutes=15)).isoformat()

        params = {
            "start_date": start_utc + "Z",
            "end_date": end_utc + "Z",
            "time_trunc": "quarter-hour"
        }

        try:
            res = requests.get(BASE_URL, headers=HEADERS, params=params)
            if res.status_code == 403:
                report_lines.append(f"❌ Token expired when fetching {ts}")
                continue
            res.raise_for_status()
            values = res.json()["indicator"]["values"]
            df_patch = pd.DataFrame(values)
            if not df_patch.empty:
                df_patch["datetime"] = pd.to_datetime(df_patch["datetime"])
                new_rows.append(df_patch)
        except Exception as e:
            report_lines.append(f"❌ Error fetching {ts}: {e}")

if new_rows:
    df_patch_all = pd.concat(new_rows).drop_duplicates(subset="datetime")
    df_combined = pd.concat([df_all, df_patch_all]).drop_duplicates(subset="datetime").sort_values("datetime")
    report_lines.append(f"✅ Added {len(df_patch_all)} missing rows.")
else:
    df_combined = df_all

# --- Save final updated database ---
df_combined.to_csv(f"{DB_PATH}.csv", index=False)
df_combined.to_parquet(f"{DB_PATH}.parquet", index=False)
con = duckdb.connect(f"{DB_PATH}.duckdb")
con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_combined")
con.close()

# --- Save report ---
report_file = os.path.join(REPORTS_DIR, f"missing_report_{datetime.utcnow().date()}.txt")
with open(report_file, "w") as f:
    f.write("\n".join(report_lines))

print("\n".join(report_lines))


