import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import os
import duckdb
from zoneinfo import ZoneInfo
from dateutil.rrule import rrule, DAILY

# --- CONFIG ---
DB_PATH = "database/full_wind_data"
QUARTER_FREQ = "15min"
API_TOKEN = "YOUR_API_TOKEN_HERE"
BASE_URL = "https://api.esios.ree.es/indicators/540"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- Load current database ---
os.makedirs("database", exist_ok=True)
if os.path.exists(f"{DB_PATH}.csv"):
    df_db = pd.read_csv(f"{DB_PATH}.csv", parse_dates=["datetime"])
else:
    df_db = pd.DataFrame(columns=["value", "datetime", "datetime_utc", "tz_time", "geo_id", "geo_name"])

# --- Generate expected timestamps ---
start = df_db["datetime"].min()
end = datetime.now(timezone.utc).replace(second=0, microsecond=0)

if pd.isna(start):
    print("⚠️ Database is empty. Exiting.")
    exit()

expected = pd.date_range(
    start=start.astimezone(timezone.utc),
    end=end.astimezone(timezone.utc),
    freq=QUARTER_FREQ,
    tz="UTC"
)

existing = pd.to_datetime(df_db["datetime_utc"])
missing = expected.difference(existing)

# --- If nothing is missing ---
if missing.empty:
    print("✅ No missing timestamps.")
    os.makedirs("reports", exist_ok=True)
    with open("reports/missing_data_report.txt", "w") as f:
        f.write(f"✅ No missing data. Checked on {datetime.now().date()}.\n")
    exit()

print(f"🔍 Found {len(missing)} missing timestamps. Attempting to fetch...")

# --- Fetch in daily chunks ---
missing_days = sorted(set(ts.date() for ts in missing))
all_new = []

for day in rrule(freq=DAILY, dtstart=missing_days[0], until=missing_days[-1]):
    day_start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    params = {
        "start_date": day_start.isoformat(),
        "end_date": day_end.isoformat(),
        "time_trunc": "quarter-hour"
    }

    try:
        print(f"📡 Fetching {day.date()}...")
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        if res.status_code == 403:
            print("⛔ 403 Forbidden – Token expired or unauthorized")
            continue
        res.raise_for_status()
        values = res.json()["indicator"]["values"]
        df = pd.DataFrame(values)
        df["datetime"] = pd.to_datetime(df["datetime"])
        all_new.append(df)
    except Exception as e:
        print(f"  ❌ Error on {day.date()}: {e}")

# --- Combine and Save ---
if all_new:
    df_new = pd.concat(all_new)
    df_combined = pd.concat([df_db, df_new])
    df_combined = df_combined.drop_duplicates(subset=["datetime"]).sort_values("datetime")

    df_combined.to_csv(f"{DB_PATH}.csv", index=False)
    df_combined.to_parquet(f"{DB_PATH}.parquet", index=False)

    con = duckdb.connect(f"{DB_PATH}.duckdb")
    con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_combined")
    con.close()

    print(f"✅ Added {len(df_new)} rows. DB now has {len(df_combined)} rows.")

else:
    print("⚠️ No new data fetched.")

# --- Save report ---
os.makedirs("reports", exist_ok=True)
report_path = "reports/missing_data_report.txt"
with open(report_path, "w") as f:
    if all_new:
        f.write(f"✅ Filled {len(df_new)} missing entries on {datetime.now().date()}.\n")
        f.write(f"🗓️ Missing days filled: {', '.join(str(d) for d in missing_days)}\n")
    else:
        f.write(f"⚠️ Could not fill missing data for days: {', '.join(str(d) for d in missing_days)}\n")

print(f"📝 Report saved to {report_path}")




