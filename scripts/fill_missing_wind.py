import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import os
import duckdb
from zoneinfo import ZoneInfo
from dateutil.rrule import rrule, DAILY

# --- CONFIG ---
DB_PATH = "database/full_wind_data"
REPORT_PATH = "reports/missing_report.md"
QUARTER_FREQ = "15min"
API_TOKEN = "YOUR_API_TOKEN_HERE"  # Replace in your secrets
BASE_URL = "https://api.esios.ree.es/indicators/540"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- Load DB ---
os.makedirs("database", exist_ok=True)
os.makedirs("reports", exist_ok=True)

if os.path.exists(f"{DB_PATH}.csv"):
    df_db = pd.read_csv(f"{DB_PATH}.csv", parse_dates=["datetime", "datetime_utc"])
else:
    df_db = pd.DataFrame(columns=["value", "datetime", "datetime_utc", "tz_time", "geo_id", "geo_name"])

# --- Determine missing UTC timestamps ---
start = df_db["datetime_utc"].min()
end = datetime.now(timezone.utc).replace(second=0, microsecond=0)

if pd.isna(start):
    print("⚠️ Database is empty. Exiting.")
    exit()

expected = pd.date_range(start=start, end=end, freq=QUARTER_FREQ, tz="UTC")
existing = pd.to_datetime(df_db["datetime_utc"], utc=True)
missing = expected.difference(existing)

if missing.empty:
    print("✅ No missing timestamps.")
    with open(REPORT_PATH, "w") as f:
        f.write("# 📊 Wind Data Missing Report\n\n✅ All data is complete.\n")
    exit()

print(f"🔍 Found {len(missing)} missing timestamps. Attempting to fetch...")

missing_days = sorted(set(ts.date() for ts in missing))
all_new = []
failed_days = []

for day in rrule(freq=DAILY, dtstart=missing_days[0], until=missing_days[-1]):
    day_start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    params = {
        "start_date": day_start.isoformat(),
        "end_date": day_end.isoformat(),
        "time_trunc": "quarter-hour"
    }

    try:
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        if res.status_code == 403:
            print(f"⛔ Forbidden – Token expired on {day.date()}")
            failed_days.append(day.date())
            continue
        res.raise_for_status()
        values = res.json()["indicator"]["values"]
        df = pd.DataFrame(values)
        df["datetime"] = pd.to_datetime(df["datetime"])
        all_new.append(df)
    except Exception as e:
        print(f"  ❌ Error on {day.date()}: {e}")
        failed_days.append(day.date())

# --- Merge + Save ---
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

# --- Write Markdown Report ---
with open(REPORT_PATH, "w") as f:
    f.write("# 📊 Weekly Wind Data Missing Report\n\n")
    if failed_days:
        f.write("## ⚠️ Days that could not be retrieved:\n\n")
        f.write("| # | Missing Day |\n|---|--------------|\n")
        for i, d in enumerate(failed_days, start=1):
            f.write(f"| {i} | {d} |\n")
    else:
        f.write("✅ All requested data was successfully filled.\n")

print("📄 Report generated at:", REPORT_PATH)






