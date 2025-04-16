import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import duckdb
from zoneinfo import ZoneInfo
from dateutil.rrule import rrule, DAILY

# --- CONFIG ---
DB_PATH = "database/full_wind_data_tidy"
REPORT_PATH = "reports/missing_report.md"
QUARTER_FREQ = "15min"
API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/540"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- Setup ---
os.makedirs("database", exist_ok=True)
os.makedirs("reports", exist_ok=True)

# --- Load existing tidy DB ---
if os.path.exists(f"{DB_PATH}.csv"):
    df_db = pd.read_csv(f"{DB_PATH}.csv")
    df_db["datetime"] = pd.to_datetime(df_db["date_utc"] + " " + df_db["time_utc"], utc=True)
    df_db["datetime"] = df_db["datetime"].dt.tz_convert("Europe/Madrid")
else:
    print("⚠️ Tidy database not found.")
    exit()

# --- Create expected 15-minute intervals ---
start = df_db["datetime"].min()
end = datetime.now(ZoneInfo("Europe/Madrid")).replace(second=0, microsecond=0)
expected = pd.date_range(start=start, end=end, freq=QUARTER_FREQ, tz="Europe/Madrid")

# --- Find missing timestamps ---
existing = df_db["datetime"]
missing = expected.difference(existing)

if missing.empty:
    print("✅ No missing timestamps.")
    with open(REPORT_PATH, "w") as f:
        f.write("# 📊 Wind Data Missing Report\n\n✅ All data is complete.\n")
    exit()

print(f"🔍 Found {len(missing)} missing timestamps. Attempting to fetch...")

# --- Fetch missing days ---
missing_days = sorted(set(ts.date() for ts in missing))
all_new = []
failed_days = []

for day in rrule(freq=DAILY, dtstart=missing_days[0], until=missing_days[-1]):
    day_start = datetime.combine(day, datetime.min.time(), tzinfo=ZoneInfo("Europe/Madrid"))
    day_end = day_start + timedelta(days=1)

    params = {
        "start_date": day_start.astimezone(ZoneInfo("UTC")).isoformat(),
        "end_date": day_end.astimezone(ZoneInfo("UTC")).isoformat(),
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
        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert("Europe/Madrid")
            df["date_local"] = df["datetime"].dt.date
            df["time_local"] = df["datetime"].dt.strftime("%H:%M")
            df["date_utc"] = df["datetime"].dt.tz_convert("UTC").dt.date
            df["time_utc"] = df["datetime"].dt.tz_convert("UTC").dt.strftime("%H:%M")
            df = df.rename(columns={"value": "value_mw"})
            df_tidy = df[["date_local", "time_local", "date_utc", "time_utc", "value_mw"]]
            all_new.append(df_tidy)
    except Exception as e:
        print(f"  ❌ Error on {day.date()}: {e}")
        failed_days.append(day.date())

# --- Merge and Save Updated DB ---
if all_new:
    df_new = pd.concat(all_new)
    df_full = pd.concat([df_db, df_new]).drop_duplicates(
        subset=["date_utc", "time_utc"]
    ).sort_values(["date_utc", "time_utc"]).reset_index(drop=True)

    if "row" in df_full.columns:
        df_full.drop(columns="row", inplace=True)
    df_full.insert(0, "row", df_full.index + 1)

    # Fix types for Parquet
    df_full["date_local"] = pd.to_datetime(df_full["date_local"]).dt.date
    df_full["date_utc"] = pd.to_datetime(df_full["date_utc"]).dt.date
    df_full["time_local"] = df_full["time_local"].astype(str)
    df_full["time_utc"] = df_full["time_utc"].astype(str)
    df_full["value_mw"] = pd.to_numeric(df_full["value_mw"], errors="coerce")

    # Save to all formats
    df_full.to_csv(f"{DB_PATH}.csv", index=False)
    df_full.to_parquet(f"{DB_PATH}.parquet", index=False)

    con = duckdb.connect(f"{DB_PATH}.duckdb")
    con.execute("CREATE OR REPLACE TABLE wind_tidy AS SELECT * FROM df_full")
    con.close()

    print(f"✅ Added {len(df_new)} new rows. DB now has {len(df_full)} rows.")
else:
    print("⚠️ No new data fetched.")

# --- Generate Markdown Report ---
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




