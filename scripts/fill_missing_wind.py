import pandas as pd
import requests
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# --- Config ---
API_TOKEN = "your_token_here"
BASE_URL = "https://api.esios.ree.es/indicators/540"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- Load full database ---
df = pd.read_csv("database/full_wind_data.csv", parse_dates=["datetime"])
df = df.drop_duplicates(subset="datetime").sort_values("datetime")

# --- Generate expected timestamp range (from min to max) ---
start = df["datetime"].min()
end = df["datetime"].max()
expected = pd.date_range(start=start, end=end, freq="15T", tz="UTC")

# --- Detect missing timestamps ---
existing = pd.to_datetime(df["datetime"]).dt.tz_localize("UTC")
missing = expected.difference(existing)

if missing.empty:
    print("✅ No missing timestamps found.")
    exit()

print(f"⚠️ Missing timestamps: {len(missing)}")

# --- Group missing timestamps into continuous time chunks ---
def group_ranges(timestamps, freq="15T"):
    timestamps = sorted(timestamps)
    ranges = []
    start = end = timestamps[0]

    for ts in timestamps[1:]:
        if ts - end == pd.Timedelta(freq):
            end = ts
        else:
            ranges.append((start, end + pd.Timedelta(freq)))
            start = end = ts
    ranges.append((start, end + pd.Timedelta(freq)))
    return ranges

missing_ranges = group_ranges(missing)

# --- Fetch missing ranges ---
filled = []
for start_ts, end_ts in missing_ranges:
    print(f"📡 Fetching: {start_ts} to {end_ts}")
    params = {
        "start_date": start_ts.isoformat(),
        "end_date": end_ts.isoformat(),
        "time_trunc": "quarter-hour"
    }

    try:
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        if res.status_code == 403:
            print("🚫 403 Forbidden – Token likely expired. Exiting.")
            exit(1)

        res.raise_for_status()
        values = res.json()["indicator"]["values"]
        df_new = pd.DataFrame(values)
        if not df_new.empty:
            df_new["datetime"] = pd.to_datetime(df_new["datetime"])
            filled.append(df_new)
    except Exception as e:
        print(f"❌ Error during fetch: {e}")

# --- Merge and save ---
if filled:
    df_all = pd.concat([df] + filled)
    df_all = df_all.drop_duplicates(subset="datetime").sort_values("datetime")

    # Save all formats
    os.makedirs("database", exist_ok=True)
    df_all.to_csv("database/full_wind_data.csv", index=False)
    df_all.to_parquet("database/full_wind_data.parquet", index=False)

    import duckdb
    con = duckdb.connect("database/full_wind_data.duckdb")
    con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_all")
    con.close()

    print(f"✅ Patched {len(missing)} timestamps.")
else:
    print("⚠️ No new data could be fetched.")
