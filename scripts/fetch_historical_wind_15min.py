import requests
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os
from dateutil.relativedelta import relativedelta
import duckdb

# --- CONFIG ---
API_TOKEN = "YOUR_TOKEN_HERE"
BASE_URL = "https://api.esios.ree.es/indicators/540"  # Wind generation
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- TIMEZONE CONFIG ---
TZ = ZoneInfo("Europe/Madrid")

# --- TIME RANGE ---
start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ)
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

print(f"📡 Fetching 15-min wind data from {start_date_local.date()} to {end_date_local.date()}...")

# --- DATA COLLECTION ---
all_data = []
current_local = start_date_local

while current_local < end_date_local:
    next_month_local = current_local + relativedelta(months=1)
    period_end_local = min(next_month_local, end_date_local)

    # Convert to UTC for API
    current_utc = current_local.astimezone(timezone.utc)
    period_end_utc = period_end_local.astimezone(timezone.utc)

    params = {
        "start_date": current_utc.isoformat(),
        "end_date": period_end_utc.isoformat(),
        "time_trunc": "quarter-hour"
    }

    try:
        print(f"  ⏳ {current_local.date()} → {period_end_local.date()}")
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        values = res.json()["indicator"]["values"]
        df = pd.DataFrame(values)

        if not df.empty and "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_convert(TZ)
            df["date"] = df["datetime"].dt.date
            df["time"] = df["datetime"].dt.strftime("%H:%M")
            df["tz_offset"] = df["datetime"].dt.strftime("%z").str.replace(r"(\d{2})(\d{2})", r"+\1:\2", regex=True)
            df["value_mw"] = df["value"]
            tidy = df[["date", "time", "tz_offset", "value_mw"]]
            all_data.append(tidy)

    except Exception as e:
        print(f"  ❌ Error on {current_local.date()}: {e}")

    current_local = period_end_local

# --- SAVE SECTION ---
if all_data:
    df_all = pd.concat(all_data).drop_duplicates().sort_values(by=["date", "time"]).reset_index(drop=True)

    # Create folder and a placeholder file so git sees it
    save_dir = os.path.join(os.path.dirname(__file__), "main_database")
    os.makedirs(save_dir, exist_ok=True)
    open(os.path.join(save_dir, ".gitkeep"), "w").close()

    # Save in all 3 formats
    df_all.to_csv(os.path.join(save_dir, "wind_data.csv"), index=False)
    df_all.to_parquet(os.path.join(save_dir, "wind_data.parquet"), index=False)

    con = duckdb.connect(os.path.join(save_dir, "wind_data.duckdb"))
    con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_all")
    con.close()

    print(f"✅ Done! Saved {len(df_all)} rows in 'main_database/' as CSV, Parquet, and DuckDB.")
else:
    print("⚠️ No data was fetched.")


