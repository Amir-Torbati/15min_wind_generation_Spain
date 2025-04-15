import requests
import pandas as pd
from datetime import datetime
import os
from dateutil.relativedelta import relativedelta
import duckdb  # You need to install this if running locally

# --- CONFIG ---
API_TOKEN = "YOUR_TOKEN_HERE"
BASE_URL = "https://api.esios.ree.es/indicators/540"  # Wind generation indicator
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- SETUP ---
start_date = datetime(2022, 1, 1)
end_date = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
all_data = []

# --- FETCH LOOP (month by month) ---
print(f"📡 Fetching wind 15-min data from {start_date.date()} to {end_date.date()}...")
current = start_date

while current < end_date:
    next_month = current + relativedelta(months=1)
    period_end = min(next_month, end_date)

    params = {
        "start_date": current.isoformat() + "Z",
        "end_date": period_end.isoformat() + "Z",
        "time_trunc": "quarter-hour"
    }

    try:
        print(f"  ⏳ {current.date()} → {period_end.date()}")
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        values = res.json()["indicator"]["values"]
        df = pd.DataFrame(values)
        if not df.empty and "datetime" in df:
            df["datetime"] = pd.to_datetime(df["datetime"])
            all_data.append(df)
    except Exception as e:
        print(f"  ❌ Error fetching {current.date()}: {e}")

    current = period_end

# --- SAVE ---
os.makedirs("database", exist_ok=True)

if all_data:
    df_all = pd.concat(all_data).drop_duplicates(subset=["datetime"]).sort_values("datetime")
    df_all.to_csv("database/full_wind_data.csv", index=False)
    df_all.to_parquet("database/full_wind_data.parquet", index=False)

    con = duckdb.connect("database/full_wind_data.duckdb")
    con.execute("CREATE OR REPLACE TABLE wind AS SELECT * FROM df_all")
    con.close()

    print(f"✅ Saved {len(df_all)} 15-min rows to database/")
else:
    print("⚠️ No data was fetched.")


