import os
import requests
import pandas as pd
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
import duckdb

# --- CONFIG ---
API_TOKEN = "YOUR_ESIOS_API_TOKEN"  # 🔐 Replace with your actual token
BASE_URL = "https://api.esios.ree.es/indicators/540"  # Wind generation (Peninsular)
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}
TZ = ZoneInfo("Europe/Madrid")

# --- DATE RANGE ---
start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ)
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

# --- OUTPUT DIR ---
output_dir = "main database"
os.makedirs(output_dir, exist_ok=True)

# --- FETCH LOOP ---
all_data = []
print(f"📡 Fetching wind data from {start_date_local.date()} to {end_date_local.date()}...")

current_local = start_date_local
while current_local < end_date_local:
    next_month_local = current_local + relativedelta(months=1)
    period_end_local = min(next_month_local, end_date_local)

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
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            all_data.append(df[["datetime", "value"]])

    except Exception as e:
        print(f"  ❌ Error on {current_local.date()}: {e}")

    current_local = period_end_local

# --- CLEAN & TRANSFORM ---
if all_data:
    df_all = pd.concat(all_data).drop_duplicates(subset=["datetime"]).sort_values("datetime")

    df_all["date"] = df_all["datetime"].dt.date.astype(str)
    df_all["time"] = df_all["datetime"].dt.strftime("%H:%M")
    df_all["offset"] = df_all["datetime"].dt.strftime("%z").str.slice(0, 3) + ":" + df_all["datetime"].dt.strftime("%z").str.slice(3, 5)

    df_clean = df_all[["date", "time", "offset", "value"]]

    # --- SAVE ---
    df_clean.to_csv(f"{output_dir}/wind_local.csv", index=False)
    df_clean.to_parquet(f"{output_dir}/wind_local.parquet", index=False)

    con = duckdb.connect(f"{output_dir}/wind_local.duckdb")
    con.execute("CREATE OR REPLACE TABLE wind_local AS SELECT * FROM df_clean")
    con.close()

    print(f"✅ Saved {len(df_clean)} rows to '{output_dir}/'")
else:
    print("⚠️ No data was fetched.")
