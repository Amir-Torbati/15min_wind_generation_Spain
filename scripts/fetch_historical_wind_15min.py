import os
import requests
import pandas as pd
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
import duckdb

# --- CONFIG ---
API_TOKEN = os.environ["ESIOS_API_TOKEN"]
BASE_URL = "https://api.esios.ree.es/indicators/540"
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

print(f"📡 Fetching wind data from {start_date_local.date()} to {end_date_local.date()}...")
all_data = []

# --- MONTHLY FETCH LOOP ---
current_local = start_date_local
while current_local < end_date_local:
    next_month_local = current_local + relativedelta(months=1)
    period_end_local = min(next_month_local, end_date_local)

    start_utc = current_local.astimezone(timezone.utc)
    end_utc = period_end_local.astimezone(timezone.utc)

    params = {
        "start_date": start_utc.isoformat(),
        "end_date": end_utc.isoformat(),
        "time_trunc": "quarter-hour"
    }

    print(f"⏳ {current_local.date()} → {period_end_local.date()}")

    try:
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        data = res.json()
        values = data.get("indicator", {}).get("values", [])

        df = pd.DataFrame(values)
        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(TZ)
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

            df["date"] = df["datetime"].dt.date.astype(str)
            df["time"] = df["datetime"].dt.strftime("%H:%M")
            df["offset"] = df["datetime"].dt.strftime("%z").str[:3] + ":" + df["datetime"].dt.strftime("%z").str[3:]

            df_clean = df[["date", "time", "offset", "value"]]
            all_data.append(df_clean)

            print(f"✅ Got {len(df_clean)} rows")
        else:
            print("⚠️ No data for this period.")

    except Exception as e:
        print(f"❌ Error on {current_local.date()}: {e}")

    current_local = period_end_local

# --- CLEAN & SAVE ---
if all_data:
    df_all = pd.concat(all_data).drop_duplicates(subset=["date", "time", "offset"]).sort_values(["date", "time"])

    df_all.to_csv(f"{output_dir}/wind_local.csv", index=False)
    df_all.to_parquet(f"{output_dir}/wind_local.parquet", index=False)

    con = duckdb.connect(f"{output_dir}/wind_local.duckdb")
    con.execute("CREATE OR REPLACE TABLE wind_local AS SELECT * FROM df_all")
    con.close()

    print(f"✅ Saved {len(df_all)} rows to '{output_dir}/'")
    print("📁 Files:", os.listdir(output_dir))
else:
    print("⚠️ No data was fetched.")

