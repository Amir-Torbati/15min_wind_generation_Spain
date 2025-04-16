import pandas as pd
from datetime import datetime
import os
from zoneinfo import ZoneInfo

# --- CONFIG ---
DB_PATH = "database/full_wind_data_tidy.csv"
REPORT_PATH = "reports/missing_report.md"
QUARTER_FREQ = "15min"
TZ = ZoneInfo("Europe/Madrid")

# --- Setup ---
os.makedirs("reports", exist_ok=True)

# --- Load tidy DB ---
if not os.path.exists(DB_PATH):
    print("❌ Tidy database not found.")
    exit()

df = pd.read_csv(DB_PATH)

# --- Rebuild datetime column ---
df["datetime"] = pd.to_datetime(df["date_utc"] + " " + df["time_utc"], utc=True)
df["datetime"] = df["datetime"].dt.tz_convert(TZ)

# --- Build expected full timestamp range ---
start = df["datetime"].min()
end = datetime.now(TZ).replace(second=0, microsecond=0)
expected = pd.date_range(start=start, end=end, freq=QUARTER_FREQ, tz=TZ)

# --- Compare to existing ---
missing = expected.difference(df["datetime"])

# --- Generate report ---
with open(REPORT_PATH, "w") as f:
    f.write("# 📊 Wind Data Missing Report\n\n")
    if missing.empty:
        f.write("✅ All 15-minute intervals are present.\n")
        print("✅ No missing timestamps.")
    else:
        f.write(f"⚠️ {len(missing)} missing timestamps found.\n\n")
        f.write("| # | Missing Timestamp (Europe/Madrid) |\n")
        f.write("|---|-----------------------------|\n")
        for i, ts in enumerate(missing, start=1):
            f.write(f"| {i} | {ts.strftime('%Y-%m-%d %H:%M')} |\n")
        print(f"🔍 Found {len(missing)} missing timestamps.")

print("📄 Report saved to:", REPORT_PATH)
