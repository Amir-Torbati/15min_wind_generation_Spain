import pandas as pd
from datetime import datetime
import os
from zoneinfo import ZoneInfo
from collections import defaultdict

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

# --- Build expected 15-min range ---
start = df["datetime"].min()
end = datetime.now(TZ).replace(second=0, microsecond=0)
expected = pd.date_range(start=start, end=end, freq=QUARTER_FREQ, tz=TZ)

# --- Find missing timestamps ---
missing = expected.difference(df["datetime"])

# --- Group by day: {date -> [time, time, ...]} ---
missing_by_day = defaultdict(list)
for ts in missing:
    date_str = ts.strftime("%Y-%m-%d")
    time_str = ts.strftime("%H:%M")
    missing_by_day[date_str].append(time_str)

# --- Generate Markdown report ---
with open(REPORT_PATH, "w") as f:
    f.write("# 📊 Wind Data Missing Report\n\n")

    if not missing_by_day:
        f.write("✅ All 15-minute intervals are present.\n")
        print("✅ No missing timestamps.")
    else:
        f.write(f"⚠️ {len(missing)} missing timestamps found across {len(missing_by_day)} days.\n\n")
        f.write("| Date | Missing Times (Europe/Madrid) |\n")
        f.write("|------|-------------------------------|\n")
        for date, times in sorted(missing_by_day.items()):
            time_str = ", ".join(times)
            f.write(f"| {date} | {time_str} |\n")
        print(f"🔍 Found missing data on {len(missing_by_day)} day(s).")

print("📄 Report saved to:", REPORT_PATH)
