import pandas as pd
from datetime import datetime
import os
from zoneinfo import ZoneInfo
from collections import defaultdict

# --- CONFIG ---
DB_PATH = "main database/wind_local.csv"
REPORT_PATH = "reports/missing_report.md"
QUARTER_FREQ = "15min"
TZ = ZoneInfo("Europe/Madrid")

# --- Setup ---
os.makedirs("reports", exist_ok=True)

# --- Load tidy DB ---
if not os.path.exists(DB_PATH):
    print("❌ Tidy database not found:", DB_PATH)
    exit()

df = pd.read_csv(DB_PATH)

# --- Validate columns ---
required_cols = {"date", "time", "offset", "value"}
if not required_cols.issubset(df.columns):
    print("⚠️ Missing required columns in tidy data.")
    exit()

# --- Build full timezone-aware datetime column ---
# Combine date + time + offset into a single datetime string, e.g. "2023-01-01 00:00+01:00"
df["datetime_str"] = df["date"] + " " + df["time"] + df["offset"]
df["datetime"] = pd.to_datetime(df["datetime_str"])  # auto-parses with offset

# --- Build expected 15-min time series ---
start = df["datetime"].min()
end = datetime.now(ZoneInfo("Europe/Madrid")).replace(second=0, microsecond=0)
expected = pd.date_range(start=start, end=end, freq=QUARTER_FREQ, tz=TZ)

# --- Compare and find missing timestamps ---
actual = df["datetime"].sort_values().drop_duplicates()
missing = expected.difference(actual)

# --- Group missing by date
missing_by_day = defaultdict(list)
for ts in missing:
    date_str = ts.strftime("%Y-%m-%d")
    time_str = ts.strftime("%H:%M")
    missing_by_day[date_str].append(time_str)

# --- Generate Markdown Report ---
with open(REPORT_PATH, "w") as f:
    f.write("# 📊 Wind Data Missing Report\n\n")

    if not missing_by_day:
        f.write("✅ All 15-minute intervals are present.\n")
        print("✅ No missing timestamps.")
    else:
        f.write(f"⚠️ {len(missing)} missing timestamps across {len(missing_by_day)} days.\n\n")
        f.write("| Date | Missing Times (Europe/Madrid) |\n")
        f.write("|------|-------------------------------|\n")
        for date, times in sorted(missing_by_day.items()):
            f.write(f"| {date} | {', '.join(times)} |\n")
        print(f"🔍 Found missing data on {len(missing_by_day)} day(s).")

print("📄 Report saved to:", REPORT_PATH)

