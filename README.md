## Collection migration - 1 October 2026

Active collection has moved to the grouped five-curve workflow in [Energy Data Collection](https://github.com/Amir-Torbati/ree-data-platform/blob/main/docs/FUNDAMENTALS.md) (owner access required). It collects verified ESIOS indicators 1295 (actual PV), 551 (actual wind), 1293 (actual demand), 542 (PV forecast) and 541 (wind forecast).

**The files below are a preserved legacy archive, not certified actual-generation data.** The review found source-ID mismatches in the solar/wind collectors and reliability problems in the demand scraper. Current code alone does not establish the origin of every historical file. Do not combine this archive with the corrected curves without checking provenance. Legacy collection/backfill workflows are retired to prevent overlapping or mislabelled downloads. Data files and Git history have not been deleted or relabelled.

---

🌬️ Wind Generation – Spain (15-Minute Resolution)
This project collects and stores real-time 15-minute wind generation data for the Peninsular region of Spain using the official Red Eléctrica de España (ESIOS) API.

We built a fully automated pipeline that updates every 15 minutes and stores results in structured daily CSV files — perfect for analytics, modeling, forecasting, or integration into dashboards.

📁 Folder Structure

data/
└── YYYY-MM-DD.csv       ← 15-minute wind data, one file per local day

scripts/
└── collect_wind_15min.py   ← Python script for ESIOS fetch

.github/workflows/
└── fetch_wind_15min.yml    ← GitHub Actions automation

🔄 What’s Automated?
✅ Real-Time Fetching (fetch_wind_15min.yml)

Pulls 15-minute wind generation data via the ESIOS API

Runs every 15 minutes using GitHub Actions

Appends new values to today's CSV (data/YYYY-MM-DD.csv)

Deduplicates and sorts by timestamp

✅ Timezone-Aware Storage

Tracks Spain local time (Europe/Madrid)

Converts timestamps to UTC for consistency

File is named by local Spain date (e.g. 2025-04-14.csv)

🛠 Tech Stack
Python 3.11

Pandas

Requests

GitHub Actions (CI/CD)

Red Eléctrica de España (ESIOS API)

📡 Data Source
Provider: Red Eléctrica de España (REE)

API: https://api.esios.ree.es/

Indicator: 540 (Wind Generation - Peninsular)

Timezone: Data in UTC, files named in Spain local time

🗺️ Roadmap
✅ Collect 15-minute wind generation
✅ Store daily files in data/
✅ GitHub Actions automation
🔜 Add Parquet + DuckDB formats
🔜 Backfill from 2023
🔜 Build live dashboard (Streamlit or Looker)
🔜 Merge with weather & OMIE market prices
🔜 Wind + BESS optimization modeling

👤 Author
Created with 💨 by Amir Torbati
All rights reserved © 2025

💬 Please cite or credit if you use this project for academic, research, or commercial work.

⚡ Let the wind power your models.
