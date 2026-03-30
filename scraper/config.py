from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("SCRAPPY_DATA_DIR", BASE_DIR / "data")).resolve()

NEPSE_DATA_DIR = DATA_DIR / "nepse"
IPO_DATA_DIR = DATA_DIR / "ipo"

MARKET_SUMMARY_CSV = NEPSE_DATA_DIR / "market_summary_timeseries.csv"
TODAY_PRICE_CSV = NEPSE_DATA_DIR / "today_price_timeseries.csv"
IPO_FEED_JSON = IPO_DATA_DIR / "ipo_feed.json"

IPO_UPCOMING_URL = os.getenv("IPO_UPCOMING_URL", "https://merolagani.com/Ipo.aspx?type=upcoming")
NEPSELINK_IPO_OPENING_URL = os.getenv(
    "NEPSELINK_IPO_OPENING_URL", "https://nepselink.com/ipo-opening"
)
IPO_RESULTS_URL = os.getenv("IPO_RESULTS_URL", "https://merolagani.com/Announcements.aspx")
SHARESANSAR_LIVE_URL = os.getenv("SHARESANSAR_LIVE_URL", "https://www.sharesansar.com/live-trading")

SCRAPPY_TIMEOUT_SECONDS = float(os.getenv("SCRAPPY_TIMEOUT_SECONDS", "15"))
NEPSE_VERIFY_SSL = os.getenv("NEPSE_VERIFY_SSL", "false").strip().lower() in {"1", "true", "yes"}
