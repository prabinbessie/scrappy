from __future__ import annotations

from typing import Any

import requests
from bs4 import BeautifulSoup

from scraper.config import SCRAPPY_TIMEOUT_SECONDS, SHARESANSAR_LIVE_URL

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}


COL_INDEX: dict[str, int] = {
    "symbol": 1,
    "last_price": 2,
    "point_change": 3,
    "pct_change": 4,
    "open_price": 5,
    "high_price": 6,
    "low_price": 7,
    "quantity": 8,
    "previous_close": 9,
}


def _to_float(value: str) -> float | None:
    if not value:
        return None
    normalized = value.strip().replace(",", "")
    if normalized in {"", "-", "--"}:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _cell_text(cells: list[Any], name: str) -> str:
    return cells[COL_INDEX[name]].get_text(strip=True)


def parse_sharesansar_live_table(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "headFixed"}) or soup.find("table", {"class": "dataTable"})
    if not table:
        return []

    body = table.find("tbody")
    rows = body.find_all("tr") if body else table.find_all("tr")[1:]
    result: list[dict[str, Any]] = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) <= max(COL_INDEX.values()):
            continue

        result.append(
            {
                "symbol": _cell_text(cols, "symbol"),
                "lastTradedPrice": _to_float(_cell_text(cols, "last_price")),
                "pointChange": _to_float(_cell_text(cols, "point_change")),
                "percentageChange": _to_float(_cell_text(cols, "pct_change").replace("%", "")),
                "openPrice": _to_float(_cell_text(cols, "open_price")),
                "highPrice": _to_float(_cell_text(cols, "high_price")),
                "lowPrice": _to_float(_cell_text(cols, "low_price")),
                "totalTradedQuantity": _to_float(_cell_text(cols, "quantity")),
                "previousDayClosePrice": _to_float(_cell_text(cols, "previous_close")),
            }
        )

    return result


def fetch_sharesansar_live_rows() -> list[dict[str, Any]]:
    response = requests.get(
        SHARESANSAR_LIVE_URL,
        headers=HEADERS,
        timeout=SCRAPPY_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return parse_sharesansar_live_table(response.text)
