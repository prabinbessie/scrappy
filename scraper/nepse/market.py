from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from scraper.config import MARKET_SUMMARY_CSV, TODAY_PRICE_CSV
from scraper.io import append_rows_to_csv
from scraper.nepse.client import NepseDataClient
from scraper.nepse.fallback import fetch_sharesansar_live_rows

MARKET_SUMMARY_FIELDS = [
    "scraped_at_utc",
    "scraped_epoch",
    "weekday",
    "hour_utc",
    "is_market_open",
    "as_of_date",
    "total_turnover",
    "total_traded_shares",
    "total_transactions",
    "total_trades",
    "total_market_cap",
    "floated_market_cap",
    "nepse_index",
    "nepse_point_change",
    "nepse_percentage_change",
]


TODAY_PRICE_FIELDS = [
    "scraped_at_utc",
    "scraped_epoch",
    "weekday",
    "hour_utc",
    "price_source",
    "is_market_open",
    "business_date",
    "symbol",
    "security_name",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "last_traded_price",
    "previous_closing_price",
    "point_change",
    "percentage_change",
    "total_traded_quantity",
    "total_traded_value",
    "total_trades",
    "intraday_range",
    "intraday_range_pct_of_open",
    "return_from_prev_close_pct",
    "turnover_per_trade",
    "vwap_proxy",
]


PRICE_KEY_ALIASES: dict[str, list[str]] = {
    "business_date": ["businessDate", "asOf"],
    "symbol": ["symbol", "securitySymbol"],
    "security_name": ["securityName", "companyName"],
    "open_price": ["openPrice"],
    "high_price": ["highPrice"],
    "low_price": ["lowPrice"],
    "close_price": ["closePrice"],
    "last_traded_price": ["lastTradedPrice", "lastUpdatedPrice", "ltp"],
    "previous_close": ["previousClosing", "previousClose", "previousDayClosePrice"],
    "point_change": ["pointChange", "change"],
    "percentage_change": ["percentageChange", "perChange"],
    "total_traded_quantity": ["totalTradedQuantity", "volume"],
    "total_traded_value": ["totalTradedValue", "turnover"],
    "total_trades": ["totalTrades", "numberOfTransactions"],
}


def _pick(source: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    for key in keys:
        if key in source and source[key] not in (None, ""):
            return source[key]
    return default


def _to_float(value: Any) -> float | None:
    if value in (None, "", "-"):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return None


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _time_dimensions(scraped_at_utc: str) -> dict[str, int]:
    scraped_dt = datetime.fromisoformat(scraped_at_utc)
    return {
        "scraped_epoch": int(scraped_dt.timestamp()),
        "weekday": scraped_dt.weekday(),
        "hour_utc": scraped_dt.hour,
    }


def _resolved_price_values(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "business_date": _pick(item, PRICE_KEY_ALIASES["business_date"]),
        "symbol": _pick(item, PRICE_KEY_ALIASES["symbol"]),
        "security_name": _pick(item, PRICE_KEY_ALIASES["security_name"]),
        "open_price": _to_float(_pick(item, PRICE_KEY_ALIASES["open_price"])),
        "high_price": _to_float(_pick(item, PRICE_KEY_ALIASES["high_price"])),
        "low_price": _to_float(_pick(item, PRICE_KEY_ALIASES["low_price"])),
        "close_price": _to_float(_pick(item, PRICE_KEY_ALIASES["close_price"])),
        "last_traded_price": _to_float(_pick(item, PRICE_KEY_ALIASES["last_traded_price"])),
        "previous_close": _to_float(_pick(item, PRICE_KEY_ALIASES["previous_close"])),
        "point_change": _to_float(_pick(item, PRICE_KEY_ALIASES["point_change"])),
        "percentage_change": _to_float(_pick(item, PRICE_KEY_ALIASES["percentage_change"])),
        "total_traded_quantity": _to_float(_pick(item, PRICE_KEY_ALIASES["total_traded_quantity"])),
        "total_traded_value": _to_float(_pick(item, PRICE_KEY_ALIASES["total_traded_value"])),
        "total_trades": _to_float(_pick(item, PRICE_KEY_ALIASES["total_trades"])),
    }


def _build_market_summary_row(
    summary: dict[str, Any], status: dict[str, Any], scraped_at_utc: str
) -> dict[str, Any]:
    time_values = _time_dimensions(scraped_at_utc)
    return {
        "scraped_at_utc": scraped_at_utc,
        **time_values,
        "is_market_open": _pick(status, ["isOpen", "status"], "CLOSE"),
        "as_of_date": _pick(summary, ["asOf", "businessDate", "date"]),
        "total_turnover": _pick(summary, ["totalTurnover", "totalTurnoverRs"]),
        "total_traded_shares": _pick(summary, ["totalTradedShares", "totalVolume"]),
        "total_transactions": _pick(summary, ["totalTransactions"]),
        "total_trades": _pick(summary, ["totalTrades"]),
        "total_market_cap": _pick(summary, ["totalMarketCapitalization", "totalMarketCap"]),
        "floated_market_cap": _pick(summary, ["floatMarketCapitalization", "floatedMarketCap"]),
        "nepse_index": _pick(summary, ["index", "nepseIndex"]),
        "nepse_point_change": _pick(summary, ["pointChange", "change"]),
        "nepse_percentage_change": _pick(summary, ["percentageChange", "perChange"]),
    }


def _build_today_price_rows(
    today_price: list[dict[str, Any]],
    status: dict[str, Any],
    scraped_at_utc: str,
    price_source: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    time_values = _time_dimensions(scraped_at_utc)
    market_state = _pick(status, ["isOpen", "status"], "CLOSE")

    for item in today_price:
        values = _resolved_price_values(item)
        open_price = values["open_price"]
        high_price = values["high_price"]
        low_price = values["low_price"]
        close_price = values["close_price"]
        last_traded_price = values["last_traded_price"]
        previous_close = values["previous_close"]
        point_change = values["point_change"]
        total_traded_quantity = values["total_traded_quantity"]
        total_traded_value = values["total_traded_value"]
        total_trades = values["total_trades"]

        intraday_range = None
        if high_price is not None and low_price is not None:
            intraday_range = high_price - low_price

        intraday_range_pct_of_open = _safe_div(intraday_range, open_price)
        if intraday_range_pct_of_open is not None:
            intraday_range_pct_of_open *= 100

        return_from_prev_close_pct = _safe_div(
            (
                (last_traded_price - previous_close)
                if last_traded_price is not None and previous_close is not None
                else None
            ),
            previous_close,
        )
        if return_from_prev_close_pct is not None:
            return_from_prev_close_pct *= 100

        turnover_per_trade = _safe_div(total_traded_value, total_trades)
        vwap_proxy = _safe_div(total_traded_value, total_traded_quantity)

        row = {
            "scraped_at_utc": scraped_at_utc,
            **time_values,
            "price_source": price_source,
            "is_market_open": market_state,
            "business_date": values["business_date"],
            "symbol": values["symbol"],
            "security_name": values["security_name"],
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "last_traded_price": last_traded_price,
            "previous_closing_price": previous_close,
            "point_change": (
                point_change
                if point_change is not None
                else (
                    last_traded_price - previous_close
                    if last_traded_price is not None and previous_close is not None
                    else None
                )
            ),
            "percentage_change": values["percentage_change"],
            "total_traded_quantity": total_traded_quantity,
            "total_traded_value": total_traded_value,
            "total_trades": total_trades,
            "intraday_range": intraday_range,
            "intraday_range_pct_of_open": intraday_range_pct_of_open,
            "return_from_prev_close_pct": return_from_prev_close_pct,
            "turnover_per_trade": turnover_per_trade,
            "vwap_proxy": vwap_proxy,
        }
        rows.append(row)

    return rows


def scrape_market_to_csv(client: NepseDataClient | None = None) -> dict[str, int]:
    data_client = client or NepseDataClient()
    scraped_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    status = data_client.fetch_market_status()
    summary = data_client.fetch_market_summary()
    price_source = "nepse_official"

    try:
        today_price = data_client.fetch_today_price()
    except Exception:
        today_price = []

    if not today_price:
        today_price = fetch_sharesansar_live_rows()
        price_source = "sharesansar_fallback"

    summary_row = _build_market_summary_row(summary, status, scraped_at_utc)
    today_price_rows = _build_today_price_rows(today_price, status, scraped_at_utc, price_source)

    summary_count = append_rows_to_csv(
        MARKET_SUMMARY_CSV,
        [summary_row],
        MARKET_SUMMARY_FIELDS,
        unique_key_fields=["scraped_at_utc"],
    )
    price_count = append_rows_to_csv(
        TODAY_PRICE_CSV,
        today_price_rows,
        TODAY_PRICE_FIELDS,
        unique_key_fields=["scraped_at_utc", "symbol"],
    )

    return {
        "summary_rows_written": summary_count,
        "price_rows_written": price_count,
        "price_source": price_source,
    }
