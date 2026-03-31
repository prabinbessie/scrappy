from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from collections.abc import Callable
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

from scraper.config import (
    IPO_RESULTS_URL,
    IPO_UPCOMING_URL,
    NEPSELINK_IPO_OPENING_URL,
    SCRAPPY_TIMEOUT_SECONDS,
)
from scraper.nepse.client import NepseDataClient

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}


def fetch_html(url: str) -> str:
    retries = 3
    last_error: requests.RequestException | None = None

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=SCRAPPY_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= retries:
                raise
            time.sleep(1.0 * attempt)

    if last_error is not None:
        raise last_error
    raise requests.RequestException(f"Failed to fetch URL: {url}")


def _join_url(base_url: str, href: str | None) -> str | None:
    if not href:
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return "https://merolagani.com" + href
    return base_url.rstrip("/") + "/" + href


def parse_upcoming_ipo_page(html: str, source_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("div", class_="announcement-list")
    if not container:
        return []

    records: list[dict[str, Any]] = []
    for item in container.find_all("div", class_="media"):
        body = item.find("div", class_="media-body")
        if not body:
            continue

        link = body.find("a")
        if not link:
            continue

        title = link.get_text(" ", strip=True)
        details = body.get_text(" ", strip=True)
        date_tag = item.find("small", class_="text-muted")
        announcement_date = date_tag.get_text(" ", strip=True) if date_tag else None

        records.append(
            {
                "title": title,
                "details": details,
                "announcement_date": announcement_date,
                "url": _join_url(source_url, link.get("href")),
                "source": "merolagani_upcoming",
            }
        )

    return records


def parse_ipo_result_page(html: str, source_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    records: list[dict[str, Any]] = []

    blocks = soup.select("div.featured-news-list")
    if blocks:
        for block in blocks:
            link = block.find("a")
            if not link:
                continue

            title_tag = block.find("h4")
            title = (
                title_tag.get_text(" ", strip=True) if title_tag else link.get_text(" ", strip=True)
            )
            date_tag = block.find("span", class_="text-org")
            announcement_date = date_tag.get_text(" ", strip=True) if date_tag else None

            records.append(
                {
                    "title": title,
                    "details": title,
                    "announcement_date": announcement_date,
                    "url": _join_url(source_url, link.get("href")),
                    "source": "merolagani_results",
                }
            )

    if not blocks:
        for link in soup.find_all("a"):
            text = link.get_text(" ", strip=True)
            lowered = text.lower()
            if "ipo" in lowered and any(
                word in lowered for word in ["allot", "result", "allotment"]
            ):
                records.append(
                    {
                        "title": text,
                        "details": text,
                        "announcement_date": None,
                        "url": _join_url(source_url, link.get("href")),
                        "source": "merolagani_results",
                    }
                )

    return records


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = value.replace(",", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _normalize_status(value: str) -> str:
    lowered = value.strip().lower()
    if lowered in {"coming soon", "upcoming"}:
        return "upcoming"
    if lowered in {"closed", "close"}:
        return "closed"
    if lowered in {"open", "live"}:
        return "open"
    return "unknown"


def parse_nepselink_ipo_opening_page(html: str, source_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    records: list[dict[str, Any]] = []

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 8:
                continue

            values = [cell.get_text(" ", strip=True) for cell in cells]
            if not values[0].strip().lower().startswith(("ipo", "fpo")):
                continue
            if values[1].strip().lower() in {"company name", "company"}:
                continue

            ipo_type = values[0].strip()
            company_name = values[1].strip()
            units = _to_float(values[2])
            price_per_unit = _to_float(values[3])
            min_apply = _to_float(values[4])
            open_date = values[5].strip() or None
            close_date = values[6].strip() or None
            status_raw = values[7].strip()
            normalized_status = _normalize_status(status_raw)
            announcement_date = open_date
            if normalized_status == "closed":
                announcement_date = close_date or open_date

            if not company_name:
                continue

            details = (
                f"{ipo_type} {company_name} units {values[2]} price per unit {values[3]} "
                f"minimum apply {values[4]} open {values[5]} close {values[6]} status {status_raw}"
            )

            records.append(
                {
                    "title": f"{company_name} {ipo_type}",
                    "details": details,
                    "announcement_date": announcement_date,
                    "url": source_url,
                    "source": "nepselink_ipo_opening",
                    "company": company_name,
                    "issue_type": ipo_type,
                    "issue_open_date": open_date,
                    "issue_close_date": close_date,
                    "min_quantity": min_apply,
                    "total_quantity": units,
                    "price_per_unit": price_per_unit,
                    "issue_status": normalized_status,
                }
            )

    return records


def fetch_upcoming_ipo_records() -> list[dict[str, Any]]:
    html = fetch_html(IPO_UPCOMING_URL)
    return parse_upcoming_ipo_page(html, IPO_UPCOMING_URL)


def fetch_ipo_result_records() -> list[dict[str, Any]]:
    html = fetch_html(IPO_RESULTS_URL)
    return parse_ipo_result_page(html, IPO_RESULTS_URL)


def fetch_nepselink_ipo_opening_records() -> list[dict[str, Any]]:
    html = fetch_html(NEPSELINK_IPO_OPENING_URL)
    return parse_nepselink_ipo_opening_page(html, NEPSELINK_IPO_OPENING_URL)


def _attachment_url(file_path: str | None) -> str | None:
    if not file_path:
        return None
    if file_path.startswith("http://") or file_path.startswith("https://"):
        return file_path
    encoded = quote(file_path, safe="/%")
    return f"https://www.nepalstock.com.np/api/nots/security/fetchFiles?fileLocation={encoded}"


def fetch_nepse_ipo_disclosure_records(
    client: NepseDataClient | None = None,
) -> list[dict[str, Any]]:
    data_client = client or NepseDataClient()
    payload = data_client.fetch_company_disclosures()

    records: list[dict[str, Any]] = []
    company_news = payload.get("companyNews", [])
    exchange_messages = payload.get("exchangeMessages", [])

    for item in company_news:
        if not isinstance(item, dict):
            continue

        title = (item.get("newsHeadline") or "").strip()
        body = (item.get("newsBody") or "").strip()
        text = f"{title} {body}".lower()
        if (
            "ipo" not in text
            and "fpo" not in text
            and "debenture" not in text
            and "right share" not in text
        ):
            continue

        url = None
        documents = item.get("applicationDocumentDetailsList")
        if isinstance(documents, list) and documents:
            first_doc = documents[0] if isinstance(documents[0], dict) else None
            if first_doc:
                url = _attachment_url(first_doc.get("filePath"))

        records.append(
            {
                "title": title,
                "details": body,
                "announcement_date": item.get("addedDate"),
                "url": url,
                "source": "nepse_company_news",
            }
        )

    for item in exchange_messages:
        if not isinstance(item, dict):
            continue

        title = (item.get("messageTitle") or "").strip()
        body = (item.get("messageBody") or "").strip()
        text = f"{title} {body}".lower()
        if (
            "ipo" not in text
            and "fpo" not in text
            and "debenture" not in text
            and "right share" not in text
        ):
            continue

        records.append(
            {
                "title": title,
                "details": body,
                "announcement_date": item.get("addedDate") or item.get("modifiedDate"),
                "url": _attachment_url(item.get("filePath")),
                "source": "nepse_exchange_message",
            }
        )

    return records


def _safe_fetch(
    fetcher: Callable[[], list[dict[str, Any]]], source_name: str
) -> list[dict[str, Any]]:
    try:
        return fetcher()
    except requests.RequestException as exc:
        logger.warning("IPO source fetch failed for %s: %s", source_name, exc)
        return []
    except Exception as exc:
        logger.warning("IPO source processing failed for %s: %s", source_name, exc)
        return []


def fetch_all_ipo_source_records(
    client: NepseDataClient | None = None,
) -> dict[str, list[dict[str, Any]]]:
    with ThreadPoolExecutor(max_workers=4) as executor:
        f_upcoming = executor.submit(_safe_fetch, fetch_upcoming_ipo_records, "merolagani_upcoming")
        f_nepselink = executor.submit(
            _safe_fetch, fetch_nepselink_ipo_opening_records, "nepselink_ipo_opening"
        )
        f_result = executor.submit(_safe_fetch, fetch_ipo_result_records, "merolagani_results")
        f_disclosure = executor.submit(
            _safe_fetch,
            lambda: fetch_nepse_ipo_disclosure_records(client=client),
            "nepse_disclosures",
        )

        upcoming_records = f_upcoming.result()
        nepselink_records = f_nepselink.result()
        result_records = f_result.result()
        disclosure_records = f_disclosure.result()

    return {
        "upcoming_sources": nepselink_records + upcoming_records,
        "result_sources": result_records,
        "nepse_disclosure_sources": disclosure_records,
        "nepselink_sources": nepselink_records,
    }
