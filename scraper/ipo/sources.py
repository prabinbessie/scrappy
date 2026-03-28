from __future__ import annotations

from typing import Any
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

from scraper.config import IPO_RESULTS_URL, IPO_UPCOMING_URL, SCRAPPY_TIMEOUT_SECONDS
from scraper.nepse.client import NepseDataClient

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=SCRAPPY_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


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


def fetch_upcoming_ipo_records() -> list[dict[str, Any]]:
    html = fetch_html(IPO_UPCOMING_URL)
    return parse_upcoming_ipo_page(html, IPO_UPCOMING_URL)


def fetch_ipo_result_records() -> list[dict[str, Any]]:
    html = fetch_html(IPO_RESULTS_URL)
    return parse_ipo_result_page(html, IPO_RESULTS_URL)


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


def fetch_all_ipo_source_records(
    client: NepseDataClient | None = None,
) -> dict[str, list[dict[str, Any]]]:
    upcoming_records = fetch_upcoming_ipo_records()
    result_records = fetch_ipo_result_records()
    disclosure_records = fetch_nepse_ipo_disclosure_records(client=client)

    return {
        "upcoming_sources": upcoming_records,
        "result_sources": result_records,
        "nepse_disclosure_sources": disclosure_records,
    }
