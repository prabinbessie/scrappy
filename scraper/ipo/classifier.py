from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any

from dateutil import parser as date_parser

ISSUE_TYPES: list[tuple[str, str]] = [
    ("debenture", "debenture"),
    ("mutual fund", "mutual_fund"),
    ("right share", "right_share"),
    ("fpo", "fpo"),
    ("further public offering", "fpo"),
    ("ipo", "ipo"),
    ("ordinary share", "ipo"),
]

RESERVED_KEYWORDS: dict[str, tuple[str, ...]] = {
    "foreign_employment": ("foreign employment", "working abroad", "nrn"),
    "mutual_fund": ("mutual fund", "unit"),
    "local_residents": ("local residents", "project affected"),
    "employees": ("employee",),
    "general_public": ("general public", "public"),
}

RESULT_KEYWORDS: tuple[str, ...] = ("allotment", "allot", "result published", "allotted")


def normalize_text(value: str) -> str:
    return " ".join((value or "").split()).strip()


def detect_issue_type(text: str) -> str:
    lowered = normalize_text(text).lower()
    for keyword, mapped_type in ISSUE_TYPES:
        if keyword in lowered:
            return mapped_type
    return "unknown"


def detect_record_nature(text: str) -> str:
    lowered = normalize_text(text).lower()
    if any(token in lowered for token in RESULT_KEYWORDS):
        return "result"
    return "issue"


def detect_reserved_for(text: str) -> list[str]:
    lowered = normalize_text(text).lower()
    reserved: list[str] = []

    for category, required_tokens in RESERVED_KEYWORDS.items():
        if all(token in lowered for token in required_tokens):
            reserved.append(category)
            continue

        if any(token in lowered for token in required_tokens) and category != "mutual_fund":
            reserved.append(category)

    return sorted(set(reserved))


def _safe_parse_date(value: str) -> str | None:
    if not value:
        return None
    try:
        parsed = date_parser.parse(value, fuzzy=True, dayfirst=False)
        return parsed.date().isoformat()
    except (ValueError, TypeError, OverflowError):
        return None


def _to_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


def extract_issue_dates(text: str) -> tuple[str | None, str | None]:
    cleaned = normalize_text(text)

    range_match = re.search(
        r"(?:from|starting\s+from|open\s+from)\s+(.+?)\s+(?:to|till|until)\s+(.+?)(?:$|,|\.)",
        cleaned,
        flags=re.IGNORECASE,
    )
    if range_match:
        start_date = _safe_parse_date(range_match.group(1))
        end_date = _safe_parse_date(range_match.group(2))
        return start_date, end_date

    starts_match = re.search(
        r"(?:from|starting\s+from|open\s+from)\s+(.+?)(?:$|,|\.)",
        cleaned,
        flags=re.IGNORECASE,
    )
    if starts_match:
        return _safe_parse_date(starts_match.group(1)), None

    return None, None


def derive_issue_status(open_date: str | None, close_date: str | None, nature: str) -> str:
    if nature == "result":
        return "result"

    today = datetime.now(timezone.utc).date()
    start = _to_date(open_date)
    end = _to_date(close_date)

    if start and start > today:
        return "upcoming"
    if start and end and start <= today <= end:
        return "open"
    if start and not end and start <= today:
        return "open"
    if end and end < today:
        return "closed"
    return "unknown"


def _extract_number(pattern: str, text: str) -> float | None:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None

    raw = match.group(1).replace(",", "").strip()
    try:
        return float(raw)
    except ValueError:
        return None


def extract_quantities_and_price(
    text: str,
) -> tuple[float | None, float | None, float | None, float | None]:
    cleaned = normalize_text(text)

    min_quantity = _extract_number(
        r"(?:min(?:imum)?\s*(?:apply|application)?\s*(?:for)?\s*)([\d,]+(?:\.\d+)?)\s*(?:kitta|units|shares)",
        cleaned,
    )
    max_quantity = _extract_number(
        r"(?:max(?:imum)?\s*(?:apply|application)?\s*(?:for)?\s*)([\d,]+(?:\.\d+)?)\s*(?:kitta|units|shares)",
        cleaned,
    )
    total_quantity = _extract_number(r"([\d,]+(?:\.\d+)?)\s*(?:kitta|units|shares)", cleaned)

    price_per_unit = _extract_number(
        r"price\s*(?:per\s*unit)?\s*(?:rs\.?|npr\.?)?\s*([\d,]+(?:\.\d+)?)",
        cleaned,
    )
    if price_per_unit is None:
        price_per_unit = _extract_number(r"(?:rs\.?|npr\.?|rupees)\s*([\d,]+(?:\.\d+)?)", cleaned)

    return min_quantity, max_quantity, total_quantity, price_per_unit


def extract_company_name(text: str) -> str | None:
    cleaned = normalize_text(text)

    pattern = re.search(
        r"^(.+?)(?:\s+is\s+issuing|\s+is\s+going\s+to\s+issue|\s+has\s+opened|\s+publishes|\s+announces|\s+ipo)",
        cleaned,
        flags=re.IGNORECASE,
    )
    if pattern:
        company = pattern.group(1).strip(" -:,")
        if len(company) > 2:
            return company
    return None


def classify_ipo_entry(entry: dict[str, Any], record_type: str) -> dict[str, Any]:
    title = normalize_text(str(entry.get("title", "")))
    details = normalize_text(str(entry.get("details", "")))
    full_text = normalize_text(f"{title} {details}")

    issue_type = detect_issue_type(full_text)
    nature = detect_record_nature(full_text)
    reserved_for = detect_reserved_for(full_text)
    open_date, close_date = extract_issue_dates(full_text)
    issue_status = derive_issue_status(open_date, close_date, nature)
    min_quantity, max_quantity, total_quantity, price_per_unit = extract_quantities_and_price(
        full_text
    )
    company_name = extract_company_name(full_text) or entry.get("company") or ""

    return {
        "record_type": record_type,
        "nature": nature,
        "issue_status": issue_status,
        "issue_type": issue_type,
        "company_name": company_name,
        "title": title,
        "details": details,
        "reserved_for": reserved_for,
        "issue_open_date": open_date,
        "issue_close_date": close_date,
        "min_quantity": min_quantity,
        "max_quantity": max_quantity,
        "total_quantity": total_quantity,
        "price_per_unit": price_per_unit,
        "announcement_date": entry.get("announcement_date"),
        "source": entry.get("source"),
        "url": entry.get("url"),
        "scraped_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "raw_text": full_text,
    }
