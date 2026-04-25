from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from dateutil import parser as date_parser

from scraper.ipo.common import normalize_issue_status

_BS_YEAR_START: dict[int, date] = {
    2082: date(2025, 4, 14),
    2083: date(2026, 4, 13),
    2084: date(2027, 4, 13),
}

_BS_MONTH_DAYS: dict[int, list[int]] = {
    2082: [31, 31, 32, 32, 31, 30, 30, 30, 29, 30, 30, 30],
    2083: [31, 31, 32, 32, 31, 30, 30, 29, 30, 29, 30, 30],
    2084: [31, 32, 31, 32, 31, 30, 30, 29, 30, 29, 30, 30],
}


def _bs_to_ad(year: int, month: int, day: int) -> date | None:
    year_start = _BS_YEAR_START.get(year)
    if not year_start or not (1 <= month <= 12):
        return None
    month_days = _BS_MONTH_DAYS[year]
    if not (1 <= day <= month_days[month - 1]):
        return None
    offset = day - 1
    for m in range(1, month):
        offset += month_days[m - 1]
    return year_start + timedelta(days=offset)


def _normalize_date_str(value: str) -> str:
    """Normalise separators and zero-pad to YYYY-MM-DD."""
    cleaned = value.strip().replace("/", "-").replace(".", "-")
    parts = cleaned.split("-")
    if len(parts) == 3:
        try:
            y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
            return f"{y:04d}-{m:02d}-{d:02d}"
        except ValueError:
            pass
    return cleaned


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

RESULT_KEYWORDS: tuple[str, ...] = (
    "allotment",
    "allot",
    "result published",
    "allotted",
    "distributed",
    "distribution",
    "ipo result",
)

NEPALI_MONTH_TO_BS_MONTH: dict[str, int] = {
    "baishakh": 1,
    "jestha": 2,
    "ashadh": 3,
    "shrawan": 4,
    "bhadra": 5,
    "ashwin": 6,
    "kartik": 7,
    "mangsir": 8,
    "poush": 9,
    "magh": 10,
    "falgun": 11,
    "chaitra": 12,
}

NEPALI_MONTH_TOKENS: tuple[str, ...] = tuple(NEPALI_MONTH_TO_BS_MONTH)

FUTURE_ISSUE_HINTS: tuple[str, ...] = (
    "going to issue",
    "will issue",
    "starting from",
)

OPEN_FROM_PATTERN = r"open(?:s)?\s+from"


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


def _safe_parse_ad_date(value: str) -> str | None:
    if not value:
        return None
    lowered = value.lower()
    if any(month in lowered for month in NEPALI_MONTH_TOKENS):
        return None
    normalized = _normalize_date_str(value)
    parts = normalized.split("-")
    if len(parts) == 3:
        try:
            if int(parts[0]) >= 2070:
                return None
        except ValueError:
            pass
    try:
        parsed = date_parser.parse(value, fuzzy=True, dayfirst=False)
        if parsed.year >= 2070:
            return None
        return parsed.date().isoformat()
    except (ValueError, TypeError, OverflowError):
        return None


def _parse_bs_day_token(token: str) -> int | None:
    match = re.match(r"(\d{1,2})", token.strip().lower())
    if not match:
        return None
    day = int(match.group(1))
    if 1 <= day <= 32:
        return day
    return None


def _parse_bs_date(day_token: str, month_token: str, year_token: str) -> str | None:
    month = NEPALI_MONTH_TO_BS_MONTH.get(month_token.strip().lower())
    day = _parse_bs_day_token(day_token)

    try:
        year = int(year_token.strip())
    except ValueError:
        return None

    if month is None or day is None or year < 1900:
        return None

    return f"{year:04d}-{month:02d}-{day:02d}"


def _extract_bs_date(value: str) -> str | None:
    match = re.search(
        r"(\d{1,2}(?:st|nd|rd|th)?)\s+([A-Za-z]+),?\s*(\d{4})",
        value,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    return _parse_bs_date(match.group(1), match.group(2), match.group(3))


def _to_date(value: str | None) -> date | None:
    if not value:
        return None
    normalized = _normalize_date_str(value)
    try:
        parsed = datetime.fromisoformat(normalized).date()
    except ValueError:
        return None

    if parsed.year >= 2070:
        return _bs_to_ad(parsed.year, parsed.month, parsed.day)

    return parsed


def extract_issue_dates(text: str) -> tuple[str | None, str | None]:
    cleaned = normalize_text(text)
    open_prefix = rf"(?:from|starting\s+from|{OPEN_FROM_PATTERN})"

    same_month_range = re.search(
        rf"{open_prefix}\s+"
        r"(\d{1,2}(?:st|nd|rd|th)?)\s*[-–]\s*"
        r"(\d{1,2}(?:st|nd|rd|th)?)\s+([A-Za-z]+),?\s*(\d{4})",
        cleaned,
        flags=re.IGNORECASE,
    )
    if same_month_range:
        month_token = same_month_range.group(3)
        year_token = same_month_range.group(4)
        start_expr = f"{same_month_range.group(1)} {month_token} {year_token}"
        end_expr = f"{same_month_range.group(2)} {month_token} {year_token}"

        start_ad = _safe_parse_ad_date(start_expr)
        end_ad = _safe_parse_ad_date(end_expr)
        if start_ad or end_ad:
            return start_ad, end_ad

        start_bs = _parse_bs_date(same_month_range.group(1), month_token, year_token)
        end_bs = _parse_bs_date(same_month_range.group(2), month_token, year_token)
        return start_bs, end_bs

    date_expr = r"(?:\d{4}[-/]\d{1,2}[-/]\d{1,2}" r"|\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+,?\s*\d{4})"

    range_match = re.search(
        rf"{open_prefix}\s+({date_expr})\s+(?:to|till|until)\s+({date_expr})",
        cleaned,
        flags=re.IGNORECASE,
    )
    if range_match:
        start_raw = range_match.group(1)
        end_raw = range_match.group(2)
        start_date = _safe_parse_ad_date(start_raw) or _extract_bs_date(start_raw)
        end_date = _safe_parse_ad_date(end_raw) or _extract_bs_date(end_raw)
        return start_date, end_date

    starts_match = re.search(
        rf"{open_prefix}\s+({date_expr})",
        cleaned,
        flags=re.IGNORECASE,
    )
    if starts_match:
        start_raw = starts_match.group(1)
        return _safe_parse_ad_date(start_raw) or _extract_bs_date(start_raw), None

    return None, None


def derive_issue_status(
    open_date: str | None,
    close_date: str | None,
    nature: str,
    full_text: str = "",
) -> str:
    if nature == "result":
        return "result"

    today = datetime.now(timezone.utc).date()
    start = _to_date(open_date)
    end = _to_date(close_date)
    lowered_text = normalize_text(full_text).lower()

    if start and start > today:
        return "upcoming"
    if start and end and start <= today <= end:
        return "open"
    if start and not end and start <= today:
        return "open"

    if (
        not start
        and not end
        and (
            any(marker in lowered_text for marker in FUTURE_ISSUE_HINTS)
            or re.search(rf"\b{OPEN_FROM_PATTERN}\b", lowered_text)
        )
    ):
        return "upcoming"

    if end and end < today:
        if any(month in lowered_text for month in NEPALI_MONTH_TOKENS):
            return "upcoming"
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


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
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

    published_for = re.search(
        r"(?:published\s+for|result\s+for)\s+(.+?)(?:$|\.|,)",
        cleaned,
        flags=re.IGNORECASE,
    )
    if published_for:
        company = published_for.group(1).strip(" -:,.")
        if len(company) > 2:
            return company

    pattern = re.search(
        r"^(.+?)(?:\s+is\s+issuing|\s+is\s+going\s+to\s+issue|\s+has\s+opened|\s+has\s+distributed|\s+has\s+allotted|\s+publishes|\s+announces|\s+ipo\b)",
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

    issue_type = normalize_text(str(entry.get("issue_type", ""))).lower() or detect_issue_type(
        full_text
    )
    nature = detect_record_nature(full_text)
    reserved_for = detect_reserved_for(full_text)
    open_date, close_date = extract_issue_dates(full_text)
    open_date = str(entry.get("issue_open_date") or open_date or "") or None
    close_date = str(entry.get("issue_close_date") or close_date or "") or None

    explicit_status = normalize_issue_status(entry.get("issue_status"), default="")
    issue_status = explicit_status or derive_issue_status(open_date, close_date, nature, full_text)
    min_quantity, max_quantity, total_quantity, price_per_unit = extract_quantities_and_price(
        full_text
    )
    min_quantity = _coerce_float(entry.get("min_quantity")) or min_quantity
    total_quantity = _coerce_float(entry.get("total_quantity")) or total_quantity
    price_per_unit = _coerce_float(entry.get("price_per_unit")) or price_per_unit
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
