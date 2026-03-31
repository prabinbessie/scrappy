from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from scraper.config import IPO_FEED_JSON
from scraper.io import write_json
from scraper.ipo.classifier import classify_ipo_entry
from scraper.ipo.sources import fetch_all_ipo_source_records


def _normalize_quantity_token(value: Any) -> str:
    text = str(value or "").replace(",", "").strip()
    if not text:
        return ""

    try:
        numeric = float(text)
    except ValueError:
        return text

    if numeric.is_integer():
        return str(int(numeric))

    normalized = f"{numeric:.8f}".rstrip("0").rstrip(".")
    return normalized


def _normalize_issue_status(value: Any) -> str:
    lowered = str(value or "").strip().lower()
    mapping = {
        "coming soon": "upcoming",
        "upcoming": "upcoming",
        "open": "open",
        "live": "open",
        "closed": "closed",
        "close": "closed",
        "result": "result",
    }
    return mapping.get(lowered, "unknown")


def _record_key(item: dict[str, Any]) -> str:
    company = str(item.get("company_name") or "").strip().lower()
    open_date = str(item.get("issue_open_date") or "").strip()
    close_date = str(item.get("issue_close_date") or "").strip()
    quantity = _normalize_quantity_token(item.get("total_quantity"))

    if company and quantity:
        return f"biz-qty::{company}::{quantity}"

    if company and (open_date or close_date):
        return f"biz-date::{company}::{open_date}::{close_date}"

    return f"raw::{item.get('source','')}::{item.get('url','')}::{item.get('title','')}"


def _deduplicate(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []

    for item in items:
        key = _record_key(item)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)

    return result


def _group_by_status(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {
        "upcoming": [],
        "open": [],
        "closed": [],
        "result": [],
        "unknown": [],
    }

    for record in records:
        status = _normalize_issue_status(record.get("issue_status", "unknown"))
        grouped.setdefault(status, []).append(record)

    return grouped


def _sort_latest(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        records,
        key=lambda item: str(item.get("announcement_date") or item.get("scraped_at_utc") or ""),
        reverse=True,
    )


def _count_dict_items(records: list[dict[str, Any]] | list[Any]) -> int:
    return sum(1 for item in records if isinstance(item, dict))


def scrape_ipo_to_json() -> dict[str, Any]:
    source_bundle = fetch_all_ipo_source_records()

    upcoming_raw = source_bundle.get("upcoming_sources", [])
    results_raw = source_bundle.get("result_sources", [])
    disclosures_raw = source_bundle.get("nepse_disclosure_sources", [])
    nepselink_raw = source_bundle.get("nepselink_sources", [])

    combined_issues_raw = upcoming_raw + disclosures_raw

    classified_issues = [classify_ipo_entry(item, "issue") for item in combined_issues_raw]
    classified_results = [classify_ipo_entry(item, "result") for item in results_raw]

    deduped_issues = _deduplicate(classified_issues)
    deduped_results = _deduplicate(classified_results)

    grouped_issues = _group_by_status(deduped_issues)

    upcoming = _sort_latest(grouped_issues.get("upcoming", []))
    open_items = _sort_latest(grouped_issues.get("open", []))
    closed = _sort_latest(grouped_issues.get("closed", []))
    unknown = _sort_latest(grouped_issues.get("unknown", []))
    results = _sort_latest(_deduplicate(deduped_results + grouped_issues.get("result", [])))

    source_counts = {
        "merolagani_upcoming": _count_dict_items(upcoming_raw),
        "nepselink_ipo_opening": _count_dict_items(nepselink_raw),
        "merolagani_results": _count_dict_items(results_raw),
        "nepse_disclosures": _count_dict_items(disclosures_raw),
    }

    payload = {
        "meta": {
            "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "sources": source_counts,
            "upcoming_count": len(upcoming),
            "open_count": len(open_items),
            "closed_count": len(closed),
            "result_count": len(results),
            "unknown_count": len(unknown),
        },
        "upcoming": upcoming,
        "open": open_items,
        "closed": closed,
        "results": results,
        "unknown": unknown,
    }

    write_json(IPO_FEED_JSON, payload)
    return payload
