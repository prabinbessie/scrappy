from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from scraper.config import IPO_FEED_JSON
from scraper.io import write_json
from scraper.ipo.classifier import classify_ipo_entry, derive_issue_status
from scraper.ipo.common import normalize_issue_status
from scraper.ipo.sources import fetch_all_ipo_source_records

logger = logging.getLogger(__name__)

STATUS_GROUP_KEYS: tuple[str, ...] = ("upcoming", "open", "closed", "result", "unknown")

_CARRY_FORWARD_SOURCES: frozenset[str] = frozenset(
    {
        "merolagani_upcoming",
        "nepselink_ipo_opening",
        "merolagani_results",
        "merolagani_announcements",
        "nepse_company_news",
        "nepse_exchange_message",
    }
)


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
    grouped: dict[str, list[dict[str, Any]]] = {status: [] for status in STATUS_GROUP_KEYS}

    for record in records:
        status = normalize_issue_status(record.get("issue_status", "unknown"))
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


def _classify_entries(records: list[dict[str, Any]], record_type: str) -> list[dict[str, Any]]:
    return [classify_ipo_entry(item, record_type) for item in records]


def _load_previous_feed() -> dict[str, Any]:
    try:
        with open(IPO_FEED_JSON, encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _prev_records_by_source(feed: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    by_source: dict[str, list[dict[str, Any]]] = {}
    for group_key in ("upcoming", "open", "closed", "results", "unknown"):
        for record in feed.get(group_key, []):
            if not isinstance(record, dict):
                continue
            src = record.get("source", "")
            if src:
                by_source.setdefault(src, []).append(record)
    return by_source


def _carry_forward(
    fresh_issues: list[dict[str, Any]],
    fresh_results: list[dict[str, Any]],
    prev_feed: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """
    For each source that returned 0 records this run but had records in the
    previous feed, carry those records forward marked stale=True.
    Fresh records go first so dedup always prefers the live version.
    """
    if not prev_feed:
        return [], [], []

    prev_by_source = _prev_records_by_source(prev_feed)

    fresh_sources: set[str] = set()
    for r in fresh_issues + fresh_results:
        src = r.get("source", "")
        if src:
            fresh_sources.add(src)

    extra_issues: list[dict[str, Any]] = []
    extra_results: list[dict[str, Any]] = []
    stale_names: list[str] = []

    for source in _CARRY_FORWARD_SOURCES:
        if source not in prev_by_source or source in fresh_sources:
            continue

        prev_records = prev_by_source[source]
        logger.warning(
            "Source %s: 0 fresh records; carrying forward %d stale record(s)",
            source,
            len(prev_records),
        )
        stale_names.append(source)

        for record in prev_records:
            stale = dict(record)
            stale["stale"] = True
            # Refresh status so an IPO that opened/closed today is correct
            fresh_status = derive_issue_status(
                stale.get("issue_open_date"),
                stale.get("issue_close_date"),
                stale.get("nature", "issue"),
                stale.get("raw_text", ""),
            )
            stale["issue_status"] = fresh_status
            if stale.get("record_type") == "result" or stale.get("nature") == "result":
                extra_results.append(stale)
            else:
                extra_issues.append(stale)

    return extra_issues, extra_results, stale_names


def scrape_ipo_to_json() -> dict[str, Any]:
    prev_feed = _load_previous_feed()

    source_bundle = fetch_all_ipo_source_records()

    upcoming_raw = source_bundle.get("upcoming_sources", [])
    merolagani_upcoming_raw = source_bundle.get("merolagani_upcoming_sources", upcoming_raw)
    results_raw = source_bundle.get("result_sources", [])
    disclosures_raw = source_bundle.get("nepse_disclosure_sources", [])
    nepselink_raw = source_bundle.get("nepselink_sources", [])

    combined_issues_raw = upcoming_raw + disclosures_raw

    classified_issues = _classify_entries(combined_issues_raw, "issue")
    classified_results = _classify_entries(results_raw, "result")

    extra_issues, extra_results, stale_sources = _carry_forward(
        classified_issues, classified_results, prev_feed
    )
    all_issues = classified_issues + extra_issues
    all_results = classified_results + extra_results

    deduped_issues = _deduplicate(all_issues)
    deduped_results = _deduplicate(all_results)

    grouped_issues = _group_by_status(deduped_issues)

    upcoming = _sort_latest(grouped_issues.get("upcoming", []))
    open_items = _sort_latest(grouped_issues.get("open", []))
    closed = _sort_latest(grouped_issues.get("closed", []))
    unknown = _sort_latest(grouped_issues.get("unknown", []))
    results = _sort_latest(_deduplicate(deduped_results + grouped_issues.get("result", [])))

    source_counts = {
        "merolagani_upcoming": _count_dict_items(merolagani_upcoming_raw),
        "nepselink_ipo_opening": _count_dict_items(nepselink_raw),
        "merolagani_results": _count_dict_items(results_raw),
        "nepse_disclosures": _count_dict_items(disclosures_raw),
    }

    payload = {
        "meta": {
            "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "sources": source_counts,
            "stale_sources": stale_sources,
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
