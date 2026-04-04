from __future__ import annotations

from typing import Any

ISSUE_STATUS_ALIASES: dict[str, str] = {
    "coming soon": "upcoming",
    "upcoming": "upcoming",
    "open": "open",
    "live": "open",
    "closed": "closed",
    "close": "closed",
    "result": "result",
}


def normalize_issue_status(value: Any, default: str = "unknown") -> str:
    lowered = str(value or "").strip().lower()
    return ISSUE_STATUS_ALIASES.get(lowered, default)
