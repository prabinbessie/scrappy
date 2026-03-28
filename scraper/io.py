from __future__ import annotations

from collections.abc import Iterable
import csv
import json
from pathlib import Path
from typing import Any


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _normalize_cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def append_rows_to_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> int:
    rows_list = list(rows)
    if not rows_list:
        return 0

    _ensure_parent(path)
    write_header = not path.exists() or path.stat().st_size == 0

    with path.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()

        for row in rows_list:
            normalized = {name: _normalize_cell(row.get(name)) for name in fieldnames}
            writer.writerow(normalized)

    return len(rows_list)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    _ensure_parent(path)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
