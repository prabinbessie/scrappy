from __future__ import annotations

from collections.abc import Iterable
import csv
import json
from pathlib import Path
from typing import Any


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _normalize_cell(value: Any) -> Any:
    if isinstance(value, dict | list):
        return json.dumps(value, ensure_ascii=False)
    return value


def _key_tuple(row: dict[str, Any], key_fields: list[str]) -> tuple[str, ...]:
    return tuple(str(row.get(field, "")) for field in key_fields)


def _read_existing_keys(path: Path, key_fields: list[str]) -> set[tuple[str, ...]]:
    if not path.exists() or path.stat().st_size == 0:
        return set()

    with path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        return {_key_tuple(row, key_fields) for row in reader}


def append_rows_to_csv(
    path: Path,
    rows: Iterable[dict[str, Any]],
    fieldnames: list[str],
    unique_key_fields: list[str] | None = None,
) -> int:
    rows_list = list(rows)
    if not rows_list:
        return 0

    _ensure_parent(path)
    if unique_key_fields:
        existing_keys = _read_existing_keys(path, unique_key_fields)
        deduped_rows: list[dict[str, Any]] = []

        for row in rows_list:
            row_key = _key_tuple(row, unique_key_fields)
            if row_key in existing_keys:
                continue
            existing_keys.add(row_key)
            deduped_rows.append(row)

        rows_list = deduped_rows
        if not rows_list:
            return 0

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
