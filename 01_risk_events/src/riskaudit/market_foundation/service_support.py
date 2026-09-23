"""Shared value conversion and file helpers for market-foundation services."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from .facts import canonical_json


class MarketFoundationError(ValueError):
    """Stable local API error with a documented MDF code."""

    def __init__(
        self, code: str, message: str, details: Mapping[str, Any] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


def as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def record_business_date(dataset_id: str, record: Mapping[str, Any]) -> date:
    field = (
        "effective_start"
        if dataset_id == "instrument_master_history"
        else "business_date"
    )
    return as_date(record[field])


def record_matches_chunk(
    dataset_id: str,
    record: Mapping[str, Any],
    start_date: date,
    end_date: date,
) -> bool:
    if dataset_id != "instrument_master_history":
        return start_date <= record_business_date(dataset_id, record) <= end_date
    effective_start = as_date(record["effective_start"])
    raw_end = record.get("effective_end")
    effective_end = as_date(raw_end) if raw_end not in {None, ""} else None
    return effective_start <= end_date and (
        effective_end is None or effective_end >= start_date
    )


def date_range(start_date: date, end_date: date) -> Iterable[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def continuous_ranges(values: list[date]) -> list[tuple[date, date]]:
    if not values:
        return []
    ranges: list[tuple[date, date]] = []
    start = values[0]
    previous = values[0]
    for current in values[1:]:
        if current != previous + timedelta(days=1):
            ranges.append((start, previous))
            start = current
        previous = current
    ranges.append((start, previous))
    return ranges


def stream_member_hash(
    partitions: Iterable[Mapping[str, Any]],
    records: Iterable[Mapping[str, Any]],
) -> tuple[str, int]:
    digest = sha256()
    digest.update(b"[")
    item_count = 0
    for collection in (partitions, records):
        for item in collection:
            if item_count:
                digest.update(b",")
            digest.update(canonical_json(dict(item)).encode("utf-8"))
            item_count += 1
    digest.update(b"]")
    return digest.hexdigest(), item_count


def as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=value.tzinfo or timezone.utc)
    return datetime.fromisoformat(str(value))


def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    return iso(value)


def json_row(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    return None if row is None else json_value(dict(row))


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    atomic_text(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
    )


def atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(content.encode("utf-8"))
    temporary.replace(path)


def csv_escape(value: Any) -> str:
    text = "" if value is None else str(value)
    return '"' + text.replace('"', '""') + '"'
