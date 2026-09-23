"""Shared records and deterministic helpers for market reconciliation."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import date, datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from .facts import canonical_json
from .service import MarketFoundationService


MAX_EXAMPLE_KEYS = 20


@dataclass(frozen=True)
class ReconciliationCheck:
    """One deterministic full-range assertion, including explicit PASS evidence."""

    rule_id: str
    name: str
    outcome: str
    severity: str
    dataset_id: str | None
    market_code: str | None
    affected_rows: int
    message: str
    suggested_action: str
    example_keys: tuple[str, ...] = ()

def _expected_security_days_sql() -> str:
    return """
        SELECT u.market_code, u.security_code, c.business_date,
               u.market_code || '|' || u.security_code || '|' ||
               CAST(c.business_date AS VARCHAR) AS issue_key
        FROM market_universe_member u
        JOIN current_market_calendar c
          ON c.market_code=u.market_code AND c.is_trading_day
        WHERE u.universe_id=? AND c.business_date BETWEEN ? AND ?
          AND c.business_date>=u.listing_date
          AND (u.termination_date IS NULL OR c.business_date<u.termination_date)
    """

def _issues(
    service: MarketFoundationService, sql: str, parameters: list[Any]
) -> tuple[int, list[str]]:
    rows = service.catalog.rows(
        f"""
        SELECT issue_key, count(*) OVER () AS affected_rows
        FROM ({sql}) AS issues
        LIMIT {MAX_EXAMPLE_KEYS}
        """,
        parameters,
    )
    if not rows:
        return 0, []
    return int(rows[0]["affected_rows"]), [str(item["issue_key"]) for item in rows]

def _check(
    rule_id: str,
    name: str,
    severity: str,
    affected_rows: int,
    failure_message: str,
    pass_message: str,
    suggested_action: str,
    example_keys: Iterable[str],
    *,
    dataset_id: str | None = None,
    market_code: str | None = None,
) -> ReconciliationCheck:
    affected = int(affected_rows)
    return ReconciliationCheck(
        rule_id=rule_id,
        name=name,
        outcome="FAIL" if affected else "PASS",
        severity=severity,
        dataset_id=dataset_id,
        market_code=market_code,
        affected_rows=affected,
        message=failure_message if affected else pass_message,
        suggested_action=suggested_action if affected else "无需处理",
        example_keys=tuple(str(item) for item in list(example_keys)[:MAX_EXAMPLE_KEYS]),
    )

def _member_tuple(item: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        str(item.get("instrument_key") or ""),
        str(item.get("source_order_book_id") or ""),
        str(item.get("source_exchange") or ""),
        str(item.get("market_code") or ""),
        str(item.get("security_code") or ""),
        str(item.get("symbol") or ""),
        str(item.get("security_type") or ""),
        str(item.get("board_code") or ""),
        _date_text(item.get("listing_date")),
        _date_text(item.get("termination_date")),
        str(item.get("source_status") or ""),
    )

def _date_text(value: Any) -> str:
    if value in {None, "", "None"}:
        return ""
    return _as_date(value).isoformat()

def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])

def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()

def _write_json_new(path: Path, payload: Mapping[str, Any]) -> None:
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")

def _write_checks_csv_new(path: Path, checks: Iterable[ReconciliationCheck]) -> None:
    fieldnames = (
        "rule_id",
        "name",
        "outcome",
        "severity",
        "dataset_id",
        "market_code",
        "affected_rows",
        "message",
        "suggested_action",
        "example_keys",
    )
    with path.open("x", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for check in checks:
            row = asdict(check)
            row["example_keys"] = canonical_json(list(check.example_keys))
            writer.writerow(row)

