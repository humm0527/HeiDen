"""Shared context and numeric normalization for broker result views."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


LIMIT_DOWN_EVENT_TYPE = "NON_ST_CONTINUOUS_LIMIT_DOWN"
NEW_ST_EVENT_TYPE = "NEW_ST"
SINGLE_CLASSIFICATION_ORDER = (
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "BLANK",
    "UNKNOWN",
)

@dataclass(frozen=True)
class BrokerResultsContext:
    output_root: Path
    final_broker_metric_batch_id: str
    default_analysis_mode: str
    assert_readable: Callable[[str], None]
    get_job: Callable[[str], dict[str, Any]]
    list_jobs: Callable[[], list[dict[str, Any]]]

def nullable_number(value: Any) -> int | float | None:
    text = str(value or "").strip()
    if not text:
        return None
    is_percent = text.endswith("%")
    number = float(text[:-1] if is_percent else text)
    if is_percent:
        return number / 100
    return int(number) if number.is_integer() else number

def nullable_difference(left: Any, right: Any) -> float | None:
    if left is None or right is None:
        return None
    return round(float(left) - float(right), 6)
