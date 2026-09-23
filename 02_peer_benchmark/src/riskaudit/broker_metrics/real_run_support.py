"""Progress, hashing, and scalar helpers for real broker-metric runs."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Mapping

def _report_progress(
    callback: Callable[[str, Mapping[str, object]], None] | None,
    stage: str,
    **details: object,
) -> None:
    if callback is not None:
        callback(stage, details)

def _file_summary(path: str | Path, **extra):
    resolved = Path(path).resolve()
    return {
        "path": str(resolved),
        "size_bytes": resolved.stat().st_size,
        "sha256": _sha256(resolved),
        **extra,
    }

def _sha256(path: str | Path):
    digest = sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def _parse_date(value: str, optional: bool = False):
    text = str(value).strip()
    if text in {"", "0000-00-00", "nan", "NaT"}:
        return None if optional else None
    parts = text[:10].replace("/", "-").split("-")
    return date(int(parts[0]), int(parts[1]), int(parts[2]))

