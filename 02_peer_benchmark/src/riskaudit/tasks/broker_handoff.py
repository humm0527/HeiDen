"""Create and resolve the portable handoff from task 01 to task 02."""

from __future__ import annotations

import json
import os
from pathlib import Path, PureWindowsPath
from typing import Any, Mapping


BROKER_INPUT_SCHEMA = "riskaudit-02-input-v1"
BROKER_INPUT_FIELDS = (
    "event_csv",
    "event_manifest",
    "calendar_csv",
    "st_csv",
    "lifecycle_csv",
    "universe_csv",
)


def write_broker_input_manifest(
    path: str | Path,
    *,
    batch_id: str,
    observation_start: str,
    observation_end: str,
    files: Mapping[str, str | Path],
) -> Path:
    """Write one relocatable manifest that points task 02 at task 01 outputs."""
    target = Path(path).resolve()
    missing = [name for name in BROKER_INPUT_FIELDS if name not in files]
    if missing:
        raise ValueError("02 输入清单缺少文件: " + ", ".join(missing))
    relative_files: dict[str, str] = {}
    for name in BROKER_INPUT_FIELDS:
        source = Path(files[name]).resolve()
        if not source.is_file():
            raise FileNotFoundError(f"02 输入清单源文件不存在: {source}")
        relative_files[name] = Path(os.path.relpath(source, target.parent)).as_posix()
    payload = {
        "schema": BROKER_INPUT_SCHEMA,
        "status": "SUCCEEDED",
        "source_task": "risk-events",
        "batch_id": str(batch_id),
        "observation_start": str(observation_start),
        "observation_end": str(observation_end),
        "files": relative_files,
    }
    target.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return target


def load_broker_input_manifest(path: str | Path) -> dict[str, Any]:
    """Resolve a task-01 handoff without machine-specific absolute paths."""
    manifest_path = Path(path).resolve()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("schema") != BROKER_INPUT_SCHEMA:
        raise ValueError("02 输入清单格式不受支持")
    if payload.get("status") != "SUCCEEDED":
        raise ValueError("02 输入清单对应的 01 批次未成功")
    raw_files = payload.get("files")
    if not isinstance(raw_files, dict):
        raise ValueError("02 输入清单缺少 files")
    resolved: dict[str, Path] = {}
    for name in BROKER_INPUT_FIELDS:
        raw = str(raw_files.get(name) or "").strip()
        if not raw:
            raise ValueError(f"02 输入清单缺少文件: {name}")
        if Path(raw).is_absolute() or PureWindowsPath(raw).is_absolute():
            raise ValueError(f"02 输入清单禁止机器绝对路径: {name}")
        source = (manifest_path.parent / raw).resolve()
        if not source.is_file():
            raise FileNotFoundError(f"02 输入清单文件不存在: {source}")
        resolved[name] = source
    return {
        "batch_id": str(payload.get("batch_id") or "").strip(),
        "observation_start": str(payload.get("observation_start") or "").strip(),
        "observation_end": str(payload.get("observation_end") or "").strip(),
        "files": resolved,
    }


def apply_broker_input_manifest(parameters: Mapping[str, Any]) -> dict[str, Any]:
    """Fill task-02 technical inputs from its single task-01 handoff manifest."""
    merged = dict(parameters)
    manifest_path = merged.get("risk_input_manifest")
    if manifest_path in (None, ""):
        return merged
    handoff = load_broker_input_manifest(manifest_path)
    for name, source in handoff["files"].items():
        existing = merged.get(name)
        if existing not in (None, "") and Path(existing).resolve() != source:
            raise ValueError(f"02 输入清单与显式参数冲突: {name}")
        merged[name] = source
    merged.setdefault("event_batch_id", handoff["batch_id"])
    return merged
