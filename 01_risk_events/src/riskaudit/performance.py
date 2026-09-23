"""
文件作用：以单调时钟记录批次各阶段耗时，并将多次执行/恢复尝试原子持久化为可审计 JSON。
编辑记录：
【首次生成：2026-08-11，实现阶段上下文、失败留痕、多 attempt 追加和原子性能报告写入。】
【第二次编辑：2026-08-18，增加 Windows 短暂文件锁下的原子替换重试。】
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from time import perf_counter, sleep
from typing import Any, Iterator


class PerformanceRecorder:
    def __init__(
        self,
        path: str | Path,
        *,
        batch_id: str,
        resume: bool,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._document = self._load()
        now = _utc_now()
        self._attempt_started = perf_counter()
        self.attempt: dict[str, Any] = {
            "attempt_id": now.strftime("%Y%m%dT%H%M%S%fZ"),
            "resume": resume,
            "started_at": now.isoformat(),
            "finished_at": None,
            "status": "RUNNING",
            "total_elapsed_seconds": None,
            "stages": [],
        }
        self._document.setdefault("schema_version", 1)
        self._document["batch_id"] = batch_id
        self._document.setdefault("attempts", []).append(self.attempt)
        self._document["latest_attempt_id"] = self.attempt["attempt_id"]
        self._persist()

    @property
    def attempt_id(self) -> str:
        return str(self.attempt["attempt_id"])

    @contextmanager
    def stage(
        self,
        name: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> Iterator[None]:
        started_at = _utc_now()
        started = perf_counter()
        item: dict[str, Any] = {
            "name": name,
            "started_at": started_at.isoformat(),
            "finished_at": None,
            "elapsed_seconds": None,
            "status": "RUNNING",
            "details": details or {},
        }
        self.attempt["stages"].append(item)
        # Persist stage entry immediately so observers can distinguish a long
        # running stage from a stalled process.
        self._persist()
        status = "SUCCEEDED"
        error: dict[str, str] | None = None
        try:
            yield
        except Exception as exc:
            status = "FAILED"
            error = {"type": type(exc).__name__, "message": str(exc)}
            self.attempt["status"] = "FAILED"
            self.attempt["finished_at"] = _utc_now().isoformat()
            self.attempt["total_elapsed_seconds"] = _seconds(
                perf_counter() - self._attempt_started
            )
            raise
        finally:
            finished_at = _utc_now()
            item.update(
                {
                    "finished_at": finished_at.isoformat(),
                    "elapsed_seconds": _seconds(perf_counter() - started),
                    "status": status,
                }
            )
            if error is not None:
                item["error"] = error
            self._persist()

    def finish(
        self,
        status: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.attempt["status"] = status
        self.attempt["finished_at"] = _utc_now().isoformat()
        self.attempt["total_elapsed_seconds"] = _seconds(
            perf_counter() - self._attempt_started
        )
        self.attempt["details"] = details or {}
        self._persist()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("性能报告根节点必须是 JSON 对象")
        return payload

    def _persist(self) -> None:
        temporary = self.path.with_name(f".{self.path.name}.tmp")
        temporary.write_text(
            json.dumps(self._document, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        _replace_with_retry(temporary, self.path)


def _replace_with_retry(source: Path, target: Path) -> None:
    """Replace an audit file despite short-lived Windows reader locks."""
    delays = (0.05, 0.1, 0.2, 0.4, 0.8, 1.6)
    for attempt in range(len(delays) + 1):
        try:
            os.replace(source, target)
            return
        except PermissionError:
            if attempt == len(delays):
                raise
            sleep(delays[attempt])


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _seconds(value: float) -> float:
    return round(value, 6)
