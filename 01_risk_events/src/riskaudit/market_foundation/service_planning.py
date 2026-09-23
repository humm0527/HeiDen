"""Refresh request validation, planning, and task creation."""

from __future__ import annotations

from datetime import date, timedelta
from hashlib import sha256
import json
from typing import Any, Mapping
from uuid import uuid4

from .constants import (
    CUSTOM_PRESET,
    DATASET_PRESET,
    DATASETS,
    MARKETS,
    PLAN_TTL_MINUTES,
    REFRESH_MODES,
)
from .facts import canonical_json
from .service_support import (
    MarketFoundationError,
    as_date as _as_date,
    as_datetime as _as_datetime,
    continuous_ranges as _continuous_ranges,
    date_range as _date_range,
    json_value as _json_value,
    now as _now,
)


class PlanningServiceMixin:
    """Plan and create immutable refresh tasks."""

    def create_plan(self, request: Mapping[str, Any]) -> dict[str, Any]:
        normalized = self._validate_refresh_request(request)
        conflict = self._find_conflicting_task(normalized)
        calendar_days = (normalized["end_date"] - normalized["start_date"]).days + 1
        estimated_weekdays = sum(
            1
            for offset in range(calendar_days)
            if (normalized["start_date"] + timedelta(days=offset)).weekday() < 5
        )
        chunks = self._plan_chunks(normalized)
        request_json = canonical_json(_json_value(normalized))
        estimate = {
            "confidence": "ESTIMATED",
            "calendar_days": calendar_days,
            "trading_days": estimated_weekdays,
            "security_count": None,
            "security_count_confidence": "UNKNOWN",
            "chunk_count": len(chunks),
            "chunks": chunks,
            "existing_coverage": self._coverage_for_request(normalized),
            "conflicting_task": conflict,
            "eligible_for_candidate": (
                self.allow_snapshot_candidates
                and normalized["dataset_preset"] == DATASET_PRESET
            ),
            "blocking_errors": (
                [
                    {
                        "error_code": "MDF006",
                        "message": "存在范围重叠的写入任务",
                        "task_id": conflict["task_id"],
                    }
                ]
                if conflict
                else []
            ),
        }
        created_at = _now()
        expires_at = created_at + timedelta(minutes=PLAN_TTL_MINUTES)
        plan_id = f"mdplan_{uuid4().hex}"
        plan_hash = sha256(
            (request_json + canonical_json(estimate)).encode("utf-8")
        ).hexdigest()
        self.catalog.execute(
            "INSERT INTO refresh_plan VALUES (?, ?, ?, ?, ?, ?)",
            [
                plan_id,
                plan_hash,
                request_json,
                canonical_json(estimate),
                created_at,
                expires_at,
            ],
        )
        return {
            "plan_id": plan_id,
            "plan_hash": plan_hash,
            "created_at": created_at.isoformat(),
            "expires_at": expires_at.isoformat(),
            "request": _json_value(normalized),
            "estimate": estimate,
            "execution_mode": self.execution_mode,
        }

    def create_task(
        self,
        *,
        plan_id: str,
        plan_hash: str,
        request_id: str,
    ) -> dict[str, Any]:
        if not request_id.strip():
            raise MarketFoundationError("MDF001", "request_id 不能为空")
        plan = self.catalog.row("SELECT * FROM refresh_plan WHERE plan_id = ?", [plan_id])
        if plan is None or plan["plan_hash"] != plan_hash:
            raise MarketFoundationError("MDF005", "刷新计划不存在或哈希已失效")
        existing = self.catalog.row(
            "SELECT * FROM refresh_task WHERE request_id = ?", [request_id]
        )
        if existing:
            if existing["plan_id"] != plan_id or existing["plan_hash"] != plan_hash:
                raise MarketFoundationError(
                    "MDF012", "相同 request_id 已用于不同刷新请求"
                )
            return self.get_task(existing["task_id"])
        if _as_datetime(plan["expires_at"]) <= _now():
            raise MarketFoundationError("MDF005", "刷新计划已过期，请重新检查范围")
        request = json.loads(plan["request_json"])
        estimate = json.loads(plan["estimate_json"])
        normalized = self._validate_refresh_request(request)
        conflict = self._find_conflicting_task(normalized)
        if conflict:
            raise MarketFoundationError(
                "MDF006",
                "存在范围重叠的写入任务",
                {"task_id": conflict["task_id"]},
            )
        task_id = f"mdtask_{uuid4().hex}"
        now = _now()
        with self.catalog.transaction() as transaction:
            transaction.execute(
                """
                INSERT INTO refresh_task VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    task_id,
                    request_id,
                    plan_id,
                    plan_hash,
                    plan["request_json"],
                    normalized["mode"],
                    normalized["start_date"],
                    normalized["end_date"],
                    normalized["dataset_preset"],
                    "QUEUED",
                    1,
                    self.execution_mode,
                    normalized["note"],
                    now,
                    now,
                    None,
                    None,
                    None,
                ],
            )
            for chunk in estimate["chunks"]:
                transaction.execute(
                    """
                    INSERT INTO refresh_chunk VALUES
                    (?, ?, ?, ?, ?, ?, 'PENDING', 0, 0, 0, 0, NULL, ?)
                    """,
                    [
                        task_id,
                        chunk["chunk_id"],
                        chunk["market_code"],
                        chunk["dataset_id"],
                        _as_date(chunk["start_date"]),
                        _as_date(chunk["end_date"]),
                        now,
                    ],
                )
        self._write_ingestion_manifest(task_id)
        return self.get_task(task_id)

    def _validate_refresh_request(self, request: Mapping[str, Any]) -> dict[str, Any]:
        foundation_start, _ = self._foundation_bounds()
        try:
            start = _as_date(request.get("start_date"))
            end = _as_date(request.get("end_date"))
        except (TypeError, ValueError) as exc:
            raise MarketFoundationError("MDF001", "日期必须是 YYYY-MM-DD") from exc
        if start > end:
            raise MarketFoundationError("MDF001", "开始日期不得晚于结束日期")
        if start < foundation_start or end > self.refresh_range_end:
            raise MarketFoundationError(
                "MDF002",
                "日期超出冻结范围",
                {
                    "minimum_date": foundation_start.isoformat(),
                    "maximum_date": self.refresh_range_end.isoformat(),
                },
            )
        mode = str(request.get("mode", "FILL_GAPS"))
        if mode not in REFRESH_MODES:
            raise MarketFoundationError("MDF001", "刷新模式无效")
        markets = tuple(dict.fromkeys(request.get("markets") or ()))
        datasets = tuple(dict.fromkeys(request.get("datasets") or ()))
        if not markets or any(item not in MARKETS for item in markets):
            raise MarketFoundationError("MDF003", "至少选择一个有效市场")
        if not datasets or any(item not in DATASETS for item in datasets):
            raise MarketFoundationError("MDF003", "至少选择一个有效数据集")
        preset = str(request.get("dataset_preset", DATASET_PRESET))
        if preset not in {DATASET_PRESET, CUSTOM_PRESET}:
            raise MarketFoundationError("MDF003", "数据范围预设无效")
        if preset == DATASET_PRESET and set(datasets) != set(DATASETS):
            raise MarketFoundationError("MDF004", "风险计算完整包必须包含六类数据")
        note = str(request.get("note", "")).strip()
        if len(note) > 500:
            raise MarketFoundationError("MDF001", "任务备注最多 500 字")
        return {
            "mode": mode,
            "start_date": start,
            "end_date": end,
            "markets": list(markets),
            "dataset_preset": preset,
            "datasets": list(datasets),
            "note": note,
        }

    def _find_conflicting_task(
        self, request: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        candidates = self.catalog.rows(
            """
            SELECT task_id, request_json, status FROM refresh_task
            WHERE status IN ('QUEUED','RUNNING','VALIDATING')
              AND start_date <= ? AND end_date >= ?
            ORDER BY created_at DESC
            """,
            [request["end_date"], request["start_date"]],
        )
        requested_markets = set(request["markets"])
        requested_datasets = set(request["datasets"])
        for candidate in candidates:
            payload = json.loads(candidate["request_json"])
            if requested_markets.intersection(payload["markets"]) and requested_datasets.intersection(
                payload["datasets"]
            ):
                return {"task_id": candidate["task_id"], "status": candidate["status"]}
        return None

    def _coverage_for_request(self, request: Mapping[str, Any]) -> list[dict[str, Any]]:
        requested = {
            (dataset_id, market_code)
            for dataset_id in request["datasets"]
            for market_code in request["markets"]
        }
        return [
            item
            for item in self.coverage()
            if (item["dataset_id"], item["market_code"]) in requested
        ]

    def _plan_chunks(self, request: Mapping[str, Any]) -> list[dict[str, Any]]:
        chunks: list[dict[str, Any]] = []
        for dataset_id in request["datasets"]:
            for market_code in request["markets"]:
                if request["mode"] == "REVALIDATE":
                    ranges = [(request["start_date"], request["end_date"])]
                else:
                    expected = self._expected_dates(
                        dataset_id,
                        market_code,
                        request["start_date"],
                        request["end_date"],
                    )
                    existing_rows = self.catalog.rows(
                        f"""
                        SELECT DISTINCT business_date FROM current_{dataset_id}
                        WHERE market_code=? AND business_date BETWEEN ? AND ?
                        ORDER BY business_date
                        """,
                        [market_code, request["start_date"], request["end_date"]],
                    )
                    existing = {item["business_date"] for item in existing_rows}
                    ranges = _continuous_ranges(sorted(expected - existing))
                for index, (start_date, end_date) in enumerate(ranges, start=1):
                    chunks.append(
                        {
                            "chunk_id": (
                                f"{dataset_id}__{market_code}__"
                                f"{start_date:%Y%m%d}__{end_date:%Y%m%d}__{index:03d}"
                            ),
                            "dataset_id": dataset_id,
                            "market_code": market_code,
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                        }
                    )
        return chunks

    def _expected_dates(
        self,
        dataset_id: str,
        market_code: str,
        start_date: date,
        end_date: date,
    ) -> set[date]:
        if dataset_id == "instrument_master_history":
            existing = self.catalog.row(
                """
                SELECT count(*) AS count FROM current_instrument_master_history
                WHERE market_code=? AND business_date <= ?
                """,
                [market_code, end_date],
            )
            return set() if existing and int(existing["count"] or 0) else {start_date}
        natural_dates = set(_date_range(start_date, end_date))
        if dataset_id == "market_calendar":
            return natural_dates
        calendar_rows = self.catalog.rows(
            """
            SELECT business_date FROM current_market_calendar
            WHERE market_code=? AND business_date BETWEEN ? AND ? AND is_trading_day
            """,
            [market_code, start_date, end_date],
        )
        if calendar_rows:
            return {item["business_date"] for item in calendar_rows}
        return {item for item in natural_dates if item.weekday() < 5}
