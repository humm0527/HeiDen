"""Quality gates and coverage reporting for the market-foundation service."""

from __future__ import annotations

from datetime import date
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from .constants import (
    DATASETS,
    EXECUTION_MODE,
    MARKETS,
    QUALITY_RULE_VERSION,
)
from .facts import canonical_json
from .service_support import (
    MarketFoundationError,
    as_date,
    atomic_json,
    atomic_text,
    csv_escape,
    iso,
    json_row,
    json_value,
    now,
)


class QualityServiceMixin:
    """Coverage and data-quality behavior shared by the concrete service."""

    def coverage(self) -> list[dict[str, Any]]:
        foundation_start, foundation_end = self._foundation_bounds()
        rows = self.catalog.rows(
            """
            SELECT * FROM coverage_watermark ORDER BY dataset_id, market_code
            """
        )
        by_key = {(row["dataset_id"], row["market_code"]): row for row in rows}
        complete_by_key = {
            (item["dataset_id"], item["market_code"]): item["complete"]
            for item in self._complete_coverage_checks(
                foundation_start,
                foundation_end,
                MARKETS,
                DATASETS,
            )
        }
        return [
            {
                "dataset_id": dataset_id,
                "market_code": market_code,
                "minimum_business_date": iso(
                    by_key.get((dataset_id, market_code), {}).get(
                        "minimum_business_date"
                    )
                ),
                "maximum_business_date": iso(
                    by_key.get((dataset_id, market_code), {}).get(
                        "maximum_business_date"
                    )
                ),
                "distinct_business_dates": int(
                    by_key.get((dataset_id, market_code), {}).get(
                        "distinct_business_dates", 0
                    )
                ),
                "complete_range": bool(
                    complete_by_key.get((dataset_id, market_code), False)
                ),
            }
            for dataset_id in DATASETS
            for market_code in MARKETS
        ]

    def run_quality(
        self, task_id: str, *, foundation_scope: bool = False
    ) -> dict[str, Any]:
        foundation_start, foundation_end = self._foundation_bounds()
        task = self._require_task(task_id)
        request = json.loads(task["request_json"])
        scope = {
            "scope_type": "FOUNDATION" if foundation_scope else "TASK",
            "start_date": (
                foundation_start if foundation_scope else as_date(task["start_date"])
            ),
            "end_date": (
                foundation_end if foundation_scope else as_date(task["end_date"])
            ),
            "markets": list(MARKETS) if foundation_scope else request["markets"],
            "datasets": list(DATASETS) if foundation_scope else request["datasets"],
        }
        quality_run_id = f"mdq_{uuid4().hex}"
        findings: list[dict[str, Any]] = []
        for item in self._complete_coverage_checks(
            scope["start_date"],
            scope["end_date"],
            scope["markets"],
            scope["datasets"],
        ):
            if not item["complete"]:
                findings.append(
                    {
                        "rule_id": "MDFQ001",
                        "severity": "SEVERE",
                        "dataset_id": item["dataset_id"],
                        "market_code": item["market_code"],
                        "message": item["message"],
                        "suggested_action": (
                            "补齐缺口后重新执行全范围质量门禁"
                            if foundation_scope
                            else "补齐本次刷新范围后重新校验"
                        ),
                        "example_keys": [],
                    }
                )
        partition_sql = "SELECT * FROM partition_catalog"
        partition_parameters: list[Any] = []
        if not foundation_scope:
            partition_sql += " WHERE ingestion_task_id=?"
            partition_parameters.append(task_id)
        for partition in self.catalog.rows(partition_sql, partition_parameters):
            path = Path(partition["file_path"])
            actual = sha256(path.read_bytes()).hexdigest() if path.is_file() else None
            if actual != partition["file_sha256"]:
                findings.append(
                    {
                        "rule_id": "MDFQ002",
                        "severity": "SEVERE",
                        "dataset_id": partition["dataset_id"],
                        "market_code": partition["market_code"],
                        "message": "Parquet 文件缺失或 SHA-256 不一致",
                        "suggested_action": "保留证据并重新生成新分区，禁止修改目录哈希",
                        "example_keys": [partition["partition_id"]],
                    }
                )
        if self.execution_mode == EXECUTION_MODE:
            findings.append(
                {
                    "rule_id": "MDFQ004",
                    "severity": "WARNING",
                    "dataset_id": None,
                    "market_code": None,
                    "message": "P2 数据仅为合成模式，风险适配状态保持 NOT_RUN",
                    "suggested_action": "完成影子对账并另行授权真实验证",
                    "example_keys": [],
                }
            )
        elif not self.allow_snapshot_candidates:
            is_p4 = self.execution_mode == "RQDATA_REAL_P4"
            findings.append(
                {
                    "rule_id": "MDFQ005",
                    "severity": "WARNING",
                    "dataset_id": None,
                    "market_code": None,
                    "message": (
                        "P4 当前仍缺北交所日行情和涨跌停，不能形成全市场快照候选"
                        if is_p4
                        else "P3 仅验证显式真实样本，不能形成全市场快照候选"
                    ),
                    "suggested_action": (
                        "完成三市场六类覆盖和全范围质量对账后再启用候选"
                        if is_p4
                        else "完成来源覆盖与影子对账后，另行授权 P4 全市场回填"
                    ),
                    "example_keys": [],
                }
            )
        severe = sum(item["severity"] == "SEVERE" for item in findings)
        warning = sum(item["severity"] == "WARNING" for item in findings)
        info = sum(item["severity"] == "INFO" for item in findings)
        status = "FAIL" if severe else "WARNING" if warning else "PASS"
        created_at = now()
        with self.catalog.transaction() as transaction:
            transaction.execute(
                "INSERT INTO data_quality_run VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    quality_run_id,
                    task_id,
                    QUALITY_RULE_VERSION,
                    canonical_json(json_value(scope)),
                    status,
                    severe,
                    warning,
                    info,
                    created_at,
                    created_at,
                ],
            )
            for index, finding in enumerate(findings, start=1):
                transaction.execute(
                    """
                    INSERT INTO data_quality_result VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        quality_run_id,
                        f"finding_{index:04d}",
                        finding["rule_id"],
                        finding["severity"],
                        finding["dataset_id"],
                        finding["market_code"],
                        scope["start_date"],
                        scope["end_date"],
                        1,
                        finding["message"],
                        finding["suggested_action"],
                        canonical_json(finding["example_keys"]),
                    ],
                )
        self._write_quality_report(quality_run_id)
        return self.get_quality_run(quality_run_id)

    def get_quality_run(self, quality_run_id: str) -> dict[str, Any]:
        run = self.catalog.row(
            "SELECT * FROM data_quality_run WHERE quality_run_id=?", [quality_run_id]
        )
        if run is None:
            raise MarketFoundationError("MDF010", "质量运行不存在")
        findings = self.catalog.rows(
            "SELECT * FROM data_quality_result WHERE quality_run_id=? ORDER BY finding_id",
            [quality_run_id],
        )
        return {
            **json_row(run),
            "scope": json.loads(run["scope_json"]),
            "findings": [json_row(row) for row in findings],
        }

    def quality_report_path(self, quality_run_id: str, format_name: str) -> Path:
        if format_name not in {"json", "csv"}:
            raise MarketFoundationError("MDF001", "质量报告格式只支持 json 或 csv")
        path = self.root / "quality" / quality_run_id / f"quality_report.{format_name}"
        if not path.is_file():
            self.get_quality_run(quality_run_id)
            self._write_quality_report(quality_run_id)
        return path

    def _foundation_coverage_complete(self) -> bool:
        foundation_start, foundation_end = self._foundation_bounds()
        return all(
            item["complete"]
            for item in self._complete_coverage_checks(
                foundation_start,
                foundation_end,
                MARKETS,
                DATASETS,
            )
        )

    def _complete_coverage_checks(
        self,
        start_date: date,
        end_date: date,
        markets: Iterable[str],
        datasets: Iterable[str],
    ) -> list[dict[str, Any]]:
        market_list = list(markets)
        dataset_set = set(datasets)
        expected_natural_days = (end_date - start_date).days + 1
        checks: list[dict[str, Any]] = []
        daily_datasets = (
            "daily_price_unadjusted",
            "daily_st_status",
            "daily_suspension_status",
            "daily_price_limits",
        )
        for market_code in market_list:
            calendar = self.catalog.row(
                """
                SELECT count(DISTINCT business_date) AS dates,
                       min(business_date) AS minimum_date,
                       max(business_date) AS maximum_date,
                           count(DISTINCT CASE
                           WHEN is_trading_day
                           THEN business_date END) AS trading_dates
                FROM current_market_calendar
                WHERE market_code=? AND business_date BETWEEN ? AND ?
                """,
                [market_code, start_date, end_date],
            )
            calendar_complete = bool(
                calendar
                and int(calendar["dates"] or 0) == expected_natural_days
                and calendar["minimum_date"] == start_date
                and calendar["maximum_date"] == end_date
            )
            if "market_calendar" in dataset_set:
                checks.append(
                    {
                        "dataset_id": "market_calendar",
                        "market_code": market_code,
                        "complete": calendar_complete,
                        "message": "交易日历未覆盖质量范围内每个自然日",
                    }
                )
            master = self.catalog.row(
                """
                SELECT count(*) AS instruments FROM current_instrument_master_history
                WHERE market_code=?
                  AND listing_date <= ?
                  AND (termination_date IS NULL OR termination_date >= ?)
                """,
                [market_code, end_date, start_date],
            )
            if "instrument_master_history" in dataset_set:
                checks.append(
                    {
                        "dataset_id": "instrument_master_history",
                        "market_code": market_code,
                        "complete": bool(
                            master and int(master["instruments"] or 0) > 0
                        ),
                        "message": "证券主数据没有覆盖质量范围的生命周期记录",
                    }
                )
            expected_trading_dates = (
                int(calendar["trading_dates"] or 0) if calendar else 0
            )
            for dataset_id in daily_datasets:
                if dataset_id not in dataset_set:
                    continue
                actual = self.catalog.row(
                    f"""
                    SELECT count(DISTINCT business_date) AS dates
                    FROM current_{dataset_id}
                    WHERE market_code=? AND business_date BETWEEN ? AND ?
                    """,
                    [market_code, start_date, end_date],
                )
                actual_dates = int(actual["dates"] or 0) if actual else 0
                mismatch = self.catalog.row(
                    f"""
                    WITH expected AS (
                        SELECT business_date FROM current_market_calendar
                        WHERE market_code=?
                          AND is_trading_day AND business_date BETWEEN ? AND ?
                    ), actual AS (
                        SELECT DISTINCT business_date FROM current_{dataset_id}
                        WHERE market_code=? AND business_date BETWEEN ? AND ?
                    )
                    SELECT count(*) AS mismatch_count FROM (
                        (SELECT * FROM expected EXCEPT SELECT * FROM actual)
                        UNION ALL
                        (SELECT * FROM actual EXCEPT SELECT * FROM expected)
                    )
                    """,
                    [
                        market_code,
                        start_date,
                        end_date,
                        market_code,
                        start_date,
                        end_date,
                    ],
                )
                checks.append(
                    {
                        "dataset_id": dataset_id,
                        "market_code": market_code,
                        "complete": bool(
                            calendar_complete
                            and expected_trading_dates > 0
                            and actual_dates == expected_trading_dates
                            and int(mismatch["mismatch_count"] or 0) == 0
                        ),
                        "message": "日事实日期集合与该市场交易日日历不一致",
                    }
                )
        return checks

    def _write_quality_report(self, quality_run_id: str) -> None:
        result = self.get_quality_run(quality_run_id)
        target = self.root / "quality" / quality_run_id
        target.mkdir(parents=True, exist_ok=True)
        atomic_json(target / "quality_report.json", result)
        columns = (
            "finding_id",
            "rule_id",
            "severity",
            "dataset_id",
            "market_code",
            "date_start",
            "date_end",
            "affected_rows",
            "message",
            "suggested_action",
        )
        lines = [",".join(columns)]
        for finding in result["findings"]:
            lines.append(
                ",".join(csv_escape(finding.get(column)) for column in columns)
            )
        atomic_text(target / "quality_report.csv", "\ufeff" + "\n".join(lines) + "\n")
