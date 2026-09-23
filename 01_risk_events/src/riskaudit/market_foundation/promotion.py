"""
文件作用：把通过 P4 全范围对账的终态数据集封装为不可变市场快照候选，保持人工发布与风险切换隔离。
编辑记录：
【首次生成：2026-08-15，实现终态、报告哈希、质量版本、数据新鲜度和幂等候选门禁。】
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from .constants import DATASETS, MARKETS, RECONCILIATION_RULE_VERSION, RISK_MARKETS
from .facts import canonical_json
from .reconciliation import inspect_reconciliation_readiness
from .service import MarketFoundationError, MarketFoundationService


def inspect_p4_candidate_readiness(
    service: MarketFoundationService,
    backfill_id: str,
    quality_run_id: str,
) -> dict[str, Any]:
    """Validate a PASS report without creating a candidate or changing catalog state."""

    if service.execution_mode != "RQDATA_REAL_P4":
        raise MarketFoundationError("MDFP001", "只有隔离的 RQDATA_REAL_P4 数据湖可生成正式候选")
    backfill = inspect_reconciliation_readiness(service, backfill_id)
    run = service.catalog.row(
        "SELECT * FROM market_backfill_run WHERE backfill_id=?", [backfill_id]
    )
    quality = service.catalog.row(
        "SELECT * FROM data_quality_run WHERE quality_run_id=?", [quality_run_id]
    )
    if quality is None:
        raise MarketFoundationError("MDFP002", "指定的全范围质量运行不存在")
    try:
        scope = json.loads(quality["scope_json"])
    except (TypeError, json.JSONDecodeError) as exc:
        raise MarketFoundationError("MDFP003", "质量运行范围无法复验") from exc
    contributing_backfill_ids = tuple(scope.get("backfill_ids") or (backfill_id,))
    if backfill_id not in contributing_backfill_ids:
        raise MarketFoundationError("MDFP003", "质量运行未绑定候选主回填")
    contributing_readiness = [
        inspect_reconciliation_readiness(service, item)
        for item in contributing_backfill_ids
    ]
    expected_scope = bool(
        quality["rule_version"] == RECONCILIATION_RULE_VERSION
        and quality["status"] == "PASS"
        and int(quality["severe_count"]) == 0
        and int(quality["warning_count"]) == 0
        and scope.get("scope_type") == "P4_FULL_RECONCILIATION"
        and scope.get("backfill_id") == backfill_id
        and list(contributing_backfill_ids) == scope.get("backfill_ids", [backfill_id])
        and scope.get("universe_id") == run["universe_id"]
        and scope.get("universe_manifest_hash") == run["universe_manifest_hash"]
        and scope.get("markets") == list(MARKETS)
        and scope.get("risk_markets") == list(RISK_MARKETS)
        and scope.get("gate_policy") == "RISK_SCOPE_ONLY"
        and scope.get("datasets") == list(DATASETS)
    )
    if not expected_scope:
        raise MarketFoundationError(
            "MDFP003",
            "候选必须绑定同一 P4 回填的 PASS 全范围质量运行",
            {
                "quality_run_id": quality_run_id,
                "quality_status": quality["status"],
                "rule_version": quality["rule_version"],
            },
        )
    report_path = (
        service.root / "quality" / quality_run_id / "full_reconciliation_report.json"
    )
    report = _verified_report(report_path)
    if (
        report.get("quality_run_id") != quality_run_id
        or report.get("status") != "PASS"
        or report.get("scope") != scope
    ):
        raise MarketFoundationError("MDFP004", "全范围报告与 DuckDB 质量运行不一致")
    quality_completed = _utc(quality["completed_at"])
    latest_mutation = _latest_evidence_timestamp(service, backfill_id)
    if latest_mutation and latest_mutation > quality_completed:
        raise MarketFoundationError(
            "MDFP005",
            "质量对账结束后数据证据发生变化，必须重新执行全范围对账",
            {
                "quality_completed_at": quality_completed.isoformat(),
                "latest_evidence_at": latest_mutation.isoformat(),
            },
        )
    _verify_file_evidence(service)
    existing = service.catalog.row(
        """
        SELECT snapshot_id FROM market_snapshot
        WHERE candidate_id=? AND quality_run_id=? AND status='READY_TO_PUBLISH'
        ORDER BY created_at LIMIT 1
        """,
        [backfill_id, quality_run_id],
    )
    return {
        **backfill,
        "quality_run_id": quality_run_id,
        "quality_status": quality["status"],
        "contributing_backfill_ids": list(contributing_backfill_ids),
        "contributing_backfills_ready": all(item["ready"] for item in contributing_readiness),
        "report_hash": report["report_hash"],
        "report_path": str(report_path),
        "latest_evidence_at": latest_mutation.isoformat() if latest_mutation else None,
        "existing_candidate_id": existing["snapshot_id"] if existing else None,
        "ready_for_candidate": True,
        "automatic_publication": "DISABLED",
        "risk_entry_switch": "NOT_AUTHORIZED",
    }


def create_p4_candidate(
    service: MarketFoundationService,
    backfill_id: str,
    quality_run_id: str,
) -> dict[str, Any]:
    """Create an idempotent READY_TO_PUBLISH candidate; never publish it."""

    readiness = inspect_p4_candidate_readiness(service, backfill_id, quality_run_id)
    if readiness["existing_candidate_id"]:
        return service.get_snapshot(readiness["existing_candidate_id"])
    quality = service.catalog.row(
        "SELECT candidate_id FROM data_quality_run WHERE quality_run_id=?",
        [quality_run_id],
    )
    if quality["candidate_id"] is not None:
        raise MarketFoundationError("MDFP006", "质量运行已经绑定其他候选，禁止覆盖")
    candidate = service._create_candidate(backfill_id, quality_run_id)
    if candidate["status"] != "READY_TO_PUBLISH":
        raise MarketFoundationError("MDFP006", "全范围 PASS 质量运行未形成可发布候选")
    return candidate


def _verified_report(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise MarketFoundationError("MDFP004", "全范围 JSON 报告不存在")
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MarketFoundationError("MDFP004", "全范围 JSON 报告无法读取") from exc
    expected_hash = report.get("report_hash")
    unsigned = dict(report)
    unsigned.pop("report_hash", None)
    actual_hash = sha256(canonical_json(unsigned).encode("utf-8")).hexdigest()
    if expected_hash != actual_hash:
        raise MarketFoundationError("MDFP004", "全范围 JSON 报告哈希复验失败")
    return report


def _latest_evidence_timestamp(
    service: MarketFoundationService, backfill_id: str
) -> datetime | None:
    values = []
    queries = (
        ("SELECT max(created_at) AS value FROM partition_catalog", []),
        ("SELECT max(created_at) AS value FROM source_record_catalog", []),
        ("SELECT max(received_at) AS value FROM raw_asset", []),
        (
            "SELECT max(updated_at) AS value FROM market_backfill_unit WHERE backfill_id=?",
            [backfill_id],
        ),
    )
    for sql, parameters in queries:
        value = service.catalog.row(sql, parameters)["value"]
        if value is not None:
            values.append(_utc(value))
    run_updated = service.catalog.row(
        "SELECT updated_at AS value FROM market_backfill_run WHERE backfill_id=?",
        [backfill_id],
    )["value"]
    if run_updated is not None:
        values.append(_utc(run_updated))
    return max(values) if values else None


def _verify_file_evidence(service: MarketFoundationService) -> None:
    for table_name, id_field in (
        ("partition_catalog", "partition_id"),
        ("raw_asset", "asset_id"),
    ):
        for item in service.catalog.rows(
            f"SELECT {id_field} AS evidence_id, file_path, file_sha256 FROM {table_name}"
        ):
            path = Path(item["file_path"])
            matches = _file_hash_matches(
                path,
                item["file_sha256"],
                allow_crlf_normalization=table_name == "raw_asset",
            )
            if not matches:
                raise MarketFoundationError(
                    "MDFP005",
                    "全范围对账后的文件证据缺失或哈希变化，必须重新对账",
                    {"evidence_id": item["evidence_id"], "file_path": str(path)},
                )


def _file_hash_matches(
    path: Path,
    expected_hash: str,
    *,
    allow_crlf_normalization: bool,
) -> bool:
    if not path.is_file():
        return False
    content = path.read_bytes()
    if sha256(content).hexdigest() == expected_hash:
        return True
    return bool(
        allow_crlf_normalization
        and b"\r\n" in content
        and sha256(content.replace(b"\r\n", b"\n")).hexdigest() == expected_hash
    )


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)
