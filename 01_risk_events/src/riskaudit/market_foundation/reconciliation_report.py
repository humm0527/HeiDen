"""Immutable JSON/CSV report persistence for market reconciliation."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Iterable, Mapping
from uuid import uuid4

from .constants import (
    DATASETS,
    MARKETS,
    RECONCILIATION_RULE_VERSION,
    RISK_MARKETS,
)
from .facts import canonical_json
from .service import MarketFoundationService
from .reconciliation_support import (
    ReconciliationCheck,
    _as_date,
    _write_checks_csv_new,
    _write_json_new,
)


def _persist_report(
    service: MarketFoundationService,
    *,
    run: Mapping[str, Any],
    readiness: Mapping[str, Any],
    contributing_backfill_ids: tuple[str, ...],
    checks: Iterable[ReconciliationCheck],
) -> dict[str, Any]:
    check_list = list(checks)
    failed = [item for item in check_list if item.outcome == "FAIL"]
    severe = sum(item.severity == "SEVERE" for item in failed)
    warning = sum(item.severity == "WARNING" for item in failed)
    info = sum(item.severity == "INFO" for item in failed)
    status = "FAIL" if severe else "WARNING" if warning else "PASS"
    quality_run_id = f"mdq_{uuid4().hex}"
    created_at = datetime.now(timezone.utc)
    scope = {
        "scope_type": "P4_FULL_RECONCILIATION",
        "backfill_id": run["backfill_id"],
        "backfill_ids": list(contributing_backfill_ids),
        "universe_id": run["universe_id"],
        "universe_manifest_hash": run["universe_manifest_hash"],
        "start_date": _as_date(run["start_date"]).isoformat(),
        "end_date": _as_date(run["end_date"]).isoformat(),
        "markets": list(MARKETS),
        "risk_markets": list(RISK_MARKETS),
        "non_blocking_markets": [market for market in MARKETS if market not in RISK_MARKETS],
        "gate_policy": "RISK_SCOPE_ONLY",
        "datasets": list(DATASETS),
        "snapshot_creation": "DISABLED",
        "snapshot_publication": "NOT_ATTEMPTED",
    }
    report: dict[str, Any] = {
        "quality_run_id": quality_run_id,
        "rule_version": RECONCILIATION_RULE_VERSION,
        "status": status,
        "severe_count": severe,
        "warning_count": warning,
        "info_count": info,
        "passed_check_count": sum(item.outcome == "PASS" for item in check_list),
        "failed_check_count": len(failed),
        "created_at": created_at.isoformat(),
        "completed_at": created_at.isoformat(),
        "scope": scope,
        "readiness": dict(readiness),
        "snapshot_candidate_created": False,
        "snapshot_publication_attempted": False,
        "checks": [asdict(item) for item in check_list],
    }
    report["report_hash"] = sha256(
        canonical_json(report).encode("utf-8")
    ).hexdigest()
    target = service.root / "quality" / quality_run_id
    staging = service.root / ".staging" / f"{quality_run_id}_reconciliation"
    staging.mkdir(parents=True, exist_ok=False)
    try:
        _write_json_new(staging / "full_reconciliation_report.json", report)
        _write_checks_csv_new(staging / "full_reconciliation_checks.csv", check_list)
        with service.catalog.transaction() as transaction:
            transaction.execute(
                "INSERT INTO data_quality_run VALUES (?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    quality_run_id,
                    RECONCILIATION_RULE_VERSION,
                    canonical_json(scope),
                    status,
                    severe,
                    warning,
                    info,
                    created_at,
                    created_at,
                ],
            )
            for index, item in enumerate(failed, start=1):
                transaction.execute(
                    """
                    INSERT INTO data_quality_result VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        quality_run_id,
                        f"finding_{index:04d}",
                        item.rule_id,
                        item.severity,
                        item.dataset_id,
                        item.market_code,
                        _as_date(run["start_date"]),
                        _as_date(run["end_date"]),
                        item.affected_rows,
                        item.message,
                        item.suggested_action,
                        canonical_json(list(item.example_keys)),
                    ],
                )
        staging.replace(target)
    except Exception:
        if staging.exists():
            for path in staging.iterdir():
                path.unlink(missing_ok=True)
            staging.rmdir()
        raise
    return {
        **report,
        "report_paths": {
            "json": str(target / "full_reconciliation_report.json"),
            "csv": str(target / "full_reconciliation_checks.csv"),
        },
    }
