"""
文件作用：使用已发布且影子适配通过的市场快照离线执行正式或影子风险计算，影子模式与旧入口结果逐键对账。
编辑记录：
【首次生成：2026-08-19，实现快照绑定、四表哈希复验、风险门禁、不可覆盖结果和结果级对账。】
【二次编辑：2026-08-19，增加不访问 RQData 的正式快照计算入口、取消检查和正式执行审计字段。】
【三次编辑：2026-08-20，为本地快照导出、输入装配、门禁、事件和压力计算增加实时进度回调。】
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from hashlib import sha256
from pathlib import Path
import shutil
from typing import Any, Callable, Iterable, Mapping

from riskaudit.risk_events import (
    build_risk_input_tables,
    calculate_limit_down_pressure_observations,
    calculate_risk_events,
    find_missing_pit_board_requests,
    load_approved_rules,
    load_business_risk_universe,
    load_limit_down_pressure_rules,
    load_standard_code_tables,
    validate_risk_inputs,
    write_limit_down_pressure_result,
    write_risk_event_result,
)
from riskaudit.validation import DataValidator
from riskaudit.cancellation import raise_if_canceled

from .facts import canonical_json
from .service import MarketFoundationError, MarketFoundationService
from .risk_shadow_reconciliation import _file_hash, reconcile_risk_results
from .risk_shadow_exports import (
    _append_missing_snapshot_master_rows,
    _normalize_bse_security_codes,
    _normalize_lifecycle_reference_date,
    _previous_trading_date,
    _require_usable_snapshot,
    _verified_export,
    _write_json,
    ensure_broker_metric_snapshot_export,
)

__all__ = [
    "_normalize_lifecycle_reference_date",
    "_run_snapshot_risk_calculation",
    "ensure_broker_metric_snapshot_export",
    "reconcile_risk_results",
    "run_snapshot_risk_calculation",
    "run_snapshot_risk_shadow",
]


def run_snapshot_risk_shadow(
    *,
    repository_root: str | Path,
    market_lake_root: str | Path,
    snapshot_id: str,
    business_file: str | Path,
    output_root: str | Path,
    batch_id: str,
    rules_path: str | Path,
    observation_start: date,
    observation_end: date,
    baseline_result_dir: str | Path,
    risk_universe_files: Iterable[str | Path] = (),
) -> dict[str, Any]:
    """Run a snapshot-bound calculation without changing the formal entry point."""

    return _run_snapshot_risk_calculation(
        repository_root=repository_root,
        market_lake_root=market_lake_root,
        snapshot_id=snapshot_id,
        business_file=business_file,
        output_root=output_root,
        batch_id=batch_id,
        rules_path=rules_path,
        observation_start=observation_start,
        observation_end=observation_end,
        baseline_result_dir=baseline_result_dir,
        risk_universe_files=risk_universe_files,
        lifecycle_reference_path=None,
        formal_entry=False,
        cancel_check=None,
        progress_callback=None,
    )


def run_snapshot_risk_calculation(
    *,
    repository_root: str | Path,
    market_lake_root: str | Path,
    snapshot_id: str,
    business_file: str | Path,
    output_root: str | Path,
    batch_id: str,
    rules_path: str | Path,
    observation_start: date,
    observation_end: date,
    risk_universe_files: Iterable[str | Path] = (),
    lifecycle_reference_path: str | Path | None = None,
    cancel_check: Callable[[], bool] | None = None,
    progress_callback: Callable[[str, Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run the formal quarterly risk entry exclusively from a published snapshot."""

    return _run_snapshot_risk_calculation(
        repository_root=repository_root,
        market_lake_root=market_lake_root,
        snapshot_id=snapshot_id,
        business_file=business_file,
        output_root=output_root,
        batch_id=batch_id,
        rules_path=rules_path,
        observation_start=observation_start,
        observation_end=observation_end,
        baseline_result_dir=None,
        risk_universe_files=risk_universe_files,
        lifecycle_reference_path=lifecycle_reference_path,
        formal_entry=True,
        cancel_check=cancel_check,
        progress_callback=progress_callback,
    )


def _run_snapshot_risk_calculation(
    *,
    repository_root: str | Path,
    market_lake_root: str | Path,
    snapshot_id: str,
    business_file: str | Path,
    output_root: str | Path,
    batch_id: str,
    rules_path: str | Path,
    observation_start: date,
    observation_end: date,
    baseline_result_dir: str | Path | None,
    risk_universe_files: Iterable[str | Path],
    lifecycle_reference_path: str | Path | None = None,
    formal_entry: bool,
    cancel_check: Callable[[], bool] | None,
    progress_callback: Callable[[str, Mapping[str, Any]], None] | None,
) -> dict[str, Any]:
    """Shared immutable calculation pipeline for formal and shadow executions."""

    if observation_start > observation_end:
        raise MarketFoundationError("MDFSC001", "本地快照计算观察区间无效")
    raise_if_canceled(cancel_check)
    repository = Path(repository_root).resolve()
    business_path = Path(business_file).resolve()
    supplemental_paths = tuple(Path(item).resolve() for item in risk_universe_files)
    baseline_root = (
        Path(baseline_result_dir).resolve()
        if baseline_result_dir is not None
        else None
    )
    if not business_path.is_file():
        raise MarketFoundationError("MDFSC002", "业务宽表不存在")
    if any(not item.is_file() for item in supplemental_paths):
        raise MarketFoundationError("MDFSC002", "补充证券宇宙文件不存在")
    if baseline_root is not None and not baseline_root.is_dir():
        raise MarketFoundationError("MDFSC003", "旧入口结果目录不存在")
    batch_root = Path(output_root).resolve() / batch_id
    if batch_root.exists():
        raise FileExistsError(f"本地快照影子批次已存在：{batch_root}")
    batch_root.mkdir(parents=True, exist_ok=False)
    started_at = datetime.now(timezone.utc)
    _report_progress(progress_callback, "local_snapshot_open")

    service = MarketFoundationService(
        market_lake_root,
        execution_mode="RQDATA_REAL_P4",
        source_system="RQDATA",
        allow_snapshot_candidates=False,
        read_only_catalog=True,
    )
    try:
        snapshot = service.get_snapshot(snapshot_id)
        _require_usable_snapshot(snapshot, observation_start, observation_end)
        raise_if_canceled(cancel_check)
        baseline_date = _previous_trading_date(
            service, snapshot_id, observation_start
        )
        _report_progress(
            progress_callback,
            "local_snapshot_export",
            snapshot_id=snapshot_id,
            start_date=baseline_date.isoformat(),
            end_date=observation_end.isoformat(),
        )
        export_root, export_manifest = _verified_export(
            service,
            snapshot_id,
            start_date=baseline_date,
            end_date=observation_end,
            output_root=batch_root / "market_snapshot_exports",
        )
        broker_metric_market_inputs = None
        if formal_entry:
            _report_progress(
                progress_callback,
                "local_snapshot_market_input",
                snapshot_id=snapshot_id,
            )
            broker_metric_market_inputs = ensure_broker_metric_snapshot_export(
                service,
                snapshot_id,
                end_date=date.fromisoformat(str(snapshot["end_date"])),
                cancel_check=cancel_check,
                output_root=batch_root / "market_snapshot_exports",
                lifecycle_reference_path=lifecycle_reference_path,
            )
        raise_if_canceled(cancel_check)

        input_dir = batch_root / "business_input"
        input_dir.mkdir()
        input_copy = input_dir / "券商分类宽表.csv"
        shutil.copy2(business_path, input_copy)
        _report_progress(progress_callback, "business_input_validation")
        validator = DataValidator(
            repository / "configs/mapping/business_field_registry_v1.yaml",
            validation_pack="broker_risk_universe_v1",
        )
        business_validation = validator.validate_directory(
            input_dir,
            observation_start=observation_start,
            observation_end=observation_end,
            output_dir=batch_root / "business_validation",
        )
        if not business_validation.allow_run:
            raise MarketFoundationError("MDFSC004", "业务宽表未通过风险专用预检")
        raise_if_canceled(cancel_check)

        supplemental_audit = []
        if supplemental_paths:
            supplemental_root = input_dir / "supplemental_universe"
            supplemental_root.mkdir()
            for index, source in enumerate(supplemental_paths, start=1):
                audit_copy = supplemental_root / f"{index:04d}_{source.name}"
                shutil.copy2(source, audit_copy)
                supplemental_audit.append(
                    {
                        "source_path": str(source),
                        "source_sha256": _file_hash(source),
                        "audit_copy_path": str(audit_copy),
                        "audit_copy_sha256": _file_hash(audit_copy),
                    }
                )

        _report_progress(progress_callback, "local_snapshot_input_assembly")
        broker_universe, business_summary = load_business_risk_universe(
            business_path,
            observation_end=observation_end,
            supplemental_source_paths=supplemental_paths,
        )
        raise_if_canceled(cancel_check)
        merged = load_standard_code_tables(
            export_root,
            repository / "configs/mapping/business_field_registry_v1.yaml",
        )
        _normalize_bse_security_codes(merged)
        _append_missing_snapshot_master_rows(
            service,
            snapshot_id,
            merged,
            broker_universe,
            observation_end,
        )
        tables = build_risk_input_tables(merged, broker_universe)
        raise_if_canceled(cancel_check)
        rules = load_approved_rules(
            rules_path,
            observation_start=observation_start,
            observation_end=observation_end,
        )
        missing_pit = find_missing_pit_board_requests(
            tables, decimal_scale=rules.decimal_scale
        )
        if missing_pit:
            raise MarketFoundationError(
                "MDFSC005",
                "本地快照缺少候选跌停日 PIT 板块状态，禁止联网补取",
                {"snapshot_dates": sorted(missing_pit)},
            )

        risk_input_root = batch_root / "risk_input_code"
        risk_input_root.mkdir()
        tables.trading_calendar.to_csv(
            risk_input_root / "trading_calendar.csv", index=False
        )
        tables.security_status_daily.to_csv(
            risk_input_root / "security_status_daily.csv", index=False
        )
        tables.stock_market_daily.to_csv(
            risk_input_root / "stock_market_daily.csv", index=False
        )
        tables.security_risk_status.to_csv(
            risk_input_root / "security_risk_status.csv", index=False
        )
        tables.broker_risk_classification.to_csv(
            risk_input_root / "broker_risk_universe.csv", index=False
        )

        _report_progress(progress_callback, "local_snapshot_risk_input_gate")
        gate = validate_risk_inputs(
            tables,
            rules,
            prevalidation_allow_run=business_validation.allow_run,
        )
        _write_json(batch_root / "risk_input_gate_result.json", gate.to_dict())
        if not gate.allow_run:
            raise MarketFoundationError(
                "MDFSC006",
                "本地快照未通过风险输入门禁",
                {"finding_count": len(gate.findings)},
            )
        raise_if_canceled(cancel_check)

        _report_progress(progress_callback, "local_snapshot_risk_event_calculation")
        result = calculate_risk_events(
            tables,
            gate,
            rules,
            calculation_batch_id=batch_id,
            upstream_snapshot_id=snapshot_id,
        )
        raise_if_canceled(cancel_check)
        result_dir = write_risk_event_result(
            result, gate, batch_root / "risk_results"
        )
        _report_progress(progress_callback, "local_snapshot_pressure_calculation")
        pressure_rules = load_limit_down_pressure_rules(
            repository / "configs/rules/approved/limit_down_pressure_v1.yaml"
        )
        pressure_result = calculate_limit_down_pressure_observations(
            tables,
            gate,
            rules,
            pressure_rules,
            result.events,
            calculation_batch_id=batch_id,
            upstream_snapshot_id=snapshot_id,
        )
        raise_if_canceled(cancel_check)
        pressure_result_dir = write_limit_down_pressure_result(
            pressure_result, batch_root / "pressure_results"
        )

        reconciliation = None
        reconciliation_path = None
        if baseline_root is not None:
            reconciliation = reconcile_risk_results(result_dir, baseline_root)
            reconciliation["market_snapshot_id"] = snapshot_id
            reconciliation["baseline_result_dir"] = str(baseline_root)
            reconciliation["local_result_dir"] = str(result_dir)
            reconciliation["created_at"] = datetime.now(timezone.utc).isoformat()
            reconciliation["report_hash"] = sha256(
                canonical_json(reconciliation).encode("utf-8")
            ).hexdigest()
            reconciliation_path = batch_root / "result_reconciliation.json"
            _write_json(reconciliation_path, reconciliation)

        _report_progress(progress_callback, "local_snapshot_summary_write")
        summary = {
            "status": (
                "SUCCEEDED"
                if formal_entry
                else "SUCCEEDED_SHADOW_MATCH"
                if reconciliation is not None and reconciliation["status"] == "PASS"
                else "SHADOW_RESULT_DIFFERENCE"
            ),
            "execution_mode": (
                "LOCAL_MARKET_SNAPSHOT_FORMAL"
                if formal_entry
                else "LOCAL_MARKET_SNAPSHOT_SHADOW"
            ),
            "rqdata_accessed": False,
            "batch_id": batch_id,
            "market_snapshot_id": snapshot_id,
            "market_snapshot_member_hash": snapshot["member_hash"],
            "risk_adapter_status": snapshot["risk_adapter_status"],
            "observation_start": observation_start.isoformat(),
            "observation_end": observation_end.isoformat(),
            "st_baseline_date": baseline_date.isoformat(),
            "business_universe": asdict(business_summary),
            "business_file": str(business_path),
            "business_file_sha256": _file_hash(business_path),
            "supplemental_risk_universe_files": supplemental_audit,
            "snapshot_manifest": str(service.snapshot_manifest_path(snapshot_id)),
            "snapshot_export_root": str(export_root),
            "snapshot_export_manifest_hash": sha256(
                canonical_json(export_manifest).encode("utf-8")
            ).hexdigest(),
            "gate": gate.to_dict(),
            "result_dir": str(result_dir),
            "result_manifest": result.manifest(),
            "pressure_result_dir": str(pressure_result_dir),
            "pressure_result_manifest": pressure_result.manifest(),
            "broker_metric_market_inputs": broker_metric_market_inputs,
            "baseline_result_dir": str(baseline_root) if baseline_root else None,
            "result_reconciliation": reconciliation,
            "result_reconciliation_path": (
                str(reconciliation_path) if reconciliation_path else None
            ),
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "formal_risk_entry": formal_entry,
            "risk_entry_switch_attempted": formal_entry,
        }
        _write_json(batch_root / "run_summary.json", summary)
        return summary
    finally:
        service.close()


def _report_progress(
    callback: Callable[[str, Mapping[str, Any]], None] | None,
    stage: str,
    **details: Any,
) -> None:
    if callback is not None:
        callback(stage, details)
