"""
文件作用：用真实集中度宽表与分块 RQData 市场事实装配五表、执行风险专用门禁并计算确定性风险事件。
编辑记录：
【首次生成：2026-08-11，实现事件专用宽表预检、证券并集预对账、可续跑分块、ST 基线、PIT 板块补取和结果落盘。】
【第二次编辑：2026-08-11，恢复模式显式覆盖阶段摘要，避免已完成预对账阻断后续续跑。】
【第三次编辑：2026-08-11，持久化批次各阶段耗时、失败信息与续跑 attempt。】
【第四次编辑：2026-08-12，支持显式观察区间参数并将其贯穿规则加载、运行摘要与正式结果 manifest。】
【第五次编辑：2026-08-14，在阶段、分块和外部请求边界响应用户取消信号。】
【第六次编辑：2026-08-18，允许全券商基准文件补足单券商窄表的风险事件证券全集。】
【第七次编辑：2026-08-18，正式事件计算后同步生成并独立落盘非 ST 跌停压力观察结果。】
【第八次编辑：2026-09-13，向命令行暴露可重复的补充证券宇宙参数，覆盖历史退市证券生命周期。】
【第九次编辑：2026-09-14，增加按完成时间归档的中文季度结果包，并精简终端输出。】
【第十次编辑：2026-09-14，生成可迁移的 02 输入清单及覆盖历史分类范围的专用交易日历。】
【第十一次编辑：2026-09-14，自动扫描全行业与券商03历史目录，按目录范围扩展 02 辅助交易日历。】
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import asdict
from datetime import date, datetime, timezone
import json
from pathlib import Path
import re
import shutil
import sys
from typing import Any, Iterable, Sequence

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from riskaudit.data_ingestion import RawStore  # noqa: E402
from riskaudit.data_ingestion.adapters import RealRQDataClient  # noqa: E402
from riskaudit.business_files import inspect_business_date_range  # noqa: E402
from riskaudit.cancellation import (  # noqa: E402
    CalculationCanceled,
    raise_if_canceled,
)
from riskaudit.performance import PerformanceRecorder  # noqa: E402
from riskaudit.tasks.broker_handoff import write_broker_input_manifest  # noqa: E402
from riskaudit.risk_events import (  # noqa: E402
    build_pit_security_status_rows,
    build_risk_input_tables,
    calculate_risk_events,
    find_missing_pit_board_requests,
    load_approved_rules,
    load_business_risk_universe,
    load_limit_down_pressure_rules,
    merge_standard_code_runs,
    calculate_limit_down_pressure_observations,
    validate_risk_inputs,
    write_limit_down_pressure_result,
    write_risk_event_result,
)
from riskaudit.validation import DataValidator  # noqa: E402
from scripts.run_rqdata_market import run_rqdata_market  # noqa: E402


_SAFE_ID = re.compile(r"^[A-Za-z0-9_-]+$")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="执行真实集中度风险事件计算")
    parser.add_argument("--business-file", type=Path, required=True)
    parser.add_argument(
        "--risk-universe-file",
        type=Path,
        action="append",
        default=[],
        help="补充证券宇宙 CSV；可重复传入，不扩展事件观察日期。",
    )
    parser.add_argument(
        "--broker-history-dir",
        type=Path,
        action="append",
        default=[],
        help="02 历史季度目录；可重复传入，自动补齐证券宇宙和辅助交易日历范围。",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument(
        "--rules",
        type=Path,
        default=ROOT / "configs/rules/approved/rules_v2.yaml",
    )
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--observation-start", type=date.fromisoformat)
    parser.add_argument("--observation-end", type=date.fromisoformat)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)
    if not _SAFE_ID.fullmatch(args.batch_id):
        parser.error("--batch-id 只能包含字母、数字、下划线和连字符")
    if not 1 <= args.chunk_size <= 1000:
        parser.error("--chunk-size 必须在 1 至 1000 之间")
    if (args.observation_start is None) != (args.observation_end is None):
        parser.error("--observation-start 与 --observation-end 必须同时提供")
    if args.observation_start and args.observation_start > args.observation_end:
        parser.error("--observation-start 不得晚于 --observation-end")
    return args


def _discover_broker_history_files(
    directories: Iterable[str | Path],
) -> tuple[Path, ...]:
    selected: list[Path] = []
    seen: set[Path] = set()
    for value in directories:
        directory = Path(value).resolve()
        if not directory.is_dir():
            raise FileNotFoundError(f"02 历史季度目录不存在: {directory}")
        quarter_files = tuple(
            path.resolve()
            for path in sorted(directory.glob("*.csv"))
            if re.search(r"20\d{2}Q[1-4]", path.name, re.IGNORECASE)
        )
        if not quarter_files:
            raise ValueError(f"02 历史季度目录没有 YYYYQn CSV: {directory}")
        for path in quarter_files:
            if path not in seen:
                selected.append(path)
                seen.add(path)
    return tuple(selected)


def _broker_history_range(files: Iterable[str | Path]) -> tuple[date, date] | None:
    ranges = [inspect_business_date_range(path)[:2] for path in files]
    if not ranges:
        return None
    return min(item[0] for item in ranges), max(item[1] for item in ranges)


def run_real_risk_events(
    *,
    client: RealRQDataClient,
    business_file: str | Path,
    output_dir: str | Path,
    batch_id: str,
    rules_path: str | Path,
    chunk_size: int = 500,
    resume: bool = False,
    observation_start: date | None = None,
    observation_end: date | None = None,
    risk_universe_files: Iterable[str | Path] = (),
    broker_history_dirs: Iterable[str | Path] = (),
    cancel_check: Callable[[], bool] | None = None,
    historical_observation_start: date | None = None,
) -> dict[str, Any]:
    raise_if_canceled(cancel_check)
    uploaded_start, source_end, _source_row_count = inspect_business_date_range(
        business_file
    )
    source_start = date(source_end.year, 1, 1)
    # The one-click scoring report needs a continuous cross-year event window.
    # Default CLI/task behavior remains the uploaded year's start.
    if historical_observation_start is not None:
        if historical_observation_start > source_start:
            raise ValueError("跨年度风险开始日不得晚于上传年份年初")
        source_start = historical_observation_start
    if observation_start is not None and observation_start != source_start:
        raise ValueError(
            "01 观察开始日期必须等于上传文件所属年份的 1 月 1 日: "
            f"{source_start.isoformat()}"
        )
    if observation_end is not None and observation_end != source_end:
        raise ValueError(
            "观察结束日期必须等于上传文件实际最大业务日期: "
            f"{source_end.isoformat()}"
        )
    observation_start = source_start
    observation_end = source_end
    broker_history_files = _discover_broker_history_files(broker_history_dirs)
    broker_history_range = _broker_history_range(broker_history_files)
    supplemental_universe_files = tuple(
        dict.fromkeys(
            [
                *(Path(value).resolve() for value in risk_universe_files),
                *broker_history_files,
            ]
        )
    )
    configured_rules = load_approved_rules(rules_path)
    rules = load_approved_rules(
        rules_path,
        observation_start=observation_start,
        observation_end=observation_end,
    )
    batch_root = Path(output_dir) / batch_id
    if batch_root.exists() and not resume:
        raise FileExistsError(f"真实风险批次已存在: {batch_root}")
    batch_root.mkdir(parents=True, exist_ok=resume)
    performance_path = batch_root / "performance_benchmark.json"
    performance = PerformanceRecorder(
        performance_path,
        batch_id=batch_id,
        resume=resume,
    )
    input_dir = batch_root / "business_input"
    input_dir.mkdir(exist_ok=resume)
    input_copy = input_dir / "券商分类宽表.csv"
    if not input_copy.exists():
        shutil.copy2(Path(business_file), input_copy)

    raise_if_canceled(cancel_check)
    validator = DataValidator(
        ROOT / "configs/mapping/business_field_registry_v1.yaml",
        validation_pack="broker_risk_universe_v1",
    )
    with performance.stage("business_input_validation"):
        business_validation = validator.validate_directory(
            input_dir,
            observation_start=rules.observation_start,
            observation_end=rules.observation_end,
            output_dir=batch_root / "business_validation",
        )
    if not business_validation.allow_run:
        summary = {
            "status": "BLOCKED_BUSINESS_INPUT",
            "batch_id": batch_id,
            "business_validation": business_validation.to_dict(),
            "performance_benchmark_path": str(performance_path),
            "performance_attempt_id": performance.attempt_id,
        }
        performance.finish("BLOCKED", details={"reason": summary["status"]})
        _write_json(batch_root / "run_summary.json", summary, overwrite=resume)
        return summary

    with performance.stage("business_universe_assembly"):
        broker_universe, business_summary = load_business_risk_universe(
            business_file,
            observation_end=rules.observation_end,
            supplemental_source_paths=supplemental_universe_files,
        )
    raise_if_canceled(cancel_check)
    security_keys = broker_universe.loc[
        :, ["market_code", "security_code"]
    ].drop_duplicates()
    rq_codes = tuple(
        sorted(
            security_keys.loc[
                security_keys["market_code"] != "XBSE", "security_code"
            ].astype(str)
        )
    )
    with performance.stage(
        "security_universe_reconciliation",
        details={"requested_security_count": len(rq_codes)},
    ):
        reconciliation = client.reconcile_instruments(
            rq_codes,
            as_of_date=rules.observation_end.isoformat(),
            interval_start_date=rules.observation_start.isoformat(),
        )
    raise_if_canceled(cancel_check)
    raw_store = RawStore(batch_root / "raw")
    retrieved_at = datetime.now(timezone.utc)
    raw_store.save_json(
        "rqdata_real",
        "rqdata_instrument_universe_reconciliation",
        {
            "requested_order_book_ids": list(rq_codes),
            "observation_start": rules.observation_start.isoformat(),
            "observation_end": rules.observation_end.isoformat(),
            "records": list(reconciliation.records),
            "overlapping_ids": list(reconciliation.overlapping_ids),
            "non_overlapping_ids": list(reconciliation.non_overlapping_ids),
            "missing_ids": list(reconciliation.missing_ids),
        },
        received_at=retrieved_at,
    )
    preflight = {
        "requested_rq_security_count": len(rq_codes),
        "overlapping_security_count": len(reconciliation.overlapping_ids),
        "non_overlapping_security_count": len(reconciliation.non_overlapping_ids),
        "missing_security_count": len(reconciliation.missing_ids),
        "missing_security_codes": list(reconciliation.missing_ids),
    }
    _write_json(
        batch_root / "security_universe_reconciliation.json",
        preflight,
        overwrite=resume,
    )
    if reconciliation.missing_ids:
        summary = {
            "status": "BLOCKED_SECURITY_RECONCILIATION",
            "batch_id": batch_id,
            "business_universe": asdict(business_summary),
            "security_reconciliation": preflight,
            "performance_benchmark_path": str(performance_path),
            "performance_attempt_id": performance.attempt_id,
        }
        performance.finish("BLOCKED", details={"reason": summary["status"]})
        _write_json(batch_root / "run_summary.json", summary, overwrite=resume)
        return summary

    with performance.stage("st_baseline_resolution"):
        baseline_value = client.get_previous_trading_date(
            rules.observation_start.isoformat()
        )
        baseline_date = _date_text(baseline_value)
    raise_if_canceled(cancel_check)
    chunk_codes = tuple(reconciliation.overlapping_ids)
    successful_runs: list[Path] = []
    failures: list[dict[str, Any]] = []
    market_root = batch_root / "market_chunks"
    for offset in range(0, len(chunk_codes), chunk_size):
        raise_if_canceled(cancel_check)
        index = offset // chunk_size + 1
        codes = chunk_codes[offset : offset + chunk_size]
        run_id = f"{batch_id}_market_{index:04d}"
        existing = market_root / "standard" / run_id
        if resume and (existing / "run_summary.json").exists():
            with performance.stage(
                "market_chunk_reuse",
                details={
                    "chunk_index": index,
                    "security_count": len(codes),
                    "run_id": run_id,
                },
            ):
                successful_runs.append(existing)
            continue
        try:
            with performance.stage(
                "market_chunk_download",
                details={
                    "chunk_index": index,
                    "security_count": len(codes),
                    "run_id": run_id,
                },
            ):
                result = run_rqdata_market(
                    client=client,
                    repository_root=ROOT,
                    output_dir=market_root,
                    run_id=run_id,
                    order_book_ids=codes,
                    start_date=rules.observation_start.isoformat(),
                    end_date=rules.observation_end.isoformat(),
                    st_baseline_date=baseline_date,
                    retrieved_at=retrieved_at,
                )
            raise_if_canceled(cancel_check)
        except CalculationCanceled:
            raise
        except Exception as exc:
            failures.append(
                {
                    "chunk_index": index,
                    "security_count": len(codes),
                    "first_security_code": codes[0],
                    "last_security_code": codes[-1],
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            )
        else:
            successful_runs.append(result.output_dir)
        _write_json(
            batch_root / "chunk_progress.json",
            {
                "successful_chunk_count": len(successful_runs),
                "failure_count": len(failures),
                "failures": failures,
            },
            overwrite=True,
        )
        raise_if_canceled(cancel_check)
    if failures:
        summary = {
            "status": "BLOCKED_MARKET_CHUNKS",
            "batch_id": batch_id,
            "successful_chunk_count": len(successful_runs),
            "failed_chunks": failures,
            "resume_supported": True,
            "performance_benchmark_path": str(performance_path),
            "performance_attempt_id": performance.attempt_id,
        }
        performance.finish("BLOCKED", details={"reason": summary["status"]})
        _write_json(batch_root / "run_summary.json", summary, overwrite=True)
        return summary

    with performance.stage(
        "market_table_merge",
        details={"successful_chunk_count": len(successful_runs)},
    ):
        merged = merge_standard_code_runs(
            successful_runs,
            ROOT / "configs/mapping/business_field_registry_v1.yaml",
        )
    raise_if_canceled(cancel_check)
    inactive_records = [
        item
        for item in reconciliation.records
        if str(item["order_book_id"]) in set(reconciliation.non_overlapping_ids)
    ]
    if inactive_records:
        merged["证券基础状态日表"] = pd.concat(
            [
                merged["证券基础状态日表"],
                build_pit_security_status_rows(
                    inactive_records,
                    status_date=rules.observation_end.isoformat(),
                ),
            ],
            ignore_index=True,
            sort=False,
        )

    with performance.stage(
        "pit_initial_snapshot",
        details={"security_count": len(reconciliation.overlapping_ids)},
    ):
        trading_dates = client.get_trading_dates(
            rules.observation_start.isoformat(), rules.observation_end.isoformat()
        )
        first_trading_date = min(_date_text(value) for value in trading_dates)
        start_snapshot = client.instrument_snapshot(
            reconciliation.overlapping_ids,
            as_of_date=first_trading_date,
        )
    raise_if_canceled(cancel_check)
    raw_store.save_json(
        "rqdata_real",
        "rqdata_security_master_pit",
        {
            "snapshot_date": first_trading_date,
            "requested_order_book_ids": list(reconciliation.overlapping_ids),
            "data": list(start_snapshot),
        },
        received_at=retrieved_at,
    )
    merged["证券基础状态日表"] = pd.concat(
        [
            merged["证券基础状态日表"],
            build_pit_security_status_rows(
                start_snapshot,
                status_date=first_trading_date,
            ),
        ],
        ignore_index=True,
        sort=False,
    ).drop_duplicates(
        ["market_code", "security_code", "status_date", "record_version"],
        keep="last",
    )
    with performance.stage("risk_input_assembly_initial"):
        tables = build_risk_input_tables(merged, broker_universe)
        missing_pit = find_missing_pit_board_requests(
            tables,
            decimal_scale=rules.decimal_scale,
        )
    supplemental_rows: list[pd.DataFrame] = []
    supplemental_raw: list[dict[str, Any]] = []
    with performance.stage(
        "pit_supplement_snapshots",
        details={"snapshot_request_count": len(missing_pit)},
    ):
        for snapshot_date, codes in missing_pit.items():
            raise_if_canceled(cancel_check)
            records = client.instrument_snapshot(codes, as_of_date=snapshot_date)
            raise_if_canceled(cancel_check)
            supplemental_raw.append(
                {
                    "snapshot_date": snapshot_date,
                    "requested_order_book_ids": list(codes),
                    "data": list(records),
                }
            )
            supplemental_rows.append(
                build_pit_security_status_rows(records, status_date=snapshot_date)
            )
    if supplemental_raw:
        raw_store.save_json(
            "rqdata_real",
            "rqdata_security_master_pit_supplement",
            supplemental_raw,
            received_at=retrieved_at,
        )
        merged["证券基础状态日表"] = pd.concat(
            [merged["证券基础状态日表"], *supplemental_rows],
            ignore_index=True,
            sort=False,
        )
        with performance.stage("risk_input_assembly_final"):
            tables = build_risk_input_tables(merged, broker_universe)

    with performance.stage("broker_history_calendar_download"):
        calendar_start = date.fromisoformat(
            str(business_summary.min_classification_date)
        )
        calendar_end = rules.observation_end
        if broker_history_range is not None:
            calendar_start = min(calendar_start, broker_history_range[0])
            calendar_end = max(calendar_end, broker_history_range[1])
        broker_history_trading_dates = client.get_trading_dates(
            calendar_start.isoformat(),
            calendar_end.isoformat(),
        )
    raise_if_canceled(cancel_check)

    with performance.stage("risk_input_snapshot_write"):
        raise_if_canceled(cancel_check)
        input_snapshot = batch_root / "risk_input_code"
        input_snapshot.mkdir(exist_ok=resume)
        tables.trading_calendar.to_csv(
            input_snapshot / "trading_calendar.csv", index=False
        )
        tables.security_status_daily.to_csv(
            input_snapshot / "security_status_daily.csv", index=False
        )
        tables.stock_market_daily.to_csv(
            input_snapshot / "stock_market_daily.csv", index=False
        )
        tables.security_risk_status.to_csv(
            input_snapshot / "security_risk_status.csv", index=False
        )
        tables.broker_risk_classification.to_csv(
            input_snapshot / "broker_risk_universe.csv", index=False
        )
        pd.DataFrame(
            {
                "market_code": "cn",
                "calendar_date": [
                    _date_text(value) for value in broker_history_trading_dates
                ],
                "is_trading_day": True,
            }
        ).to_csv(input_snapshot / "broker_trading_calendar.csv", index=False)
    with performance.stage("risk_input_gate"):
        gate = validate_risk_inputs(
            tables,
            rules,
            prevalidation_allow_run=business_validation.allow_run,
        )
    raise_if_canceled(cancel_check)
    _write_json(
        batch_root / "risk_input_gate_result.json",
        gate.to_dict(),
        overwrite=resume,
    )
    if not gate.allow_run:
        summary = {
            "status": "BLOCKED_RISK_INPUT_GATE",
            "batch_id": batch_id,
            "business_universe": asdict(business_summary),
            "security_reconciliation": preflight,
            "gate": gate.to_dict(),
            "performance_benchmark_path": str(performance_path),
            "performance_attempt_id": performance.attempt_id,
        }
        performance.finish("BLOCKED", details={"reason": summary["status"]})
        _write_json(batch_root / "run_summary.json", summary, overwrite=True)
        return summary

    with performance.stage("risk_event_calculation"):
        result = calculate_risk_events(
            tables,
            gate,
            rules,
            calculation_batch_id=batch_id,
            upstream_snapshot_id=f"rqdata-real:{batch_id}",
        )
    raise_if_canceled(cancel_check)
    expected_result_dir = batch_root / "risk_results" / batch_id
    if resume and expected_result_dir.exists():
        with performance.stage("risk_result_reuse_verification"):
            manifest_path = expected_result_dir / "calculation_manifest.json"
            stored_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            current_manifest = result.manifest()
            legacy_window = {
                "observation_start": configured_rules.observation_start.isoformat(),
                "observation_end": configured_rules.observation_end.isoformat(),
            }
            mismatched = {}
            for key, value in current_manifest.items():
                if stored_manifest.get(key) == value:
                    continue
                if (
                    key in legacy_window
                    and key not in stored_manifest
                    and value == legacy_window[key]
                ):
                    continue
                mismatched[key] = {
                    "stored": stored_manifest.get(key),
                    "current": value,
                }
            if mismatched:
                raise ValueError(
                    "续跑计算结果与已落盘结果不一致: "
                    + json.dumps(mismatched, ensure_ascii=False, default=str)
                )
            result_dir = expected_result_dir
    else:
        with performance.stage("risk_result_write"):
            raise_if_canceled(cancel_check)
            result_dir = write_risk_event_result(
                result,
                gate,
                batch_root / "risk_results",
            )
    raise_if_canceled(cancel_check)
    pressure_rules = load_limit_down_pressure_rules(
        ROOT / "configs/rules/approved/limit_down_pressure_v1.yaml"
    )
    with performance.stage("limit_down_pressure_calculation"):
        pressure_result = calculate_limit_down_pressure_observations(
            tables,
            gate,
            rules,
            pressure_rules,
            result.events,
            calculation_batch_id=batch_id,
            upstream_snapshot_id=f"rqdata-real:{batch_id}",
        )
    raise_if_canceled(cancel_check)
    expected_pressure_dir = batch_root / "pressure_results" / batch_id
    if resume and expected_pressure_dir.exists():
        with performance.stage("limit_down_pressure_reuse_verification"):
            stored_pressure_manifest = json.loads(
                (expected_pressure_dir / "limit_down_pressure_manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            current_pressure_manifest = pressure_result.manifest()
            mismatched_pressure = {
                key: {
                    "stored": stored_pressure_manifest.get(key),
                    "current": value,
                }
                for key, value in current_pressure_manifest.items()
                if stored_pressure_manifest.get(key) != value
            }
            if mismatched_pressure:
                raise ValueError(
                    "续跑压力观察结果与已落盘结果不一致: "
                    + json.dumps(
                        mismatched_pressure, ensure_ascii=False, default=str
                    )
                )
            pressure_result_dir = expected_pressure_dir
    else:
        with performance.stage("limit_down_pressure_write"):
            pressure_result_dir = write_limit_down_pressure_result(
                pressure_result,
                batch_root / "pressure_results",
            )
    raise_if_canceled(cancel_check)
    summary = {
        "status": "SUCCEEDED",
        "batch_id": batch_id,
        "observation_start": rules.observation_start.isoformat(),
        "observation_end": rules.observation_end.isoformat(),
        "observation_range_policy": ("HISTORICAL_START_TO_UPLOADED_MAX" if historical_observation_start else "UPLOAD_YEAR_START_TO_UPLOADED_MAX"),
        "uploaded_source_min_date": uploaded_start.isoformat(),
        "uploaded_source_max_date": source_end.isoformat(),
        "business_universe": asdict(business_summary),
        "risk_universe_source_files": [
            str(value) for value in supplemental_universe_files
        ],
        "broker_history_directories": [
            str(Path(value).resolve()) for value in broker_history_dirs
        ],
        "broker_history_file_count": len(broker_history_files),
        "broker_history_range": (
            {
                "start": broker_history_range[0].isoformat(),
                "end": broker_history_range[1].isoformat(),
            }
            if broker_history_range is not None
            else None
        ),
        "broker_auxiliary_calendar_range": {
            "start": calendar_start.isoformat(),
            "end": calendar_end.isoformat(),
        },
        "security_reconciliation": preflight,
        "st_baseline_date": baseline_date,
        "successful_chunk_count": len(successful_runs),
        "pit_supplement_snapshot_count": len(supplemental_raw),
        "result_dir": str(result_dir),
        "result_manifest": result.manifest(),
        "pressure_result_dir": str(pressure_result_dir),
        "pressure_result_manifest": pressure_result.manifest(),
        "performance_benchmark_path": str(performance_path),
        "performance_attempt_id": performance.attempt_id,
    }
    with performance.stage("run_summary_write"):
        _write_json(batch_root / "run_summary.json", summary, overwrite=True)
    performance.finish("SUCCEEDED", details={"result_dir": str(result_dir)})
    user_output = _write_user_output_bundle(
        output_dir=output_dir,
        batch_id=batch_id,
        observation_start=rules.observation_start,
        observation_end=rules.observation_end,
        uploaded_start=uploaded_start,
        uploaded_end=source_end,
        result_dir=result_dir,
        pressure_result_dir=pressure_result_dir,
        performance_path=performance_path,
        event_count=result.manifest()["event_count"],
        segment_count=result.manifest()["segment_count"],
        pressure_count=pressure_result.manifest()["observation_event_count"],
        supporting_start=calendar_start,
        supporting_end=calendar_end,
    )
    summary["user_output"] = user_output
    _write_json(batch_root / "run_summary.json", summary, overwrite=True)
    return summary


def _write_user_output_bundle(
    *,
    output_dir: str | Path,
    batch_id: str,
    observation_start: date,
    observation_end: date,
    uploaded_start: date,
    uploaded_end: date,
    result_dir: str | Path,
    pressure_result_dir: str | Path,
    performance_path: str | Path,
    event_count: int,
    segment_count: int,
    pressure_count: int,
    completed_at: datetime | None = None,
    supporting_start: date | None = None,
    supporting_end: date | None = None,
) -> dict[str, Any]:
    """Collect human-facing results in one timestamped, Chinese-named folder."""
    finished = completed_at or datetime.now().astimezone()
    timestamp = finished.strftime("%Y%m%d_%H%M%S")
    quarter = f"{observation_end.year}Q{(observation_end.month - 1) // 3 + 1}"
    target = Path(output_dir) / "结果输出" / timestamp
    target.mkdir(parents=True, exist_ok=False)
    sources = {
        f"风险事件_{quarter}.csv": Path(result_dir) / "risk_events.csv",
        f"连续跌停区段_{quarter}.csv": Path(result_dir)
        / "continuous_limit_down_segments.csv",
        f"低门槛压力观察_{quarter}.csv": Path(pressure_result_dir)
        / "limit_down_pressure_observations.csv",
        f"运行性能_{quarter}.json": Path(performance_path),
    }
    for name, source in sources.items():
        shutil.copy2(source, target / name)
    technical_batch = Path(result_dir).resolve().parents[1]
    risk_input_dir = technical_batch / "risk_input_code"
    handoff_name = f"02输入清单_{quarter}.json"
    write_broker_input_manifest(
        target / handoff_name,
        batch_id=batch_id,
        observation_start=observation_start.isoformat(),
        observation_end=observation_end.isoformat(),
        files={
            "event_csv": Path(result_dir) / "risk_events.csv",
            "event_manifest": Path(result_dir) / "calculation_manifest.json",
            "calendar_csv": risk_input_dir / "broker_trading_calendar.csv",
            "st_csv": risk_input_dir / "security_risk_status.csv",
            "lifecycle_csv": risk_input_dir / "security_status_daily.csv",
            "universe_csv": risk_input_dir / "broker_risk_universe.csv",
        },
    )
    summary_name = f"运行摘要_{quarter}.json"
    concise = {
        "状态": "成功",
        "批次": batch_id,
        "季度": quarter,
        "完成时间": finished.isoformat(timespec="seconds"),
        "上传文件日期": f"{uploaded_start.isoformat()} 至 {uploaded_end.isoformat()}",
        "计算日期": f"{observation_start.isoformat()} 至 {observation_end.isoformat()}",
        "02辅助数据日期": (
            f"{(supporting_start or observation_start).isoformat()} 至 "
            f"{(supporting_end or observation_end).isoformat()}"
        ),
        "风险事件数": int(event_count),
        "连续跌停区段数": int(segment_count),
        "低门槛压力观察数": int(pressure_count),
        "输出文件": [*sources, handoff_name, summary_name],
    }
    _write_json(target / summary_name, concise)
    return {
        "status": "SUCCEEDED",
        "batch_id": batch_id,
        "quarter": quarter,
        "completed_at": concise["完成时间"],
        "uploaded_date_range": concise["上传文件日期"],
        "observation_date_range": concise["计算日期"],
        "broker_auxiliary_date_range": concise["02辅助数据日期"],
        "counts": {
            "risk_events": int(event_count),
            "continuous_limit_down_segments": int(segment_count),
            "pressure_observations": int(pressure_count),
        },
        "output_dir": str(target),
        "files": [*sources, handoff_name, summary_name],
    }


def _date_text(value: object) -> str:
    if isinstance(value, (list, tuple)) and value:
        value = value[0]
    if hasattr(value, "date") and not isinstance(value, date):
        value = value.date()
    return str(value).split("T", 1)[0].split(" ", 1)[0]


def _write_json(
    path: Path,
    payload: dict[str, Any],
    *,
    overwrite: bool = False,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if overwrite else "x"
    with path.open(mode, encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2, default=str)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    client = RealRQDataClient.from_environment()
    result = run_real_risk_events(
        client=client,
        business_file=args.business_file,
        output_dir=args.output_dir,
        batch_id=args.batch_id,
        rules_path=args.rules,
        chunk_size=args.chunk_size,
        resume=args.resume,
        observation_start=args.observation_start,
        observation_end=args.observation_end,
        risk_universe_files=args.risk_universe_file,
        broker_history_dirs=args.broker_history_dir,
    )
    print(json.dumps(result.get("user_output", result), ensure_ascii=False, indent=2))
    return 0 if result["status"] == "SUCCEEDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
