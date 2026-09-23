"""
文件作用：只读装配真实风险事件、十二季度券商历史、完整日历/ST/生命周期并流式计算券商指标。
编辑记录：
【首次生成：2026-08-12，实现不依赖 rqdatac 的真实批次输入门禁、事件明细和全市场危险档集合。】
【二次编辑内容：2026-08-12，将预警率分母限定为冻结风险批次的沪深 eligible 证券宇宙。】
【三次改进：2026-08-12，在读取大文件前拒绝已存在的指标批次，强化不可覆盖门禁。】
【四次改进：2026-08-12，按 D-068/rules v5 输出全券商×全风险事件命中明细。】
【五次改进：2026-08-12，按 D-069/rules v6 输出合并三类指标的单一中文最终汇总。】
【六次改进：2026-08-12，支持前端任务以逻辑季度名显式替换本次上传的历史文件。】
【七次改进：2026-08-14，在大文件分块、事件和观察日循环中响应任务取消。】
【八次改进：2026-08-17，支持前端单券商上传只计算文件内明确提供的券商。】
【九次改进：2026-08-18，ST 表额外包含观察期外证券时自动排除并留痕，仅在历史证券缺少 ST 状态时阻断。】
【十次改进：2026-08-18，显式历史文件允许使用新模型实际存在的连续季度窗口，不再硬编码十二季度。】
【十一次改进：2026-08-20，为市场输入、ST、历史分类、事件评价、暴露汇总和结果写入增加实时进度回调。】
【十二次改进：2026-08-31，为全行业任务新增按券商原始档位计算的 T+1 收益率表现。】
【十三次改进：2026-09-09，合并全行业与券商03单模型运行口径，固化 A-G 事件产物和历史快照标识。】
【十四次改进：2026-09-14，以 01 风险事件范围与 02 历史目录范围的交集作为实际计算范围并披露差异。】
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from contextlib import nullcontext
from datetime import date
from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterable, Mapping

from riskaudit.cancellation import raise_if_canceled

from .constants import BUSINESS_BROKER_COLUMNS
from .event_input import load_risk_events
from .metrics import (
    final_broker_metric_summary,
    single_grade_event_artifacts,
    risk_hit_rates,
    warning_days_distribution,
    warning_rates,
)
from .models import (
    BrokerMetricFinding,
)
from .rules import load_broker_mapping, load_broker_metric_rules
from .return_performance import (
    PRICE_ADJUSTMENT,
    PRICE_RETURN_FORMULA,
    UNADJUSTED_PRICE_ADJUSTMENT,
    UNADJUSTED_RETURN_FORMULA,
    calculate_broker_grade_returns,
)
from .storage import write_broker_metric_result
from .real_run_support import (
    _file_summary,
    _report_progress,
)
from .real_run_inputs import (
    CompleteStStates,
    _load_calendar,
    _load_lifecycle,
    _load_metric_universe,
    _load_security_names,
    _load_snapshot_st_states,
    _load_st_states,
    _read_history,
    _reconcile_history_st_securities,
    _resolve_history_sources,
)
from .real_run_assessment import (
    _all_broker_risk_event_hits,
    _assessment_findings,
    _assess_events_from_wide_history,
    _build_streaming_exposures,
    _reconcile,
)
from .user_output import write_broker_user_output_bundle

__all__ = [
    "CompleteStStates",
    "RealBrokerMetricRunResult",
    "_all_broker_risk_event_hits",
    "_build_streaming_exposures",
    "_load_calendar",
    "_load_lifecycle",
    "_load_metric_universe",
    "_load_security_names",
    "_load_snapshot_st_states",
    "_load_st_states",
    "_read_history",
    "_reconcile_history_st_securities",
    "_resolve_history_sources",
    "run_real_broker_metrics",
]


EXPECTED_HISTORY_COLUMNS = ("biz_date", "stk_code", *BUSINESS_BROKER_COLUMNS)


@dataclass(frozen=True)
class RealBrokerMetricRunResult:
    output_dir: Path
    assessment_count: int
    history_row_count: int
    weekend_row_count: int
    exposure_count: int
    user_output: Mapping[str, Any]


def _derive_history_snapshot_id(history_files: Iterable[Mapping[str, object]]) -> str:
    """Build a path-independent identity from ordered logical names and hashes."""
    payload = "\n".join(
        f"{item['logical_file_name']}:{item['sha256']}" for item in history_files
    )
    return "sha256:" + sha256(payload.encode("utf-8")).hexdigest()


def run_real_broker_metrics(
    *,
    event_csv: str | Path,
    event_manifest: str | Path,
    history_dir: str | Path,
    calendar_csv: str | Path,
    st_csv: str | Path,
    lifecycle_csv: str | Path,
    universe_csv: str | Path,
    output_root: str | Path,
    metric_batch_id: str,
    rules_path: str | Path,
    mapping_path: str | Path,
    observation_start: date,
    observation_end: date,
    read_chunksize: int = 100_000,
    supersedes_metric_batch_id: str = "",
    history_files: Mapping[str, str | Path] | None = None,
    selected_broker_ids: Iterable[str] | None = None,
    allow_incomplete_history: bool = False,
    cancel_check: Callable[[], bool] | None = None,
    progress_callback: Callable[[str, Mapping[str, object]], None] | None = None,
    market_price_csv: str | Path | None = None,
    market_price_adjustment: str | None = None,
    market_return_formula: str | None = None,
    run_scope: str = "peer-benchmark",
    history_snapshot_id: str | None = None,
    observation_range_context: Mapping[str, Any] | None = None,
    compact_output: bool = False,
) -> RealBrokerMetricRunResult:
    raise_if_canceled(cancel_check)
    final_dir = Path(output_root) / metric_batch_id
    if final_dir.exists():
        raise FileExistsError(f"Broker metric batch already exists: {final_dir}")
    if compact_output:
        for previous in Path(output_root).glob("结果输出/*/核对材料/broker_metric_manifest.json"):
            if json.loads(previous.read_text(encoding="utf-8")).get("metric_batch_id") == metric_batch_id:
                raise FileExistsError(f"Broker metric batch already exists: {previous.parent.parent}")
    rules = load_broker_metric_rules(rules_path)
    mapping = load_broker_mapping(mapping_path)
    if rules.mapping_version != mapping.mapping_version:
        raise ValueError(
            "BM302_MAPPING_VERSION_MISMATCH: "
            f"rules require {rules.mapping_version}, got {mapping.mapping_version}"
        )
    if run_scope not in {"peer-benchmark", "single-model"}:
        raise ValueError(f"BM403_RUN_SCOPE_INVALID: {run_scope}")
    broker_ids = (
        tuple(dict.fromkeys(str(item) for item in selected_broker_ids))
        if selected_broker_ids is not None
        else tuple(BUSINESS_BROKER_COLUMNS)
    )
    if not broker_ids:
        raise ValueError("BM102_HISTORY_SCHEMA_MISMATCH: no selected broker")
    unsupported = set(broker_ids) - set(BUSINESS_BROKER_COLUMNS)
    if unsupported or not set(broker_ids) <= set(mapping.broker_ids):
        raise ValueError("BM102_HISTORY_SCHEMA_MISMATCH: mapping broker set")

    _report_progress(progress_callback, "broker_metric_market_input")
    source_events = load_risk_events(event_csv, event_manifest)
    events = tuple(
        item
        for item in source_events
        if observation_start <= item.first_fact_date <= observation_end
    )
    raise_if_canceled(cancel_check)
    event_securities = {(item.market_code, item.security_code) for item in events}
    calendar, calendar_dates, calendar_snapshot_id = _load_calendar(
        calendar_csv, observation_start, observation_end
    )
    lifecycle = _load_lifecycle(lifecycle_csv)
    _report_progress(progress_callback, "broker_metric_st_load")
    pre_observation_dates = tuple(
        value for value in calendar_dates if value < observation_start
    )
    st_coverage_start = (
        pre_observation_dates[-1] if pre_observation_dates else observation_start
    )
    st_states, st_securities = _load_st_states(
        st_csv,
        calendar_dates,
        observation_end,
        start=st_coverage_start,
        lifecycle=lifecycle,
        cancel_check=cancel_check,
    )
    raise_if_canceled(cancel_check)
    security_names = _load_security_names(lifecycle_csv)
    metric_universe = _load_metric_universe(universe_csv, observation_end)
    quarter_files = _resolve_history_sources(history_dir, history_files)
    _report_progress(
        progress_callback,
        "broker_metric_history_load",
        quarter_count=len(quarter_files),
    )
    history = _read_history(
        quarter_files,
        broker_ids,
        event_securities,
        calendar,
        observation_start,
        observation_end,
        read_chunksize,
        require_end_coverage=True,
        cancel_check=cancel_check,
    )
    raise_if_canceled(cancel_check)
    securities = history["securities"]
    st_security_findings = _reconcile_history_st_securities(
        securities,
        st_securities,
        lifecycle,
        observation_start,
        observation_end,
        allow_incomplete_history=allow_incomplete_history,
    )
    missing_lifecycle = securities - set(lifecycle)
    if missing_lifecycle:
        raise ValueError(
            f"BM204_LIFECYCLE_EVIDENCE_MISSING: {sorted(missing_lifecycle)[:10]}"
        )
    missing_event_history = event_securities - securities
    if missing_event_history and not allow_incomplete_history:
        raise ValueError(
            f"BM101_HISTORY_FILE_SET_MISMATCH: {sorted(missing_event_history)}"
        )
    if not event_securities <= metric_universe:
        raise ValueError("BM001_EVENT_MANIFEST_MISMATCH: event outside metric universe")

    _report_progress(
        progress_callback,
        "broker_metric_event_assessment",
        event_count=len(events),
        broker_count=len(broker_ids),
    )
    assessments = _assess_events_from_wide_history(
        events,
        broker_ids,
        history["event_rows"],
        st_states,
        calendar,
        mapping,
        rules,
        metric_batch_id,
        calendar_snapshot_id,
        missing_history_securities=missing_event_history,
        cancel_check=cancel_check,
    )
    snapshot_dates = tuple(
        value
        for value in calendar_dates
        if observation_start <= value <= observation_end
    )
    _report_progress(progress_callback, "broker_metric_exposure")
    exposures, unknown_counts, period_unknown = _build_streaming_exposures(
        history["daily_rows"],
        broker_ids,
        metric_universe,
        snapshot_dates,
        st_states,
        lifecycle,
        mapping,
        history["initial_rows"],
        missing_history_securities=(
            metric_universe - securities if allow_incomplete_history else set()
        ),
        cancel_check=cancel_check,
    )
    raise_if_canceled(cancel_check)
    _report_progress(progress_callback, "broker_metric_summary")
    warning_days_summary = warning_days_distribution(
        assessments,
        decimal_scale=rules.decimal_scale,
        metric_rule_version=rules.rule_version,
        metric_batch_id=metric_batch_id,
    )
    hit_rate_summary = risk_hit_rates(
        assessments,
        metric_rule_version=rules.rule_version,
        metric_batch_id=metric_batch_id,
    )
    warning_rate_summary = warning_rates(
        exposures,
        events,
        broker_ids,
        metric_rule_version=rules.rule_version,
        metric_batch_id=metric_batch_id,
        unknown_counts=unknown_counts,
        snapshot_dates=snapshot_dates,
        period_unknown_counts=period_unknown,
    )
    final_summary = final_broker_metric_summary(
        warning_days_summary, hit_rate_summary, warning_rate_summary
    )
    single_grade_summary = None
    single_grade_detail = None
    if run_scope == "single-model":
        if broker_ids != ("券商03",):
            raise ValueError(
                "BM403_RUN_SCOPE_INVALID: single-model only supports 券商03"
            )
        single_grade_summary, single_grade_detail = single_grade_event_artifacts(
            assessments,
            decimal_scale=rules.decimal_scale,
            metric_rule_version=rules.rule_version,
            metric_batch_id=metric_batch_id,
        )
    grade_return_summary = None
    grade_return_daily = None
    grade_return_quality = None
    if market_price_csv is not None:
        adjusted_input = "后复权" in Path(market_price_csv).name
        effective_price_adjustment = market_price_adjustment or (
            PRICE_ADJUSTMENT if adjusted_input else UNADJUSTED_PRICE_ADJUSTMENT
        )
        effective_return_formula = market_return_formula or (
            PRICE_RETURN_FORMULA if adjusted_input else UNADJUSTED_RETURN_FORMULA
        )
        _report_progress(progress_callback, "broker_metric_return_performance")
        (
            grade_return_summary,
            grade_return_daily,
            grade_return_quality,
        ) = calculate_broker_grade_returns(
            market_price_csv=market_price_csv,
            daily_rows=history["daily_rows"],
            initial_rows=history["initial_rows"],
            broker_ids=broker_ids,
            securities=securities,
            snapshot_dates=snapshot_dates,
            lifecycle=lifecycle,
            price_adjustment=effective_price_adjustment,
            return_formula=effective_return_formula,
            cancel_check=cancel_check,
        )
    raise_if_canceled(cancel_check)
    findings = list(history["findings"])
    findings.extend(st_security_findings)
    findings.extend(_assessment_findings(assessments))
    if grade_return_quality is not None:
        if grade_return_quality["price_adjustment"] != PRICE_ADJUSTMENT:
            findings.append(
                BrokerMetricFinding(
                    "BM505_GRADE_RETURN_INPUT_NOT_POST_ADJUSTED",
                    "WARNING",
                    "Grade-return input is not the D-084 RQData post-adjusted price series; retain only as legacy audit comparison",
                    source_file=str(market_price_csv),
                )
            )
        extreme_count = int(
            grade_return_quality["extreme_absolute_return_over_30pct_count"]
        )
        if extreme_count:
            findings.append(
                BrokerMetricFinding(
                    "BM503_RETURN_EXTREMES_PRESENT",
                    "WARNING",
                    (
                        f"Grade returns contain {extreme_count} stock-days with "
                        "absolute return above 30%; retain without winsorization"
                    ),
                    source_file=str(market_price_csv),
                )
            )
        missing_return_brokers = grade_return_quality["broker_without_return"]
        if missing_return_brokers:
            findings.append(
                BrokerMetricFinding(
                    "BM504_BROKER_RETURN_CLASSIFICATION_EMPTY",
                    "INFO",
                    (
                        "No nonblank original classification was available for return "
                        "analysis: " + ", ".join(missing_return_brokers)
                    ),
                    source_file="broker classification history",
                )
            )
    history_file_summaries = [
        _file_summary(path, logical_file_name=name, **history["file_stats"][name])
        for name, path in quarter_files
    ]
    derived_history_snapshot_id = _derive_history_snapshot_id(
        history_file_summaries
    )
    input_manifest = {
        "status": "VALIDATED",
        "run_scope": run_scope,
        "history_snapshot_id": history_snapshot_id or derived_history_snapshot_id,
        "event_input": _file_summary(
            event_csv,
            source_event_count=len(source_events),
            selected_event_count=len(events),
            selection_date_field="first_fact_date",
        ),
        "event_manifest": _file_summary(event_manifest),
        "history_files": history_file_summaries,
        "calendar_input": _file_summary(
            calendar_csv, trading_date_count=len(calendar_dates)
        ),
        "st_input": _file_summary(
            st_csv, security_count=len(st_securities), key_count=st_states.key_count
        ),
        "lifecycle_input": _file_summary(lifecycle_csv, security_count=len(lifecycle)),
        "metric_universe_input": _file_summary(
            universe_csv,
            security_count=len(metric_universe),
            universe_definition="FROZEN_EVENT_BATCH_ELIGIBLE_HS_SECURITY_UNION",
        ),
        "rules_input": _file_summary(rules_path, rule_version=rules.rule_version),
        "mapping_input": _file_summary(
            mapping_path, mapping_version=mapping.mapping_version
        ),
        "selected_broker_ids": list(broker_ids),
        "history_total_row_count": history["row_count"],
        "history_unique_key_count": history["unique_key_count"],
        "history_security_count": len(securities),
        "allow_incomplete_history": allow_incomplete_history,
        "missing_event_history_security_count": len(missing_event_history),
        "missing_event_history_securities": [
            f"{market}|{security}"
            for market, security in sorted(missing_event_history)
        ],
        "history_st_outside_observation_security_count": len(
            st_security_findings
        ),
        "metric_universe_security_count": len(metric_universe),
        "ignored_weekend_row_count": history["weekend_row_count"],
        "ignored_weekend_eligible_market_row_count": history[
            "weekend_eligible_row_count"
        ],
        "ignored_weekend_nonblank_broker_cell_count": history["weekend_nonblank_count"],
        "ignored_weekend_dates": sorted(
            value.isoformat() for value in history["weekend_dates"]
        ),
        "calendar_snapshot_id": calendar_snapshot_id,
        "observation_start": observation_start.isoformat(),
        "observation_end": observation_end.isoformat(),
        "event_scope_policy": "INTERSECT_01_EVENT_AND_HISTORY_DIRECTORY_RANGE",
        "observation_range_context": dict(observation_range_context or {}),
    }
    if market_price_csv is not None:
        input_manifest["market_price_input"] = _file_summary(
            market_price_csv,
            return_formula=effective_return_formula,
            classification_timing="T-1日收盘档位评价T日收益",
            price_adjustment=effective_price_adjustment,
        )
    metric_manifest = {
        "run_scope": run_scope,
        "history_snapshot_id": history_snapshot_id or derived_history_snapshot_id,
        "event_batch_id": source_events[0].event_batch_id if source_events else "",
        "source_event_count": len(source_events),
        "excluded_out_of_range_event_count": len(source_events) - len(events),
        "event_scope_policy": "INTERSECT_01_EVENT_AND_HISTORY_DIRECTORY_RANGE",
        "observation_start": observation_start.isoformat(),
        "observation_end": observation_end.isoformat(),
        "observation_range_context": dict(observation_range_context or {}),
        "event_count": len(events),
        "distinct_risk_security_count": len(event_securities),
        "broker_count": len(broker_ids),
        "expected_assessment_count": len(events) * len(broker_ids),
        "warning_days_summary_count": len(warning_days_summary),
        "hit_rate_summary_count": len(hit_rate_summary),
        "warning_rate_summary_count": len(warning_rate_summary),
        "final_summary_count": len(final_summary),
        "dangerous_exposure_count": len(exposures),
        "finding_count": len(findings),
        "rule_version": rules.rule_version,
        "rule_sha256": rules.rule_sha256,
        "mapping_version": mapping.mapping_version,
        "real_data_run": True,
        "rqdata_accessed": False,
        "supersedes_metric_batch_id": supersedes_metric_batch_id,
        "output_schema_version": rules.csv_schema,
        "grade_return_summary_count": len(grade_return_summary or ()),
        "grade_return_daily_count": len(grade_return_daily or ()),
        "grade_return_quality": grade_return_quality,
        "single_grade_summary_count": len(single_grade_summary or ()),
        "single_grade_detail_count": len(single_grade_detail or ()),
    }
    _reconcile(
        events,
        broker_ids,
        assessments,
        warning_days_summary,
        hit_rate_summary,
        warning_rate_summary,
        snapshot_dates,
    )
    all_broker_hits = (
        _all_broker_risk_event_hits(assessments, security_names)
        if rules.rule_version in {"v5", "v6"}
        else None
    )
    if all_broker_hits is not None and len(all_broker_hits) != len(assessments):
        raise ValueError(
            "BM402_SUMMARY_RECONCILIATION_FAILED: all broker hit detail count"
        )
    _report_progress(progress_callback, "broker_metric_result_write")
    # 独立02任务仅交付一套中文CSV。技术格式仅在临时目录中用于写出与校验。
    with (TemporaryDirectory(prefix="riskaudit_broker_") if compact_output
          else nullcontext(output_root)) as staging_root:
        output_dir = write_broker_metric_result(
            staging_root,
            metric_batch_id,
            assessments,
            warning_days_summary,
            hit_rate_summary,
            warning_rate_summary,
            findings,
            input_manifest,
            metric_manifest,
            all_broker_risk_event_hits=all_broker_hits,
            final_summary=final_summary if rules.rule_version == "v6" else None,
            grade_return_summary=grade_return_summary,
            grade_return_daily=grade_return_daily,
            single_grade_summary=single_grade_summary,
            single_grade_detail=single_grade_detail,
        )
        raise_if_canceled(cancel_check)
        user_output = write_broker_user_output_bundle(
            output_root=output_root,
            technical_output_dir=output_dir,
            metric_batch_id=metric_batch_id,
            run_scope=run_scope,
            observation_start=observation_start,
            observation_end=observation_end,
            observation_range_context=observation_range_context,
            preserve_manifests=compact_output,
        )
        if compact_output:
            output_dir = Path(user_output["output_dir"])
    return RealBrokerMetricRunResult(
        output_dir=output_dir,
        assessment_count=len(assessments),
        history_row_count=history["row_count"],
        weekend_row_count=history["weekend_row_count"],
        exposure_count=len(exposures),
        user_output=user_output,
    )
