"""Dispatch portable JSON task specifications to independent domain modules."""

from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
import re
from typing import Any, Callable

from .catalog import TASK_CATALOG
from .broker_handoff import apply_broker_input_manifest
from .config import PortableTaskConfig
from .model_comparison import run_model_comparison


def _as_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _required(parameters: dict[str, Any], *names: str) -> None:
    missing = [name for name in names if parameters.get(name) in (None, "", (), {})]
    if missing:
        raise ValueError("任务缺少必需参数: " + ", ".join(missing))


def _uploaded_observation_range(
    uploaded_file: str | Path,
    observation_start: str | date | None = None,
    observation_end: str | date | None = None,
) -> tuple[date, date]:
    from riskaudit.business_files import inspect_business_date_range

    source_start, source_end, _row_count = inspect_business_date_range(uploaded_file)
    if observation_start not in (None, "") and _as_date(observation_start) != source_start:
        raise ValueError(
            "观察开始日期必须等于上传文件实际最小业务日期: "
            f"{source_start.isoformat()}"
        )
    if observation_end not in (None, "") and _as_date(observation_end) != source_end:
        raise ValueError(
            "观察结束日期必须等于上传文件实际最大业务日期: "
            f"{source_end.isoformat()}"
        )
    return source_start, source_end


def _risk_observation_range(
    uploaded_file: str | Path,
    observation_start: str | date | None = None,
    observation_end: str | date | None = None,
) -> tuple[date, date]:
    from riskaudit.business_files import inspect_business_date_range

    _source_start, source_end, _row_count = inspect_business_date_range(uploaded_file)
    risk_start = date(source_end.year, 1, 1)
    if observation_start not in (None, "") and _as_date(observation_start) != risk_start:
        raise ValueError(
            "01 观察开始日期必须等于上传文件所属年份的 1 月 1 日: "
            f"{risk_start.isoformat()}"
        )
    if observation_end not in (None, "") and _as_date(observation_end) != source_end:
        raise ValueError(
            "01 观察结束日期必须等于上传文件实际最大业务日期: "
            f"{source_end.isoformat()}"
        )
    return risk_start, source_end


def _require_event_range(
    manifest_path: str | Path, observation_start: date, observation_end: date
) -> None:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    actual = (
        str(manifest.get("observation_start") or ""),
        str(manifest.get("observation_end") or ""),
    )
    expected = (observation_start.isoformat(), observation_end.isoformat())
    try:
        actual_dates = tuple(date.fromisoformat(value) for value in actual)
    except ValueError as exc:
        raise ValueError("风险事件 manifest 缺少有效观察日期") from exc
    if actual_dates[0] > observation_start or actual_dates[1] < observation_end:
        raise ValueError(
            "风险事件批次未完整覆盖本次上传文件日期范围；请先按 01 口径重跑。"
            f"事件批次={actual[0]}—{actual[1]}，02 上传范围={expected[0]}—{expected[1]}"
        )


def _infer_history_files(
    history_dir: str | Path,
    uploaded_history_file: str | Path,
) -> dict[str, Path]:
    """Map human-named quarterly files to task-02 logical quarter names."""
    directory = Path(history_dir)
    uploaded = Path(uploaded_history_file).resolve()
    source_start, source_end = _uploaded_observation_range(uploaded)
    source_quarter = (source_end.month - 1) // 3 + 1
    if source_start.year != source_end.year or (source_start.month - 1) // 3 + 1 != source_quarter:
        raise ValueError("uploaded_history_file 必须属于同一自然季度")
    selected: dict[str, Path] = {}
    for path in sorted(directory.glob("*.csv")):
        match = re.search(r"(20\d{2})Q([1-4])", path.name, re.IGNORECASE)
        if match is None:
            continue
        logical = f"集中度{match.group(1)}Q{match.group(2)}.csv"
        existing = selected.get(logical)
        if existing is not None:
            if existing == uploaded:
                continue
            if path.resolve() != uploaded:
                raise ValueError(f"同一季度存在多个历史文件: {logical}")
        selected[logical] = path.resolve()
    uploaded_logical = f"集中度{source_end.year}Q{source_quarter}.csv"
    selected[uploaded_logical] = uploaded
    if not selected:
        raise ValueError("上传文件所在目录未找到季度历史文件")
    return dict(sorted(selected.items()))


def _run_validation(parameters: dict[str, Any], project_root: Path) -> dict[str, Any]:
    from riskaudit.validation import DataValidator, write_validation_reports

    _required(parameters, "input_dir", "output_dir", "observation_start", "observation_end")
    registry = parameters.get("registry_path") or (
        project_root / "configs/mapping/business_field_registry_v1.yaml"
    )
    validator = DataValidator(
        registry, validation_pack=parameters.get("validation_pack", "riskaudit_full_v1")
    )
    result = validator.validate_directory(
        parameters["input_dir"],
        observation_start=parameters["observation_start"],
        observation_end=parameters["observation_end"],
    )
    json_path, xlsx_path = write_validation_reports(result, parameters["output_dir"])
    return {
        "status": result.overall_status,
        "allow_run": result.allow_run,
        "json_path": str(json_path),
        "xlsx_path": str(xlsx_path),
    }


def _run_risk_events(parameters: dict[str, Any], project_root: Path) -> dict[str, Any]:
    from riskaudit.market_foundation.risk_shadow import run_snapshot_risk_calculation

    _required(
        parameters,
        "market_lake_root",
        "snapshot_id",
        "business_file",
        "output_root",
        "batch_id",
    )
    observation_start, observation_end = _risk_observation_range(
        parameters["business_file"],
        parameters.get("observation_start"),
        parameters.get("observation_end"),
    )
    result = run_snapshot_risk_calculation(
        repository_root=project_root,
        market_lake_root=parameters["market_lake_root"],
        snapshot_id=str(parameters["snapshot_id"]),
        business_file=parameters["business_file"],
        output_root=parameters["output_root"],
        batch_id=str(parameters["batch_id"]),
        rules_path=parameters.get("rules_path")
        or project_root / "configs/rules/approved/rules_v2.yaml",
        observation_start=observation_start,
        observation_end=observation_end,
        risk_universe_files=parameters.get("risk_universe_files", ()),
        lifecycle_reference_path=parameters.get("lifecycle_reference_path"),
    )
    return result


def _run_broker(parameters: dict[str, Any], *, peer: bool, project_root: Path) -> dict[str, Any]:
    from riskaudit.broker_metrics.real_run import run_real_broker_metrics

    parameters = apply_broker_input_manifest(parameters)
    if "broker_ids" in parameters:
        raise ValueError("券商评价任务不支持 broker_ids；全行业不传券商，单模型只传 broker_id=券商03")
    broker_id = str(parameters.get("broker_id") or "券商03")
    if not peer and broker_id != "券商03":
        raise ValueError("single-model 仅支持 broker_id=券商03")
    selected = None if peer else (broker_id,)
    fixed_rules_path = project_root / "configs/rules/approved/rules_v6.yaml"
    fixed_mapping_path = project_root / "configs/broker_mapping/broker_mapping_v3.yaml"
    if parameters.get("rules_path") not in (None, fixed_rules_path):
        raise ValueError("券商评价任务固定使用 rules_v6.yaml，不支持运行时替换")
    if parameters.get("mapping_path") not in (None, fixed_mapping_path):
        raise ValueError("券商评价任务固定使用 broker_mapping_v3.yaml，不支持运行时替换")
    _required(parameters, "uploaded_history_file")
    uploaded_file = Path(parameters["uploaded_history_file"])
    parameters.setdefault("history_dir", uploaded_file.parent)
    if parameters.get("history_files") in (None, "", {}):
        parameters["history_files"] = _infer_history_files(
            parameters["history_dir"], uploaded_file
        )
    parameters.setdefault("output_root", project_root / "data" / "outputs" / "broker_metrics")
    if parameters.get("batch_id") in (None, ""):
        mode = "peer" if peer else "single"
        timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S_%f")
        parameters["batch_id"] = f"broker_{mode}_{timestamp}"
    _required(
        parameters,
        "event_csv",
        "event_manifest",
        "history_dir",
        "calendar_csv",
        "st_csv",
        "lifecycle_csv",
        "universe_csv",
        "output_root",
        "batch_id",
    )
    observation_start, observation_end = _uploaded_observation_range(
        parameters["uploaded_history_file"],
        parameters.get("observation_start"),
        parameters.get("observation_end"),
    )
    _require_event_range(parameters["event_manifest"], observation_start, observation_end)
    result = run_real_broker_metrics(
        event_csv=parameters["event_csv"],
        event_manifest=parameters["event_manifest"],
        history_dir=parameters["history_dir"],
        calendar_csv=parameters["calendar_csv"],
        st_csv=parameters["st_csv"],
        lifecycle_csv=parameters["lifecycle_csv"],
        universe_csv=parameters["universe_csv"],
        output_root=parameters["output_root"],
        metric_batch_id=str(parameters["batch_id"]),
        rules_path=fixed_rules_path,
        mapping_path=fixed_mapping_path,
        observation_start=observation_start,
        observation_end=observation_end,
        history_files=parameters.get("history_files"),
        selected_broker_ids=selected,
        allow_incomplete_history=bool(parameters.get("allow_incomplete_history", not peer)),
        read_chunksize=int(parameters.get("read_chunksize", 100_000)),
        market_price_csv=parameters.get("market_price_csv"),
        market_price_adjustment=parameters.get("market_price_adjustment"),
        market_return_formula=parameters.get("market_return_formula"),
        run_scope="peer-benchmark" if peer else "single-model",
        history_snapshot_id=parameters.get("history_snapshot_id"),
    )
    return dict(result.user_output)


def _run_pressure(parameters: dict[str, Any], project_root: Path) -> dict[str, Any]:
    from riskaudit.broker_metrics import run_pressure_classification

    _required(
        parameters,
        "observation_csv",
        "history_dir",
        "calendar_csv",
        "st_csv",
        "output_root",
        "batch_id",
        "event_batch_id",
        "observation_start",
        "observation_end",
    )
    result = run_pressure_classification(
        observation_csv=parameters["observation_csv"],
        history_dir=parameters["history_dir"],
        calendar_csv=parameters["calendar_csv"],
        st_csv=parameters["st_csv"],
        output_root=parameters["output_root"],
        metric_batch_id=str(parameters["batch_id"]),
        event_batch_id=str(parameters["event_batch_id"]),
        rules_path=parameters.get("rules_path")
        or project_root / "configs/rules/approved/rules_v6.yaml",
        mapping_path=parameters.get("mapping_path")
        or project_root / "configs/broker_mapping/broker_mapping_v3.yaml",
        observation_start=_as_date(parameters["observation_start"]),
        observation_end=_as_date(parameters["observation_end"]),
        history_files=parameters.get("history_files"),
        read_chunksize=int(parameters.get("read_chunksize", 100_000)),
    )
    return {
        "status": "SUCCEEDED",
        "output_dir": str(result.output_dir),
        "assessment_count": result.assessment_count,
    }


def _run_descriptive(parameters: dict[str, Any], project_root: Path) -> dict[str, Any]:
    from riskaudit.broker_metrics.descriptive_tables import build_model_descriptive_tables

    _required(
        parameters,
        "output_dir",
        "observation_start",
        "observation_end",
        "new_history_files",
        "legacy_history_files",
    )
    return build_model_descriptive_tables(**parameters)


def _run_period_report(parameters: dict[str, Any], project_root: Path) -> dict[str, Any]:
    from riskaudit.broker_metrics.period_report import build_period_report, write_period_report

    _required(parameters, "event_csv", "event_manifest", "models", "start", "end", "output_dir")
    report = build_period_report(
        event_csv=parameters["event_csv"],
        event_manifest=parameters["event_manifest"],
        models=parameters["models"],
        start=str(parameters["start"]),
        end=str(parameters["end"]),
    )
    write_period_report(parameters["output_dir"], report)
    return {"status": "SUCCEEDED", "output_dir": str(parameters["output_dir"])}


_RUNNERS: dict[str, Callable[[dict[str, Any], Path], dict[str, Any]]] = {
    "validate-input": _run_validation,
    "risk-events": _run_risk_events,
    "peer-benchmark": lambda params, root: _run_broker(params, peer=True, project_root=root),
    "single-model": lambda params, root: _run_broker(params, peer=False, project_root=root),
    "pressure-observation": _run_pressure,
    "model-comparison": lambda params, root: run_model_comparison(**params),
    "descriptive-comparison": _run_descriptive,
    "period-report": _run_period_report,
}


def run_configured_task(config: PortableTaskConfig) -> dict[str, Any]:
    if config.task_type not in TASK_CATALOG or config.task_type not in _RUNNERS:
        raise ValueError(f"不支持的任务类型: {config.task_type}")
    return _RUNNERS[config.task_type](
        config.resolved_parameters(), config.project_root
    )
