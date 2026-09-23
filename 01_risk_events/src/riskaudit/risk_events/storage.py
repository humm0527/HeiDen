"""
文件作用：将风险专用门禁和事件算法结果原子写入不可覆盖的批次审计目录。
编辑记录：
【首次生成：2026-08-11，输出证券集合、排除项、连续段、事件、发现项和计算 manifest。】
【第二次编辑：2026-08-11，在结果存储边界将连续跌停段和风险事件 CSV 字段稳定映射为中文并登记模式版本。】
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import tempfile

import pandas as pd

from .models import RiskEventCalculationResult, RiskInputGateResult


SEGMENT_OUTPUT_FIELDS = {
    "market_code": "交易市场代码",
    "security_code": "证券代码",
    "segment_number": "连续段序号",
    "event_type": "事件类型",
    "event_source": "事件来源",
    "board_code": "板块代码",
    "applicable_threshold": "适用连续跌停门槛",
    "consecutive_length": "连续跌停交易日数",
    "first_fact_date": "首次事实日期",
    "threshold_reached_date": "达到门槛日期",
    "last_fact_date": "最后事实日期",
    "threshold_reached": "是否达到门槛",
    "open_at_observation_end": "观察期结束时是否未闭合",
    "rule_version": "风险规则版本号",
    "calculation_batch_id": "计算批次标识",
    "upstream_snapshot_id": "上游数据快照标识",
    "source_record_ids": "来源记录标识列表",
}
EVENT_OUTPUT_FIELDS = {
    "market_code": "交易市场代码",
    "security_code": "证券代码",
    "event_type": "事件类型",
    "event_source": "事件来源",
    "first_fact_date": "首次事实日期",
    "risk_date": "风险认定日期",
    "last_fact_date": "最后事实日期",
    "threshold_reached_date": "达到门槛日期",
    "segment_number": "连续段序号",
    "consecutive_length": "连续跌停交易日数",
    "previous_st_state": "前一ST状态",
    "current_st_state": "当前ST状态",
    "rule_version": "风险规则版本号",
    "calculation_batch_id": "计算批次标识",
    "upstream_snapshot_id": "上游数据快照标识",
    "source_record_ids": "来源记录标识列表",
}


def write_risk_event_result(
    result: RiskEventCalculationResult,
    gate: RiskInputGateResult,
    output_root: str | Path,
) -> Path:
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    final_dir = root / result.calculation_batch_id
    if final_dir.exists():
        raise FileExistsError(f"Risk calculation batch already exists: {final_dir}")
    staging = Path(
        tempfile.mkdtemp(prefix=f".{result.calculation_batch_id}.", dir=root)
    )
    try:
        _write_json(staging / "risk_input_gate_result.json", gate.to_dict())
        _universe_frame(gate).to_csv(
            staging / "risk_universe.csv", index=False, encoding="utf-8-sig"
        )
        _exclusion_frame(gate).to_csv(
            staging / "risk_universe_exclusions.csv",
            index=False,
            encoding="utf-8-sig",
        )
        result.segment_frame().rename(columns=SEGMENT_OUTPUT_FIELDS).to_csv(
            staging / "continuous_limit_down_segments.csv",
            index=False,
            encoding="utf-8-sig",
        )
        result.event_frame().rename(columns=EVENT_OUTPUT_FIELDS).to_csv(
            staging / "risk_events.csv", index=False, encoding="utf-8-sig"
        )
        pd.DataFrame(
            [item.to_dict() for item in gate.findings],
            columns=[
                "code",
                "severity",
                "message",
                "market_code",
                "security_code",
                "fact_date",
            ],
        ).to_csv(
            staging / "calculation_findings.csv",
            index=False,
            encoding="utf-8-sig",
        )
        manifest = result.manifest() | {
            "output_schema_version": "risk_event_csv_cn_v1",
            "gate_allow_run": gate.allow_run,
            "raw_business_universe_count": len(gate.raw_business_universe),
            "eligible_universe_count": len(gate.eligible_universe),
            "excluded_security_count": len(gate.excluded_securities),
        }
        _write_json(staging / "calculation_manifest.json", manifest)
        os.replace(staging, final_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return final_dir


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _universe_frame(gate: RiskInputGateResult) -> pd.DataFrame:
    rows = []
    eligible = set(gate.eligible_universe)
    for key in gate.raw_business_universe:
        rows.append(
            {
                "market_code": key[0],
                "security_code": key[1],
                "eligibility": "ELIGIBLE" if key in eligible else "EXCLUDED",
                "exclusion_reason": gate.excluded_securities.get(key, ""),
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "market_code",
            "security_code",
            "eligibility",
            "exclusion_reason",
        ],
    )


def _exclusion_frame(gate: RiskInputGateResult) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "market_code": key[0],
                "security_code": key[1],
                "exclusion_reason": reason,
            }
            for key, reason in sorted(gate.excluded_securities.items())
        ],
        columns=["market_code", "security_code", "exclusion_reason"],
    )
