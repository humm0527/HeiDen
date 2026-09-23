"""
文件作用：按 broker_metric_csv_cn_v1 原子发布中文券商指标 CSV 与不可覆盖 manifest。
编辑记录：
【首次生成：2026-08-12，实现事件明细、三类汇总、发现项和输入审计摘要的稳定中文存储。】
【二次编辑内容：2026-08-12，补充日历快照字段、发现项稳定表头和正式输出文件摘要。】
【三次改进：2026-08-12，将审计发现项 CSV 表头同步为中文，确保全部正式 CSV 字段中文化。】
【四次改进：2026-08-12，按 D-068/rules v5 新增全券商×全风险事件中文命中明细。】
【五次改进：2026-08-12，按 D-069/rules v6 新增单一中文最终汇总结果表。】
【六次改进：2026-08-31，新增全行业券商各原始档位收益率汇总与逐日明细。】
【七次改进：2026-09-09，新增券商03 A-G 档位风险事件汇总与明细稳定输出。】
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Iterable
from hashlib import sha256

import pandas as pd

from .models import BrokerMetricFinding, EventBrokerAssessment


ASSESSMENT_FIELDS = {
    "event_key": "事件唯一键",
    "market_code": "交易市场代码",
    "security_code": "证券代码",
    "event_type": "风险事件类型",
    "first_fact_date": "第一次事实日期",
    "risk_date": "风险认定日期",
    "event_rule_version": "风险事件规则版本",
    "event_batch_id": "风险事件批次标识",
    "broker_id": "券商标识",
    "cutoff_trading_date": "评价截止交易日",
    "cutoff_at": "评价截止时点",
    "selected_classification_date": "选中分类日期",
    "classification_first_usable_date": "分类首次可用交易日",
    "raw_classification": "原始分类值",
    "mapping_version": "分类映射版本",
    "mapping_record_id": "映射记录标识",
    "cutoff_st_state": "截止时ST状态",
    "final_bucket": "最终分类档位",
    "classification_resolution": "分类解析来源",
    "identification_status": "提前识别状态",
    "is_hit": "是否命中",
    "warning_days_status": "预警天数状态",
    "dangerous_run_start_date": "危险轮次起算分类日期",
    "warning_trading_days": "预警交易日数",
    "reason_code": "结果原因代码",
    "source_quarter_file": "来源季度文件",
    "source_file_sha256": "来源文件SHA256",
    "source_row_number": "来源数据行号",
    "source_record_id": "来源记录标识",
    "calendar_snapshot_id": "市场日历快照标识",
    "metric_rule_version": "指标规则版本",
    "metric_batch_id": "指标计算批次标识",
}
WARNING_DAYS_FIELDS = {
    "broker_id": "券商标识",
    "event_type_view": "风险类型视图",
    "defined_sample_count": "有效样本数",
    "identified_days_unknown_count": "已识别但天数未知数",
    "not_identified_count": "未识别数",
    "unknown_assessment_count": "未知评价数",
    "warning_days_min": "预警天数最小值",
    "warning_days_max": "预警天数最大值",
    "warning_days_median": "预警天数中位数",
    "warning_days_mean": "预警天数平均数",
    "result_status": "结果状态",
    "metric_rule_version": "指标规则版本",
    "metric_batch_id": "指标计算批次标识",
}
HIT_RATE_FIELDS = {
    "broker_id": "券商标识",
    "event_type_view": "风险类型视图",
    "risk_event_count": "风险事件数",
    "hit_event_count": "命中事件数",
    "unknown_event_count": "事件评价未知数",
    "event_hit_rate": "事件级命中率",
    "risk_security_count": "风险证券数",
    "hit_risk_security_count": "命中风险证券数",
    "unknown_security_count": "证券评价未知数",
    "security_hit_rate": "证券级命中率",
    "result_status": "结果状态",
    "metric_rule_version": "指标规则版本",
    "metric_batch_id": "指标计算批次标识",
}
WARNING_RATE_FIELDS = {
    "broker_id": "券商标识",
    "event_type_view": "风险类型视图",
    "aggregation_scope": "聚合范围",
    "snapshot_trading_date": "快照交易日",
    "dangerous_risk_security_count": "危险档风险证券数",
    "dangerous_security_count": "危险档证券数",
    "unknown_classification_security_count": "未知分类证券数",
    "warning_rate": "预警率",
    "result_status": "结果状态",
    "metric_rule_version": "指标规则版本",
    "metric_batch_id": "指标计算批次标识",
}
FINDING_FIELDS = {
    "code": "发现项代码",
    "severity": "严重程度",
    "message": "发现项说明",
    "broker_id": "券商标识",
    "market_code": "交易市场代码",
    "security_code": "证券代码",
    "fact_date": "事实日期",
    "source_file": "来源文件",
    "source_row_number": "来源数据行号",
}
ALL_BROKER_HIT_FIELDS = {
    "broker_id": "券商",
    "security_code": "股票代码",
    "security_name": "股票名称",
    "event_type": "风险类型",
    "risk_date": "风险日期",
    "first_dangerous_date": "首次危险日期",
    "is_hit": "是否提前命中",
    "warning_trading_days": "预警交易天数",
}
FINAL_SUMMARY_FIELDS = {
    "broker_id": "券商",
    "risk_type_zh": "风险股票类型",
    "warning_days_defined_count": "可计算预警天数的命中事件数",
    "hit_warning_days_unknown_count": "命中但预警天数未知事件数",
    "warning_days_min": "预警天数最小值",
    "warning_days_max": "预警天数最大值",
    "warning_days_median": "预警天数中位数",
    "warning_days_mean": "预警天数平均数",
    "risk_event_count": "风险事件数",
    "hit_event_count": "命中风险事件数",
    "event_hit_rate": "风险事件命中率",
    "risk_security_count": "风险股票数",
    "hit_risk_security_count": "命中风险股票数",
    "security_hit_rate": "风险股票命中率",
    "dangerous_risk_security_count": "危险档中的风险股票数",
    "dangerous_security_count": "危险档股票数",
    "unknown_classification_security_count": "分类未知股票数",
    "warning_rate": "预警率",
    "metric_rule_version": "指标规则版本",
    "metric_batch_id": "指标计算批次标识",
}
GRADE_RETURN_SUMMARY_FIELDS = {
    "broker_id": "券商",
    "raw_classification": "原始分类档位",
    "return_start_date": "收益观察开始日",
    "return_end_date": "收益观察结束日",
    "classified_trading_day_count": "有档位交易日数",
    "valid_return_trading_day_count": "有效收益交易日数",
    "classified_stock_day_count": "档位累计股票日数",
    "valid_return_stock_day_count": "有效收益股票日数",
    "return_coverage_rate": "收益覆盖率",
    "mean_equal_weight_daily_return": "日均等权收益率",
    "compounded_equal_weight_return": "区间复合等权收益率",
    "stock_day_weighted_mean_return": "股票日加权平均收益率",
    "positive_return_stock_day_count": "上涨股票日数",
    "positive_return_rate": "上涨股票日占比",
    "return_formula": "个股收益率公式",
    "classification_timing": "档位时点口径",
    "price_adjustment": "价格口径",
}
GRADE_RETURN_DAILY_FIELDS = {
    "broker_id": "券商",
    "raw_classification": "原始分类档位",
    "return_date": "收益交易日",
    "classified_security_count": "档位股票数",
    "valid_return_security_count": "有效收益股票数",
    "return_coverage_rate": "收益覆盖率",
    "equal_weight_daily_return": "等权日收益率",
    "positive_return_security_count": "上涨股票数",
    "positive_return_rate": "上涨股票占比",
    "return_formula": "个股收益率公式",
    "classification_timing": "档位时点口径",
    "price_adjustment": "价格口径",
}
SINGLE_GRADE_SUMMARY_FIELDS = {
    "broker_id": "券商",
    "event_type_view": "风险类型视图",
    "grade": "券商03A-G档位",
    "risk_event_count": "风险事件数",
    "hit_event_count": "命中风险事件数",
    "unknown_event_count": "事件评价未知数",
    "event_hit_rate": "风险事件命中率",
    "risk_security_count": "风险股票数",
    "hit_risk_security_count": "命中风险股票数",
    "security_hit_rate": "风险股票命中率",
    "defined_warning_days_count": "可计算预警天数事件数",
    "warning_days_min": "预警天数最小值",
    "warning_days_max": "预警天数最大值",
    "warning_days_median": "预警天数中位数",
    "warning_days_mean": "预警天数平均数",
    "result_status": "结果状态",
    "metric_rule_version": "指标规则版本",
    "metric_batch_id": "指标计算批次标识",
}
SINGLE_GRADE_DETAIL_FIELDS = {
    "event_key": "事件唯一键",
    "market_code": "交易市场代码",
    "security_code": "证券代码",
    "event_type": "风险事件类型",
    "first_fact_date": "第一次事实日期",
    "risk_date": "风险认定日期",
    "selected_classification_date": "选中分类日期",
    "raw_classification": "原始分类值",
    "grade": "券商03A-G档位",
    "final_bucket": "最终分类档位",
    "classification_resolution": "分类解析来源",
    "identification_status": "提前识别状态",
    "is_hit": "是否命中",
    "warning_days_status": "预警天数状态",
    "warning_trading_days": "预警交易日数",
    "mapping_version": "分类映射版本",
    "metric_rule_version": "指标规则版本",
    "metric_batch_id": "指标计算批次标识",
}


def write_broker_metric_result(
    output_root: str | Path,
    metric_batch_id: str,
    assessments: Iterable[EventBrokerAssessment],
    warning_days_summary: Iterable[dict],
    hit_rate_summary: Iterable[dict],
    warning_rate_summary: Iterable[dict],
    findings: Iterable[BrokerMetricFinding],
    input_manifest: dict,
    metric_manifest: dict,
    all_broker_risk_event_hits: Iterable[dict] | None = None,
    final_summary: Iterable[dict] | None = None,
    grade_return_summary: Iterable[dict] | None = None,
    grade_return_daily: Iterable[dict] | None = None,
    single_grade_summary: Iterable[dict] | None = None,
    single_grade_detail: Iterable[dict] | None = None,
) -> Path:
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    final_dir = root / metric_batch_id
    if final_dir.exists():
        raise FileExistsError(f"Broker metric batch already exists: {final_dir}")
    staging = Path(tempfile.mkdtemp(prefix=f".{metric_batch_id}.", dir=root))
    try:
        assessment_items = tuple(assessments)
        _csv(
            staging / "event_broker_assessments.csv",
            [item.to_dict() for item in assessment_items],
            ASSESSMENT_FIELDS,
        )
        _csv(staging / "broker_warning_days_summary.csv", warning_days_summary, WARNING_DAYS_FIELDS)
        _csv(staging / "broker_hit_rate_summary.csv", hit_rate_summary, HIT_RATE_FIELDS)
        _csv(staging / "broker_warning_rate_summary.csv", warning_rate_summary, WARNING_RATE_FIELDS)
        _csv(
            staging / "broker_metric_findings.csv",
            [item.to_dict() for item in findings],
            FINDING_FIELDS,
        )
        if all_broker_risk_event_hits is not None:
            _csv(
                staging / "all_broker_risk_event_hits.csv",
                all_broker_risk_event_hits,
                ALL_BROKER_HIT_FIELDS,
            )
        if final_summary is not None:
            _csv(
                staging / "券商预警指标最终汇总.csv",
                final_summary,
                FINAL_SUMMARY_FIELDS,
            )
        if grade_return_summary is not None:
            _csv(
                staging / "券商档位收益率表现汇总.csv",
                grade_return_summary,
                GRADE_RETURN_SUMMARY_FIELDS,
            )
        if grade_return_daily is not None:
            _csv(
                staging / "券商档位逐日收益率明细.csv",
                grade_return_daily,
                GRADE_RETURN_DAILY_FIELDS,
            )
        if single_grade_summary is not None:
            _csv(
                staging / "券商03A-G档位风险事件汇总.csv",
                single_grade_summary,
                SINGLE_GRADE_SUMMARY_FIELDS,
            )
        if single_grade_detail is not None:
            _csv(
                staging / "券商03A-G档位风险事件明细.csv",
                single_grade_detail,
                SINGLE_GRADE_DETAIL_FIELDS,
            )
        _json(staging / "broker_history_input_manifest.json", input_manifest)
        manifest = dict(metric_manifest)
        output_files = {}
        for path in sorted(staging.glob("*.csv")):
            output_files[path.name] = {
                "sha256": sha256(path.read_bytes()).hexdigest(),
                "row_count": max(sum(1 for _ in path.open(encoding="utf-8-sig")) - 1, 0),
            }
        manifest.update(
            {
                "status": "SUCCEEDED",
                "metric_batch_id": metric_batch_id,
                "output_schema_version": str(
                    metric_manifest.get("output_schema_version", "broker_metric_csv_cn_v1")
                ),
                "assessment_count": len(assessment_items),
                "output_files": output_files,
            }
        )
        _json(staging / "broker_metric_manifest.json", manifest)
        os.replace(staging, final_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return final_dir


def _csv(path: Path, rows: Iterable[dict], fields: dict[str, str]) -> None:
    frame = pd.DataFrame(list(rows), columns=list(fields))
    frame.rename(columns=fields).to_csv(path, index=False, encoding="utf-8-sig")


def _json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
