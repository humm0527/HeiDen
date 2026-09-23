"""
文件作用：将结构化预检结果保存为不可隐去异常的 JSON 和人工审核 XLSX 报告。
编辑记录：
【首次生成：2026-08-06，建立四工作表数据质量报告输出。】
【二次编辑内容：2026-08-06，在人工报告摘要中加入验证规则包及版本。】
【三次编辑：2026-08-14，将季度上传覆盖检查的通过/失败证据写入既有检查摘要。】
【四次编辑：2026-08-14，增加预检执行状态、处理后行数及异常处理记录工作表。】
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .result import ValidationResult


def write_validation_reports(
    result: ValidationResult, output_dir: str | Path
) -> tuple[Path, Path]:
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    json_path = target / "validation_result.json"
    xlsx_path = target / "data_quality_report.xlsx"
    json_path.write_text(
        json.dumps(result.to_dict(), ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    summary_rows = [
        {"项目": "预检执行状态", "值": result.inspection_status},
        {"项目": "预检结论", "值": result.inspection_message},
        {"项目": "检查完成时间", "值": result.checked_at},
        {"项目": "总体状态", "值": result.overall_status},
        {"项目": "是否允许进入下一阶段", "值": "是" if result.allow_run else "否"},
        {"项目": "原始数据行数", "值": result.source_row_count},
        {"项目": "处理后计算行数", "值": result.calculation_row_count},
        {"项目": "严重异常数", "值": sum(i.severity == "严重" for i in result.errors)},
        {"项目": "一般异常数", "值": sum(i.severity == "一般" for i in result.errors)},
        {"项目": "观察开始日期", "值": result.observation_start},
        {"项目": "观察结束日期", "值": result.observation_end},
        {"项目": "验证规则包", "值": result.validation_pack_id},
        {"项目": "规则包版本", "值": result.validation_pack_version},
    ]
    summary_rows.extend(
        {
            "项目": f"检查：{item.label}",
            "值": f"{item.status} · {item.message}",
        }
        for item in result.checks
    )
    summary = pd.DataFrame(summary_rows)
    issues = pd.DataFrame([item.to_dict() for item in result.errors])
    fields = pd.DataFrame([item.__dict__ for item in result.field_checks])
    statistics = pd.DataFrame([item.__dict__ for item in result.statistics])
    remediations = pd.DataFrame(result.remediations)
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="检查摘要", index=False)
        issues.to_excel(writer, sheet_name="异常明细", index=False)
        fields.to_excel(writer, sheet_name="字段检查结果", index=False)
        statistics.to_excel(writer, sheet_name="数据统计", index=False)
        if not remediations.empty:
            remediations.to_excel(writer, sheet_name="处理记录", index=False)
    return json_path, xlsx_path
