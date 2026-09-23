"""Stable catalog for independently runnable RiskAudit task modules."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class TaskDefinition:
    task_type: str
    label: str
    description: str
    input_contract: tuple[str, ...]
    output_contract: tuple[str, ...]
    runnable: bool = True

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


TASK_CATALOG: dict[str, TaskDefinition] = {
    "validate-input": TaskDefinition(
        "validate-input",
        "标准输入预检",
        "只读校验标准表并生成 JSON/XLSX 质量报告。",
        ("input_dir", "observation_start", "observation_end"),
        ("validation_result.json", "data_quality_report.xlsx"),
    ),
    "risk-events": TaskDefinition(
        "risk-events",
        "风险事件事实生成",
        "从已发布市场快照生成连续跌停与新增 ST 风险事实。",
        ("market_lake_root", "snapshot_id", "business_file", "risk_universe_files"),
        ("risk_events.csv", "continuous_limit_down_segments.csv", "calculation_manifest.json"),
    ),
    "peer-benchmark": TaskDefinition(
        "peer-benchmark",
        "全行业命中与预警对比",
        "对完整券商集合执行事件×券商 PIT 评价及横向汇总。",
        ("risk_input_manifest", "uploaded_history_file"),
        (
            "event_broker_assessments.csv",
            "券商预警指标最终汇总.csv",
        ),
    ),
    "single-model": TaskDefinition(
        "single-model",
        "单券商单模型评价",
        "使用共享券商评价引擎，对高频更新的券商03历史模型执行 PIT 评价。",
        ("risk_input_manifest", "uploaded_history_file"),
        (
            "event_broker_assessments.csv",
            "券商预警指标最终汇总.csv",
            "券商03A-G档位风险事件汇总.csv",
            "券商03A-G档位风险事件明细.csv",
        ),
    ),
    "pressure-observation": TaskDefinition(
        "pressure-observation",
        "低门槛跌停压力评价",
        "对已冻结压力观察事件执行券商03 PIT 分类及升级率汇总。",
        ("observation_csv", "history_dir", "calendar_csv", "st_csv"),
        ("pressure_event_assessments.csv", "低门槛A-G档位分布.csv"),
    ),
    "model-comparison": TaskDefinition(
        "model-comparison",
        "单券商新旧模型比较",
        "比较两个已冻结评价批次，不重复计算市场风险事实。",
        ("new_assessments", "legacy_assessments"),
        ("event_differences.csv", "comparison_summary.json", "comparison_manifest.json"),
    ),
    "descriptive-comparison": TaskDefinition(
        "descriptive-comparison",
        "新旧模型描述性分析",
        "生成档位数量、市值、流动性、波动率和回撤对比表。",
        ("new_history_files", "legacy_history_files"),
        ("描述性表格计算清单.json",),
    ),
    "period-report": TaskDefinition(
        "period-report",
        "冻结批次区间报告",
        "从既有风险事件与模型证据导出指定日期窗口的正式表。",
        ("event_csv", "event_manifest", "models", "start", "end"),
        ("report_manifest.json",),
    ),
}


def list_task_definitions() -> list[dict[str, object]]:
    return [definition.to_dict() for definition in TASK_CATALOG.values()]
