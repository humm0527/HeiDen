"""
文件作用：对上传季度集中度宽表执行交易日完整性、非交易日和逐日证券行覆盖检查。
编辑记录：
【首次生成：2026-08-14，新增正式交易日历对账与异常塌缩检测。】
【二次编辑：2026-08-14，非交易日异常按实际命中行计数，并输出精简行级证据。】
【三次编辑：2026-08-17，整个交易日缺失改为非阻断审计警告，明确沿用前态且不中断危险轮次。】
"""

from __future__ import annotations

from calendar import monthrange
from datetime import date
from typing import Iterable

import pandas as pd

from .result import ValidationCheckResult, ValidationIssue


def analyze_quarter_classification_coverage(
    frame: pd.DataFrame,
    trading_dates: Iterable[date],
    *,
    quarter: str,
    date_field: str = "biz_date",
    security_field: str = "stk_code",
    minimum_daily_coverage_ratio: float = 0.8,
) -> tuple[list[ValidationIssue], list[ValidationCheckResult]]:
    """Return blocking coverage issues and explicit check evidence."""
    if not 0 < minimum_daily_coverage_ratio <= 1:
        raise ValueError("minimum_daily_coverage_ratio 必须位于 (0, 1]")
    if date_field not in frame or security_field not in frame:
        return [], []

    quarter_start, quarter_end = _quarter_bounds(quarter)
    calendar = sorted(
        value for value in set(trading_dates) if quarter_start <= value <= quarter_end
    )
    parsed = pd.to_datetime(frame[date_field], errors="coerce")
    actual_dates = {
        value.date()
        for value in parsed.dropna()
        if quarter_start <= value.date() <= quarter_end
    }
    calendar_set = set(calendar)
    issues: list[ValidationIssue] = []
    checks: list[ValidationCheckResult] = []

    if not calendar:
        issue = _issue(
            "E928",
            date_field,
            0,
            [{"quarter": quarter}],
            "提供覆盖该季度的权威交易日历后重新预检",
            "权威交易日历未覆盖上传季度，无法证明数据完整性",
        )
        issues.append(issue)
        checks.append(
            _check("TRADING_CALENDAR_AVAILABLE", "权威交易日历可用", "FAIL", issue.message)
        )
        return issues, checks

    missing = sorted(calendar_set - actual_dates)
    if missing:
        issues.append(
            _issue(
                "E925",
                date_field,
                len(missing),
                [{"缺失交易日期": value.isoformat()} for value in missing[:20]],
                "核对并记录整日缺失原因；计算跳过缺失快照、沿用此前最近有效分类",
                "季度集中度文件缺少权威交易日历中的交易日；已留痕并按分类状态延续口径继续计算",
                severity="一般",
            )
        )
    checks.append(
        _check(
            "QUARTER_TRADING_DATE_COVERAGE",
            "季度交易日完整性",
            "PASS_WITH_GAPS" if missing else "PASS",
            (
                f"应有 {len(calendar)} 日，实际 {len(actual_dates & calendar_set)} 日，"
                f"缺失 {len(missing)} 日；缺失快照已跳过，分类前态持续有效"
            ),
            {
                "expected_dates": len(calendar),
                "actual_dates": len(actual_dates & calendar_set),
                "missing_dates": [value.isoformat() for value in missing],
                "calculation_policy": "SKIP_MISSING_SNAPSHOT_CARRY_FORWARD_STATE",
                "breaks_dangerous_run_continuity": False,
                "warning_days_calendar_policy": "COUNT_AUTHORITATIVE_TRADING_CALENDAR",
            },
        )
    )

    weekdays_outside_calendar = sorted(
        value
        for value in actual_dates
        if value.weekday() < 5 and value not in calendar_set
    )
    if weekdays_outside_calendar:
        weekday_mask = parsed.dt.date.isin(weekdays_outside_calendar)
        issues.append(
            _issue(
                "E926",
                date_field,
                int(weekday_mask.sum()),
                _row_examples(
                    frame,
                    parsed,
                    weekday_mask,
                    date_field=date_field,
                    security_field=security_field,
                ),
                "删除非交易日分类记录，或更正权威交易日历版本",
                "季度集中度文件包含工作日形式但并非正式交易日的分类日期",
            )
        )
    weekend_dates = sorted(value for value in actual_dates if value.weekday() >= 5)
    if weekend_dates:
        weekend_mask = parsed.dt.date.isin(weekend_dates)
        issues.append(
            _issue(
                "E921",
                date_field,
                int(weekend_mask.sum()),
                _row_examples(
                    frame,
                    parsed,
                    weekend_mask,
                    date_field=date_field,
                    security_field=security_field,
                ),
                "删除周末日期的集中度分类记录并重新预检",
                "季度集中度文件包含周末分类日期",
            )
        )
    checks.append(
        _check(
            "OFFICIAL_TRADING_DATES_ONLY",
            "非交易日分类",
            "FAIL" if weekend_dates or weekdays_outside_calendar else "PASS",
            f"周末 {len(weekend_dates)} 日，其他非交易日 {len(weekdays_outside_calendar)} 日",
            {
                "weekend_dates": [value.isoformat() for value in weekend_dates],
                "other_non_trading_dates": [
                    value.isoformat() for value in weekdays_outside_calendar
                ],
            },
        )
    )

    valid_rows = frame.loc[parsed.notna()].copy()
    valid_rows["_validation_date"] = parsed.loc[parsed.notna()].dt.date
    valid_rows = valid_rows.loc[valid_rows["_validation_date"].isin(calendar_set)]
    per_date = valid_rows.groupby("_validation_date")[security_field].nunique()
    baseline = float(per_date.median()) if not per_date.empty else 0.0
    threshold = baseline * minimum_daily_coverage_ratio
    thin = per_date.loc[per_date < threshold].sort_index() if baseline else per_date
    if not thin.empty:
        examples = [
            {
                "交易日期": current.isoformat(),
                "实际证券数": int(count),
                "季度单日中位数": int(baseline),
                "最低覆盖比例": minimum_daily_coverage_ratio,
            }
            for current, count in thin.items()
        ]
        issues.append(
            _issue(
                "E927",
                security_field,
                len(thin),
                examples[:20],
                "核对异常日期是否发生文件截断或证券记录批量缺失",
                "单日证券记录数显著低于本季度正常水平",
            )
        )
    checks.append(
        _check(
            "DAILY_SECURITY_ROW_COVERAGE",
            "逐日证券行覆盖",
            "FAIL" if not thin.empty else "PASS",
            f"单日中位数 {int(baseline)}，低于 {minimum_daily_coverage_ratio:.0%} 的异常日期 {len(thin)} 个",
            {
                "median_daily_security_count": int(baseline),
                "minimum_ratio": minimum_daily_coverage_ratio,
                "thin_dates": {
                    current.isoformat(): int(count) for current, count in thin.items()
                },
            },
        )
    )
    return issues, checks


def _row_examples(
    frame: pd.DataFrame,
    parsed: pd.Series,
    mask: pd.Series,
    *,
    date_field: str,
    security_field: str,
) -> list[dict[str, object]]:
    """Return concise row evidence instead of serialising every broker column."""
    examples: list[dict[str, object]] = []
    for index in frame.index[mask][:20]:
        current = parsed.loc[index]
        examples.append(
            {
                "源文件行号": int(index) + 2 if isinstance(index, int) else str(index),
                "业务日期": current.date().isoformat(),
                "证券代码": str(frame.at[index, security_field]),
            }
        )
    return examples


def _quarter_bounds(value: str) -> tuple[date, date]:
    text = str(value).upper()
    if (
        len(text) != 6
        or text[4] != "Q"
        or not text[:4].isdigit()
        or text[5] not in "1234"
    ):
        raise ValueError(f"季度格式无效：{value}")
    year = int(text[:4])
    quarter = int(text[5])
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2
    return date(year, start_month, 1), date(
        year, end_month, monthrange(year, end_month)[1]
    )


def _issue(
    code: str,
    field: str,
    affected: int,
    examples: list[dict[str, object]],
    action: str,
    message: str,
    *,
    severity: str = "严重",
) -> ValidationIssue:
    return ValidationIssue(
        code, severity, "券商分类宽表", field, affected, examples, action, message
    )


def _check(
    check_id: str,
    label: str,
    status: str,
    message: str,
    details: dict[str, object] | None = None,
) -> ValidationCheckResult:
    return ValidationCheckResult(check_id, label, status, message, details or {})
