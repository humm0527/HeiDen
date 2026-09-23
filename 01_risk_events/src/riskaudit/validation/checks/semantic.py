"""
文件作用：提供显式注册的确定性跨表语义检查器，供不同版本化验证规则包按名称启用。
编辑记录：
【首次生成：2026-08-06，实现市场覆盖、证券生命周期和券商映射一致性检查器。】
【二次编辑内容：2026-08-06，先按上市退市边界计算当日应有证券，再判断整日和证券覆盖。】
【第三次改进：2026-08-06，将上市前、终止上市后及显式停牌覆盖影响作为不阻断提示返回。】
【第四次编辑：2026-08-06，新增券商分类宽表的周末、市场范围、代码格式和低填充率检查。】
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping

import pandas as pd
import yaml

from ..result import ValidationIssue
from ..rules import ValidationPack


@dataclass(frozen=True)
class SemanticContext:
    tables: Mapping[str, pd.DataFrame]
    parsed_dates: Mapping[tuple[str, str], pd.Series]
    observation_start: date
    observation_end: date
    pack: ValidationPack


SemanticChecker = Callable[[SemanticContext, Mapping[str, Any]], list[ValidationIssue]]


def run_semantic_checks(context: SemanticContext) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for rule in context.pack.semantic_checks:
        checker = SEMANTIC_CHECKERS.get(rule.name)
        if checker is None:
            raise ValueError(f"验证规则包引用了未注册检查器：{rule.name}")
        issues.extend(checker(context, rule.options))
    return issues


def check_market_calendar_coverage(
    context: SemanticContext,
    options: Mapping[str, Any],
) -> list[ValidationIssue]:
    calendar_name = str(options.get("calendar_table", "交易日历表"))
    market_name = str(options.get("market_daily_table", "股票日行情表"))
    master_name = str(options.get("master_table", "证券基础状态日表"))
    delisted_date_exclusive = bool(options.get("delisted_date_exclusive", False))
    calendar = context.tables.get(calendar_name)
    market_daily = context.tables.get(market_name)
    master = context.tables.get(master_name)
    if calendar is None or market_daily is None or calendar.empty:
        return []
    calendar_dates = context.parsed_dates.get((calendar_name, "日历日期"))
    market_dates = context.parsed_dates.get((market_name, "交易日期"))
    if calendar_dates is None or market_dates is None or "是否交易日" not in calendar:
        return []

    issues: list[ValidationIssue] = []
    trading_mask = calendar["是否交易日"].astype(str).str.strip().str.lower().isin(
        {"true", "1", "是"}
    )
    expected_dates = {
        value
        for value in calendar_dates[trading_mask].dt.date.dropna()
        if context.observation_start <= value <= context.observation_end
    }
    market_business_dates = market_dates.map(
        lambda value: value.date() if pd.notna(value) else None
    ).astype(object)
    non_trading = market_business_dates.notna() & ~market_business_dates.isin(
        expected_dates
    )
    if non_trading.any():
        issues.append(
            _issue(
                "E601",
                "严重",
                market_name,
                "交易日期",
                int(non_trading.sum()),
                _examples(market_daily.loc[non_trading]),
                "删除非交易日行情或修正交易日历版本",
                "存在非交易日行情数据",
            )
        )
    if master is None or master.empty:
        return issues

    issues.extend(
        _lifecycle_coverage_warnings(
            master,
            expected_dates,
            delisted_date_exclusive=delisted_date_exclusive,
        )
    )
    if "成交状态" in market_daily.columns:
        non_trading_status = market_daily["成交状态"].astype(str).str.strip().isin(
            {"停牌", "无成交"}
        )
        if non_trading_status.any():
            issues.append(
                _issue(
                    "W605",
                    "一般",
                    market_name,
                    "成交状态",
                    int(non_trading_status.sum()),
                    _examples(market_daily.loc[non_trading_status]),
                    "保留明确停牌/无成交状态；不得将这些日期误判为行情缺失",
                    "观察区间存在明确停牌或无成交记录，已作为可解释覆盖处理",
                )
            )

    missing_dates: list[date] = []
    for trading_date in sorted(expected_dates):
        expected_securities = _eligible_securities(
            master,
            trading_date,
            delisted_date_exclusive=delisted_date_exclusive,
        )
        on_date = market_daily.loc[market_business_dates == trading_date]
        if expected_securities and on_date.empty:
            missing_dates.append(trading_date)
            continue
        if not expected_securities:
            continue
        if not {"交易市场代码", "证券代码"}.issubset(on_date.columns):
            continue
        actual_securities = set(
            zip(
                on_date["交易市场代码"].astype(str).str.strip(),
                on_date["证券代码"].astype(str).str.strip(),
            )
        )
        expected_markets = {market for market, _ in expected_securities}
        actual_markets = {market for market, _ in actual_securities}
        missing_markets = sorted(expected_markets - actual_markets)
        if missing_markets:
            issues.append(
                _issue(
                    "E603",
                    "严重",
                    market_name,
                    "交易市场代码",
                    len(missing_markets),
                    [
                        {"交易日期": trading_date.isoformat(), "缺失市场": market}
                        for market in missing_markets[:5]
                    ],
                    "补充该交易日缺失市场的行情数据",
                    "部分市场数据缺失",
                )
            )
        missing_security_dates = sorted(expected_securities - actual_securities)
        if missing_security_dates:
            issues.append(
                _issue(
                    "E604",
                    "严重",
                    market_name,
                    "证券代码 + 交易日期",
                    len(missing_security_dates),
                    [
                        {
                            "交易日期": trading_date.isoformat(),
                            "交易市场代码": market,
                            "证券代码": security,
                        }
                        for market, security in missing_security_dates[:5]
                    ],
                    "补充处于上市有效期且具备交易资格证券的缺失行情",
                    "证券交易日行情覆盖不完整",
                )
            )
    if missing_dates:
        issues.append(
            _issue(
                "E602",
                "严重",
                market_name,
                "交易日期",
                len(missing_dates),
                [{"交易日期": value.isoformat()} for value in missing_dates[:5]],
                "补充生命周期内应有证券的整日行情，或提供停牌/无交易资格状态行",
                "存在无法由证券生命周期解释的整日行情缺失",
            )
        )
    return issues


def check_security_lifecycle_consistency(
    context: SemanticContext,
    options: Mapping[str, Any],
) -> list[ValidationIssue]:
    master_name = str(options.get("master_table", "证券基础状态日表"))
    delisted_date_exclusive = bool(options.get("delisted_date_exclusive", False))
    master = context.tables.get(master_name)
    if master is None or master.empty:
        return []
    required = {"交易市场代码", "证券代码", "上市日期", "退市日期"}
    if not required.issubset(master.columns):
        return []
    lifecycle = _lifecycle_index(master)
    security_markets: dict[str, set[str]] = {}
    for market, security in lifecycle:
        security_markets.setdefault(security, set()).add(market)

    issues: list[ValidationIssue] = []
    for target in options.get("targets", []):
        table_name = str(target["table"])
        date_field = str(target["date_field"])
        frame = context.tables.get(table_name)
        parsed = context.parsed_dates.get((table_name, date_field))
        if frame is None or parsed is None or not {"交易市场代码", "证券代码"}.issubset(frame.columns):
            continue
        before_listing: list[int] = []
        after_delisting: list[int] = []
        missing_master: list[int] = []
        wrong_market: list[int] = []
        for index in frame.index:
            market = str(frame.at[index, "交易市场代码"]).strip()
            security = str(frame.at[index, "证券代码"]).strip()
            business_timestamp = parsed.loc[index]
            if pd.isna(business_timestamp):
                continue
            business_date = business_timestamp.date()
            life = lifecycle.get((market, security))
            if life is None:
                if security in security_markets:
                    wrong_market.append(index)
                else:
                    missing_master.append(index)
                continue
            listed_date, delisted_date = life
            if listed_date is not None and business_date < listed_date:
                before_listing.append(index)
            is_after_delisting = delisted_date is not None and (
                business_date >= delisted_date
                if delisted_date_exclusive
                else business_date > delisted_date
            )
            if is_after_delisting:
                after_delisting.append(index)
        for code, indexes, message, action in (
            ("E801", missing_master, "业务记录中的证券不存在于基础状态表", "补充证券基础状态或删除错误证券记录"),
            ("E802", wrong_market, "业务记录中的证券市场与基础状态不一致", "修正交易市场代码并保持证券关联键一致"),
            ("E803", before_listing, "证券上市前出现业务数据", "移除上市日前数据或修正上市日期"),
            ("E804", after_delisting, "证券退市后出现业务数据", "移除退市后数据或修正退市日期"),
        ):
            if indexes:
                issues.append(
                    _issue(
                        code,
                        "严重",
                        table_name,
                        "交易市场代码 + 证券代码 + " + date_field,
                        len(indexes),
                        _examples(frame.loc[indexes]),
                        action,
                        message,
                    )
                )
    return issues


def check_broker_mapping_consistency(
    context: SemanticContext,
    options: Mapping[str, Any],
) -> list[ValidationIssue]:
    table_name = str(options.get("table", "券商风险分类表"))
    frame = context.tables.get(table_name)
    if frame is None or frame.empty:
        return []
    relative_mapping = Path(str(options["mapping_file"]))
    mapping_path = (
        relative_mapping
        if relative_mapping.is_absolute()
        else context.pack.repository_root / relative_mapping
    )
    try:
        payload: dict[str, Any] = yaml.safe_load(mapping_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        return [
            _issue(
                "E901",
                "严重",
                table_name,
                "分类映射版本号",
                len(frame),
                [{"映射文件": str(mapping_path), "错误": str(exc)}],
                "提供可读取的版本化精确映射文件",
                "券商分类映射配置无法读取",
            )
        ]

    issues: list[ValidationIssue] = []
    mapping_version = str(payload.get("mapping_version") or "")
    mappings = {
        (str(broker), str(raw_value)): str(normalized)
        for broker, details in payload.get("brokers", {}).items()
        for raw_value, normalized in details.get("mappings", {}).items()
    }
    required_fields = {
        "券商标识",
        "原始分类值",
        "分类映射版本号",
        "标准风险档位",
        "映射记录标识",
    }
    if not required_fields.issubset(frame.columns):
        return issues
    unknown: list[int] = []
    wrong_version: list[int] = []
    wrong_bucket: list[int] = []
    wrong_record_id: list[int] = []
    for index, row in frame.iterrows():
        broker = str(row["券商标识"])
        raw_value = str(row["原始分类值"])
        expected_bucket = mappings.get((broker, raw_value))
        if expected_bucket is None:
            unknown.append(index)
            continue
        if str(row["分类映射版本号"]) != mapping_version:
            wrong_version.append(index)
        if str(row["标准风险档位"]) != expected_bucket:
            wrong_bucket.append(index)
        expected_record_id = f"{mapping_version}:{broker}:{raw_value}"
        if str(row["映射记录标识"]) != expected_record_id:
            wrong_record_id.append(index)
    for code, indexes, field, message, action in (
        ("E902", unknown, "券商标识 + 原始分类值", "券商分类值未精确命中版本化映射", "补充经审批的精确映射，禁止自动推断"),
        ("E903", wrong_version, "分类映射版本号", "分类映射版本与指定配置不一致", "使用任务指定的映射版本重新标准化"),
        ("E904", wrong_bucket, "标准风险档位", "标准风险档位与精确映射结果不一致", "依据原始值和映射版本重新生成标准档位"),
        ("E905", wrong_record_id, "映射记录标识", "映射记录标识无法追溯到精确映射条目", "修正映射记录标识并保留原始分类值"),
    ):
        if indexes:
            issues.append(
                _issue(
                    code,
                    "严重",
                    table_name,
                    field,
                    len(indexes),
                    _examples(frame.loc[indexes]),
                    action,
                    message,
                )
            )
    if str(payload.get("status") or "") != "approved":
        issues.append(
            _issue(
                "W901",
                "一般",
                table_name,
                "分类映射版本号",
                len(frame),
                [{"映射版本": mapping_version, "配置状态": payload.get("status")}],
                "正式任务使用前完成映射审批并发布不可覆盖版本",
                "当前券商映射尚未处于 approved 状态",
            )
        )
    return issues


def check_wide_broker_classification_quality(
    context: SemanticContext,
    options: Mapping[str, Any],
) -> list[ValidationIssue]:
    table_name = str(options.get("table", "券商分类宽表"))
    date_field = str(options.get("date_field", "biz_date"))
    security_field = str(options.get("security_field", "stk_code"))
    broker_fields = [str(value) for value in options.get("broker_fields", [])]
    allowed_suffixes = {
        str(value).upper() for value in options.get("allowed_market_suffixes", [])
    }
    minimum_filled = int(options.get("minimum_filled_brokers", 1))
    frame = context.tables.get(table_name)
    parsed_dates = context.parsed_dates.get((table_name, date_field))
    if (
        frame is None
        or frame.empty
        or parsed_dates is None
        or security_field not in frame.columns
    ):
        return []

    issues: list[ValidationIssue] = []
    valid_dates = parsed_dates.notna()
    weekend = valid_dates & (parsed_dates.dt.weekday >= 5)
    if weekend.any():
        issues.append(
            _issue(
                "E921",
                "严重",
                table_name,
                date_field,
                int(weekend.sum()),
                _examples(frame.loc[weekend]),
                "删除周末业务记录，或提供权威交易日历证明该日有效",
                "业务日期落在周末，不能作为正常交易日分类快照",
            )
        )

    securities = frame[security_field].astype(str).str.strip().str.upper()
    valid_security_format = securities.str.fullmatch(r"\d{6}\.(?:SZ|SH|BJ)")
    invalid_security = ~valid_security_format
    if invalid_security.any():
        issues.append(
            _issue(
                "E924",
                "严重",
                table_name,
                security_field,
                int(invalid_security.sum()),
                _examples(frame.loc[invalid_security]),
                "使用六位数字加 .SZ/.SH/.BJ 后缀的业务证券代码",
                "业务证券代码格式错误",
            )
        )

    suffixes = securities.str.rsplit(".", n=1).str[-1]
    outside_scope = valid_security_format & ~suffixes.isin(allowed_suffixes)
    if outside_scope.any():
        issues.append(
            _issue(
                "E922",
                "严重",
                table_name,
                security_field,
                int(outside_scope.sum()),
                _examples(frame.loc[outside_scope]),
                "将范围外市场记录移出当前任务，或发布包含该市场的新校验包版本",
                "证券所属市场不在当前业务统计范围",
            )
        )

    available_broker_fields = [
        field for field in broker_fields if field in frame.columns
    ]
    if available_broker_fields:
        broker_values = frame.loc[:, available_broker_fields]
        filled_count = (~(
            broker_values.isna()
            | broker_values.astype(str).apply(lambda column: column.str.strip().eq(""))
        )).sum(axis=1)
        eligible_for_density = (
            valid_dates
            & ~weekend
            & valid_security_format
            & ~outside_scope
        )
        low_fill = eligible_for_density & (filled_count < minimum_filled)
        if low_fill.any():
            examples = _examples(frame.loc[low_fill])
            for example, count in zip(examples, filled_count.loc[low_fill].head(5)):
                example["已填券商数"] = int(count)
                example["要求最少券商数"] = minimum_filled
            issues.append(
                _issue(
                    "E923",
                    "严重",
                    table_name,
                    "券商分类列",
                    int(low_fill.sum()),
                    examples,
                    "补充至少达到校验包阈值的券商分类，或确认该记录不应进入任务",
                    "单行券商分类填充率过低",
                )
            )
    return issues


def _eligible_securities(
    master: pd.DataFrame,
    business_date: date,
    *,
    delisted_date_exclusive: bool = False,
) -> set[tuple[str, str]]:
    if not {"交易市场代码", "证券代码"}.issubset(master.columns):
        return set()
    lifecycle = _lifecycle_index(master)
    eligible: set[tuple[str, str]] = set()
    for key, (listed_date, delisted_date) in lifecycle.items():
        if listed_date is not None and business_date < listed_date:
            continue
        if delisted_date is not None and (
            business_date >= delisted_date
            if delisted_date_exclusive
            else business_date > delisted_date
        ):
            continue
        exact_status = master.loc[
            (master["交易市场代码"].astype(str).str.strip() == key[0])
            & (master["证券代码"].astype(str).str.strip() == key[1])
            & (pd.to_datetime(master.get("状态日期"), errors="coerce").dt.date == business_date)
        ]
        if not exact_status.empty and "交易资格状态" in exact_status:
            status = str(exact_status.iloc[-1]["交易资格状态"]).strip()
            if status in {"停牌", "暂停", "终止", "无成交"}:
                continue
        eligible.add(key)
    return eligible


def _lifecycle_coverage_warnings(
    master: pd.DataFrame,
    trading_dates: set[date],
    *,
    delisted_date_exclusive: bool,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for (market, security), (listed_date, delisted_date) in _lifecycle_index(
        master
    ).items():
        before_listing = sorted(
            value
            for value in trading_dates
            if listed_date is not None and value < listed_date
        )
        after_delisting = sorted(
            value
            for value in trading_dates
            if delisted_date is not None
            and (
                value >= delisted_date
                if delisted_date_exclusive
                else value > delisted_date
            )
        )
        if before_listing:
            issues.append(
                _issue(
                    "W805",
                    "一般",
                    "证券基础状态日表",
                    "上市日期",
                    len(before_listing),
                    [
                        {
                            "交易市场代码": market,
                            "证券代码": security,
                            "上市日期": listed_date.isoformat(),
                            "排除起始日期": before_listing[0].isoformat(),
                            "排除结束日期": before_listing[-1].isoformat(),
                        }
                    ],
                    "确认观察区间是否允许包含上市前日期；这些日期不要求行情覆盖",
                    "观察区间受到上市日期影响，上市前交易日已从应有行情中排除",
                )
            )
        if after_delisting:
            issues.append(
                _issue(
                    "W806",
                    "一般",
                    "证券基础状态日表",
                    "退市日期",
                    len(after_delisting),
                    [
                        {
                            "交易市场代码": market,
                            "证券代码": security,
                            "退市日期": delisted_date.isoformat(),
                            "排除起始日期": after_delisting[0].isoformat(),
                            "排除结束日期": after_delisting[-1].isoformat(),
                        }
                    ],
                    "确认观察区间是否允许包含终止上市日及之后日期；这些日期不要求行情覆盖",
                    "观察区间受到终止上市日期影响，终止上市日及之后已从应有行情中排除",
                )
            )
    return issues


def _lifecycle_index(master: pd.DataFrame) -> dict[tuple[str, str], tuple[date | None, date | None]]:
    output: dict[tuple[str, str], tuple[date | None, date | None]] = {}
    for (market, security), group in master.groupby(
        ["交易市场代码", "证券代码"], dropna=False, sort=False
    ):
        listed = pd.to_datetime(group["上市日期"], errors="coerce").dropna()
        delisted = pd.to_datetime(group["退市日期"], errors="coerce").dropna()
        output[(str(market).strip(), str(security).strip())] = (
            None if listed.empty else listed.min().date(),
            None if delisted.empty else delisted.max().date(),
        )
    return output


def _examples(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return frame.head(5).fillna("").astype(object).to_dict(orient="records")


def _issue(
    code: str,
    severity: str,
    table: str,
    field: str,
    affected_rows: int,
    examples: list[dict[str, Any]],
    action: str,
    message: str,
) -> ValidationIssue:
    return ValidationIssue(
        code,
        severity,
        table,
        field,
        affected_rows,
        examples,
        action,
        message,
    )


SEMANTIC_CHECKERS: dict[str, SemanticChecker] = {
    "market_calendar_coverage": check_market_calendar_coverage,
    "security_lifecycle_consistency": check_security_lifecycle_consistency,
    "broker_mapping_consistency": check_broker_mapping_consistency,
    "wide_broker_classification_quality": check_wide_broker_classification_quality,
}
