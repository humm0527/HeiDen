"""复用01风险事件和02前一交易日评价逻辑，计算议题风险股票拦截、风险股票分档的股票打分结果。

2026-09-17：按用户确认，风险股票分档从2025-01-15开始；990001/990002全期按G。
覆盖只在计算内存中应用，不修改源Excel或single。
终端显示两张表，CSV结果与核对材料分别保存；默认读取模块内的JSON配置。
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from itertools import groupby
import csv
import hashlib
import json
from pathlib import Path
import sys
import unicodedata

MODULE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE / "src"))

from riskaudit.broker_metrics.event_input import load_risk_events
from riskaudit.broker_metrics.classification import resolve_final_classification
from riskaudit.broker_metrics.history import normalize_security
from riskaudit.broker_metrics.metrics import single_grade_event_artifacts, risk_hit_rates
from riskaudit.broker_metrics.models import BrokerClassificationRecord, EventBrokerAssessment
from riskaudit.broker_metrics.real_run_inputs import _load_calendar
from riskaudit.broker_metrics.rules import load_broker_mapping, load_broker_metric_rules


START = date(2025, 1, 15)
END = date(2026, 8, 31)
TABLE8_START = date(2026, 1, 1)
OVERRIDES = {}  # Add only your own explicitly approved security overrides.
EVENT_NAMES = {
    "ALL_RISK_EVENTS": "两类风险股票合计",
    "NEW_ST": "新增ST风险股票",
    "NON_ST_CONTINUOUS_LIMIT_DOWN": "非ST连续跌停风险股票",
}


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def percent(numerator, denominator):
    if denominator == 0:
        return None
    return str((Decimal(numerator) / Decimal(denominator) * 100).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP)) + "%"


def load_cutoff_st_evidence(manifest, event_csv, events, calendar):
    """仅接受从原始ST事实提取的证据，不把缺失值推断为非ST。"""
    evidence = manifest.get("cutoff_st_evidence", {})
    if evidence.get("schema_version") != 1 or evidence.get("method") != "EXACT_DAILY_ST_LOOKUP_AT_PREVIOUS_TRADING_DAY":
        raise ValueError("计算清单缺少事件截止日ST证据，请先运行 prepare_scoring_event_inputs.py 准备两个输入文件")
    if evidence.get("event_csv_sha256") != sha256(event_csv):
        raise ValueError("风险事件CSV与清单中的ST证据不匹配")
    entries = evidence.get("entries", {})
    expected = {"|".join(map(str, e.event_key)) for e in events}
    if set(entries) != expected or evidence.get("entry_count") != len(expected):
        raise ValueError("事件截止日ST证据缺失、多余或条数不一致")
    states = {}
    for event in events:
        value = entries["|".join(map(str, event.event_key))]
        cutoff = calendar.previous_trading_day("cn", event.first_fact_date)
        if value.get("cutoff_date") != cutoff.isoformat() or value.get("st_state") not in {"ST", "NON_ST"}:
            raise ValueError(f"事件截止日ST证据的日期或状态无效：{event.event_key}")
        states[event.event_key] = value["st_state"] == "ST"
    return states


def assess_cutoff_events(events, records, states, calendar, mapping, rules, calendar_id):
    """风险股票拦截只需截止日命中判定，复用02分类优先级，不计算无关的预警天数。"""
    assessments = []
    for event in events:
        cutoff = calendar.previous_trading_day("cn", event.first_fact_date)
        record = records[(cutoff, event.market_code, event.security_code)]
        resolved = resolve_final_classification(record, states[event.event_key], mapping)
        if resolved.final_bucket not in {"DANGEROUS", "NON_DANGEROUS"}:
            raise ValueError(f"事件分类无法确定：{event.event_key}")
        hit = resolved.final_bucket == "DANGEROUS"
        assessments.append(EventBrokerAssessment(
            event=event, broker_id="券商03", cutoff_trading_date=cutoff,
            selected_record=record, cutoff_st_state="ST" if states[event.event_key] else "NON_ST",
            final_bucket=resolved.final_bucket, classification_resolution=resolved.resolution,
            mapping_version=mapping.mapping_version, mapping_record_id=resolved.mapping_record_id,
            identification_status="IDENTIFIED" if hit else "NOT_IDENTIFIED", is_hit=hit,
            warning_days_status="NOT_CALCULATED", dangerous_run_start_date=None,
            warning_trading_days=None, reason_code="SCORING_CUTOFF_ONLY",
            metric_rule_version=rules.rule_version, metric_batch_id="scoring_tables_8_9_20260917",
            calendar_snapshot_id=calendar_id))
    return tuple(assessments)


def calculate(history_dir, event_dir, calendar_path, *, start=START, end=END, table8_start=None):
    table8_start = table8_start or date(end.year, 1, 1)
    if not start <= end or not table8_start <= end:
        raise ValueError("风险股票拦截/风险股票分档日期区间无效")
    batch_name = event_dir.name
    event_csv = event_dir / "risk_events.csv"
    manifest_path = event_dir / "calculation_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["observation_start"] <= start.isoformat()
    assert manifest["observation_end"] >= end.isoformat()
    all_events = load_risk_events(event_csv, manifest_path)
    events = tuple(e for e in all_events if start <= e.first_fact_date <= end)
    calendar, days, calendar_id = _load_calendar(calendar_path, start, end)
    states = load_cutoff_st_evidence(manifest, event_csv, all_events, calendar)
    expected_days = {d for d in days if start <= d <= end}
    event_codes = {e.security_code.replace(".XSHG", ".SH").replace(".XSHE", ".SZ")
                   for e in events}
    records = {}
    daily_counts = {}
    direct_lookup = {}
    overrides_added = Counter()
    overrides_changed = Counter()
    inputs = []
    for path in sorted(history_dir.glob("集中度分类_*.csv")):
        inputs.append({"file": str(path.resolve()), "sha256": sha256(path)})
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            assert reader.fieldnames == ["biz_date", "stk_code", "证券名称", "券商03"]
            for day_text, rows in groupby(reader, key=lambda row: row["biz_date"]):
                day = date.fromisoformat(day_text)
                if not start <= day <= end:
                    continue
                assert day in expected_days and day not in daily_counts, day
                grades = {}
                for row in rows:
                    code = row["stk_code"]
                    assert code not in grades, (day, code)
                    assert code.endswith((".SH", ".SZ")), code
                    assert row["券商03"] in set("ABCDEFG"), row
                    grades[code] = row["券商03"]
                for code, grade in OVERRIDES.items():
                    if code not in grades:
                        overrides_added[code] += 1
                    elif grades[code] != grade:
                        overrides_changed[code] += 1
                    grades[code] = grade
                daily_counts[day] = Counter(grades.values())
                for code in event_codes & grades.keys():
                    market, security = normalize_security(code)
                    direct_lookup[(day, market, security)] = grades[code]
                    records[(day, market, security)] = BrokerClassificationRecord(
                        "券商03", market, security, day,
                        calendar.next_trading_day("cn", day), grades[code],
                        source_file=path.name, source_record_id=f"{day}:{code}")
    assert set(daily_counts) == expected_days, "分类日期与日历不一致"

    # 所有事件的评价截止日必须具有分类与清单内的ST证据。
    for event in events:
        cutoff = calendar.previous_trading_day("cn", event.first_fact_date)
        assert event.event_key in states, event.event_key
        assert (cutoff, event.market_code, event.security_code) in direct_lookup, event.event_key

    rules_path = MODULE / "configs/rules/approved/rules_v6.yaml"
    mapping_path = MODULE / "configs/broker_mapping/broker_mapping_v3.yaml"
    rules = load_broker_metric_rules(rules_path)
    mapping = load_broker_mapping(mapping_path)
    print("正在计算风险命中情况和档位汇总。", file=sys.stderr, flush=True)
    assessments = assess_cutoff_events(events, records, states, calendar, mapping, rules, calendar_id)

    # 独立复核：直接查前一交易日日表，逐事件验证原始档位与F/G命中规则。
    direct_groups = Counter()
    for item in assessments:
        event = item.event
        day = calendar.previous_trading_day("cn", event.first_fact_date)
        grade = direct_lookup[(day, event.market_code, event.security_code)]
        expected_hit = states[event.event_key] or grade in {"F", "G"}
        assert item.selected_record.raw_classification == grade
        assert item.selected_record.classification_date == day
        assert item.is_hit is expected_hit
        direct_groups[grade] += 1

    annual = tuple(a for a in assessments if a.event.first_fact_date >= table8_start)
    table8 = risk_hit_rates(annual, metric_rule_version=rules.rule_version)
    if not annual:
        table8 = [{"broker_id": "券商03", "event_type_view": view,
                   "risk_event_count": 0, "hit_event_count": 0, "unknown_event_count": 0,
                   "risk_security_count": 0, "hit_risk_security_count": 0,
                   "event_hit_rate": None, "security_hit_rate": None,
                   "result_status": "NOT_APPLICABLE"} for view in EVENT_NAMES]
    order = {"ALL_RISK_EVENTS": 0, "NEW_ST": 1, "NON_ST_CONTINUOUS_LIMIT_DOWN": 2}
    table8.sort(key=lambda row: order[row["event_type_view"]])
    for row in table8:
        group = [a for a in annual if row["event_type_view"] == "ALL_RISK_EVENTS"
                 or a.event.event_type == row["event_type_view"]]
        hit_codes = {a.event.security_code for a in group if a.is_hit}
        assert len(hit_codes) == row["hit_risk_security_count"]
        row["event_hit_rate_display"] = percent(row["hit_event_count"], row["risk_event_count"])
        row["security_hit_rate_display"] = percent(row["hit_risk_security_count"], row["risk_security_count"])

    summary9, details = single_grade_event_artifacts(assessments, metric_rule_version=rules.rule_version)
    selected9 = {r["grade"]: r for r in summary9 if r["event_type_view"] == "ALL_RISK_EVENTS"}
    totals = Counter()
    for counts in daily_counts.values():
        totals.update(counts)
    table9 = []
    for grade in "ABCDEFG":
        count = selected9[grade]["risk_event_count"]
        assert count == direct_groups[grade]
        average = Decimal(totals[grade]) / Decimal(len(expected_days))
        table9.append({"grade": grade, "risk_event_count": count,
                       "stock_days": totals[grade], "trading_days": len(expected_days),
                       "daily_average": str(average),
                       "daily_average_display": str(average.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
                       "ratio_display": percent(count * len(expected_days), totals[grade])})
    assert sum(r["risk_event_count"] for r in table9) == len(events)
    assert selected9["UNKNOWN"]["risk_event_count"] == selected9["BLANK"]["risk_event_count"] == 0
    assert all(r["unknown_event_count"] == 0 for r in table8)
    return {
        "table8_period": [str(table8_start), str(end)], "table9_period": [str(start), str(end)],
        "table8": table8, "table9": table9,
        "table9_event_count": len(events),
        "table9_event_types": dict(Counter(e.event_type for e in events)),
        "overrides": OVERRIDES, "override_added_stock_days": dict(overrides_added),
        "override_changed_stock_days": dict(overrides_changed),
        "st_override_events": sum(a.classification_resolution == "ST_OVERRIDE" for a in assessments),
        "validation": "PASS: all events reconciled to prior-day classification; no missing grade or cutoff ST state",
        "source_event_batch": batch_name, "history_inputs": inputs,
        "event_sha256": sha256(event_csv), "event_manifest_sha256": sha256(manifest_path),
        "calendar_sha256": sha256(calendar_path), "st_evidence_source": str(manifest_path.resolve()),
        "st_evidence_method": manifest["cutoff_st_evidence"]["method"],
        "warning_days_scope": "NOT_CALCULATED: 风险股票拦截、风险股票分档不使用预警天数",
        "rules_sha256": sha256(rules_path), "mapping_sha256": sha256(mapping_path),
        "event_details": details,
    }


def display_width(value):
    return sum(2 if unicodedata.east_asian_width(char) in {"W", "F"} else 1
               for char in str(value))


def print_table(headers, rows):
    """按中文终端显示宽度对齐；长表头可分两行，数值列右对齐。"""
    cells = [[str(value).split("\n") for value in row] for row in [headers, *rows]]
    widths = [max(display_width(line) for row in cells for line in row[column])
              for column in range(len(headers))]
    separator = "-+-".join("-" * width for width in widths)
    for row_index, row in enumerate(cells):
        for line_index in range(max(map(len, row))):
            pieces = []
            for column, lines in enumerate(row):
                value = lines[line_index] if line_index < len(lines) else ""
                padding = " " * (widths[column] - display_width(value))
                pieces.append(value + padding if column == 0 else padding + value)
            print(" | ".join(pieces))
        if row_index == 0:
            print(separator)


def report_rows(result):
    table8 = [[EVENT_NAMES[row["event_type_view"]], row["risk_event_count"],
               row["hit_event_count"], row["event_hit_rate_display"],
               row["risk_security_count"], row["hit_risk_security_count"],
               row["security_hit_rate_display"]] for row in result["table8"]]
    table9 = [[row["grade"], row["risk_event_count"], row["daily_average_display"],
               row["ratio_display"]] for row in result["table9"]]
    return table8, table9


def write_csv(path, headers, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def save_reports(result, output_root):
    # 每次运行生成新目录，保留以往结果；时间由程序处理，无需终端设置变量。
    target = output_root / datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    audit = target / "核对材料"
    audit.mkdir(parents=True, exist_ok=False)
    table8, table9 = report_rows(result)
    write_csv(target / "风险股票拦截_股票打分.csv",
              ["风险类型", "风险事件数", "命中风险事件数", "事件命中率",
               "风险股票数", "命中风险股票数", "证券命中率"], table8)
    write_csv(target / "风险股票分档_股票打分.csv",
              ["股票打分档位", "区间风险事件数量", "日均档位数量", "占比"], table9)
    details = [[row["event_key"], row["security_code"], EVENT_NAMES[row["event_type"]],
                row["first_fact_date"], row["risk_date"], row["selected_classification_date"],
                row["grade"], "是" if row["is_hit"] is True else "否" if row["is_hit"] is False else "未知",
                "是" if result["table8_period"][0] <= row["first_fact_date"] <= result["table8_period"][1] else "否"]
               for row in result["event_details"]]
    write_csv(audit / "风险事件评价明细.csv",
              ["事件编号", "证券代码", "风险类型", "首次事实日期", "风险认定日期",
               "分类取值日期", "模型档位", "是否命中", "计入风险股票拦截"], details)
    record = {key: value for key, value in result.items() if key != "event_details"}
    record["completed_at"] = datetime.now().astimezone().isoformat()
    record["event_details_file"] = "风险事件评价明细.csv"
    record["method"] = {
        "classification_date": "首次事实日前一交易日",
        "table9_daily_average": "区间各交易日档位股票数之和 / 交易日数",
        "table9_ratio": "区间风险事件数 / 未取整日均档位股票数",
        "overrides": "990001.SZ、990002.SZ全期按G，缺失日期补入G档日均数量",
    }
    (audit / "运行记录.json").write_text(
        json.dumps(record, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    return target


def print_report(result, target, extra_json=None):
    table8, table9 = report_rows(result)
    print("\n股票打分模型计算完成\n")
    print(f"风险股票拦截：风险股票拦截情况（{' 至 '.join(result['table8_period'])}）")
    print_table(["风险类型", "风险\n事件数", "命中风险\n事件数", "事件\n命中率",
                 "风险\n股票数", "命中风险\n股票数", "证券\n命中率"], table8)
    print(f"\n风险股票分档：风险股票分档情况（{' 至 '.join(result['table9_period'])}）")
    print_table(["档位", "区间风险事件数", "日均股票数", "占比"], table9)
    print(f"共 {result['table9'][0]['trading_days']} 个交易日，{result['table9_event_count']} 个风险事件。")
    print("占比按未取整的日均数量计算；990001、990002全期按G并补齐缺失日期。")
    print("校验通过：事件分类、命中结果和汇总数量一致。")
    print(f"\n结果目录：{target.resolve()}")
    print("  风险股票拦截_股票打分.csv\n  风险股票分档_股票打分.csv")
    print("  核对材料/风险事件评价明细.csv\n  核对材料/运行记录.json")
    if extra_json:
        print(f"额外完整JSON：{extra_json.resolve()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=MODULE / "scoring_tables_8_9.json",
                        help="输入与输出路径配置；配置中的相对路径以配置文件目录为基准")
    parser.add_argument("--history-dir", type=Path, help="覆盖配置中的逐日分类目录")
    parser.add_argument("--event-dir", type=Path, help="覆盖配置中的跨年度风险批次目录")
    parser.add_argument("--calendar", type=Path, help="覆盖配置中的交易日历")
    parser.add_argument("--output-dir", type=Path, help="覆盖配置中的结果根目录，每次运行自动创建新子目录")
    parser.add_argument("--end-date", type=date.fromisoformat, default=END, help="本次截止日期；风险股票分档仍从2025-01-15开始，风险股票拦截从截止日所在年份年初开始")
    parser.add_argument("--output-json", type=Path, help="兼容旧命令：额外保存完整JSON，不在终端展开")
    args = parser.parse_args()
    try:
        config = json.loads(args.config.read_text(encoding="utf-8-sig"))
        paths = {}
        for field in ("history_dir", "event_dir", "calendar", "output_dir"):
            override = getattr(args, field)
            if override is not None:
                paths[field] = override.resolve()
            else:
                value = config.get(field)
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(f"配置缺少有效路径：{field}")
                paths[field] = (args.config.resolve().parent / value).resolve()
    except (OSError, ValueError, AttributeError) as exc:
        parser.error(f"配置读取失败：{exc}")
    print("正在计算风险股票拦截、风险股票分档，请稍候。", file=sys.stderr, flush=True)
    result = calculate(paths["history_dir"], paths["event_dir"], paths["calendar"], end=args.end_date)
    target = save_reports(result, paths["output_dir"])
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print_report(result, target, args.output_json)


if __name__ == "__main__":
    main()
