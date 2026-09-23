"""Input loading and cross-source validation for real broker-metric runs."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping
from datetime import date
from pathlib import Path
import re

import pandas as pd

from riskaudit.cancellation import raise_if_canceled

from .calendar import MarketCalendar
from .history import discover_quarter_files, normalize_security
from .models import BrokerMetricFinding
from .real_run_support import _parse_date, _sha256


_QUARTER = re.compile(r"集中度(\d{4})Q([1-4])\.csv")


class CompleteStStates(Mapping):
    """压缩保存完整矩形 ST 覆盖，仅显式存储 True 键。"""

    def __init__(self, securities, dates, true_by_date, key_count):
        self.securities = frozenset(securities)
        self.dates = frozenset(dates)
        self.true_by_date = {
            key: frozenset(value) for key, value in true_by_date.items()
        }
        self.key_count = key_count

    def __getitem__(self, key):
        value = self.get(key, None)
        if value is None:
            raise KeyError(key)
        return value

    def __iter__(self):
        return iter(())

    def __len__(self):
        return self.key_count

    def get(self, key, default=None):
        market, code, trading_date = key
        security = (market, code)
        if security not in self.securities or trading_date not in self.dates:
            return default
        return security in self.true_by_date.get(trading_date, ())

def _resolve_history_sources(
    directory: str | Path,
    explicit_files: Mapping[str, str | Path] | None,
) -> tuple[tuple[str, Path], ...]:
    if explicit_files is None:
        return tuple((path.name, path) for path in discover_quarter_files(directory))
    parsed = []
    for name in explicit_files:
        match = _QUARTER.fullmatch(name)
        if match is None:
            raise ValueError(f"BM101_HISTORY_FILE_SET_MISMATCH: {sorted(explicit_files)}")
        year, quarter = int(match.group(1)), int(match.group(2))
        parsed.append((year * 4 + quarter - 1, name))
    parsed.sort()
    ordinals = [item[0] for item in parsed]
    if not ordinals or ordinals != list(range(ordinals[0], ordinals[-1] + 1)):
        raise ValueError(
            "BM101_HISTORY_FILE_SET_MISMATCH: non-contiguous quarters "
            f"{[item[1] for item in parsed]}"
        )
    sources = tuple((name, Path(explicit_files[name])) for _, name in parsed)
    missing = [str(path) for _, path in sources if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"BM101_HISTORY_FILE_SET_MISMATCH: {missing}")
    return sources

def _load_calendar(path: str | Path, start: date, end: date):
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    if {"交易日期", "交易日序号"} <= set(frame.columns):
        dates = tuple(date.fromisoformat(value[:10]) for value in frame["交易日期"])
    elif {"交易市场代码", "日历日期", "是否交易日"} <= set(frame.columns):
        market_codes = frame["交易市场代码"].astype(str).str.upper()
        selected = frame.loc[
            market_codes.isin({"CN", "XSHG", "XSHE"})
            & frame["是否交易日"].str.lower().isin({"true", "1"}),
            "日历日期",
        ]
        dates = tuple(
            sorted({date.fromisoformat(value[:10]) for value in selected})
        )
    elif {"market_code", "calendar_date", "is_trading_day"} <= set(frame.columns):
        market_codes = frame["market_code"].astype(str).str.upper()
        selected = frame.loc[
            market_codes.isin({"CN", "XSHG", "XSHE"})
            & frame["is_trading_day"].str.lower().isin({"true", "1"}),
            "calendar_date",
        ]
        dates = tuple(
            sorted({date.fromisoformat(value[:10]) for value in selected})
        )
    else:
        raise ValueError("BM201_CALENDAR_COVERAGE_MISSING: schema")
    if (
        dates != tuple(sorted(set(dates)))
        or not dates
        or dates[0] > start
        or dates[-1] < end
    ):
        raise ValueError("BM201_CALENDAR_COVERAGE_MISSING: range/order")
    return MarketCalendar({"cn": dates}), dates, f"sha256:{_sha256(path)}"

def _load_st_states(
    path: str | Path,
    calendar_dates: tuple[date, ...],
    end: date,
    *,
    start: date | None = None,
    lifecycle=None,
    cancel_check: Callable[[], bool] | None = None,
):
    if start is not None:
        calendar_dates = tuple(day for day in calendar_dates if start <= day <= end)
    header = tuple(
        pd.read_csv(
            path,
            nrows=0,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
        ).columns
    )
    standard_columns = {
        "market_code",
        "security_code",
        "risk_status_type",
        "risk_status_value",
        "status_start_date",
    }
    if standard_columns <= set(header):
        return _load_snapshot_st_states(
            path,
            calendar_dates,
            end,
            lifecycle=lifecycle,
            cancel_check=cancel_check,
        )
    allowed_dates = set(calendar_dates)
    securities = set()
    true_by_date = defaultdict(set)
    date_counts = defaultdict(int)
    key_count = 0
    expected_columns = ("交易日期", "米筐代码", "证券代码", "是否ST")
    for frame in pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
        chunksize=200_000,
    ):
        raise_if_canceled(cancel_check)
        if tuple(frame.columns) != expected_columns:
            raise ValueError("BM203_ST_STATUS_MISSING: schema")
        if frame.duplicated(["交易日期", "米筐代码"]).any():
            raise ValueError("BM203_ST_STATUS_MISSING: duplicate")
        for day_text, code, value in frame.loc[:, ["交易日期", "米筐代码", "是否ST"]].itertuples(
            index=False, name=None
        ):
            trading_date = date.fromisoformat(day_text[:10])
            if trading_date not in allowed_dates or not code.endswith(
                (".XSHG", ".XSHE")
            ):
                raise ValueError("BM203_ST_STATUS_MISSING: date/market")
            if value not in {"True", "False"}:
                raise ValueError("BM203_ST_STATUS_MISSING: invalid value")
            market = code.rsplit(".", 1)[-1]
            security = (market, code)
            securities.add(security)
            date_counts[trading_date] += 1
            key_count += 1
            if value == "True":
                true_by_date[trading_date].add(security)
    if not date_counts or max(date_counts) < end:
        raise ValueError("BM203_ST_STATUS_MISSING: date coverage")
    if set(date_counts) != allowed_dates or any(
        count != len(securities) for count in date_counts.values()
    ):
        raise ValueError("BM203_ST_STATUS_MISSING: incomplete rectangular coverage")
    return (
        CompleteStStates(securities, allowed_dates, true_by_date, key_count),
        securities,
    )

def _load_snapshot_st_states(
    path: str | Path,
    calendar_dates: tuple[date, ...],
    end: date,
    *,
    lifecycle=None,
    cancel_check: Callable[[], bool] | None = None,
):
    allowed_dates = set(calendar_dates)
    securities = set()
    values_by_security = defaultdict(dict)
    for frame in pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
        chunksize=200_000,
    ):
        raise_if_canceled(cancel_check)
        frame = frame.loc[
            (frame["risk_status_type"].str.upper() == "ST")
            & frame["market_code"].isin({"XSHG", "XSHE"})
        ]
        if frame.duplicated(["status_start_date", "security_code"]).any():
            raise ValueError("BM203_ST_STATUS_MISSING: duplicate")
        for market, code, day_text, value in frame.loc[
            :,
            [
                "market_code",
                "security_code",
                "status_start_date",
                "risk_status_value",
            ],
        ].itertuples(index=False, name=None):
            trading_date = date.fromisoformat(day_text[:10])
            if trading_date not in allowed_dates:
                continue
            if value not in {"生效", "未生效", "True", "False"}:
                raise ValueError("BM203_ST_STATUS_MISSING: invalid value")
            security = (market, code)
            securities.add(security)
            values_by_security[security][trading_date] = value in {"生效", "True"}
    if not values_by_security or max(
        day for values in values_by_security.values() for day in values
    ) < end:
        raise ValueError("BM203_ST_STATUS_MISSING: date coverage")
    true_by_date = defaultdict(set)
    key_count = 0
    if lifecycle is not None:
        for security, values in values_by_security.items():
            if security not in lifecycle:
                raise ValueError(
                    f"BM204_LIFECYCLE_EVIDENCE_MISSING: {security[1]}"
                )
            listed, delisted = lifecycle[security]
            expected = {
                day
                for day in allowed_dates
                if listed <= day and (delisted is None or day <= delisted)
            }
            state = None
            for day in sorted(expected):
                if day in values:
                    state = values[day]
                if state is None:
                    raise ValueError(
                        "BM203_ST_STATUS_MISSING: lifecycle baseline "
                        f"{security[1]} {day.isoformat()}"
                    )
                key_count += 1
                if state:
                    true_by_date[day].add(security)
    else:
        for security, values in values_by_security.items():
            first = min(values)
            state = None
            for day in calendar_dates:
                if day < first:
                    continue
                if day in values:
                    state = values[day]
                if state is None:
                    continue
                key_count += 1
                if state:
                    true_by_date[day].add(security)
    if key_count == 0:
        raise ValueError("BM203_ST_STATUS_MISSING: lifecycle coverage")
    return (
        CompleteStStates(securities, allowed_dates, true_by_date, key_count),
        securities,
    )

def _load_lifecycle(path: str | Path):
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    legacy = {"order_book_id", "listed_date_norm", "de_listed_date_norm"} <= set(
        frame.columns
    )
    standard = {"market_code", "security_code", "listed_date", "delisted_date"} <= set(
        frame.columns
    )
    if not legacy and not standard:
        raise ValueError("BM204_LIFECYCLE_EVIDENCE_MISSING: schema")
    output = {}
    for row in frame.to_dict(orient="records"):
        code = (
            row["order_book_id"].strip()
            if legacy
            else row["security_code"].strip()
        )
        if not code.endswith((".XSHG", ".XSHE")):
            continue
        market = code.rsplit(".", 1)[-1]
        listed = _parse_date(
            row["listed_date_norm"] or row.get("listed_date", "")
            if legacy
            else row["listed_date"]
        )
        delisted = _parse_date(
            row["de_listed_date_norm"] or row.get("de_listed_date", "")
            if legacy
            else row["delisted_date"],
            optional=True,
        )
        if listed is None:
            raise ValueError(f"BM204_LIFECYCLE_EVIDENCE_MISSING: {code}")
        previous = output.get((market, code))
        value = (listed, delisted)
        if previous is not None and previous != value:
            raise ValueError(f"BM204_LIFECYCLE_EVIDENCE_MISSING: conflict {code}")
        output[(market, code)] = value
    return output

def _reconcile_history_st_securities(
    history_securities,
    st_securities,
    lifecycle,
    observation_start: date,
    observation_end: date,
    *,
    allow_incomplete_history: bool = False,
) -> tuple[BrokerMetricFinding, ...]:
    """Require ST coverage for history while auditing harmless ST-only rows."""
    missing_st = set(history_securities) - set(st_securities)
    missing_st_lifecycle = missing_st - set(lifecycle)
    if missing_st_lifecycle:
        raise ValueError(
            "BM204_LIFECYCLE_EVIDENCE_MISSING: history-only securities "
            f"{sorted(missing_st_lifecycle)[:10]}"
        )
    in_scope_missing_st = {
        security
        for security in missing_st
        if lifecycle[security][0] <= observation_end
        and (
            lifecycle[security][1] is None
            or lifecycle[security][1] >= observation_start
        )
    }
    if in_scope_missing_st:
        raise ValueError(
            "BM203_ST_STATUS_MISSING: history securities missing from ST input "
            f"count={len(in_scope_missing_st)} "
            f"examples={sorted(in_scope_missing_st)[:10]}"
        )

    st_only = set(st_securities) - set(history_securities)
    missing_lifecycle = st_only - set(lifecycle)
    if missing_lifecycle:
        raise ValueError(
            "BM204_LIFECYCLE_EVIDENCE_MISSING: ST-only securities "
            f"{sorted(missing_lifecycle)[:10]}"
        )

    in_scope = {
        security
        for security in st_only
        if lifecycle[security][0] <= observation_end
        and (
            lifecycle[security][1] is None
            or lifecycle[security][1] >= observation_start
        )
    }
    if in_scope and not allow_incomplete_history:
        raise ValueError(
            "BM101_HISTORY_FILE_SET_MISMATCH: in-scope ST securities "
            f"absent from history count={len(in_scope)} "
            f"examples={sorted(in_scope)[:10]}"
        )

    findings = []
    for market_code, security_code in sorted(missing_st - in_scope_missing_st):
        listed, delisted = lifecycle[(market_code, security_code)]
        findings.append(
            BrokerMetricFinding(
                "BM108_HISTORY_ONLY_SECURITY_OUTSIDE_OBSERVATION",
                "INFO",
                (
                    "Broker history contains a security outside the observation "
                    f"lifecycle and absent from ST input; excluded "
                    f"(listed={listed.isoformat()}, "
                    f"delisted={delisted.isoformat() if delisted else ''})"
                ),
                market_code=market_code,
                security_code=security_code,
                fact_date=observation_end.isoformat(),
                source_file="history/ST/lifecycle reconciliation",
            )
        )
    for market_code, security_code in sorted(st_only - in_scope):
        listed, delisted = lifecycle[(market_code, security_code)]
        findings.append(
            BrokerMetricFinding(
                "BM107_ST_ONLY_SECURITY_OUTSIDE_OBSERVATION",
                "INFO",
                (
                    "ST input contains a security outside the observation "
                    f"lifecycle; excluded from broker history reconciliation "
                    f"(listed={listed.isoformat()}, "
                    f"delisted={delisted.isoformat() if delisted else ''})"
                ),
                market_code=market_code,
                security_code=security_code,
                fact_date=observation_end.isoformat(),
                source_file="ST/lifecycle reconciliation",
            )
        )
    if allow_incomplete_history:
        for market_code, security_code in sorted(in_scope):
            findings.append(
                BrokerMetricFinding(
                    "BM109_SECURITY_HISTORY_MISSING",
                    "WARNING",
                    (
                        "Security is active in the observation window but absent "
                        "from the selected legacy broker history; its broker "
                        "classification, event assessment and exposure are UNKNOWN"
                    ),
                    market_code=market_code,
                    security_code=security_code,
                    fact_date=observation_end.isoformat(),
                    source_file="history/ST/lifecycle reconciliation",
                )
            )
    return tuple(findings)

def _load_security_names(path: str | Path) -> dict[tuple[str, str], str]:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    if {"order_book_id", "symbol"} <= set(frame.columns):
        code_column = "order_book_id"
        name_column = "symbol"
    elif {"market_code", "security_code"} <= set(frame.columns):
        code_column = "security_code"
        name_column = None
    else:
        raise ValueError("BM204_LIFECYCLE_EVIDENCE_MISSING: security name schema")
    output = {}
    for row in frame.to_dict(orient="records"):
        code = row[code_column].strip()
        if code.endswith((".XSHG", ".XSHE")):
            output[(code.rsplit(".", 1)[-1], code)] = (
                row[name_column].strip() if name_column else code
            )
    return output

def _load_metric_universe(path: str | Path, observation_end: date):
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    required = {"classification_date", "market_code", "security_code"}
    if not required <= set(frame.columns):
        raise ValueError("BM001_EVENT_MANIFEST_MISMATCH: universe schema")
    classification_dates = frame["classification_date"].map(
        lambda value: date.fromisoformat(value[:10])
    )
    if not (classification_dates == observation_end).any():
        raise ValueError("BM001_EVENT_MANIFEST_MISMATCH: universe cutoff")
    cutoff_rows = frame.loc[classification_dates <= observation_end]
    universe = {
        (market, code)
        for market, code in cutoff_rows.loc[
            :, ["market_code", "security_code"]
        ].itertuples(
            index=False, name=None
        )
        if market in {"XSHG", "XSHE"}
    }
    if not universe:
        raise ValueError("BM001_EVENT_MANIFEST_MISMATCH: empty metric universe")
    return universe

def _read_history(
    sources,
    broker_ids,
    event_securities,
    calendar,
    start,
    end,
    chunksize,
    *,
    require_end_coverage: bool = False,
    cancel_check: Callable[[], bool] | None = None,
):
    selected_columns = ("biz_date", "stk_code", *broker_ids)
    event_codes = {code for _, code in event_securities}
    securities = set()
    event_rows = defaultdict(list)
    daily_rows = defaultdict(dict)
    initial_rows = {}
    initial_dates = {}
    file_stats = {}
    findings = []
    total_rows = weekend_rows = weekend_eligible = weekend_nonblank = 0
    unique_key_count = 0
    weekend_dates = set()
    latest_trading_date = None
    for logical_name, path in sources:
        raise_if_canceled(cancel_check)
        match = _QUARTER.fullmatch(logical_name)
        if match is None:
            raise ValueError(f"BM101_HISTORY_FILE_SET_MISMATCH: {logical_name}")
        year, quarter = int(match.group(1)), int(match.group(2))
        file_rows = file_weekend = file_eligible = file_nonblank = 0
        file_min = file_max = None
        source_hash = _sha256(path)
        row_number = 1
        file_seen = set()
        for chunk in pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            chunksize=chunksize,
            encoding="utf-8-sig",
        ):
            raise_if_canceled(cancel_check)
            if tuple(chunk.columns[:2]) != ("biz_date", "stk_code") or not set(
                broker_ids
            ) <= set(chunk.columns):
                raise ValueError(f"BM102_HISTORY_SCHEMA_MISMATCH: {logical_name}")
            chunk = chunk.loc[:, list(selected_columns)]
            for row in chunk.itertuples(index=False, name=None):
                row_number += 1
                file_rows += 1
                biz_date = date.fromisoformat(row[0][:10])
                if (
                    biz_date.year != year
                    or not (quarter - 1) * 3 + 1 <= biz_date.month <= quarter * 3
                ):
                    raise ValueError(
                        f"BM104_HISTORY_QUARTER_DATE_INVALID: {logical_name}"
                    )
                file_min = biz_date if file_min is None else min(file_min, biz_date)
                file_max = biz_date if file_max is None else max(file_max, biz_date)
                raw_code = row[1].strip()
                key = (biz_date, raw_code)
                if key in file_seen:
                    raise ValueError(f"BM103_HISTORY_KEY_DUPLICATE: {key}")
                file_seen.add(key)
                if biz_date.weekday() >= 5:
                    file_weekend += 1
                    weekend_dates.add(biz_date)
                    if raw_code.endswith((".SH", ".SZ")):
                        file_eligible += 1
                        file_nonblank += sum(bool(value.strip()) for value in row[2:])
                    continue
                if not raw_code.endswith((".SH", ".SZ")):
                    continue
                market, code = normalize_security(raw_code)
                securities.add((market, code))
                if not calendar.contains("cn", biz_date):
                    raise ValueError(
                        f"BM104_HISTORY_NON_TRADING_DATE_INVALID: {biz_date}"
                    )
                latest_trading_date = (
                    biz_date
                    if latest_trading_date is None
                    else max(latest_trading_date, biz_date)
                )
                values = tuple(value.strip() for value in row[2:])
                if start <= biz_date <= end:
                    daily_rows[biz_date][(market, code)] = values
                elif biz_date < start:
                    security = (market, code)
                    if security not in initial_dates or biz_date > initial_dates[security]:
                        initial_rows[security] = values
                        initial_dates[security] = biz_date
                if code in event_codes:
                    event_rows[(market, code)].append(
                        (biz_date, values, logical_name, source_hash, row_number)
                    )
        file_stats[logical_name] = {
            "row_count": file_rows,
            "date_min": file_min.isoformat() if file_min else "",
            "date_max": file_max.isoformat() if file_max else "",
            "weekend_row_count": file_weekend,
            "eligible_market_weekend_row_count": file_eligible,
            "weekend_nonblank_broker_cell_count": file_nonblank,
        }
        total_rows += file_rows
        unique_key_count += len(file_seen)
        weekend_rows += file_weekend
        weekend_eligible += file_eligible
        weekend_nonblank += file_nonblank
        if file_weekend:
            findings.append(
                BrokerMetricFinding(
                    "BM106_WEEKEND_HISTORY_ROW_IGNORED",
                    "INFO",
                    f"Ignored {file_weekend} weekend rows before the state chain",
                    source_file=logical_name,
                )
            )
        del file_seen
    if require_end_coverage:
        expected_end = calendar.through("cn", end)[-1]
        if latest_trading_date != expected_end:
            actual = latest_trading_date.isoformat() if latest_trading_date else "NONE"
            raise ValueError(
                "BM105_HISTORY_COVERAGE_MISSING: "
                f"expected {expected_end.isoformat()}, got {actual}"
            )
    return {
        "row_count": total_rows,
        "unique_key_count": unique_key_count,
        "securities": securities,
        "event_rows": event_rows,
        "daily_rows": daily_rows,
        "initial_rows": initial_rows,
        "initial_dates": initial_dates,
        "file_stats": file_stats,
        "findings": tuple(findings),
        "weekend_row_count": weekend_rows,
        "weekend_eligible_row_count": weekend_eligible,
        "weekend_nonblank_count": weekend_nonblank,
        "weekend_dates": weekend_dates,
        "latest_trading_date": latest_trading_date,
    }
