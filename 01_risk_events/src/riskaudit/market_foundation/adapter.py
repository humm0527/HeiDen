"""
文件作用：从不可变市场快照影子导出冻结五表契约中的四张市场中文标准表。
编辑记录：
【首次生成：2026-08-13，实现 P2 合成快照的日历、基础状态、行情和 ST 状态适配与清单。】
【二次编辑：2026-08-15，对齐正式 RQData 四表的证券键、公开来源、停牌占位和逐日 ST 语义。】
"""

from __future__ import annotations

import csv
from datetime import date, datetime, time, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable

from .facts import canonical_json
from .service import MarketFoundationError, MarketFoundationService, _atomic_json


TABLE_FILES = {
    "交易日历表": "交易日历表.csv",
    "证券基础状态日表": "证券基础状态日表.csv",
    "股票日行情表": "股票日行情表.csv",
    "证券风险状态表": "证券风险状态表.csv",
}


class FiveTableShadowAdapter:
    """Read a frozen snapshot record set; never reads drifting current views."""

    def __init__(self, service: MarketFoundationService) -> None:
        self.service = service

    def export(
        self,
        snapshot_id: str,
        output_root: str | Path | None = None,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        snapshot = self.service.get_snapshot(snapshot_id)
        if snapshot["status"] != "PUBLISHED":
            raise MarketFoundationError("MDF010", "只有已发布快照可执行影子导出")
        if (start_date is None) != (end_date is None) or (
            start_date is not None and end_date is not None and start_date > end_date
        ):
            raise MarketFoundationError("MDF010", "影子导出日期范围无效")
        scope_suffix = (
            f"_{start_date:%Y%m%d}_{end_date:%Y%m%d}"
            if start_date is not None and end_date is not None
            else ""
        )
        target = (
            Path(output_root or self.service.root / "exports")
            / snapshot_id
            / f"five_table_shadow{scope_suffix}"
        )
        if target.exists() and any(target.iterdir()):
            raise FileExistsError(f"影子导出目录已存在且不可覆盖：{target}")
        target.mkdir(parents=True, exist_ok=True)
        manifest_tables: dict[str, Any] = {}
        calendar = self._calendar(snapshot_id, start_date, end_date)
        table_builders = (
            ("交易日历表", lambda: calendar),
            (
                "证券基础状态日表",
                lambda: self._security_status(
                    snapshot_id, calendar, start_date, end_date
                ),
            ),
            (
                "股票日行情表",
                lambda: self._market_daily(snapshot_id, start_date, end_date),
            ),
            (
                "证券风险状态表",
                lambda: self._risk_status(snapshot_id, start_date, end_date),
            ),
        )
        for table_name, build_rows in table_builders:
            rows = build_rows()
            path = target / TABLE_FILES[table_name]
            _write_csv(path, rows)
            manifest_tables[table_name] = {
                "path": str(path),
                "row_count": len(rows),
                "sha256": sha256(path.read_bytes()).hexdigest(),
                "columns": list(rows[0]) if rows else [],
            }
            del rows
        manifest = {
            "adapter_version": "market_foundation_shadow_v1",
            "market_snapshot_id": snapshot_id,
            "market_snapshot_member_hash": snapshot["member_hash"],
            "execution_mode": snapshot["execution_mode"],
            "risk_adapter_status": "NOT_RUN",
            "export_scope": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
            },
            "tables": manifest_tables,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        _atomic_json(target / "shadow_manifest.json", manifest)
        return manifest

    def _snapshot_rows(
        self,
        snapshot_id: str,
        dataset_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        date_filter = ""
        parameters: list[Any] = [snapshot_id, dataset_id]
        if start_date is not None and end_date is not None and dataset_id != "instrument_master_history":
            date_filter = """
              AND CAST(json_extract_string(source.payload_json, '$.business_date') AS DATE)
                  BETWEEN ? AND ?
            """
            parameters.extend([start_date, end_date])
        return self.service.catalog.rows(
            f"""
            SELECT source.business_key, source.source_record_version,
                   source.payload_hash, source.payload_json,
                   source.source_received_at, source.source_request_id
            FROM market_snapshot_record member
            JOIN source_record_catalog source
              ON source.dataset_id = member.dataset_id
             AND source.business_key = member.business_key
             AND source.source_record_version = member.source_record_version
             AND source.payload_hash = member.payload_hash
             AND source.partition_id = member.partition_id
            WHERE member.snapshot_id=? AND member.dataset_id=?
            {date_filter}
            ORDER BY source.business_key
            """,
            parameters,
        )

    def _calendar(
        self,
        snapshot_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        result = []
        for item in self._snapshot_rows(
            snapshot_id, "market_calendar", start_date, end_date
        ):
            payload = json.loads(item["payload_json"])
            current = date.fromisoformat(payload["business_date"])
            received = _iso_timestamp(item["source_received_at"])
            result.append(
                {
                    "交易市场代码": payload["market_code"],
                    "日历日期": current.isoformat(),
                    "记录版本号": item["source_record_version"],
                    "是否交易日": bool(payload["is_trading_day"]),
                    "交易时段状态": "正常开市" if payload["is_trading_day"] else "休市",
                    "日历生效时间": _day_start(current),
                    "信息可得时间": received,
                    "记录入库时间": received,
                    "来源类型": "公开",
                    "来源记录标识": item["source_request_id"],
                }
            )
        return result

    def _security_status(
        self,
        snapshot_id: str,
        calendar: list[dict[str, Any]],
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        calendar_dates: dict[str, list[date]] = {}
        for row in calendar:
            calendar_dates.setdefault(row["交易市场代码"], []).append(
                date.fromisoformat(row["日历日期"])
            )
        suspension = _payload_index(
            self._snapshot_rows(
                snapshot_id, "daily_suspension_status", start_date, end_date
            )
        )
        result = []
        for item in self._snapshot_rows(snapshot_id, "instrument_master_history"):
            payload = json.loads(item["payload_json"])
            listed = date.fromisoformat(payload["listing_date"])
            effective = date.fromisoformat(payload["effective_start"])
            terminated = (
                date.fromisoformat(payload["termination_date"])
                if payload.get("termination_date")
                else None
            )
            effective_end = (
                date.fromisoformat(payload["effective_end"])
                if payload.get("effective_end")
                else None
            )
            received = _iso_timestamp(item["source_received_at"])
            for current in calendar_dates.get(payload["market_code"], []):
                if current < effective or (effective_end and current > effective_end):
                    continue
                if current < listed:
                    continue
                lifecycle = "Delisted" if terminated and current >= terminated else "Active"
                suspended = suspension.get(
                    (payload["market_code"], payload["security_code"], current.isoformat()),
                    {},
                ).get("is_suspended", False)
                result.append(
                    {
                        "交易市场代码": payload["market_code"],
                        "证券代码": payload["instrument_key"],
                        "状态日期": current.isoformat(),
                        "记录版本号": item["source_record_version"],
                        "板块代码": payload["board_code"],
                        "证券类型": payload["security_type"],
                        "生命周期状态": lifecycle,
                        "上市日期": listed.isoformat(),
                        "退市日期": terminated.isoformat() if terminated else "",
                        "交易资格状态": (
                            "终止" if lifecycle == "Delisted" else "停牌" if suspended else "正常"
                        ),
                        "状态生效时间": _day_start(current),
                        "信息可得时间": received,
                        "记录入库时间": received,
                        "来源类型": "公开",
                        "来源记录标识": item["source_request_id"],
                    }
                )
        return sorted(
            result,
            key=lambda row: (row["交易市场代码"], row["证券代码"], row["状态日期"]),
        )

    def _market_daily(
        self,
        snapshot_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        prices = _record_index(
            self._snapshot_rows(
                snapshot_id, "daily_price_unadjusted", start_date, end_date
            )
        )
        suspension = _record_index(
            self._snapshot_rows(
                snapshot_id, "daily_suspension_status", start_date, end_date
            )
        )
        limits = _payload_index(
            self._snapshot_rows(
                snapshot_id, "daily_price_limits", start_date, end_date
            )
        )
        previous_close = self._previous_closes(snapshot_id, start_date)
        result = []
        keys = sorted(
            set(prices)
            | {
                key
                for key, value in suspension.items()
                if bool(value[0].get("is_suspended"))
            }
        )
        for key in keys:
            price_entry = prices.get(key)
            suspension_entry = suspension.get(key)
            payload = price_entry[0] if price_entry else suspension_entry[0]
            item = price_entry[1] if price_entry else suspension_entry[1]
            security_key = (payload["market_code"], payload["security_code"])
            suspended = bool(
                suspension_entry and suspension_entry[0].get("is_suspended", False)
            )
            limit = limits.get(key, {})
            received = _iso_timestamp(item["source_received_at"])
            result.append(
                {
                    "交易市场代码": payload["market_code"],
                    "证券代码": payload["instrument_key"],
                    "交易日期": payload["business_date"],
                    "行情版本号": item["source_record_version"],
                    "成交状态": "停牌" if suspended else "正常成交",
                    "前收盘价": previous_close.get(security_key, ""),
                    "开盘价": payload.get("open", "") if price_entry else "",
                    "最高价": payload.get("high", "") if price_entry else "",
                    "最低价": payload.get("low", "") if price_entry else "",
                    "收盘价": payload.get("close", "") if price_entry else "",
                    "成交量": payload.get("volume", "") if price_entry else "",
                    "成交额": payload.get("amount", "") if price_entry else "",
                    "涨停价": limit.get("limit_up", ""),
                    "跌停价": limit.get("limit_down", ""),
                    "信息可得时间": received,
                    "记录入库时间": received,
                    "来源类型": "公开",
                    "来源记录标识": item["source_request_id"],
                }
            )
            if price_entry and payload.get("close") is not None:
                previous_close[security_key] = payload.get("close", "")
        return result

    def _previous_closes(
        self, snapshot_id: str, start_date: date | None
    ) -> dict[tuple[str, str], Any]:
        if start_date is None:
            return {}
        rows = self.service.catalog.rows(
            """
            SELECT json_extract_string(source.payload_json, '$.market_code') AS market_code,
                   json_extract_string(source.payload_json, '$.security_code') AS security_code,
                   arg_max(
                       json_extract_string(source.payload_json, '$.close'),
                       CAST(json_extract_string(source.payload_json, '$.business_date') AS DATE)
                   ) AS previous_close
            FROM market_snapshot_record member
            JOIN source_record_catalog source
              ON source.dataset_id = member.dataset_id
             AND source.business_key = member.business_key
             AND source.source_record_version = member.source_record_version
             AND source.payload_hash = member.payload_hash
             AND source.partition_id = member.partition_id
            WHERE member.snapshot_id=?
              AND member.dataset_id='daily_price_unadjusted'
              AND CAST(json_extract_string(source.payload_json, '$.business_date') AS DATE) < ?
            GROUP BY market_code, security_code
            """,
            [snapshot_id, start_date],
        )
        return {
            (item["market_code"], item["security_code"]): item["previous_close"]
            for item in rows
        }

    def _risk_status(
        self,
        snapshot_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        result = []
        for item in self._snapshot_rows(
            snapshot_id, "daily_st_status", start_date, end_date
        ):
            payload = json.loads(item["payload_json"])
            current = date.fromisoformat(payload["business_date"])
            result.append(
                _risk_daily(
                    snapshot_id,
                    (payload["market_code"], payload["instrument_key"]),
                    current,
                    bool(payload["is_st"]),
                    item,
                )
            )
        return sorted(
            result,
            key=lambda row: (row["交易市场代码"], row["证券代码"], row["状态开始日期"]),
        )


def _risk_daily(
    snapshot_id: str,
    security_key: tuple[str, str],
    current: date,
    is_st: bool,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    stable = sha256(
        canonical_json([snapshot_id, *security_key, current.isoformat(), is_st]).encode("utf-8")
    ).hexdigest()[:24]
    received = _iso_timestamp(evidence["source_received_at"])
    return {
        "风险状态记录标识": f"mdst_{stable}",
        "记录版本号": evidence["source_record_version"],
        "交易市场代码": security_key[0],
        "证券代码": security_key[1],
        "风险状态类型": "ST",
        "风险状态值": "生效" if is_st else "未生效",
        "状态来源类别": "公开事实",
        "状态开始日期": current.isoformat(),
        "状态结束日期": current.isoformat(),
        "首次事实日期": "",
        "达到门槛日期": "",
        "风险认定日期": "",
        "状态生效时间": _day_start(current),
        "证据截止时间": received,
        "信息可得时间": received,
        "风险规则版本号": "",
        "计算批次标识": "",
        "上游数据快照标识": snapshot_id,
        "记录入库时间": received,
        "来源记录标识": evidence["source_request_id"],
    }


def _payload_index(rows: Iterable[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    result = {}
    for item in rows:
        payload = json.loads(item["payload_json"])
        result[_daily_key(payload)] = payload
    return result


def _record_index(
    rows: Iterable[dict[str, Any]],
) -> dict[tuple[str, str, str], tuple[dict[str, Any], dict[str, Any]]]:
    result = {}
    for item in rows:
        payload = json.loads(item["payload_json"])
        result[_daily_key(payload)] = (payload, item)
    return result


def _daily_key(payload: dict[str, Any]) -> tuple[str, str, str]:
    return payload["market_code"], payload["security_code"], payload["business_date"]


def _day_start(value: date) -> str:
    return datetime.combine(value, time.min, tzinfo=timezone.utc).isoformat()


def _iso_timestamp(value: Any) -> str:
    return value.isoformat() if isinstance(value, datetime) else str(value)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise MarketFoundationError("MDF010", f"影子导出表为空：{path.stem}")
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)
