"""
文件作用：定义 RQData 客户端协议、真实/模拟客户端及确定性原始数据适配逻辑。
编辑记录：
【首次生成：2026-08-04，建立无凭证客户端协议、模拟客户端和米筐响应适配器。】
【二次编辑内容：2026-08-05，增加从环境变量初始化的真实 RQData 客户端和停牌独立 Raw 支持。】
【三次改进：2026-08-05，按截止日读取历史证券快照，并修正多日日期与 PIT 生效时间。】
【四次编辑：2026-08-06，允许生命周期与观察区间相交的退市证券通过截止日快照回退门禁。】
【五次编辑：2026-08-06，拒绝无法解析的非空生命周期日期，避免错误放行。】
【六次编辑：2026-08-06，将独立停牌事实确定性展开为成交状态为停牌的标准行情行。】
【七次改进：2026-08-06，将 RQData 无行情时返回的 None 规范化为空记录列表。】
【八次编辑：2026-08-06，将终止上市日统一为不可交易的生命周期右开边界。】
【九次编辑：2026-08-11，增加证券并集预对账、非重叠生命周期保留和观察期前最近交易日 ST 基线请求。】
【十次编辑：2026-08-12，Windows 直连被策略拒绝时允许使用显式配置的本机代理回退。】
【十一次编辑：2026-09-13，使观察期前基线日同时覆盖 ST 与交易日历，保证下游 PIT 能定位首日事件的前一交易日。】
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Sequence

from .rqdata_client import (
    InstrumentUniverseReconciliation,
    MockRQDataClient,
    RealRQDataClient,
    RQDataClientProtocol,
    RQDataCredentialError,
    RQDataSDKUnavailableError,
)


class RQDataAdapter:
    """Fetch RQData resources as JSON-safe bundles without changing raw fields."""

    PRICE_FIELDS = (
        "prev_close",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "total_turnover",
        "limit_up",
        "limit_down",
    )

    def __init__(self, client: RQDataClientProtocol) -> None:
        self.client = client

    def fetch_bundle(
        self,
        *,
        order_book_ids: Sequence[str],
        start_date: str,
        end_date: str,
        st_baseline_date: str | None = None,
        include_non_overlapping: bool = False,
        market: str = "cn",
        retrieved_at: datetime | None = None,
    ) -> dict[str, dict[str, Any]]:
        if not order_book_ids:
            raise ValueError("At least one order_book_id is required")
        retrieved = (retrieved_at or datetime.now(timezone.utc)).astimezone(
            timezone.utc
        )
        common = {
            "source": "rqdata",
            "retrieved_at": retrieved.isoformat(),
            "market": market,
            "start_date": start_date,
            "end_date": end_date,
            "requested_order_book_ids": list(order_book_ids),
        }
        baseline_start_date = (
            st_baseline_date
            if st_baseline_date is not None and st_baseline_date < start_date
            else start_date
        )
        st_records = self._records(
            self.client.is_st_stock(
                order_book_ids,
                start_date=start_date,
                end_date=end_date,
                market=market,
            )
        )
        if st_baseline_date is not None and st_baseline_date < start_date:
            baseline_records = self._records(
                self.client.is_st_stock(
                    order_book_ids,
                    start_date=st_baseline_date,
                    end_date=st_baseline_date,
                    market=market,
                )
            )
            st_records = baseline_records + st_records
        return {
            "rqdata_trading_calendar": {
                **common,
                "api": "get_trading_dates",
                "request_policy": {
                    "observation_start_date": start_date,
                    "observation_end_date": end_date,
                    "baseline_date": (
                        baseline_start_date
                        if baseline_start_date != start_date
                        else None
                    ),
                },
                "data": self._records(
                    self.client.get_trading_dates(
                        baseline_start_date, end_date, market=market
                    ),
                    scalar_field="date",
                ),
            },
            "rqdata_security_master": {
                **common,
                "api": "all_instruments",
                "request_policy": {
                    "type": "CS",
                    "as_of_date": end_date,
                    "interval_start_date": start_date,
                    "fallback": "complete_contracts_with_lifecycle_overlap",
                    "raw_filter": "requested_order_book_ids_only",
                },
                "data": self._records(
                    self.client.instruments(
                        order_book_ids,
                        as_of_date=end_date,
                        interval_start_date=start_date,
                        include_non_overlapping=True,
                        market=market,
                    )
                    if include_non_overlapping
                    else self.client.instruments(
                        order_book_ids,
                        as_of_date=end_date,
                        interval_start_date=start_date,
                        market=market,
                    )
                ),
            },
            "rqdata_market_daily": {
                **common,
                "api": "get_price",
                "request_policy": {
                    "frequency": "1d",
                    "fields": list(self.PRICE_FIELDS),
                    "adjust_type": "none",
                    "skip_suspended": True,
                },
                "data": self._records(
                    self.client.get_price(
                        order_book_ids,
                        start_date=start_date,
                        end_date=end_date,
                        frequency="1d",
                        fields=self.PRICE_FIELDS,
                        adjust_type="none",
                        skip_suspended=True,
                        market=market,
                    )
                ),
            },
            "rqdata_suspension_status": {
                **common,
                "api": "is_suspended",
                "data": self._records(
                    self.client.is_suspended(
                        order_book_ids,
                        start_date=start_date,
                        end_date=end_date,
                        market=market,
                    )
                ),
            },
            "rqdata_st_status": {
                **common,
                "api": "is_st_stock",
                "request_policy": {
                    "observation_start_date": start_date,
                    "observation_end_date": end_date,
                    "baseline_date": st_baseline_date,
                },
                "data": st_records,
            },
        }

    def build_mapping_payloads(
        self,
        bundle: dict[str, dict[str, Any]],
        *,
        status_date: str,
    ) -> dict[str, dict[str, Any]]:
        """Build deterministic mapping views while leaving raw bundles untouched."""

        suspension_records = self._long_boolean_records(
            bundle["rqdata_suspension_status"]["data"], "is_suspended"
        )
        suspension_by_key = {
            (item["order_book_id"], item["date"]): item["is_suspended"]
            for item in suspension_records
        }

        instruments: list[dict[str, Any]] = []
        for raw in bundle["rqdata_security_master"]["data"]:
            item = dict(raw)
            order_book_id = str(item["order_book_id"])
            suspended = self._as_bool(
                suspension_by_key.get((order_book_id, status_date), False)
            )
            item.setdefault("exchange", self._exchange_from_order_book_id(order_book_id))
            item["de_listed_date"] = self._optional_date(item.get("de_listed_date"))
            item["status_date"] = status_date
            item["trading_eligibility_status"] = "停牌" if suspended else "正常"
            item["source_record_id"] = f"rqdata-instrument:{order_book_id}:{status_date}"
            instruments.append(item)

        prices: list[dict[str, Any]] = []
        price_keys: set[tuple[str, str]] = set()
        for raw in bundle["rqdata_market_daily"]["data"]:
            item = dict(raw)
            order_book_id = str(item["order_book_id"])
            trading_date = self._date_only(
                item.get("date") or item.get("trading_date")
            )
            suspended = self._as_bool(
                suspension_by_key.get((order_book_id, trading_date), False)
            )
            item.setdefault("exchange", self._exchange_from_order_book_id(order_book_id))
            item["date"] = trading_date
            item["trading_status"] = "停牌" if suspended else "正常成交"
            item["source_record_id"] = f"rqdata-price:{order_book_id}:{trading_date}"
            prices.append(item)
            price_keys.add((order_book_id, trading_date))

        nullable_price_fields = {
            "prev_close": None,
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": None,
            "total_turnover": None,
            "limit_up": None,
            "limit_down": None,
        }
        for raw in suspension_records:
            order_book_id = str(raw["order_book_id"])
            trading_date = self._date_only(raw["date"])
            key = (order_book_id, trading_date)
            if not self._as_bool(raw["is_suspended"]) or key in price_keys:
                continue
            prices.append(
                {
                    **nullable_price_fields,
                    "order_book_id": order_book_id,
                    "exchange": self._exchange_from_order_book_id(order_book_id),
                    "date": trading_date,
                    "trading_status": "停牌",
                    "source_record_id": (
                        f"rqdata-suspension:{order_book_id}:{trading_date}"
                    ),
                }
            )
            price_keys.add(key)
        prices.sort(key=lambda item: (str(item["date"]), str(item["order_book_id"])))

        risk_status: list[dict[str, Any]] = []
        for raw in self._long_boolean_records(
            bundle["rqdata_st_status"]["data"], "is_st"
        ):
            order_book_id = str(raw["order_book_id"])
            status_day = self._date_only(raw["date"])
            record_id = f"rqdata-st:{order_book_id}:{status_day}"
            risk_status.append(
                {
                    **raw,
                    "exchange": self._exchange_from_order_book_id(order_book_id),
                    "risk_status_record_id": record_id,
                    "risk_status_type": "ST",
                    "risk_status_value": "生效"
                    if self._as_bool(raw["is_st"])
                    else "未生效",
                    "status_source_category": "公开事实",
                    "status_start_date": status_day,
                    "status_end_date": status_day,
                    "status_effective_at": f"{status_day}T00:00:00+08:00",
                    "source_record_id": record_id,
                }
            )

        calendar = [
            {
                **item,
                "date": self._date_only(item["date"]),
                "calendar_effective_at": (
                    f"{self._date_only(item['date'])}T00:00:00+08:00"
                ),
                "source_record_id": (
                    f"rqdata-calendar:{self._date_only(item['date'])}"
                ),
            }
            for item in bundle["rqdata_trading_calendar"]["data"]
        ]
        return {
            "rqdata_trading_calendar": {"data": calendar},
            "rqdata_security_master": {"data": instruments},
            "rqdata_market_daily": {"data": prices},
            "rqdata_st_status": {"data": risk_status},
        }

    @staticmethod
    def _long_boolean_records(
        records: Sequence[dict[str, Any]], value_field: str
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for record in records:
            if "order_book_id" in record and value_field in record:
                item = dict(record)
                item["date"] = RQDataAdapter._date_only(item["date"])
                output.append(item)
                continue
            date_value = record.get("date") or record.get("index")
            for key, value in record.items():
                if key in {"date", "index"}:
                    continue
                output.append(
                    {
                        "order_book_id": key,
                        "date": RQDataAdapter._date_only(date_value),
                        value_field: value,
                    }
                )
        return output

    @staticmethod
    def _exchange_from_order_book_id(order_book_id: str) -> str:
        if "." not in order_book_id:
            raise ValueError(
                "RQData order_book_id must contain an explicit market suffix"
            )
        return order_book_id.rsplit(".", 1)[1]

    @staticmethod
    def _as_bool(value: Any) -> bool:
        if value in {True, 1, "1", "true", "True", "是", "Y", "YES"}:
            return True
        if value in {False, 0, "0", "false", "False", "否", "N", "NO"}:
            return False
        raise ValueError(f"Invalid boolean value from source adapter: {value!r}")

    @staticmethod
    def _records(value: Any, scalar_field: str | None = None) -> list[dict[str, Any]]:
        if value is None:
            return []
        if hasattr(value, "reset_index") and hasattr(value, "to_dict"):
            value = value.reset_index().to_dict(orient="records")
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            raise TypeError("RQData response must be a sequence or DataFrame")
        records: list[dict[str, Any]] = []
        for item in value:
            if isinstance(item, dict):
                records.append(
                    {
                        str(key): RQDataAdapter._json_value(val)
                        for key, val in item.items()
                    }
                )
            elif scalar_field:
                records.append({scalar_field: RQDataAdapter._json_value(item)})
            else:
                raise TypeError("Expected record-like RQData response")
        return records

    @staticmethod
    def _json_value(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if hasattr(value, "item"):
            return RQDataAdapter._json_value(value.item())
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    @staticmethod
    def _optional_date(value: Any) -> Any:
        if value is None or str(value) in {"", "0000-00-00", "NaT", "None"}:
            return None
        return value

    @staticmethod
    def _date_only(value: Any) -> str:
        text = str(value)
        return text.split("T", 1)[0].split(" ", 1)[0]

__all__ = [
    "InstrumentUniverseReconciliation",
    "MockRQDataClient",
    "RealRQDataClient",
    "RQDataAdapter",
    "RQDataClientProtocol",
    "RQDataCredentialError",
    "RQDataSDKUnavailableError",
]
