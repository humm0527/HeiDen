"""
文件作用：编排显式证券范围的 RQData API 拉取并生成标准化市场底座批次。
编辑记录：
【首次生成：2026-09-01，从市场底座 RQData 模块拆出受限抓取提供器职责。】
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence

from riskaudit.data_ingestion.adapters.rqdata import (
    RQDataAdapter,
    RQDataClientProtocol,
)

from .constants import DATASETS
from .rqdata_normalization import (
    _market_from_order_book_id,
    normalize_foundation_bundle,
)


P3_MAX_SECURITIES = 5
P3_MAX_CALENDAR_DAYS = 10


@dataclass(frozen=True)
class RQDataFoundationBatch:
    """Normalized facts plus JSON-safe source rows retained as Raw evidence."""

    records_by_dataset: Mapping[str, tuple[dict[str, Any], ...]]
    raw_records_by_dataset: Mapping[str, tuple[dict[str, Any], ...]]
    order_book_ids: tuple[str, ...]



class RQDataFoundationProvider:
    """Explicit-security RQData provider used by bounded P3/P4 work units."""

    def __init__(
        self,
        client: RQDataClientProtocol,
        *,
        order_book_ids: Sequence[str],
        max_securities: int | None = P3_MAX_SECURITIES,
        max_calendar_days: int | None = P3_MAX_CALENDAR_DAYS,
        phase_label: str = "P3",
    ) -> None:
        normalized = tuple(dict.fromkeys(str(item).strip() for item in order_book_ids))
        if not normalized or (
            max_securities is not None and len(normalized) > max_securities
        ):
            maximum = "无限制" if max_securities is None else str(max_securities)
            raise ValueError(
                f"{phase_label} 真实任务必须配置至少 1 只且最多 {maximum} 只显式证券"
            )
        for order_book_id in normalized:
            _market_from_order_book_id(order_book_id)
        self.client = client
        self.order_book_ids = normalized
        self.max_calendar_days = max_calendar_days
        self.phase_label = phase_label

    @property
    def markets(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(_market_from_order_book_id(item) for item in self.order_book_ids))

    def fetch(
        self,
        *,
        start_date: date,
        end_date: date,
        markets: Sequence[str],
        datasets: Sequence[str],
    ) -> RQDataFoundationBatch:
        if end_date < start_date:
            raise ValueError("结束日期不得早于开始日期")
        if self.max_calendar_days is not None and (
            end_date - start_date
        ).days + 1 > self.max_calendar_days:
            raise ValueError(
                f"{self.phase_label} 真实任务一次最多允许 {self.max_calendar_days} 个自然日"
            )
        unknown = set(datasets) - set(DATASETS)
        if unknown:
            raise ValueError(f"未知市场数据集：{sorted(unknown)}")
        selected_markets = tuple(dict.fromkeys(markets))
        selected_ids = tuple(
            item
            for item in self.order_book_ids
            if _market_from_order_book_id(item) in selected_markets
        )
        missing_markets = set(selected_markets) - {
            _market_from_order_book_id(item) for item in selected_ids
        }
        if missing_markets:
            raise ValueError(
                f"{self.phase_label} 当前没有以下市场的显式证券："
                + ", ".join(sorted(missing_markets))
            )

        bundle = self._fetch_selected_bundle(
            selected_ids,
            start_date=start_date,
            end_date=end_date,
            datasets=datasets,
        )
        records = self._normalize(
            bundle,
            start_date=start_date,
            end_date=end_date,
            markets=selected_markets,
            datasets=datasets,
        )
        raw = {
            "market_calendar": tuple(bundle["rqdata_trading_calendar"]["data"]),
            "instrument_master_history": tuple(bundle["rqdata_security_master"]["data"]),
            "daily_price_unadjusted": tuple(bundle["rqdata_market_daily"]["data"]),
            "daily_st_status": tuple(bundle["rqdata_st_status"]["data"]),
            "daily_suspension_status": tuple(
                bundle["rqdata_suspension_status"]["data"]
            ),
            "daily_price_limits": tuple(bundle["rqdata_market_daily"]["data"]),
        }
        return RQDataFoundationBatch(
            records_by_dataset={item: tuple(records[item]) for item in datasets},
            raw_records_by_dataset={item: raw[item] for item in datasets},
            order_book_ids=selected_ids,
        )

    def _fetch_selected_bundle(
        self,
        order_book_ids: Sequence[str],
        *,
        start_date: date,
        end_date: date,
        datasets: Sequence[str],
    ) -> dict[str, dict[str, Any]]:
        """Fetch only requested APIs so partial source capabilities remain auditable."""

        selected = set(datasets)
        start_text = start_date.isoformat()
        end_text = end_date.isoformat()
        needs_master = bool(selected - {"market_calendar"})
        needs_price = bool(
            selected & {"daily_price_unadjusted", "daily_price_limits"}
        )
        bundle = {
            "rqdata_trading_calendar": {
                "api": "get_trading_dates",
                "data": RQDataAdapter._records(
                    self.client.get_trading_dates(start_text, end_text, market="cn"),
                    scalar_field="date",
                )
                if "market_calendar" in selected
                else [],
            },
            "rqdata_security_master": {
                "api": "all_instruments",
                "data": RQDataAdapter._records(
                    self.client.instruments(
                        order_book_ids,
                        as_of_date=end_text,
                        interval_start_date=start_text,
                        include_non_overlapping=True,
                        market="cn",
                    )
                )
                if needs_master
                else [],
            },
            "rqdata_market_daily": {
                "api": "get_price",
                "data": RQDataAdapter._records(
                    self.client.get_price(
                        order_book_ids,
                        start_date=start_text,
                        end_date=end_text,
                        frequency="1d",
                        fields=RQDataAdapter.PRICE_FIELDS,
                        adjust_type="none",
                        skip_suspended=True,
                        market="cn",
                    )
                )
                if needs_price
                else [],
            },
            "rqdata_st_status": {
                "api": "is_st_stock",
                "data": RQDataAdapter._records(
                    self.client.is_st_stock(
                        order_book_ids,
                        start_date=start_text,
                        end_date=end_text,
                        market="cn",
                    )
                )
                if "daily_st_status" in selected
                else [],
            },
            "rqdata_suspension_status": {
                "api": "is_suspended",
                "data": RQDataAdapter._records(
                    self.client.is_suspended(
                        order_book_ids,
                        start_date=start_text,
                        end_date=end_text,
                        market="cn",
                    )
                )
                if "daily_suspension_status" in selected
                else [],
            },
        }
        return bundle

    _normalize = staticmethod(normalize_foundation_bundle)


