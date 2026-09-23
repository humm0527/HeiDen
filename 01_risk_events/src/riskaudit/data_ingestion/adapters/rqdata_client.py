"""
文件作用：封装 RQData 客户端协议、环境凭证初始化、真实 SDK 调用与确定性模拟客户端。
编辑记录：
【首次生成：2026-09-01，从 rqdata 适配器中拆出客户端边界，不改变查询参数与生命周期规则。】
"""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol, Sequence


class RQDataCredentialError(RuntimeError):
    """Raised when the real client has no complete environment credential."""


class RQDataSDKUnavailableError(RuntimeError):
    """Raised when the optional rqdatac SDK is not installed."""


class RQDataClientProtocol(Protocol):
    """Small documented RQData surface needed by the ingestion boundary."""

    def get_trading_dates(
        self, start_date: str, end_date: str, *, market: str = "cn"
    ) -> Sequence[Any]: ...

    def instruments(
        self,
        order_book_ids: Sequence[str],
        *,
        as_of_date: str,
        interval_start_date: str | None = None,
        include_non_overlapping: bool = False,
        market: str = "cn",
    ) -> Sequence[dict[str, Any]]: ...


    def get_price(
        self,
        order_book_ids: Sequence[str],
        *,
        start_date: str,
        end_date: str,
        frequency: str = "1d",
        fields: Sequence[str] | None = None,
        adjust_type: str = "none",
        skip_suspended: bool = True,
        market: str = "cn",
    ) -> Sequence[dict[str, Any]]: ...

    def is_suspended(
        self,
        order_book_ids: Sequence[str],
        *,
        start_date: str,
        end_date: str,
        market: str = "cn",
    ) -> Sequence[dict[str, Any]]: ...

    def is_st_stock(
        self,
        order_book_ids: Sequence[str],
        *,
        start_date: str,
        end_date: str,
        market: str = "cn",
    ) -> Sequence[dict[str, Any]]: ...


@dataclass(frozen=True)
class InstrumentUniverseReconciliation:
    records: tuple[dict[str, Any], ...]
    overlapping_ids: tuple[str, ...]
    non_overlapping_ids: tuple[str, ...]
    missing_ids: tuple[str, ...]


class RealRQDataClient:
    """Thin wrapper around rqdatac initialized only from environment variables.

    Supported credential forms are the SDK-native ``RQDATAC2_CONF`` or
    ``RQDATAC_CONF`` URI, or the pair ``RQDATA_USERNAME`` and
    ``RQDATA_PASSWORD``. Credential values are never returned or logged.
    """

    CREDENTIAL_URI_NAMES = ("RQDATAC2_CONF", "RQDATAC_CONF")
    USERNAME_NAME = "RQDATA_USERNAME"
    PASSWORD_NAME = "RQDATA_PASSWORD"
    PROXY_NAME = "RQDATAC_PROXY"
    PROXY_FALLBACK_NAME = "RISKAUDIT_RQDATA_PROXY_FALLBACK"

    def __init__(self, sdk: Any) -> None:
        self._sdk = sdk

    @classmethod
    def from_environment(
        cls,
        *,
        sdk: Any | None = None,
        enable_bjse: bool = False,
    ) -> "RealRQDataClient":
        username = os.environ.get(cls.USERNAME_NAME)
        password = os.environ.get(cls.PASSWORD_NAME)
        credential_uri_present = any(
            os.environ.get(name) for name in cls.CREDENTIAL_URI_NAMES
        )

        if bool(username) != bool(password):
            raise RQDataCredentialError(
                "RQDATA_USERNAME and RQDATA_PASSWORD must be set together"
            )
        if not credential_uri_present and not (username and password):
            raise RQDataCredentialError(
                "Set RQDATAC2_CONF/RQDATAC_CONF or both "
                "RQDATA_USERNAME and RQDATA_PASSWORD"
            )

        try:
            sdk_module = sdk or importlib.import_module("rqdatac")
        except ModuleNotFoundError as exc:
            raise RQDataSDKUnavailableError(
                "rqdatac is not installed in the active Python environment"
            ) from exc

        init_args = (username, password) if username and password else ()
        init_kwargs = {"enable_bjse": True} if enable_bjse else {}
        try:
            sdk_module.init(*init_args, **init_kwargs)
        except PermissionError as exc:
            fallback = os.environ.get(cls.PROXY_FALLBACK_NAME)
            is_windows_socket_denial = getattr(exc, "winerror", None) == 10013
            if (
                not is_windows_socket_denial
                or os.environ.get(cls.PROXY_NAME)
                or not fallback
            ):
                raise
            # RQData SDK reads this variable itself. The fallback is explicit so
            # credentials are never routed through an unconfigured proxy.
            os.environ[cls.PROXY_NAME] = fallback
            sdk_module.init(*init_args, **init_kwargs)
        return cls(sdk_module)

    def all_instruments(
        self,
        *,
        as_of_date: str | None = None,
        market: str = "cn",
    ) -> tuple[dict[str, Any], ...]:
        """Return the complete or point-in-time common-stock universe."""

        frame = self._sdk.all_instruments(
            type="CS", date=as_of_date, market=market
        )
        if not hasattr(frame, "to_dict"):
            raise TypeError("RQData all_instruments must return a DataFrame")
        if "order_book_id" not in getattr(frame, "columns", ()):
            if bool(getattr(frame, "empty", False)):
                return ()
            raise TypeError("RQData all_instruments must include order_book_id")
        return tuple(frame.copy().to_dict(orient="records"))

    def get_trading_dates(
        self, start_date: str, end_date: str, *, market: str = "cn"
    ) -> Sequence[Any]:
        return self._sdk.get_trading_dates(start_date, end_date, market=market)

    def get_previous_trading_date(
        self, value: str, *, market: str = "cn"
    ) -> Any:
        return self._sdk.get_previous_trading_date(value, market=market)

    def account_quota(self) -> dict[str, Any]:
        """Return sanitized account quota metadata from the public SDK API."""

        user_api = getattr(self._sdk, "user", None)
        get_quota = getattr(user_api, "get_quota", None)
        if not callable(get_quota):
            raise RuntimeError("rqdatac.user.get_quota is unavailable")
        quota = get_quota()
        if not isinstance(quota, dict):
            raise TypeError("rqdatac.user.get_quota must return a dict")
        return dict(quota)

    def instrument_snapshot(
        self,
        order_book_ids: Sequence[str],
        *,
        as_of_date: str,
        market: str = "cn",
    ) -> tuple[dict[str, Any], ...]:
        snapshot = self._sdk.all_instruments(
            type="CS", date=as_of_date, market=market
        )
        if not hasattr(snapshot, "loc"):
            raise TypeError("RQData all_instruments must return a DataFrame")
        if "order_book_id" not in snapshot.columns:
            if bool(getattr(snapshot, "empty", False)):
                return ()
            raise TypeError("RQData all_instruments must include order_book_id")
        requested = set(order_book_ids)
        return tuple(
            snapshot.loc[snapshot["order_book_id"].isin(requested)]
            .copy()
            .to_dict(orient="records")
        )

    def reconcile_instruments(
        self,
        order_book_ids: Sequence[str],
        *,
        as_of_date: str,
        interval_start_date: str,
        market: str = "cn",
    ) -> InstrumentUniverseReconciliation:
        requested = tuple(dict.fromkeys(str(value) for value in order_book_ids))
        end_records = self.instrument_snapshot(
            requested, as_of_date=as_of_date, market=market
        )
        records_by_id = {
            str(item["order_book_id"]): item for item in end_records
        }
        absent_at_end = set(requested) - set(records_by_id)
        if absent_at_end:
            complete = self._sdk.all_instruments(type="CS", market=market)
            if not hasattr(complete, "loc"):
                raise TypeError("RQData all_instruments must return a DataFrame")
            if "order_book_id" not in complete.columns:
                if bool(getattr(complete, "empty", False)):
                    complete_records: list[dict[str, Any]] = []
                else:
                    raise TypeError("RQData all_instruments must include order_book_id")
            else:
                complete_records = complete.loc[
                    complete["order_book_id"].isin(absent_at_end)
                ].to_dict(orient="records")
            for item in complete_records:
                records_by_id[str(item["order_book_id"])] = item

        interval_start = date.fromisoformat(interval_start_date)
        interval_end = date.fromisoformat(as_of_date)
        overlapping: list[str] = []
        non_overlapping: list[str] = []
        missing: list[str] = []
        records: list[dict[str, Any]] = []
        for order_book_id in requested:
            item = records_by_id.get(order_book_id)
            if item is None:
                missing.append(order_book_id)
                continue
            records.append(item)
            if self._lifecycle_overlaps(item, interval_start, interval_end):
                overlapping.append(order_book_id)
            else:
                non_overlapping.append(order_book_id)
        return InstrumentUniverseReconciliation(
            records=tuple(records),
            overlapping_ids=tuple(overlapping),
            non_overlapping_ids=tuple(non_overlapping),
            missing_ids=tuple(missing),
        )

    def instruments(
        self,
        order_book_ids: Sequence[str],
        *,
        as_of_date: str,
        interval_start_date: str | None = None,
        include_non_overlapping: bool = False,
        market: str = "cn",
    ) -> Sequence[dict[str, Any]]:
        reconciliation = self.reconcile_instruments(
            order_book_ids,
            as_of_date=as_of_date,
            interval_start_date=interval_start_date or as_of_date,
            market=market,
        )
        rejected = list(reconciliation.missing_ids)
        if not include_non_overlapping:
            rejected.extend(reconciliation.non_overlapping_ids)
        if rejected:
            raise ValueError(
                "Securities absent from the historical instrument snapshot "
                "and observation interval: " + ", ".join(sorted(rejected))
            )
        records_by_id = {
            str(item["order_book_id"]): item
            for item in reconciliation.records
        }
        return [records_by_id[order_book_id] for order_book_id in order_book_ids]

    @staticmethod
    def _lifecycle_overlaps(
        instrument: dict[str, Any], interval_start: date, interval_end: date
    ) -> bool:
        listed = RealRQDataClient._parse_lifecycle_date(
            instrument.get("listed_date")
        )
        delisted = RealRQDataClient._parse_lifecycle_date(
            instrument.get("de_listed_date")
        )
        if listed is None or listed > interval_end:
            return False
        return delisted is None or delisted > interval_start

    @staticmethod
    def _parse_lifecycle_date(value: Any) -> date | None:
        text = str(value or "").split("T", 1)[0].split(" ", 1)[0]
        if text in {"", "0000-00-00", "NaT", "None"}:
            return None
        try:
            return date.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"Invalid RQData lifecycle date: {value!r}") from exc

    def get_price(
        self,
        order_book_ids: Sequence[str],
        *,
        start_date: str,
        end_date: str,
        frequency: str = "1d",
        fields: Sequence[str] | None = None,
        adjust_type: str = "none",
        skip_suspended: bool = True,
        market: str = "cn",
    ) -> Any:
        return self._sdk.get_price(
            list(order_book_ids),
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            fields=list(fields) if fields is not None else None,
            adjust_type=adjust_type,
            skip_suspended=skip_suspended,
            expect_df=True,
            market=market,
        )

    def is_suspended(
        self,
        order_book_ids: Sequence[str],
        *,
        start_date: str,
        end_date: str,
        market: str = "cn",
    ) -> Any:
        return self._sdk.is_suspended(
            list(order_book_ids), start_date, end_date, market=market
        )

    def is_st_stock(
        self,
        order_book_ids: Sequence[str],
        *,
        start_date: str,
        end_date: str,
        market: str = "cn",
    ) -> Any:
        return self._sdk.is_st_stock(
            list(order_book_ids), start_date, end_date, market=market
        )


@dataclass
class MockRQDataClient:
    """Return caller-supplied official-like responses; never access a network."""

    responses: dict[str, Any]

    def _response(self, name: str) -> Any:
        if name not in self.responses:
            raise KeyError(f"No mock response configured for {name}")
        return self.responses[name]

    def get_trading_dates(
        self, start_date: str, end_date: str, *, market: str = "cn"
    ) -> Sequence[Any]:
        return self._response("get_trading_dates")

    def instruments(
        self,
        order_book_ids: Sequence[str],
        *,
        as_of_date: str,
        interval_start_date: str | None = None,
        include_non_overlapping: bool = False,
        market: str = "cn",
    ) -> Sequence[dict[str, Any]]:
        key = "instruments" if "instruments" in self.responses else "all_instruments"
        return self._response(key)

    def get_price(
        self,
        order_book_ids: Sequence[str],
        *,
        start_date: str,
        end_date: str,
        frequency: str = "1d",
        fields: Sequence[str] | None = None,
        adjust_type: str = "none",
        skip_suspended: bool = True,
        market: str = "cn",
    ) -> Sequence[dict[str, Any]]:
        return self._response("get_price")

    def is_suspended(
        self,
        order_book_ids: Sequence[str],
        *,
        start_date: str,
        end_date: str,
        market: str = "cn",
    ) -> Sequence[dict[str, Any]]:
        return self._response("is_suspended")

    def is_st_stock(
        self,
        order_book_ids: Sequence[str],
        *,
        start_date: str,
        end_date: str,
        market: str = "cn",
    ) -> Sequence[dict[str, Any]]:
        return self._response("is_st_stock")

__all__ = [
    "InstrumentUniverseReconciliation",
    "MockRQDataClient",
    "RealRQDataClient",
    "RQDataClientProtocol",
    "RQDataCredentialError",
    "RQDataSDKUnavailableError",
]

