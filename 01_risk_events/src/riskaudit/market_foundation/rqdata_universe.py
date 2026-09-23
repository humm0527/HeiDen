"""
文件作用：发现、分片、写出并复验全市场 RQData 证券清单。
编辑记录：
【首次生成：2026-09-01，从市场底座 RQData 模块拆出 P3.5 全市场发现职责。】
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from .constants import FOUNDATION_END, FOUNDATION_START, MARKETS
from .rqdata_normalization import (
    _as_date,
    _market_from_order_book_id,
    _optional_date,
    _security_code,
)


P3_5_DEFAULT_SHARD_SIZE = 500


@dataclass(frozen=True)
class RQDataUniversePlan:
    """Immutable P3.5 security discovery result and deterministic shard plan."""

    universe_id: str
    manifest_hash: str
    discovered_at: str
    start_date: str
    end_date: str
    source_row_count: int
    records: tuple[dict[str, Any], ...]
    shards: tuple[dict[str, Any], ...]
    counts_by_market: Mapping[str, int]
    delisted_count: int
    shard_size: int

    def as_manifest(self) -> dict[str, Any]:
        return {
            "schema_version": "rqdata_universe_v1",
            "universe_id": self.universe_id,
            "manifest_hash": self.manifest_hash,
            "source_system": "RQDATA",
            "discovered_at": self.discovered_at,
            "scope": {
                "start_date": self.start_date,
                "end_date": self.end_date,
                "markets": list(MARKETS),
                "security_type": "CS",
                "include_historical_delisted": True,
            },
            "source_row_count": self.source_row_count,
            "eligible_security_count": len(self.records),
            "counts_by_market": dict(self.counts_by_market),
            "delisted_count": self.delisted_count,
            "shard_size": self.shard_size,
            "shard_count": len(self.shards),
            "records": [dict(item) for item in self.records],
            "shards": [dict(item) for item in self.shards],
        }


def discover_full_market_universe(
    client: Any,
    *,
    start_date: date = FOUNDATION_START,
    end_date: date = FOUNDATION_END,
    shard_size: int = P3_5_DEFAULT_SHARD_SIZE,
) -> RQDataUniversePlan:
    """Discover all lifecycle-overlapping A shares without downloading facts."""

    if end_date < start_date:
        raise ValueError("结束日期不得早于开始日期")
    if shard_size < 1:
        raise ValueError("证券分片大小必须为正整数")
    source_rows = tuple(client.all_instruments(as_of_date=None, market="cn"))
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source in source_rows:
        order_book_id = str(source.get("order_book_id") or "").strip()
        if not order_book_id:
            raise ValueError("RQData 证券主数据缺少 order_book_id")
        if order_book_id in seen:
            raise ValueError(f"RQData 证券主数据存在重复代码：{order_book_id}")
        seen.add(order_book_id)
        try:
            market_code = _market_from_order_book_id(order_book_id)
        except ValueError:
            continue
        listed = _as_date(source.get("listed_date"))
        terminated = _optional_date(source.get("de_listed_date"))
        if listed > end_date or (terminated is not None and terminated <= start_date):
            continue
        normalized.append(
            {
                "instrument_key": order_book_id,
                "source_order_book_id": order_book_id,
                "source_exchange": str(source.get("exchange") or order_book_id.rsplit(".", 1)[1]),
                "market_code": market_code,
                "security_code": _security_code(order_book_id),
                "symbol": str(source.get("symbol") or ""),
                "security_type": str(source.get("type") or "CS"),
                "board_code": str(source.get("board_type") or "UNKNOWN"),
                "listing_date": listed.isoformat(),
                "termination_date": terminated.isoformat() if terminated else None,
                "source_status": str(source.get("status") or "UNKNOWN"),
            }
        )
    market_rank = {market: index for index, market in enumerate(MARKETS)}
    normalized.sort(
        key=lambda item: (
            market_rank[item["market_code"]],
            item["source_order_book_id"],
        )
    )
    counts_by_market = {
        market: sum(1 for item in normalized if item["market_code"] == market)
        for market in MARKETS
    }
    shards: list[dict[str, Any]] = []
    for market in MARKETS:
        identifiers = [
            item["source_order_book_id"]
            for item in normalized
            if item["market_code"] == market
        ]
        for ordinal, offset in enumerate(range(0, len(identifiers), shard_size), 1):
            members = identifiers[offset : offset + shard_size]
            digest = sha256(
                json.dumps(members, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            shards.append(
                {
                    "shard_id": f"mdushard_{market.lower()}_{ordinal:04d}_{digest[:12]}",
                    "market_code": market,
                    "shard_ordinal": ordinal,
                    "security_count": len(members),
                    "first_order_book_id": members[0],
                    "last_order_book_id": members[-1],
                    "order_book_ids": members,
                    "status": "PLANNED",
                }
            )
    hash_payload = {
        "schema_version": "rqdata_universe_v1",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "markets": list(MARKETS),
        "shard_size": shard_size,
        "records": normalized,
        "shards": shards,
    }
    manifest_hash = sha256(
        json.dumps(
            hash_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return RQDataUniversePlan(
        universe_id=f"mduni_{manifest_hash[:32]}",
        manifest_hash=manifest_hash,
        discovered_at=datetime.now(timezone.utc).isoformat(),
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        source_row_count=len(source_rows),
        records=tuple(normalized),
        shards=tuple(shards),
        counts_by_market=counts_by_market,
        delisted_count=sum(
            1
            for item in normalized
            if item["termination_date"] is not None
            or item["source_status"].upper() == "DELISTED"
        ),
        shard_size=shard_size,
    )


def write_universe_plan(
    plan: RQDataUniversePlan, output_dir: str | Path
) -> tuple[Path, Path]:
    """Write content-addressed JSON/CSV evidence without overwriting prior plans."""

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    stem = f"rqdata_universe_{plan.manifest_hash[:16]}"
    manifest_path = destination / f"{stem}.json"
    members_path = destination / f"{stem}.csv"
    if not manifest_path.exists():
        with manifest_path.open("x", encoding="utf-8", newline="") as handle:
            json.dump(plan.as_manifest(), handle, ensure_ascii=False, indent=2)
            handle.write("\n")
    if not members_path.exists():
        fieldnames = list(plan.records[0]) if plan.records else ["instrument_key"]
        with members_path.open("x", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(plan.records)
    return manifest_path, members_path


def read_universe_plan(path: str | Path) -> RQDataUniversePlan:
    """Read and verify a content-addressed P3.5 universe manifest for P4."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    scope = payload["scope"]
    records = tuple(dict(item) for item in payload["records"])
    shards = tuple(dict(item) for item in payload["shards"])
    hash_payload = {
        "schema_version": payload["schema_version"],
        "start_date": scope["start_date"],
        "end_date": scope["end_date"],
        "markets": list(scope["markets"]),
        "shard_size": int(payload["shard_size"]),
        "records": list(records),
        "shards": list(shards),
    }
    actual_hash = sha256(
        json.dumps(
            hash_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    if actual_hash != payload["manifest_hash"]:
        raise ValueError("证券清单 manifest_hash 与实际内容不一致")
    if payload["universe_id"] != f"mduni_{actual_hash[:32]}":
        raise ValueError("证券清单 universe_id 与实际内容不一致")
    return RQDataUniversePlan(
        universe_id=payload["universe_id"],
        manifest_hash=actual_hash,
        discovered_at=payload["discovered_at"],
        start_date=scope["start_date"],
        end_date=scope["end_date"],
        source_row_count=int(payload["source_row_count"]),
        records=records,
        shards=shards,
        counts_by_market=dict(payload["counts_by_market"]),
        delisted_count=int(payload["delisted_count"]),
        shard_size=int(payload["shard_size"]),
    )




