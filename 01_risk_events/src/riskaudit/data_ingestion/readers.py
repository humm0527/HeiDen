"""Readers for immutable raw CSV, Excel, and JSON inputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


class DataReader:
    """Read heterogeneous inputs without renaming or dropping raw fields."""

    @staticmethod
    def read_csv(path: str | Path, *, encoding: str = "utf-8-sig") -> pd.DataFrame:
        return pd.read_csv(
            path,
            dtype=object,
            keep_default_na=False,
            encoding=encoding,
        )

    @staticmethod
    def read_excel(
        path: str | Path,
        *,
        sheet_name: str | int = 0,
    ) -> pd.DataFrame:
        frame = pd.read_excel(path, sheet_name=sheet_name, dtype=object)
        return frame.where(pd.notna(frame), "")

    @staticmethod
    def read_json(payload: str | bytes | list[Any] | dict[str, Any]) -> pd.DataFrame:
        parsed: Any
        if isinstance(payload, bytes):
            parsed = json.loads(payload.decode("utf-8"))
        elif isinstance(payload, str):
            parsed = json.loads(payload)
        else:
            parsed = payload

        if isinstance(parsed, dict):
            records = parsed.get("data")
            if records is None:
                records = [parsed]
        else:
            records = parsed

        if not isinstance(records, list) or any(
            not isinstance(record, dict) for record in records
        ):
            raise ValueError("JSON payload must be an object or a list under the 'data' key")
        return pd.DataFrame.from_records(records)

    @classmethod
    def read_file(
        cls,
        path: str | Path,
        *,
        sheet_name: str | int = 0,
    ) -> pd.DataFrame:
        suffix = Path(path).suffix.lower()
        if suffix == ".csv":
            return cls.read_csv(path)
        if suffix in {".xlsx", ".xlsm"}:
            return cls.read_excel(path, sheet_name=sheet_name)
        raise ValueError(f"Unsupported file format: {suffix}")

