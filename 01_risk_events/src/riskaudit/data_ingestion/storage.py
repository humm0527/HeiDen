"""Immutable raw storage and versioned standard-table output storage."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from .models import RawArtifact


_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9_-]+$")


def _safe_segment(value: str, label: str) -> str:
    if not _SAFE_SEGMENT.fullmatch(value):
        raise ValueError(f"Unsafe {label}: {value!r}")
    return value


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RawStore:
    """Persist source bytes or JSON payloads with immutable metadata sidecars."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def save_file(
        self,
        source_name: str,
        dataset_name: str,
        source_path: str | Path,
        *,
        received_at: datetime | None = None,
    ) -> RawArtifact:
        source = Path(source_path)
        payload = source.read_bytes()
        return self._save_bytes(
            source_name,
            dataset_name,
            payload,
            suffix=source.suffix.lower(),
            original_name=source.name,
            received_at=received_at,
        )

    def save_json(
        self,
        source_name: str,
        dataset_name: str,
        payload: Any,
        *,
        received_at: datetime | None = None,
    ) -> RawArtifact:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return self._save_bytes(
            source_name,
            dataset_name,
            encoded,
            suffix=".json",
            original_name=None,
            received_at=received_at,
        )

    def _save_bytes(
        self,
        source_name: str,
        dataset_name: str,
        payload: bytes,
        *,
        suffix: str,
        original_name: str | None,
        received_at: datetime | None,
    ) -> RawArtifact:
        source_name = _safe_segment(source_name, "source name")
        dataset_name = _safe_segment(dataset_name, "dataset name")
        received = received_at or _utc_now()
        received_iso = received.astimezone(timezone.utc).isoformat()
        checksum = hashlib.sha256(payload).hexdigest()
        timestamp = received.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        artifact_id = f"{timestamp}-{checksum[:16]}"
        folder = self.root / source_name / dataset_name
        folder.mkdir(parents=True, exist_ok=True)
        data_path = folder / f"{artifact_id}{suffix}"
        metadata_path = folder / f"{artifact_id}.metadata.json"

        with data_path.open("xb") as stream:
            stream.write(payload)
        metadata = {
            "source_name": source_name,
            "dataset_name": dataset_name,
            "original_name": original_name,
            "format": suffix.lstrip("."),
            "received_at": received_iso,
            "sha256": checksum,
            "raw_data_path": data_path.name,
        }
        with metadata_path.open("x", encoding="utf-8") as stream:
            json.dump(metadata, stream, ensure_ascii=False, indent=2)

        return RawArtifact(
            source_name=source_name,
            dataset_name=dataset_name,
            data_path=data_path,
            metadata_path=metadata_path,
            sha256=checksum,
            received_at=received_iso,
        )


class StandardStore:
    """Save Chinese business tables and a lineage manifest without overwrite."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def save_tables(
        self,
        run_id: str,
        tables: Mapping[str, pd.DataFrame],
        *,
        manifest: Mapping[str, Any],
    ) -> Path:
        run_id = _safe_segment(run_id, "run id")
        run_dir = self.root / run_id
        run_dir.mkdir(parents=True, exist_ok=False)

        table_files: dict[str, str] = {}
        for table_name, frame in tables.items():
            if any(separator in table_name for separator in ("/", "\\")):
                raise ValueError(f"Unsafe table name: {table_name!r}")
            filename = f"{table_name}.csv"
            frame.to_csv(run_dir / filename, index=False, encoding="utf-8-sig")
            table_files[table_name] = filename

        full_manifest = dict(manifest)
        full_manifest["run_id"] = run_id
        full_manifest["table_files"] = table_files
        full_manifest["saved_at"] = _utc_now().isoformat()
        with (run_dir / "manifest.json").open("x", encoding="utf-8") as stream:
            json.dump(full_manifest, stream, ensure_ascii=False, indent=2, default=str)
        return run_dir
