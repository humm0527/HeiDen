"""Offline integrity and output-contract acceptance for this handover bundle."""

from __future__ import annotations

import argparse
import csv
from hashlib import sha256
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
TASK_TYPE = 'risk-events'
OUTPUT_CONTRACT = ('risk_events.csv', 'continuous_limit_down_segments.csv', 'calculation_manifest.json')
OUTPUT_CONTRACTS = {'risk-events': ('risk_events.csv', 'continuous_limit_down_segments.csv', 'calculation_manifest.json')}


def _hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_integrity() -> int:
    manifest = json.loads((ROOT / "SHA256SUMS.json").read_text(encoding="utf-8"))
    failures = []
    for relative, expected in manifest["files"].items():
        path = ROOT / relative
        if not path.is_file():
            failures.append({"file": relative, "reason": "missing"})
        elif _hash(path) != expected:
            failures.append({"file": relative, "reason": "hash_mismatch"})
    if failures:
        raise SystemExit(json.dumps({"status": "FAILED", "failures": failures}, ensure_ascii=False, indent=2))
    return len(manifest["files"])


def _verify_config() -> None:
    payload = json.loads((ROOT / "task.example.json").read_text(encoding="utf-8"))
    if payload.get("task") != TASK_TYPE:
        raise SystemExit(f"task.example.json task mismatch: {payload.get('task')}")


def _verify_import() -> str:
    from riskaudit.market_foundation.risk_shadow import run_snapshot_risk_calculation
    target = run_snapshot_risk_calculation
    if not callable(target):
        raise SystemExit("task entry point is not callable")
    return f"{target.__module__}.{target.__name__}"


def _csv_row_count(path: Path) -> tuple[int, tuple[str, ...]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream)
        header = tuple(next(reader, ()))
        return sum(1 for _ in reader), header


def _result_failure(reason: str, **details: object) -> None:
    raise SystemExit(
        json.dumps(
            {"status": "FAILED", "reason": reason, **details},
            ensure_ascii=False,
            indent=2,
        )
    )


def _verify_results(result_dir: Path) -> dict[str, object]:
    if not result_dir.is_dir():
        raise SystemExit(f"result directory does not exist: {result_dir}")
    task_type = TASK_TYPE
    if TASK_TYPE not in {"risk-events", "peer-benchmark"}:
        found = {
            name: [str(path.relative_to(result_dir)) for path in result_dir.rglob(name)]
            for name in OUTPUT_CONTRACT
        }
        missing = [name for name, matches in found.items() if not matches]
        if missing:
            _result_failure("missing_outputs", missing_outputs=missing)
        return {"task_type": task_type, "outputs": found}
    manifest_name = (
        "calculation_manifest.json"
        if TASK_TYPE == "risk-events"
        else "broker_metric_manifest.json"
    )
    manifest_path = result_dir / manifest_name
    if not manifest_path.is_file():
        _result_failure("missing_manifest", file=manifest_name)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("status") != "SUCCEEDED":
        _result_failure("manifest_not_succeeded", status=payload.get("status"))
    if TASK_TYPE == "peer-benchmark":
        task_type = str(payload.get("run_scope") or TASK_TYPE)
    output_contract = OUTPUT_CONTRACTS.get(task_type, OUTPUT_CONTRACT)
    paths = {name: result_dir / name for name in output_contract}
    missing = [name for name, path in paths.items() if not path.is_file()]
    if missing:
        _result_failure("missing_outputs", missing_outputs=missing)
    counts = {}
    for name, path in paths.items():
        if path.suffix.lower() != ".csv":
            continue
        row_count, header = _csv_row_count(path)
        if not header:
            _result_failure("empty_csv_header", file=name)
        counts[name] = row_count
    if TASK_TYPE == "risk-events":
        expected = {
            "risk_events.csv": int(payload.get("event_count", -1)),
            "continuous_limit_down_segments.csv": int(payload.get("segment_count", -1)),
        }
        mismatches = {
            name: {"expected": expected[name], "actual": counts[name]}
            for name in expected
            if counts[name] != expected[name]
        }
        event_path = paths["risk_events.csv"]
        with event_path.open("r", encoding="utf-8-sig", newline="") as stream:
            rows = csv.DictReader(stream)
            key_columns = ("交易市场代码", "证券代码", "事件类型", "首次事实日期")
            if not set(key_columns) <= set(rows.fieldnames or ()):
                _result_failure("risk_event_schema_mismatch", columns=rows.fieldnames)
            keys = [tuple(row[column] for column in key_columns) for row in rows]
        if len(keys) != len(set(keys)):
            _result_failure("risk_event_key_duplicate")
        if mismatches:
            _result_failure("manifest_row_count_mismatch", mismatches=mismatches)
    else:
        expected_assessments = int(payload.get("expected_assessment_count", -1))
        if counts["event_broker_assessments.csv"] != expected_assessments:
            _result_failure(
                "assessment_count_mismatch",
                expected=expected_assessments,
                actual=counts["event_broker_assessments.csv"],
            )
        recorded = payload.get("output_files", {})
        mismatches = {}
        for name, path in paths.items():
            item = recorded.get(name, {})
            if item.get("row_count") != counts[name] or item.get("sha256") != _hash(path):
                mismatches[name] = {
                    "manifest_row_count": item.get("row_count"),
                    "actual_row_count": counts[name],
                    "manifest_sha256": item.get("sha256"),
                    "actual_sha256": _hash(path),
                }
        if mismatches:
            _result_failure("output_manifest_mismatch", mismatches=mismatches)
    return {
        "task_type": task_type,
        "manifest": manifest_name,
        "row_counts": counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-dir", type=Path)
    parser.add_argument("--source-only", action="store_true", help="Verify exported source and imports without local task JSON")
    args = parser.parse_args()
    file_count = _verify_integrity()
    if not args.source_only:
        _verify_config()
    entry_point = _verify_import()
    result = {
        "status": "PASSED",
        "task": TASK_TYPE,
        "verified_file_count": file_count,
        "verification_scope": "SOURCE_AND_IMPORTS" if args.source_only else "LOCAL_BUNDLE",
        "entry_point": entry_point,
        "result_contract": None,
    }
    if args.result_dir:
        result["result_contract"] = _verify_results(args.result_dir.resolve())
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
