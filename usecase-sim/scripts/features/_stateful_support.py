"""Shared helpers for focused native stateful qualification cases.

The helpers keep each case under a new simulator-owned data root while using
the current shared framework log root.  They intentionally execute the normal
``DataCoolieDriver`` path; direct engine writes are used only to seed source
files and native Delta targets where a case needs an existing table.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import importlib.metadata
import json
import os
import platform as py_platform
import subprocess
import sys
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any


SCRIPT = Path(__file__).resolve()
USECASE_SIM_DIR = SCRIPT.parents[2]
PRODUCT_ROOT = USECASE_SIM_DIR.parent
if str(USECASE_SIM_DIR) not in sys.path:
    sys.path.insert(0, str(USECASE_SIM_DIR))
if str(PRODUCT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PRODUCT_ROOT / "src"))

from runner.runtime_logs import job_sessions, records  # noqa: E402
from runner.runtime_paths import DATA_DIR, LOG_DIR, WATERMARKS_DIR  # noqa: E402
from cases.receipts import worktree_fingerprint  # noqa: E402

from datacoolie.core.models.run_config import DataCoolieRunConfig  # noqa: E402
from datacoolie.engines.polars_engine import PolarsEngine  # noqa: E402
from datacoolie.metadata.file_provider import FileProvider  # noqa: E402
from datacoolie.orchestration.driver import DataCoolieDriver  # noqa: E402
from datacoolie.platforms.local_platform import LocalPlatform  # noqa: E402
from datacoolie.utils.identity import name_to_uuid  # noqa: E402


GENERATED_COLUMNS = frozenset(
    {
        "__created_at",
        "__updated_at",
        "__updated_by",
        "__dataflow_run_id",
        "__file_name",
        "__file_path",
        "__file_modification_time",
        "__valid_from",
        "__valid_to",
        "__is_current",
    }
)


@dataclass(frozen=True)
class CasePaths:
    """Case-owned paths and the mapped receipt/watermark roots."""

    group: str
    root: Path
    metadata: Path
    input: Path
    output: Path
    state: Path
    watermarks: Path
    receipt_root: Path


@dataclass(frozen=True)
class DriverEvidence:
    """One normal Driver invocation plus its current-session records."""

    job_id: str
    stage: str
    session_id: str
    result: dict[str, Any]
    records: tuple[dict[str, Any], ...]


def _mapped_runtime_root(root: Path, base: Path) -> Path:
    """Map a data case path beneath the corresponding runtime base."""

    relative = root.resolve().relative_to(DATA_DIR.resolve())
    return (base.resolve() / relative).resolve()


def resolve_case_root(group: str, requested: Path | None = None) -> CasePaths:
    """Allocate a fresh case root and validate all derived runtime roots."""

    if requested is None:
        root = DATA_DIR / "behavior_cases" / f"{group}_polars_{uuid.uuid4().hex[:12]}"
    else:
        root = requested if requested.is_absolute() else PRODUCT_ROOT / requested
    root = root.resolve()
    data_root = DATA_DIR.resolve()
    if root == data_root or not root.is_relative_to(data_root):
        raise ValueError(
            "--root must be a new directory strictly below usecase-sim/.runtime/data"
        )

    receipt_root = _mapped_runtime_root(root, LOG_DIR)
    watermark_root = _mapped_runtime_root(root, WATERMARKS_DIR)
    if root.exists():
        raise ValueError(f"case root must be new and unused: {root}")
    if receipt_root.exists():
        raise ValueError(f"native receipt root must be new and unused: {receipt_root}")
    if watermark_root.exists():
        raise ValueError(
            f"native watermark root must be new and unused: {watermark_root}"
        )

    root.mkdir(parents=True, exist_ok=False)
    receipt_root.mkdir(parents=True, exist_ok=False)
    watermark_root.mkdir(parents=True, exist_ok=False)
    paths = CasePaths(
        group=group,
        root=root,
        metadata=root / "metadata.json",
        input=root / "input",
        output=root / "output",
        state=root / "state",
        watermarks=watermark_root,
        receipt_root=receipt_root,
    )
    for path in (paths.input, paths.output, paths.state):
        path.mkdir(parents=True, exist_ok=False)
    return paths


def materialize_fixture(case: CasePaths, fixture_name: str) -> None:
    """Copy an authored metadata fixture and substitute the case root."""

    fixture = USECASE_SIM_DIR / "metadata" / "file" / case.group / fixture_name
    if not fixture.is_file():
        raise FileNotFoundError(f"missing focused fixture: {fixture}")
    # POSIX separators keep the authored JSON valid when a case is run from
    # Windows; FileProvider and the local platform accept the resulting path.
    payload = fixture.read_text(encoding="utf-8").replace(
        "__CASE_ROOT__", case.root.as_posix()
    )
    document = json.loads(payload)
    if not isinstance(document, dict) or not document.get("connections"):
        raise ValueError(f"fixture is not a FileProvider document: {fixture}")
    case.metadata.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def write_parquet(
    engine: PolarsEngine,
    path: Path,
    rows: list[dict[str, Any]],
    *,
    mode: str = "overwrite",
    mtime: float | None = None,
) -> tuple[Path, ...]:
    """Write a source parquet folder and optionally set selected file mtimes."""

    before = set(path.rglob("*.parquet")) if path.exists() else set()
    engine.write_to_path(
        engine.create_dataframe(rows), str(path), mode=mode, fmt="parquet"
    )
    after = set(path.rglob("*.parquet"))
    new_files = tuple(sorted(after - before))
    if mtime is not None:
        for file_path in new_files or tuple(sorted(after)):
            os.utime(file_path, (mtime, mtime))
    return new_files


def read_delta_rows(engine: PolarsEngine, path: Path) -> list[dict[str, Any]]:
    """Read a Delta target as eagerly collected Python records."""

    if not path.exists():
        return []
    return engine.read_delta(str(path)).collect().to_dicts()


def read_delta_schema(engine: PolarsEngine, path: Path) -> dict[str, str]:
    """Return stable string forms of the native Delta schema."""

    return {name: str(dtype) for name, dtype in engine.read_delta(str(path)).collect_schema().items()}


def business_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Drop framework-generated columns while preserving authored values."""

    result = []
    for row in rows:
        result.append(
            {
                key: value
                for key, value in row.items()
                if key not in GENERATED_COLUMNS
            }
        )
    return result


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_value(item) for item in value]
    if dataclasses.is_dataclass(value):
        return _json_value(dataclasses.asdict(value))
    if hasattr(value, "value") and not isinstance(value, (str, int, float, bool)):
        return _json_value(value.value)
    return value


def canonical_rows(rows: Iterable[Mapping[str, Any]], *, key: str | None = None) -> list[dict[str, Any]]:
    values = [{name: _json_value(item) for name, item in row.items()} for row in rows]
    return sorted(values, key=lambda row: (_json_value(row.get(key)) if key else json.dumps(row, sort_keys=True)))


def assert_exact_rows(
    actual: Iterable[Mapping[str, Any]],
    expected: Iterable[Mapping[str, Any]],
    *,
    key: str | None = None,
    label: str = "rows",
) -> None:
    """Require exact persisted IDs, values, and column presence."""

    actual_value = canonical_rows(actual, key=key)
    expected_value = canonical_rows(expected, key=key)
    if actual_value != expected_value:
        raise AssertionError(f"{label} mismatch: actual={actual_value!r}, expected={expected_value!r}")


def assert_schema(
    actual: Mapping[str, str],
    expected: Mapping[str, str],
    *,
    label: str = "schema",
) -> None:
    if dict(actual) != dict(expected):
        raise AssertionError(f"{label} mismatch: actual={dict(actual)!r}, expected={dict(expected)!r}")


def run_driver(
    case: CasePaths,
    *,
    stage: str,
    iteration: int = 1,
    maintenance: bool = False,
    do_compact: bool = True,
    do_cleanup: bool = True,
    retention_hours: int = 168,
    engine: Any = None,
    metadata_provider: Any = None,
) -> DriverEvidence:
    """Execute one stage through a normal, isolated Driver session."""

    job_id = f"focused-{case.group}-{stage}-{iteration}-{case.root.name}"
    previous = job_sessions(LOG_DIR, job_id)
    platform = LocalPlatform()
    provider = metadata_provider if metadata_provider is not None else FileProvider(
        config_path=str(case.metadata),
        platform=platform,
        watermark_base_path=str(case.watermarks / "provider"),
    )
    with DataCoolieDriver(
        engine=engine if engine is not None else PolarsEngine(platform=platform),
        platform=platform,
        metadata_provider=provider,
        state_base_path=str(case.state),
        log_base_path=str(LOG_DIR),
        config=DataCoolieRunConfig(
            job_id=job_id,
            max_workers=1,
            retry_count=0,
            retry_delay=0,
            retention_hours=retention_hours,
        ),
    ) as driver:
        if maintenance:
            result = driver.run_maintenance(
                dataflows=provider.get_dataflows(stage=stage),
                do_compact=do_compact,
                do_cleanup=do_cleanup,
            )
        else:
            result = driver.run(stage=stage)

    sessions = job_sessions(LOG_DIR, job_id) - previous
    if len(sessions) != 1:
        raise AssertionError(f"expected one current framework session for {job_id}, got {sessions}")
    session_id = next(iter(sessions))
    matching: list[dict[str, Any]] = []
    for path, value in records(LOG_DIR, job_id, "execution_logs", session_id):
        matching.append(
            {
                "path": str(path.resolve().relative_to(LOG_DIR.resolve())).replace("\\", "/"),
                "record_type": value.get("_type"),
                "dataflow_id": value.get("dataflow_id"),
                "status": value.get("status"),
                "destination": value.get("destination"),
                "operation_details": _find_operation_details(value),
            }
        )
    return DriverEvidence(
        job_id=job_id,
        stage=stage,
        session_id=session_id,
        result=dataclasses.asdict(result),
        records=tuple(matching),
    )


def assert_successful_flow(evidence: DriverEvidence, flow_name: str) -> None:
    """Require execution of the intended flow in this invocation's session."""

    expected_counts = {"succeeded": 1, "failed": 0, "skipped": 0}
    actual_counts = {key: evidence.result.get(key) for key in expected_counts}
    if actual_counts != expected_counts:
        raise AssertionError(f"{flow_name} invocation did not succeed: {actual_counts!r}")
    if not evidence.session_id:
        raise AssertionError(f"{flow_name} has no current session evidence")
    expected_id = name_to_uuid(flow_name)
    matching = [
        record for record in evidence.records
        if record.get("record_type") == "dataflow_run_log"
        and record.get("dataflow_id") == expected_id
    ]
    if len(matching) != 1 or matching[0].get("status") != "succeeded":
        raise AssertionError(f"{flow_name} has no unique successful expected-flow evidence: {matching!r}")


def _find_operation_details(value: Any) -> list[dict[str, Any]] | None:
    """Find the serialized destination operation detail list in one record."""

    if isinstance(value, Mapping):
        candidate = value.get("operation_details")
        if isinstance(candidate, list) and all(isinstance(item, dict) for item in candidate):
            return candidate
        for nested in value.values():
            found = _find_operation_details(nested)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _find_operation_details(nested)
            if found is not None:
                return found
    return None


def _revision_details() -> dict[str, Any]:
    try:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PRODUCT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        head = "unknown"
    try:
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain", "--untracked-files=no"],
                cwd=PRODUCT_ROOT,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        )
    except (OSError, subprocess.CalledProcessError):
        dirty = True
    return {"git_head": head, "worktree_dirty": dirty}


def _package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "unavailable"


def receipt_payload(
    case: CasePaths,
    *,
    runs: Sequence[DriverEvidence],
    details: Mapping[str, Any],
    script: Path | None = None,
    status: str = "passed",
    actual_exit_code: int = 0,
    qualification_gaps: Sequence[str] = (),
    references: Sequence[str] = (),
) -> dict[str, Any]:
    """Build the portable receipt consumed by simulator catalog tooling."""

    receipt_script = (script or Path(sys.argv[0])).resolve()
    command = [sys.executable, str(receipt_script), *sys.argv[1:]]
    return {
        "schema_version": 1,
        "case": case.group,
        "command": command,
        "script": str(receipt_script),
        "script_sha256": hashlib.sha256(receipt_script.read_bytes()).hexdigest(),
        "status": status,
        "actual_exit_code": actual_exit_code,
        "expected_exit_code": 0,
        "root": str(case.root),
        "receipt": str(case.receipt_root / "receipt.json"),
        "framework_log_root": str(LOG_DIR),
        "runtime": {
            "python": py_platform.python_version(),
            "implementation": py_platform.python_implementation(),
            "platform": sys.platform,
            "polars": _package_version("polars"),
            "deltalake": _package_version("deltalake"),
            "datacoolie": _package_version("datacoolie"),
        },
        "revision": worktree_fingerprint(USECASE_SIM_DIR),
        "revision_details": _revision_details(),
        "profile": {
            "engine": "polars",
            "platform": "local",
            "destination_format": "delta",
            "qualification_scope": "host-local-native",
        },
        "framework_sessions": [
            {"job_id": item.job_id, "stage": item.stage, "log_session_id": item.session_id}
            for item in runs
        ],
        "runs": [_json_value(dataclasses.asdict(item)) for item in runs],
        "details": _json_value(details),
        "qualification_gaps": list(qualification_gaps),
        "references": list(references),
    }


def write_receipt(case: CasePaths, payload: Mapping[str, Any]) -> Path:
    receipt_path = case.receipt_root / "receipt.json"
    if not receipt_path.resolve().is_relative_to(LOG_DIR.resolve()):
        raise AssertionError(f"receipt escaped canonical log root: {receipt_path}")
    with receipt_path.open("x", encoding="utf-8") as handle:
        json.dump(_json_value(payload), handle, indent=2, sort_keys=True)
        handle.write("\n")
    return receipt_path


def root_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="fresh case root below usecase-sim/.runtime/data (default: generated)",
    )


__all__ = [
    "CasePaths",
    "DriverEvidence",
    "GENERATED_COLUMNS",
    "PolarsEngine",
    "assert_exact_rows",
    "assert_schema",
    "assert_successful_flow",
    "business_rows",
    "canonical_rows",
    "materialize_fixture",
    "read_delta_rows",
    "read_delta_schema",
    "receipt_payload",
    "resolve_case_root",
    "root_argument",
    "run_driver",
    "write_parquet",
    "write_receipt",
]
