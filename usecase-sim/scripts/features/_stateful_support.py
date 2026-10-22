"""Shared helpers for focused native stateful qualification cases.

The helpers keep each case under a new simulator-owned data root while using
the current shared framework log root.  They intentionally execute the normal
``DataCoolieDriver`` path; direct engine writes are used only to seed source
files and native Delta targets where a case needs an existing table.
"""

from __future__ import annotations

import argparse
import copy
import dataclasses
import hashlib
import importlib.metadata
import json
import os
import platform as py_platform
import subprocess
import sys
import time as monotonic_time
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
from runner.runtime_paths import (  # noqa: E402
    DATA_DIR,
    INPUT_DIR,
    LOG_DIR,
    OUTPUT_DIR,
    STATE_DIR,
    WATERMARKS_DIR,
)
from runner.runtime_timing import measured, phase  # noqa: E402
from cases.receipts import worktree_fingerprint  # noqa: E402

from datacoolie.core.models.run_config import DataCoolieRunConfig  # noqa: E402
from datacoolie.engines.polars_engine import PolarsEngine  # noqa: E402
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


@dataclass
class CasePaths:
    """Case-owned paths and the mapped receipt/watermark roots."""

    group: str
    root: Path
    input: Path
    output: Path
    state: Path
    watermarks: Path
    receipt_root: Path
    # Authored cases keep their canonical flat source paths so the provider
    # can validate the same files used by the simulator.  Generated cases
    # carry a validated document in memory and leave ``sources`` empty.
    document: dict[str, Any] = dataclasses.field(default_factory=dict)
    sources: tuple[str, ...] = ()


@dataclass(frozen=True)
class DriverEvidence:
    """One normal Driver invocation plus its current-session records."""

    job_id: str
    stage: str
    session_id: str
    result: dict[str, Any]
    records: tuple[dict[str, Any], ...]
    metadata: dict[str, Any] = dataclasses.field(default_factory=dict)
    timings: dict[str, float] = dataclasses.field(default_factory=dict)


def _case_relative(root: Path) -> Path:
    """Return the output-root relative suffix used by every case namespace."""

    resolved = root.resolve()
    try:
        return resolved.relative_to(OUTPUT_DIR.resolve())
    except ValueError:
        # Keep explicit legacy roots below DATA_DIR usable during cutover.
        return resolved.relative_to(DATA_DIR.resolve())


def _mapped_runtime_root(root: Path, base: Path, *, scenario_logs: bool = False) -> Path:
    """Map one output case root beneath a canonical runtime namespace."""

    relative = _case_relative(root)
    prefix = base / "scenarios" if scenario_logs else base
    return (prefix.resolve() / relative).resolve()


def resolve_case_root(group: str, requested: Path | None = None) -> CasePaths:
    """Allocate a fresh case root and validate all derived runtime roots."""

    if requested is None:
        relative = Path("behavior_cases") / f"{group}_polars_{uuid.uuid4().hex[:12]}"
        root = (OUTPUT_DIR / relative).resolve()
    else:
        root = requested if requested.is_absolute() else PRODUCT_ROOT / requested
        root = root.resolve()
    data_root = DATA_DIR.resolve()
    if root == data_root or not root.is_relative_to(data_root):
        raise ValueError(
            "--root must be a new directory strictly below usecase-sim/.runtime/data"
        )

    relative = _case_relative(root)
    input_root = (INPUT_DIR / relative).resolve()
    state_root = (STATE_DIR / relative).resolve()
    receipt_root = _mapped_runtime_root(root, LOG_DIR, scenario_logs=True)
    watermark_root = _mapped_runtime_root(root, WATERMARKS_DIR)
    for label, path, base in (
        ("input", input_root, INPUT_DIR),
        ("state", state_root, STATE_DIR),
        ("receipt", receipt_root, LOG_DIR / "scenarios"),
        ("watermark", watermark_root, WATERMARKS_DIR),
    ):
        namespace = base.resolve()
        if path == namespace or not path.is_relative_to(namespace):
            raise ValueError(f"native {label} root escaped its canonical namespace: {path}")
    owned = [root, input_root, state_root, receipt_root, watermark_root]
    if any(left == right or left.is_relative_to(right) or right.is_relative_to(left)
           for index, left in enumerate(owned) for right in owned[index + 1:]):
        raise ValueError("native case runtime roots must be separate directories")
    existing = [
        ("output", root),
        ("input", input_root),
        ("state", state_root),
        ("receipt", receipt_root),
        ("watermark", watermark_root),
    ]
    for label, path in existing:
        if path.exists():
            raise ValueError(f"native {label} root must be new and unused: {path}")

    root.mkdir(parents=True, exist_ok=False)
    input_root.mkdir(parents=True, exist_ok=False)
    state_root.mkdir(parents=True, exist_ok=False)
    receipt_root.mkdir(parents=True, exist_ok=False)
    watermark_root.mkdir(parents=True, exist_ok=False)
    paths = CasePaths(
        group=group,
        root=root,
        input=input_root,
        output=root,
        state=state_root,
        watermarks=watermark_root,
        receipt_root=receipt_root,
    )
    return paths


def materialize_fixture(case: CasePaths, fixture_name: str) -> None:
    """Select one flat authored recipe and bind it to this case root.

    The old focused fixture copies were generated metadata artifacts.  The
    canonical source is now ``metadata/file/<group>.json`` plus the shared
    section files; ``metadata_sources.recipe_document`` returns the exact
    flow/connection projection without creating a runtime metadata file.
    """

    from runner.metadata_sources import recipe_document

    recipe = Path(fixture_name).stem
    document = recipe_document(
        case.group,
        recipe,
        tokens={
            "__CASE_ROOT__/input": case.input.as_posix(),
            "__CASE_ROOT__/output": case.output.as_posix(),
            "__CASE_ROOT__": case.root.as_posix(),
        },
    )
    if not isinstance(document, dict) or not document.get("connections"):
        raise ValueError(f"metadata recipe is not a FileProvider document: {case.group}/{recipe}")
    case.document = copy.deepcopy(document)
    case.sources = (
        str((USECASE_SIM_DIR / "metadata" / "file" / f"{case.group}.json").resolve()),
    )


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


@measured("driver_and_evidence_s")
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
    invocation_started = monotonic_time.perf_counter()
    evidence_before_started = invocation_started
    previous = job_sessions(LOG_DIR, job_id)
    evidence_before_s = monotonic_time.perf_counter() - evidence_before_started
    metadata_started = monotonic_time.perf_counter()
    platform = LocalPlatform()
    if metadata_provider is not None:
        provider = metadata_provider
    else:
        from runner.metadata_sources import make_case_provider

        provider_kwargs = {
            "platform": platform,
            "watermark_base_path": str(case.watermarks / "provider"),
            "stage": stage,
        }
        if maintenance:
            provider_kwargs["maintenance"] = True
        provider = make_case_provider(case, **provider_kwargs)
    from runner.metadata_sources import case_metadata_provenance

    provenance = case_metadata_provenance(case, stage=stage, maintenance=maintenance)
    provenance["stage"] = stage
    metadata_s = monotonic_time.perf_counter() - metadata_started
    startup_started = monotonic_time.perf_counter()
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
        startup_s = monotonic_time.perf_counter() - startup_started
        execution_started = monotonic_time.perf_counter()
        with phase("execution_s"):
            if maintenance:
                result = driver.run_maintenance(
                    dataflows=provider.get_dataflows(stage=stage),
                    do_compact=do_compact,
                    do_cleanup=do_cleanup,
                )
            else:
                result = driver.run(stage=stage)
        execution_s = monotonic_time.perf_counter() - execution_started
        close_started = monotonic_time.perf_counter()

    close_s = monotonic_time.perf_counter() - close_started
    evidence_started = monotonic_time.perf_counter()
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
    evidence_s = evidence_before_s + monotonic_time.perf_counter() - evidence_started
    return DriverEvidence(
        job_id=job_id,
        stage=stage,
        session_id=session_id,
        result=dataclasses.asdict(result),
        records=tuple(matching),
        metadata=provenance,
        timings={"metadata_preparation_s": metadata_s, "driver_startup_s": startup_s,
                 "execution_s": execution_s, "driver_close_s": close_s,
                 "evidence_lookup_s": evidence_s,
                 "total_s": monotonic_time.perf_counter() - invocation_started},
    )


def assert_successful_flow(
    evidence: DriverEvidence,
    flow_name: str,
    *,
    expected_dataflow_id: str | None = None,
    workspace_id: str | None = None,
) -> None:
    """Require one successful intended-flow row in this invocation's session.

    ``expected_dataflow_id`` is used by fixtures that persist an explicit
    backend identity. Existing callers retain the authored-name UUID behavior
    when it is omitted.
    """

    expected_counts = {"succeeded": 1, "failed": 0, "skipped": 0}
    actual_counts = {key: evidence.result.get(key) for key in expected_counts}
    if actual_counts != expected_counts:
        raise AssertionError(f"{flow_name} invocation did not succeed: {actual_counts!r}")
    if not evidence.session_id:
        raise AssertionError(f"{flow_name} has no current session evidence")
    expected_id = (
        expected_dataflow_id
        if expected_dataflow_id is not None
        else name_to_uuid(flow_name, workspace_id)
    )
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
    from runner.metadata_sources import case_metadata_provenance

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
        "metadata_provenance": [item.metadata for item in runs if item.metadata] or [case_metadata_provenance(case)],
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
    print("SIM_METADATA_PROVENANCE=" + json.dumps({"invocations": payload.get("metadata_provenance", [])}, separators=(",", ":")), flush=True)
    return receipt_path


def root_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="fresh output case root below usecase-sim/.runtime/data (default: generated)",
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
