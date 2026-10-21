"""Qualify native Polars append persistence for each supported flat format."""

from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import polars as pl


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))

from _stateful_support import (  # noqa: E402
    GENERATED_COLUMNS,
    PolarsEngine,
    assert_exact_rows,
    assert_schema,
    assert_successful_flow,
    business_rows,
    materialize_fixture,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_parquet,
    write_receipt,
)


FORMATS = ("csv", "json", "jsonl", "avro")
EXPECTED_SCHEMA = {
    "id": "String",
    "value": "String",
    "details": "String",
}
ROWS_A = [
    {
        "id": "A-001",
        "value": 'café "quoted", comma',
        "details": "Xin chào; giữ nguyên",
    },
    {
        "id": "A-002",
        "value": "東京 🚲",
        "details": "field with | delimiter",
    },
]
ROWS_B = [
    {
        "id": "B-001",
        "value": 'mañana "B", comma',
        "details": "中文 / বাংলা",
    },
    {
        "id": "B-002",
        "value": "quote 'single' and \"double\"",
        "details": "pipe|comma,semicolon;",
    },
]
EXPECTED_ROWS = [*ROWS_A, *ROWS_B]


def _format_config(fmt: str) -> tuple[str, str, str]:
    normalized = fmt.strip().lower()
    if normalized not in FORMATS:
        raise ValueError(f"unsupported flat append format: {fmt!r}")
    return (
        f"append_{normalized}.json",
        f"flat-append-{normalized}",
        f"flat_append_{normalized}",
    )


def _output_files(path: Path, fmt: str) -> tuple[Path, ...]:
    files = tuple(sorted(item for item in path.rglob(f"*.{fmt}") if item.is_file()))
    all_files = tuple(sorted(item for item in path.rglob("*") if item.is_file()))
    if not files:
        raise AssertionError(f"flat append output has no .{fmt} files: {path}")
    if files != all_files:
        raise AssertionError(
            f"flat append output contains files with the wrong format: "
            f"expected .{fmt}, found {[item.name for item in all_files]!r}"
        )
    return files


def _read_flat_output(
    path: Path,
    fmt: str,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    files = _output_files(path, fmt)
    frames: list[pl.DataFrame] = []
    for item in files:
        if fmt == "csv":
            # CSV append must preserve every token as a string, including
            # values containing quotes and delimiters.
            frame = pl.read_csv(item, has_header=True, infer_schema=False)
        elif fmt == "json":
            frame = pl.read_json(item)
        elif fmt == "jsonl":
            frame = pl.read_ndjson(item)
        elif fmt == "avro":
            frame = pl.read_avro(item)
        else:  # pragma: no cover - _format_config validates the format first.
            raise ValueError(f"unsupported flat append format: {fmt!r}")
        frames.append(frame)
    frame = pl.concat(frames, how="vertical")
    schema = {
        name: str(dtype)
        for name, dtype in frame.collect_schema().items()
        if name not in GENERATED_COLUMNS
    }
    return frame.to_dicts(), schema


def _file_hashes(path: Path, fmt: str) -> dict[str, str]:
    files = _output_files(path, fmt)
    return {
        item.relative_to(path).as_posix(): hashlib.sha256(item.read_bytes()).hexdigest()
        for item in files
    }


def assert_flat_append_output(
    actual_rows: Iterable[Mapping[str, Any]],
    actual_schema: Mapping[str, str],
    expected_rows: Iterable[Mapping[str, Any]],
    *,
    label: str = "flat-append-output",
) -> None:
    """Require exact business rows and string schema after an append run."""

    try:
        assert_exact_rows(
            business_rows(actual_rows),
            expected_rows,
            key="id",
            label=f"{label}-rows",
        )
    except TypeError as exc:
        # The shared canonical sorter intentionally assumes comparable keys;
        # make a wrong-type business key a stable oracle failure instead of an
        # incidental Python sorting error.
        raise AssertionError(f"{label}-rows type mismatch") from exc
    assert_schema(actual_schema, EXPECTED_SCHEMA, label=f"{label}-schema")


def assert_append_file_history(
    hashes_after_a: Mapping[str, str],
    hashes_after_b: Mapping[str, str],
    *,
    fmt: str,
    label: str = "flat-append-files",
) -> None:
    """Require immutable A files plus at least one newly written B file."""

    normalized = fmt.strip().lower()
    if normalized not in FORMATS:
        raise AssertionError(f"{label}: unsupported format {fmt!r}")
    old_names = set(hashes_after_a)
    new_names = set(hashes_after_b)
    if not old_names:
        raise AssertionError(f"{label}: no files were present after batch A")
    missing = old_names - new_names
    if missing:
        raise AssertionError(f"{label}: batch-A files disappeared: {sorted(missing)!r}")
    changed = sorted(
        name
        for name in old_names & new_names
        if hashes_after_a[name] != hashes_after_b[name]
    )
    if changed:
        raise AssertionError(f"{label}: batch-A files changed: {changed!r}")
    added = new_names - old_names
    if not added:
        raise AssertionError(f"{label}: batch B did not add a new output file")
    wrong_extension = sorted(
        name for name in new_names if Path(name).suffix.lower() != f".{normalized}"
    )
    if wrong_extension:
        raise AssertionError(
            f"{label}: output files have the wrong extension: {wrong_extension!r}"
        )


def assert_flat_append_profile(
    payload: Mapping[str, Any],
    fmt: str,
    *,
    authored_destination_format: str | None = None,
    label: str = "flat-append-profile",
) -> None:
    """Require authored and receipt identities to name the selected format."""

    expected = fmt.strip().lower()
    profile = payload.get("profile")
    if not isinstance(profile, Mapping):
        raise AssertionError(f"{label}: receipt has no profile")
    actual = profile.get("destination_format")
    if actual != expected:
        raise AssertionError(
            f"{label}: destination format mismatch: actual={actual!r}, expected={expected!r}"
        )
    if authored_destination_format is not None:
        authored = authored_destination_format.strip().lower()
        if authored != expected:
            raise AssertionError(
                f"{label}: authored destination format mismatch: "
                f"actual={authored!r}, expected={expected!r}"
            )


def _validate_format(root: Path | None, fmt: str) -> dict[str, Any]:
    normalized = fmt.strip().lower()
    fixture_name, flow_name, stage = _format_config(normalized)
    case = resolve_case_root("write_basic", root)
    materialize_fixture(case, fixture_name)
    metadata = case.document
    connections = {item["name"]: item for item in metadata["connections"]}
    authored_destination_format = connections[
        f"flat_append_{normalized}_destination"
    ]["format"]
    engine = PolarsEngine()
    source_path = case.input / "source"
    target_path = case.output / "target"
    epoch = datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()

    write_parquet(engine, source_path, ROWS_A, mtime=epoch)
    run_a = run_driver(case, stage=stage, iteration=1)
    assert_successful_flow(run_a, flow_name)
    rows_a, schema_a = _read_flat_output(target_path, normalized)
    assert_flat_append_output(rows_a, schema_a, ROWS_A, label=f"{normalized}-after-a")
    hashes_a = _file_hashes(target_path, normalized)

    # Replacing the source folder creates a disjoint observation.  The target
    # is append-only, so the prior output files must remain byte-for-byte equal.
    write_parquet(
        engine,
        source_path,
        ROWS_B,
        mode="overwrite",
        mtime=epoch + 60,
    )
    run_b = run_driver(case, stage=stage, iteration=2)
    assert_successful_flow(run_b, flow_name)
    rows_ab, schema_ab = _read_flat_output(target_path, normalized)
    assert_flat_append_output(
        rows_ab,
        schema_ab,
        EXPECTED_ROWS,
        label=f"{normalized}-after-ab",
    )
    hashes_ab = _file_hashes(target_path, normalized)
    assert_append_file_history(
        hashes_a,
        hashes_ab,
        fmt=normalized,
        label=f"{normalized}-files",
    )

    payload = receipt_payload(
        case,
        runs=(run_a, run_b),
        details={
            "format": normalized,
            "flow": flow_name,
            "stage": stage,
            "expected_schema": EXPECTED_SCHEMA,
            "schema_after_a": schema_a,
            "schema_after_ab": schema_ab,
            "rows_after_a": business_rows(rows_a),
            "rows_after_ab": business_rows(rows_ab),
            "output_files_after_a": sorted(hashes_a),
            "output_files_after_ab": sorted(hashes_ab),
            "output_hashes_after_a": hashes_a,
            "output_hashes_after_ab": hashes_ab,
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies local Polars flat-file output only; other engines and remote profiles remain unqualified.",
        ),
        references=(
            "src/datacoolie/destinations/file_writer.py",
            "src/datacoolie/engines/_polars/file_io.py",
        ),
    )
    # receipt_payload intentionally keeps Delta as its shared default.  This
    # validator owns the selected flat destination identity for each profile.
    payload["profile"]["destination_format"] = normalized
    assert_flat_append_profile(
        payload,
        normalized,
        authored_destination_format=authored_destination_format,
    )
    write_receipt(case, payload)
    return payload


def validate(root: Path | None = None, fmt: str = "csv") -> dict[str, Any]:
    """Run one format-specific flat append case under a fresh case root."""

    return _validate_format(root, fmt)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    parser.add_argument("--format", dest="fmt", choices=FORMATS, required=True)
    args = parser.parse_args()
    payload = validate(args.root, args.fmt)
    print(
        {
            "status": payload["status"],
            "receipt": payload["receipt"],
            "root": payload["root"],
            "profile": payload["profile"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
