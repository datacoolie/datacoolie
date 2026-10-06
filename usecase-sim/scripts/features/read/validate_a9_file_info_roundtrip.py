"""Validate a normal Driver roundtrip over its own flat-file output.

Native Polars writes a CSV snapshot containing historical file metadata.  The
single authored flow reads that persisted snapshot through ``FileReader`` and
writes a Delta target.  Native Polars reads are used only as independent
persisted-output oracles.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
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
    assert_successful_flow,
    business_rows,
    materialize_fixture,
    read_delta_rows,
    read_delta_schema,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_receipt,
)


READ_FLOW = "a9-file-info-roundtrip-read"
READ_STAGE = "a9_file_info_roundtrip_read"
SOURCE_ROWS = [
    {"id": "A-001", "value": "café / 你好"},
    {"id": "A-002", "value": "quoted, semicolon; and | pipe"},
]
HISTORICAL_SOURCE_PATH = "historical/source.csv"
HISTORICAL_SOURCE_ROWS = [
    {
        **row,
        "__file_path": HISTORICAL_SOURCE_PATH,
        "__file_name": "source.csv",
        "__file_modification_time": "2020-01-01T00:00:00Z",
        "__created_at": "created-a" if index == 0 else "created-b",
    }
    for index, row in enumerate(SOURCE_ROWS)
]
EXPECTED_SCHEMA = {"id": "String", "value": "String"}
FILE_INFO_COLUMNS = {
    "__file_name",
    "__file_path",
    "__file_modification_time",
}


def _flat_files(path: Path) -> tuple[Path, ...]:
    files = tuple(sorted(item for item in path.rglob("*.csv") if item.is_file()))
    if len(files) != 1:
        raise AssertionError(
            f"expected one framework CSV output under {path}, found "
            f"{[item.name for item in files]!r}"
        )
    return files


def _read_native_flat_output(path: Path) -> tuple[pl.DataFrame, dict[str, str]]:
    """Read one output file with Polars outside the framework reader path."""

    [file_path] = _flat_files(path)
    frame = pl.read_csv(file_path, has_header=True, infer_schema=False)
    schema = {
        name: str(dtype)
        for name, dtype in frame.schema.items()
        if name not in GENERATED_COLUMNS
    }
    return frame, schema


def assert_native_flat_output(
    frame: pl.DataFrame,
    expected_rows: Iterable[Mapping[str, Any]],
    *,
    expected_source_path: str,
    label: str = "a9-file-info-flat",
) -> None:
    """Check persisted business rows and source metadata independently."""

    assert_exact_rows(
        business_rows(frame.to_dicts()),
        expected_rows,
        key="id",
        label=f"{label}-rows",
    )
    actual_schema = {
        name: str(dtype)
        for name, dtype in frame.schema.items()
        if name not in GENERATED_COLUMNS
    }
    if actual_schema != EXPECTED_SCHEMA:
        raise AssertionError(
            f"{label}-schema mismatch: actual={actual_schema!r}, "
            f"expected={EXPECTED_SCHEMA!r}"
        )
    missing = FILE_INFO_COLUMNS - set(frame.columns)
    if missing:
        raise AssertionError(f"{label}: missing persisted file-info columns {sorted(missing)!r}")
    if "__created_at" not in frame.columns:
        raise AssertionError(f"{label}: destination SystemColumn __created_at was dropped")
    if any(column.endswith("_right") for column in frame.columns):
        raise AssertionError(f"{label}: suffixed duplicate columns were persisted")
    if set(frame["__file_path"].to_list()) != {expected_source_path}:
        raise AssertionError(
            f"{label}: source file path metadata mismatch: "
            f"actual={frame['__file_path'].unique().to_list()!r}, "
            f"expected={expected_source_path!r}"
        )


def assert_roundtrip_read_output(
    rows: Iterable[Mapping[str, Any]],
    schema: Mapping[str, str],
    expected_rows: Iterable[Mapping[str, Any]],
    *,
    expected_file_path: str,
    expected_file_name: str,
    expected_file_mtime: datetime,
    label: str = "a9-file-info-roundtrip",
) -> None:
    """Check the framework-read Delta output and current file metadata."""

    actual_rows = list(rows)
    assert_exact_rows(
        business_rows(actual_rows),
        expected_rows,
        key="id",
        label=f"{label}-rows",
    )
    actual_schema = {
        name: value for name, value in schema.items() if name not in GENERATED_COLUMNS
    }
    if actual_schema != EXPECTED_SCHEMA:
        raise AssertionError(
            f"{label}-schema mismatch: actual={actual_schema!r}, "
            f"expected={EXPECTED_SCHEMA!r}"
        )
    for column in FILE_INFO_COLUMNS | {"__created_at"}:
        if not all(column in row for row in actual_rows):
            raise AssertionError(f"{label}: output is missing required column {column!r}")
    if any(column.endswith("_right") for column in actual_rows[0]):
        raise AssertionError(f"{label}: suffixed duplicate columns reached the destination")
    if {row["__file_path"] for row in actual_rows} != {expected_file_path}:
        raise AssertionError(f"{label}: current file path metadata is stale")
    if {row["__file_name"] for row in actual_rows} != {expected_file_name}:
        raise AssertionError(f"{label}: current file name metadata is stale")
    expected_mtime = expected_file_mtime
    actual_mtimes = {row["__file_modification_time"] for row in actual_rows}
    if actual_mtimes != {expected_mtime}:
        raise AssertionError(
            f"{label}: current file modification metadata is stale: "
            f"actual={actual_mtimes!r}, expected={expected_mtime!r}"
        )


def validate(root: Path | None = None) -> dict[str, Any]:
    """Seed native flat output, then run the read phase through normal Driver."""

    case = resolve_case_root("read", root)
    materialize_fixture(case, "a9_file_info_roundtrip.json")
    engine = PolarsEngine()
    flat_path = case.input / "flat" / "own_output"
    delta_path = case.output / "delta" / "roundtrip"

    # Native setup is the independent writer proof: the framework must read
    # historical metadata persisted in its own flat output directory.
    engine.write_to_path(
        engine.create_dataframe(HISTORICAL_SOURCE_ROWS),
        str(flat_path),
        "overwrite",
        "csv",
    )

    native_frame, native_schema = _read_native_flat_output(flat_path)
    assert_native_flat_output(
        native_frame,
        SOURCE_ROWS,
        expected_source_path=HISTORICAL_SOURCE_PATH,
    )

    run_read = run_driver(case, stage=READ_STAGE, iteration=1)
    assert_successful_flow(run_read, READ_FLOW)

    [flat_file] = _flat_files(flat_path)
    from datacoolie.platforms.local_platform import LocalPlatform

    [flat_info] = LocalPlatform().list_files(str(flat_path), extension=".csv")
    flat_file_mtime = flat_info.modification_time
    if flat_file_mtime is None:  # pragma: no cover - LocalPlatform always reports mtime.
        raise AssertionError(f"flat output has no modification time: {flat_file}")
    roundtrip_rows = read_delta_rows(engine, delta_path)
    roundtrip_schema = read_delta_schema(engine, delta_path)
    assert_roundtrip_read_output(
        roundtrip_rows,
        roundtrip_schema,
        SOURCE_ROWS,
        expected_file_path=flat_info.path,
        expected_file_name=flat_info.name,
        expected_file_mtime=flat_file_mtime,
    )

    payload = receipt_payload(
        case,
        runs=(run_read,),
        details={
            "read_flow": READ_FLOW,
            "native_flat_schema": native_schema,
            "roundtrip_delta_schema": roundtrip_schema,
            "native_flat_rows": business_rows(native_frame.to_dicts()),
            "roundtrip_rows": business_rows(roundtrip_rows),
            "flat_output": flat_file.as_posix(),
            "current_file_info": {
                "path": flat_info.path,
                "name": flat_info.name,
                "modification_time": flat_file_mtime.isoformat(),
            },
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies local Polars plus native Delta-rs only; "
            "other engines, services and cloud profiles remain unqualified.",
        ),
        references=(
            "src/datacoolie/engines/_polars/file_io.py",
            "src/datacoolie/sources/file_reader.py",
        ),
    )
    payload["profile"]["destination_format"] = "delta"
    payload["profile"]["qualification_scope"] = "host-polars-file-info-roundtrip"
    write_receipt(case, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    args = parser.parse_args()
    payload = validate(args.root)
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
