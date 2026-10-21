"""Qualify persisted CSV header/separator precedence through normal Driver."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping

import polars as pl


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))

from _stateful_support import (  # noqa: E402
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


FLOW_NAME = "a9-write-csv-options"
STAGE_NAME = "a9_write_csv_options"
EXPECTED_BUSINESS_ROWS = [
    {"id": "A-001", "label": 'café "quoted", comma'},
    {"id": "A-002", "label": "東京 🚲 | pipe"},
]
EXPECTED_HEADER = (
    "id",
    "label",
    "__file_name",
    "__file_path",
    "__file_modification_time",
    "__created_at",
    "__updated_at",
    "__updated_by",
    "__dataflow_run_id",
)
EXPECTED_SCHEMA = {column: "String" for column in EXPECTED_HEADER}
EXPECTED_EFFECTIVE_OPTIONS = {"header": True, "sep": "|"}


def _csv_rows_from_bytes(raw: bytes) -> list[dict[str, str]]:
    text = raw.decode("utf-8")
    return list(csv.DictReader(io.StringIO(text), delimiter="|"))


def assert_csv_options_output(
    raw: bytes,
    parsed_rows: Iterable[Mapping[str, Any]],
    parsed_schema: Mapping[str, str],
    *,
    label: str = "a9-csv-options",
) -> None:
    """Require native bytes plus parsed rows/schema for the effective options."""

    expected_header_bytes = ("|".join(EXPECTED_HEADER) + "\n").encode("utf-8")
    if not raw.startswith(expected_header_bytes):
        raise AssertionError(
            f"{label}-header-bytes mismatch: {raw[: len(expected_header_bytes)]!r}"
        )
    if b";" in raw:
        raise AssertionError(f"{label}-bytes still contain the connection separator")
    if b"|" not in raw:
        raise AssertionError(f"{label}-bytes contain no configured separator")
    if raw.count(b"\n") != len(EXPECTED_BUSINESS_ROWS) + 1:
        raise AssertionError(f"{label}-bytes row count/newline shape mismatch")
    for fragment in (
        'A-001|"café ""quoted"", comma"|'.encode("utf-8"),
        'A-002|"東京 🚲 | pipe"|'.encode("utf-8"),
    ):
        if fragment not in raw:
            raise AssertionError(f"{label}-bytes missing exact business fragment: {fragment!r}")

    text_header = raw.decode("utf-8").splitlines()[0].split("|")
    if tuple(text_header) != EXPECTED_HEADER:
        raise AssertionError(
            f"{label}-header mismatch: actual={text_header!r}, expected={EXPECTED_HEADER!r}"
        )

    rows = list(parsed_rows)
    assert_exact_rows(
        business_rows(rows),
        EXPECTED_BUSINESS_ROWS,
        key="id",
        label=f"{label}-rows",
    )
    assert_schema(parsed_schema, EXPECTED_SCHEMA, label=f"{label}-schema")

    native_rows = _csv_rows_from_bytes(raw)
    assert_exact_rows(
        [{key: row[key] for key in ("id", "label")} for row in native_rows],
        EXPECTED_BUSINESS_ROWS,
        key="id",
        label=f"{label}-native-csv-rows",
    )


def assert_a9_csv_metadata(metadata: Mapping[str, Any]) -> None:
    """Verify connection defaults and destination-level overrides are authored."""

    connections = {item["name"]: item for item in metadata["connections"]}
    flow = metadata["dataflows"][0]
    assert len(metadata["dataflows"]) == 1
    assert flow["name"] == FLOW_NAME
    assert flow["stage"] == STAGE_NAME
    assert connections["a9_csv_options_destination"]["configure"]["write_options"] == {
        "header": False,
        "sep": ";",
    }
    assert flow["destination"]["configure"]["write_options"] == EXPECTED_EFFECTIVE_OPTIONS
    assert connections["a9_csv_options_destination"]["format"] == "csv"


def assert_a9_csv_profile(payload: Mapping[str, Any]) -> None:
    profile = payload.get("profile")
    if not isinstance(profile, Mapping) or profile.get("destination_format") != "csv":
        raise AssertionError(f"a9-csv-options-profile mismatch: {profile!r}")


def validate(root: Path | None = None) -> dict[str, Any]:
    case = resolve_case_root("write_basic", root)
    materialize_fixture(case, "a9_csv_options.json")
    metadata = case.document
    assert_a9_csv_metadata(metadata)

    engine = PolarsEngine()
    write_parquet(engine, case.input / "rows", EXPECTED_BUSINESS_ROWS)
    evidence = run_driver(case, stage=STAGE_NAME, iteration=1)
    assert_successful_flow(evidence, FLOW_NAME)

    target = case.output / "records"
    files = tuple(sorted(target.glob("*.csv")))
    if files != (target / "records.csv",):
        raise AssertionError(f"unexpected CSV output files: {files!r}")
    raw = files[0].read_bytes()
    parsed = pl.read_csv(
        files[0],
        separator="|",
        has_header=True,
        infer_schema=False,
    )
    parsed_rows = parsed.to_dicts()
    parsed_schema = {name: str(dtype) for name, dtype in parsed.collect_schema().items()}
    assert_csv_options_output(raw, parsed_rows, parsed_schema)

    payload = receipt_payload(
        case,
        runs=[evidence],
        details={
            "effective_write_options": EXPECTED_EFFECTIVE_OPTIONS,
            "header": raw.decode("utf-8").splitlines()[0],
            "bytes_sha256": hashlib.sha256(raw).hexdigest(),
            "byte_length": len(raw),
            "rows": business_rows(parsed_rows),
            "schema": parsed_schema,
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies local Polars file CSV persistence only; Spark and remote file profiles remain unqualified.",
        ),
        references=(
            "src/datacoolie/core/models/destination.py",
            "src/datacoolie/engines/_polars/file_io.py",
        ),
    )
    # The shared helper defaults to Delta because most focused stateful cases
    # target native Delta.  This validator owns the actual flat-file profile.
    payload["profile"]["destination_format"] = "csv"
    assert_a9_csv_profile(payload)
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
