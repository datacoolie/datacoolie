"""Qualify native Polars overwrite/append persistence and rerun behavior."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))

from _stateful_support import (  # noqa: E402
    PolarsEngine,
    assert_exact_rows,
    business_rows,
    materialize_fixture,
    read_delta_rows,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_parquet,
    write_receipt,
)


def validate(root: Path | None = None) -> dict:
    case = resolve_case_root("write_basic", root)
    materialize_fixture(case, "write_basic.json")
    engine = PolarsEngine()
    epoch = datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()

    write_parquet(
        engine,
        case.input / "overwrite_source",
        [{"id": 1, "value": "one"}, {"id": 2, "value": "two"}],
        mtime=epoch,
    )
    write_parquet(
        engine,
        case.input / "append_source",
        [{"id": 10, "value": "ten"}],
        mtime=epoch + 10,
    )

    runs = [
        run_driver(case, stage="write_basic_overwrite", iteration=1),
        run_driver(case, stage="write_basic_append", iteration=1),
    ]
    assert runs[0].result["succeeded"] == 1, runs[0].result
    assert runs[1].result["succeeded"] == 1, runs[1].result
    assert_exact_rows(
        business_rows(read_delta_rows(engine, case.output / "overwrite_target")),
        [{"id": 1, "value": "one"}, {"id": 2, "value": "two"}],
        key="id",
        label="overwrite after initial run",
    )
    assert_exact_rows(
        business_rows(read_delta_rows(engine, case.output / "append_target")),
        [{"id": 10, "value": "ten"}],
        key="id",
        label="append after initial run",
    )

    # A later file is a real incremental observation.  The prior file stays
    # below the persisted mtime watermark and must remain in the target.
    write_parquet(
        engine,
        case.input / "append_source",
        [{"id": 11, "value": "eleven"}],
        mode="append",
        mtime=epoch + 20,
    )
    runs.append(run_driver(case, stage="write_basic_append", iteration=2))
    assert runs[-1].result["succeeded"] == 1, runs[-1].result
    assert_exact_rows(
        business_rows(read_delta_rows(engine, case.output / "append_target")),
        [{"id": 10, "value": "ten"}, {"id": 11, "value": "eleven"}],
        key="id",
        label="append after later file",
    )

    # A changed overwrite source proves replacement semantics: an old ID must
    # disappear while a retained ID changes and a new ID is added.
    write_parquet(
        engine,
        case.input / "overwrite_source",
        [{"id": 1, "value": "one-updated"}, {"id": 3, "value": "three"}],
        mtime=epoch + 20,
    )

    # Rerunning without a new append source file is a source-empty no-op.
    runs.append(run_driver(case, stage="write_basic_append", iteration=3))
    runs.append(run_driver(case, stage="write_basic_overwrite", iteration=2))
    assert runs[-2].result["skipped"] == 1, runs[-2].result
    assert runs[-1].result["succeeded"] == 1, runs[-1].result
    assert_exact_rows(
        business_rows(read_delta_rows(engine, case.output / "append_target")),
        [{"id": 10, "value": "ten"}, {"id": 11, "value": "eleven"}],
        key="id",
        label="append rerun",
    )
    assert_exact_rows(
        business_rows(read_delta_rows(engine, case.output / "overwrite_target")),
        [{"id": 1, "value": "one-updated"}, {"id": 3, "value": "three"}],
        key="id",
        label="overwrite rerun",
    )

    payload = receipt_payload(
        case,
        runs=runs,
        details={
            "overwrite_rows": business_rows(
                read_delta_rows(engine, case.output / "overwrite_target")
            ),
            "append_rows": business_rows(
                read_delta_rows(engine, case.output / "append_target")
            ),
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies local Polars plus native Delta-rs only; Spark and remote lakehouse profiles remain unqualified.",
        ),
        references=(
            "src/datacoolie/destinations/delta_writer.py",
            "src/datacoolie/engines/polars_engine.py",
        ),
    )
    write_receipt(case, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    args = parser.parse_args()
    payload = validate(args.root)
    print({"status": payload["status"], "receipt": payload["receipt"], "root": payload["root"], "profile": payload["profile"]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
