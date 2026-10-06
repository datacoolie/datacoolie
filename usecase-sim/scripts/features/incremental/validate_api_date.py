"""Date-only initial/new-date/stable sequence and lossless precision rejection."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _stateful_support import (
    resolve_case_root,
    materialize_fixture,
    root_argument,
    run_driver,
    assert_successful_flow,
    assert_exact_rows,
    business_rows,
    receipt_payload,
    write_receipt,
    LOG_DIR,
)
from read.api_rejection_support import seed_checkpoint, run_rejection, snapshot
from runner.runtime_logs import records
from datacoolie.watermark.base import WatermarkSerializer
from datacoolie.utils.identity import name_to_uuid

PRECISION_ERROR = (
    "API lower bound for field 'event_date' cannot be represented losslessly with format 'date': "
    "API date range format cannot represent a non-midnight datetime without loss of precision. "
    "| Details: {'field': 'event_date', 'side': 'lower', 'format': 'date'}"
)
EXPECTED = [
    {"order_id": 101, "event_date": date(2024, 1, 1), "category": "alpha"},
    {"order_id": 102, "event_date": date(2024, 1, 2), "category": "beta"},
    {"order_id": 103, "event_date": date(2024, 1, 3), "category": "gamma"},
]


def assert_date_state(rows, checkpoint, expected):
    assert_exact_rows(rows, expected, key="order_id")
    # API observed_max is captured from the response before DATE projection;
    # the existing scalar contract preserves the canonical source string.
    if checkpoint != {"event_date": expected[-1]["event_date"].isoformat()}:
        raise AssertionError(f"Exact date checkpoint mismatch: {checkpoint!r}")


def _checkpoint(case):
    files = list(case.watermarks.rglob("watermark_value.json"))
    if len(files) != 1:
        raise AssertionError(f"Expected one date checkpoint, got {files!r}")
    return WatermarkSerializer.deserialize(files[0].read_text())


def _trace(url, token):
    import httpx

    response = httpx.get(
        url.rstrip("/") + "/__sim/date-only/trace",
        params={"case_id": hashlib.sha256(token.encode()).hexdigest()[:16]},
        timeout=5,
    )
    response.raise_for_status()
    return response.json()["requests"]


def validate(
    root=None, *, negative=False, engine_name="polars", qualification_scope=None
):
    import polars as pl

    case = resolve_case_root("incremental", root)
    materialize_fixture(
        case,
        "api_date_precision_negative.json" if negative else "api_date_positive.json",
    )
    document = json.loads(case.metadata.read_text())
    token = "date-" + uuid4().hex
    url = os.getenv(
        "DATACOOLIE_MOCK_API_URL",
        "http://mock-api:8082" if engine_name == "spark" else "http://localhost:8082",
    )
    document["connections"][0]["configure"].update(
        base_url=url, default_headers={"X-Sim-Case-Token": token}
    )
    flow = document["dataflows"][0]
    case.metadata.write_text(json.dumps(document, indent=2))
    target = case.output / "orders"
    runs, details = [], {"engine": engine_name, "fresh_case_roots": True}
    if negative:
        # An occupied target and non-midnight checkpoint make preservation observable.
        target.mkdir(parents=True, exist_ok=False)
        pl.DataFrame(EXPECTED[:1]).write_parquet(target / "seed.parquet")
        seed_checkpoint(case, flow["name"], {"event_date": datetime(2024, 1, 2, 10, 5)})
        if _trace(url, token) != []:
            raise AssertionError("Fresh date case already has HTTP activity")
        run, detail = run_rejection(
            case,
            flow=flow["name"],
            stage=flow["stage"],
            error=PRECISION_ERROR,
            engine=engine_name,
        )
        runs.append(run)
        if _trace(url, token) != []:
            raise AssertionError("Precision rejection dispatched HTTP")
        details.update(detail, no_http=True)
    else:
        spark = None
        if engine_name == "spark":
            from runner._runner_utils import build_spark_session
            from datacoolie.engines.spark_engine import SparkEngine

            spark = build_spark_session(
                app_name="date-only-case",
                extra_config={"spark.sql.shuffle.partitions": "1"},
            )
        try:
            for iteration in (1, 2, 3):
                flow["source"]["configure"]["params"] = {
                    "include_late": "false" if iteration == 1 else "true"
                }
                case.metadata.write_text(json.dumps(document, indent=2))
                before_output, before_state = (
                    snapshot(target),
                    snapshot(case.watermarks),
                )
                # Each Driver owns/binds a fresh engine/platform instance.
                engine = SparkEngine(spark_session=spark) if spark else None
                run = run_driver(
                    case, stage=flow["stage"], iteration=iteration, engine=engine
                )
                runs.append(run)
                if iteration < 3:
                    assert_successful_flow(run, flow["name"])
                else:
                    matching = [
                        r
                        for _, r in records(
                            LOG_DIR, run.job_id, "execution_logs", run.session_id
                        )
                        if r.get("_type") == "dataflow_run_log"
                    ]
                    if (
                        len(matching) != 1
                        or matching[0].get("dataflow_id") != name_to_uuid(flow["name"])
                        or matching[0].get("status") != "skipped"
                    ):
                        raise AssertionError(
                            "Stable rerun lacks current expected-flow skip"
                        )
                    if (
                        snapshot(target) != before_output
                        or snapshot(case.watermarks) != before_state
                    ):
                        raise AssertionError("Stable rerun changed output/state")
                files = sorted(target.rglob("*.parquet"))
                rows = business_rows(
                    pl.concat([pl.read_parquet(p) for p in files]).to_dicts()
                )
                assert_date_state(
                    rows,
                    _checkpoint(case),
                    EXPECTED[:2] if iteration == 1 else EXPECTED,
                )
            trace = _trace(url, token)
            expected_requests = [
                {"include_late": "false", "offset": "0", "limit": "5"},
                {
                    "include_late": "true",
                    "offset": "0",
                    "limit": "5",
                    "modified_since": "2024-01-02",
                },
                {
                    "include_late": "true",
                    "offset": "0",
                    "limit": "5",
                    "modified_since": "2024-01-03",
                },
            ]
            if trace != expected_requests:
                raise AssertionError(
                    f"Noncanonical/nonexclusive date request sequence: {trace!r}"
                )
            details.update(
                requests=trace,
                checkpoint=_checkpoint(case),
                exact_rows=True,
                stable_rerun=True,
            )
        finally:
            if spark:
                spark.stop()
    payload = receipt_payload(case, runs=runs, details=details, script=Path(__file__))
    payload["profile"].update(
        engine=engine_name,
        destination_format="parquet",
        qualification_scope=qualification_scope or f"{engine_name}-mock-api-date-only",
    )
    return write_receipt(case, payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    parser.add_argument("--negative", action="store_true")
    parser.add_argument("--engine", choices=["polars", "spark"], default="polars")
    args = parser.parse_args()
    print(validate(args.root, negative=args.negative, engine_name=args.engine))
