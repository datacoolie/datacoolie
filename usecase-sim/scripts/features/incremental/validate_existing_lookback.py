"""Existing file-mtime/lookback fixtures: exact selected IDs and checkpoint."""

from __future__ import annotations
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _fixture_support import compile_existing  # noqa: E402
from _stateful_support import (  # noqa: E402
    assert_exact_rows,
    assert_successful_flow,
    business_rows,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_receipt,
)
from datacoolie.metadata.file_provider import FileProvider  # noqa: E402
from datacoolie.platforms.local_platform import LocalPlatform  # noqa: E402
from datacoolie.utils.identity import name_to_uuid  # noqa: E402


def validate(root=None, mode="days"):
    import polars as pl

    flow = "watermark__file_mtime" if mode == "mtime" else "backward__" + mode
    case = resolve_case_root("incremental", root)
    document, identity = compile_existing(case, "incremental", flow)
    destination = document["dataflows"][0]["destination"]
    destination["load_type"] = "overwrite"  # Observe this phase's selection directly.
    case.metadata.write_text(json.dumps(document, indent=2))
    current = datetime(2024, 3, 20, 12, tzinfo=timezone.utc)
    # Closing-day uses the current operation reference clock, not the saved
    # watermark. Prepare known files around the real month's boundary.
    if mode == "months_closing_day":
        current = datetime.now(timezone.utc).replace(microsecond=0)
        month = current.month - (2 if current.day <= 10 else 1)
        year = current.year
        while month <= 0:
            month += 12
            year -= 1
        closing_lower = datetime(year, month, 1, tzinfo=timezone.utc)
    cutoffs = {
        "mtime": current,
        "days": datetime(2024, 3, 13, 12, tzinfo=timezone.utc),
        "months_closing_day": closing_lower
        if mode == "months_closing_day"
        else current,
        "hours": datetime(2024, 3, 20, 0, tzinfo=timezone.utc),
        "years": datetime(2023, 3, 20, 12, tzinfo=timezone.utc),
    }
    lower = cutoffs[mode]
    folder = case.input / document["dataflows"][0]["source"]["table"]
    folder.mkdir(parents=True, exist_ok=False)
    points = [
        ("excluded", lower.timestamp() - 60),
        ("eligible", lower.timestamp() + 60),
        ("newest", current.timestamp() + 3600),
    ]
    for index, (label, mtime) in enumerate(points, 1):
        path = folder / f"{label}.csv"
        row = {"id": index, "value": label}
        if mode == "mtime":
            row.update(
                __file_path="historical/source.csv",
                __file_name="historical.csv",
                __file_modification_time="2099-01-01T00:00:00",
                __created_at="2020-01-01T00:00:00",
            )
        pl.DataFrame([row]).write_csv(path)
        os.utime(path, (mtime, mtime))
    provider = FileProvider(
        config_path=str(case.metadata),
        platform=LocalPlatform(),
        watermark_base_path=str(case.watermarks / "provider"),
    )
    provider.initialize()
    try:
        provider.update_watermark(
            name_to_uuid(identity["runtime_flow"]),
            json.dumps(
                {"__file_modification_time": {"__datetime__": current.isoformat()}}
            ),
        )
    finally:
        provider.close()
    evidence = run_driver(case, stage=identity["runtime_stage"])
    assert_successful_flow(evidence, identity["runtime_flow"])
    output = case.output / destination["table"]
    persisted = pl.concat([pl.read_parquet(p) for p in output.rglob("*.parquet")])
    actual = business_rows(persisted.to_dicts())
    expected = [{"id": 2, "value": "eligible"}, {"id": 3, "value": "newest"}]
    assert_exact_rows(actual, expected, key="id")
    if mode == "mtime":
        for row in persisted.to_dicts():
            filename = row["value"] + ".csv"
            assert Path(row["__file_path"]).resolve() == (folder / filename).resolve()
            assert row["__file_name"] == filename
            expected_mtime = dict(points)[row["value"]]
            stamp_value = row["__file_modification_time"]
            assert (
                stamp_value.replace(tzinfo=timezone.utc).timestamp() == expected_mtime
            )
        assert "__created_at" in persisted.columns
    checkpoint_files = list(case.watermarks.rglob("watermark_value.json"))
    assert len(checkpoint_files) == 1
    checkpoint = json.loads(checkpoint_files[0].read_text())
    stamp = checkpoint["__file_modification_time"]["__datetime__"]
    assert datetime.fromisoformat(stamp).timestamp() == current.timestamp() + 3600, (
        stamp
    )
    payload = receipt_payload(
        case,
        runs=[evidence],
        script=Path(__file__),
        details={
            "mode": mode,
            "identity_mapping": identity,
            "effective_lower": lower.isoformat(),
            "current_precondition": current.isoformat(),
            "selection": actual,
            "checkpoint": checkpoint,
            "historical_metadata_guard": mode == "mtime",
            "compiled_variations": [
                "case-owned paths/name",
                "destination overwrite to observe selected batch",
            ],
        },
    )
    payload["profile"]["destination_format"] = "parquet"
    return write_receipt(case, payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    parser.add_argument(
        "--mode",
        choices=["mtime", "days", "months_closing_day", "hours", "years"],
        default="days",
    )
    args = parser.parse_args()
    print(validate(args.root, args.mode))
