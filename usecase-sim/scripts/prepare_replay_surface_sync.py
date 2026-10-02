"""Prepare owned Amendment 5 replay/API fixtures.

The script creates only ``usecase-sim/.runtime/data/replay_surface_sync``
content.  It is intentionally independent of the scenario runner so setup
receipts capture the exact metadata, input, output, and state roots used by a
multi-invocation scenario.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polars as pl


SCRIPT = Path(__file__).resolve()
USECASE_SIM = SCRIPT.parents[1]
BASE = USECASE_SIM / ".runtime" / "data" / "replay_surface_sync"


def _path(case: str, engine: str, *parts: str) -> Path:
    root = (BASE / case / engine).resolve()
    if root == BASE.resolve() or not root.is_relative_to(BASE.resolve()):
        raise ValueError(f"fixture case escaped owned root: {case!r}/{engine!r}")
    return root.joinpath(*parts)


def _write_metadata(path: Path, *, case: str, engine: str, source: str, endpoint: str | None = None,
                    variant: str | None = None, next_mode: str | None = None,
                    response_column: str | None = "covered_at",
                    wire_format: str = "iso", watermark_value: str = "observed_max",
                    max_pages: int = 100,
                    input_subdir: str | None = None,
                    root_prefix: str = "./usecase-sim",
                    api_base_url: str = "http://localhost:8082") -> None:
    case_id = f"surface-{case}-{variant or 'base'}"
    stage = f"surface_sync_{case}"
    name = f"surface_{case}_{variant or 'base'}"
    root = f"{root_prefix}/.runtime/data/replay_surface_sync/{case}/{engine}"
    if source == "file":
        connections = [
            {
                "name": "surface_source",
                "connection_type": "file",
                "format": "parquet",
                "configure": {
                    "base_path": f"{root}/input"
                    + (f"/{input_subdir}" if input_subdir else "")
                },
            },
        ]
        source_doc = {
            "connection_name": "surface_source",
            "table": "sample",
            "watermark_columns": ["event_time"],
            "configure": {"backward_days": 1},
        }
    elif source == "api":
        connections = [
            {
                "name": "surface_source",
                "connection_type": "api",
                "format": "api",
                "configure": {"base_url": api_base_url, "timeout": 10},
            },
        ]
        source_doc = {
            "connection_name": "surface_source",
            "table": "orders",
            "watermark_columns": ["covered_at" if case == "c3" else "event_time"],
            "configure": {
                "endpoint": endpoint,
                "pagination_type": "next_link",
                "data_path": "data",
                "next_link_path": "paging.next",
                "page_size": 1,
                "max_pages": max_pages,
                "params": {"limit": 1},
                "range_param_mapping": {
                    "covered_at" if case == "c3" else "event_time": {
                        "lower": {"location": "params", "name": "covered_from" if case == "c3" else "from_time", "operator": ">="},
                        "upper": {"location": "params", "name": "covered_to" if case == "c3" else "to_time", "operator": "<"},
                        "format": wire_format,
                        "response_column": response_column,
                        "watermark_value": watermark_value,
                    }
                },
            },
        }
        if next_mode is not None:
            # The fixture mode selects the server response variant.  The
            # reader contract still receives one of its two supported
            # continuation policies: opaque for the opaque case and
            # repeat_query_bounds for the four bound-reconciliation cases.
            source_doc["configure"]["next_link_bound_mode"] = (
                "opaque" if next_mode == "opaque" else "repeat_query_bounds"
            )
            source_doc["configure"]["params"]["mode"] = next_mode
        if variant is not None and case == "c4":
            # Keep the variant visible in the request trace so a pre-HTTP
            # validation failure can prove that no case-specific request was
            # dispatched.
            source_doc["configure"]["params"]["variant"] = variant
    else:
        raise ValueError(f"unsupported surface source: {source}")

    connections.append(
        {
            "name": "surface_dest",
            "connection_type": "lakehouse",
            "format": "delta",
            "configure": {"base_path": f"{root}/output"},
        }
    )
    destination = {
        "connection_name": "surface_dest",
        "table": variant or case,
        "load_type": "append" if case in {"c3", "c4", "c5"} else "merge_overwrite",
    }
    if case == "c1":
        destination["merge_keys"] = ["id"]
        destination["configure"] = {"replace_by_watermark": True}
    payload = {
        "connections": connections,
        "dataflows": [{
            "dataflow_id": case_id,
            "name": name,
            "stage": stage,
            "processing_mode": "batch",
            "source": source_doc,
            "destination": destination,
        }],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_file_fixture(case_root: Path) -> None:
    rows = [
        {"id": 1, "event_time": datetime(2024, 1, 15, 10, tzinfo=timezone.utc), "value": "inside-1"},
        {"id": 2, "event_time": datetime(2024, 1, 16, 10, tzinfo=timezone.utc), "value": "inside-2"},
        {"id": 3, "event_time": datetime(2024, 1, 17, 10, tzinfo=timezone.utc), "value": "inside-3"},
        {"id": 4, "event_time": datetime(2024, 1, 18, 10, tzinfo=timezone.utc), "value": "outside-upper"},
        {"id": 5, "event_time": datetime(2024, 2, 1, 10, tzinfo=timezone.utc), "value": "outside-later"},
    ]
    # Keep the ordinary initial read outside the replay range.  Replay
    # invocations switch to the replay-only fixture through their metadata,
    # so final IDs prove that the bounded reads actually ran.
    initial_root = case_root / "input" / "initial" / "sample"
    replay_root = case_root / "input" / "replay" / "sample"
    initial_root.mkdir(parents=True, exist_ok=True)
    replay_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows[3:]).write_parquet(initial_root / "events.parquet")
    pl.DataFrame(rows[:3]).write_parquet(replay_root / "events.parquet")


def _reset_api_trace(run_id: str) -> None:
    query = urlencode({"run_id": run_id})
    request = Request(
        f"http://localhost:8082/__sim/trace/reset?{query}",
        method="POST",
    )
    with urlopen(request, timeout=3) as response:
        if response.status != 200:
            raise RuntimeError(f"mock API trace reset failed: HTTP {response.status}")


def _record(case: str, engine: str, metadata: list[Path]) -> None:
    receipt = {
        "case": case,
        "engine": engine,
        "prepared_at": datetime.now(timezone.utc).isoformat(),
        "script": str(SCRIPT),
        "script_sha256": hashlib.sha256(SCRIPT.read_bytes()).hexdigest(),
        "base": str(_path(case, engine)),
        "metadata": [str(item) for item in metadata],
    }
    receipt_path = _path(case, engine, "setup_receipt.json")
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")


def prepare(case: str, engine: str) -> None:
    case_root = _path(case, engine)
    if case_root.exists():
        shutil.rmtree(case_root)
    case_root.mkdir(parents=True, exist_ok=True)
    metadata: list[Path] = []
    root_prefix = "/datacoolie/usecase-sim" if engine == "spark" else "./usecase-sim"
    api_base_url = "http://mock-api:8082" if engine == "spark" else "http://localhost:8082"

    if case == "c1":
        _write_file_fixture(case_root)
        initial = case_root / "metadata" / f"c1_initial_{engine}.json"
        replay = case_root / "metadata" / f"c1_replay_{engine}.json"
        _write_metadata(initial, case="c1", engine=engine, source="file", input_subdir="initial", root_prefix=root_prefix,
                        api_base_url=api_base_url)
        _write_metadata(replay, case="c1", engine=engine, source="file", input_subdir="replay", root_prefix=root_prefix,
                        api_base_url=api_base_url)
        metadata.extend([initial, replay])
    elif case == "c3":
        primary = case_root / "metadata" / f"c3_{engine}.json"
        failed = case_root / "metadata" / f"c3_incomplete_{engine}.json"
        _write_metadata(primary, case="c3", engine=engine, source="api", endpoint="/api/orders/surface-request-end", watermark_value="request_end", max_pages=100, root_prefix=root_prefix, api_base_url=api_base_url)
        _write_metadata(failed, case="c3", engine=engine, source="api", endpoint="/api/orders/surface-request-end", watermark_value="request_end", max_pages=1, root_prefix=root_prefix, api_base_url=api_base_url)
        metadata.extend([primary, failed])
        _reset_api_trace(f"surface-c3-{engine}")
    elif case == "c4":
        for variant, fmt, column, kind in (
            ("ms", "timestamp_ms", "covered_at", "request_end"),
            ("iso", "iso", "covered_at", "request_end"),
            ("invalid_precision", "timestamp_ms", "covered_at", "request_end"),
            ("invalid_observed", "iso", None, "observed_max"),
        ):
            path = case_root / "metadata" / f"c4_{variant}_{engine}.json"
            _write_metadata(path, case="c4", engine=engine, source="api", endpoint="/api/orders/surface-request-end", variant=variant, response_column=column, wire_format=fmt, watermark_value=kind, root_prefix=root_prefix, api_base_url=api_base_url)
            metadata.append(path)
        _reset_api_trace(f"surface-c4-{engine}")
    elif case == "c5":
        for variant in ("opaque", "matching", "missing", "conflict", "duplicate"):
            path = case_root / "metadata" / f"c5_{variant}_{engine}.json"
            mode = "opaque" if variant == "opaque" else variant
            _write_metadata(path, case="c5", engine=engine, source="api", endpoint="/api/orders/surface-next-link", variant=variant, next_mode=mode, response_column="event_time", wire_format="iso", root_prefix=root_prefix, api_base_url=api_base_url)
            metadata.append(path)
        _reset_api_trace(f"surface-c5-{engine}")
    else:
        raise ValueError(f"unsupported surface case: {case}")
    _record(case, engine, metadata)
    print(json.dumps({"case": case, "engine": engine, "metadata": [str(p) for p in metadata]}))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("c1", "c3", "c4", "c5"), required=True)
    parser.add_argument("--engine", choices=("polars", "spark"), required=True)
    args = parser.parse_args()
    prepare(args.case, args.engine)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
