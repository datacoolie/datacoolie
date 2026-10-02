"""Validate Amendment 5 C1/C3-C5 simulator oracles and write a receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs
from urllib.request import urlopen

from deltalake import DeltaTable


SCRIPT = Path(__file__).resolve()
BASE = SCRIPT.parents[1] / ".runtime" / "data" / "replay_surface_sync"


def _root(case: str, engine: str) -> Path:
    root = (BASE / case / engine).resolve()
    if root == BASE.resolve() or not root.is_relative_to(BASE.resolve()):
        raise AssertionError(f"surface case escaped owned root: {case}/{engine}")
    return root


def _receipts(case: str, engine: str) -> list[dict]:
    candidate = (
        BASE.parent.parent
        / "logs"
        / "scenarios"
        / f"local_{engine}_replay_surface_sync_{case}.invocations.json"
    )
    if not candidate.is_file():
        # The validator is also useful when called directly against the root
        # simulator receipt naming convention.
        candidates = sorted(
            (BASE.parent.parent / "logs" / "scenarios").glob(
                f"*{engine}*{case}*.invocations.json"
            )
        )
        if not candidates:
            raise AssertionError(f"missing invocation receipt below {BASE.parent.parent / 'logs' / 'scenarios'}")
        candidate = candidates[-1]
    return json.loads(candidate.read_text(encoding="utf-8"))


def _trace() -> dict:
    with urlopen("http://localhost:8082/__sim/trace", timeout=3) as response:
        return json.loads(response.read().decode("utf-8"))


def _ids(path: Path, column: str = "id") -> list[int]:
    table = DeltaTable(str(path)).to_pyarrow_table()
    return [int(value) for value in table.column(column).to_pylist()]


def _history_length(path: Path) -> int:
    return len(DeltaTable(str(path)).history())


def _state_values(receipt: dict) -> list[dict]:
    values = []
    for item in receipt.get("files", []):
        if not item["path"].endswith("watermark_value.json"):
            continue
        # Multi-invocation scenarios can advance one shared state root after
        # an earlier invocation. Prefer the runner's historical payload
        # snapshot; fall back to the live file for older receipts.
        payload = item.get("value")
        if payload is None:
            payload = json.loads((Path(receipt["root"]) / item["path"]).read_text(encoding="utf-8"))
        values.append(payload)
    return values


def _assert_no_duplicate(values: list[int], label: str) -> None:
    if len(values) != len(set(values)):
        raise AssertionError(f"{label} contains duplicate keyed IDs: {values}")


def _same_datetime(value: object, expected: str) -> bool:
    """Compare serialized instants across Polars/Spark timezone spellings."""
    if isinstance(value, dict):
        value = value.get("__datetime__")
    if not isinstance(value, str):
        return False
    try:
        actual = datetime.fromisoformat(value.replace("Z", "+00:00"))
        target = datetime.fromisoformat(expected.replace("Z", "+00:00"))
    except ValueError:
        return False
    if actual.tzinfo is None:
        actual = actual.replace(tzinfo=timezone.utc)
    if target.tzinfo is None:
        target = target.replace(tzinfo=timezone.utc)
    return actual.astimezone(timezone.utc) == target.astimezone(timezone.utc)


def _validate_c1(engine: str) -> dict:
    case_root = _root("c1", engine)
    output = case_root / "output" / "c1"
    if not output.is_dir():
        raise AssertionError(f"missing C1 output: {output}")
    ids = _ids(output)
    _assert_no_duplicate(ids, "C1 output")
    if set(ids) != {1, 2, 3, 4, 5}:
        raise AssertionError(f"C1 expected all keyed IDs after replay, got {ids}")
    runs = _receipts("c1", engine)
    labels = [run["label"] for run in runs]
    if labels != ["initial", "replay_no_save", "replay_save"]:
        raise AssertionError(f"C1 invocation labels drifted: {labels}")
    initial = runs[0]["state_after"]
    no_save_before = runs[1]["state_before"]
    no_save_after = runs[1]["state_after"]
    if initial["sha256"] != no_save_before["sha256"] or no_save_before["sha256"] != no_save_after["sha256"]:
        raise AssertionError("C1 save=False changed the stored state")
    replay_commands = [runs[index]["command"] for index in (1, 2)]
    if any("--replay-start" not in command or "--replay-end" not in command for command in replay_commands):
        raise AssertionError("C1 replay invocations did not carry explicit bounds")
    history_length = _history_length(output)
    if history_length < 3:
        raise AssertionError(
            f"C1 expected initial plus both replay writes in Delta history, got {history_length}"
        )
    save_values = _state_values(runs[2]["state_after"])
    if not save_values or not any(
        _same_datetime(item.get("event_time"), "2024-02-01T10:00:00+00:00")
        for item in save_values
    ):
        raise AssertionError("C1 save=True did not retain the monotonic high event_time watermark")
    return {"engine": engine, "ids": sorted(ids), "delta_history_length": history_length,
            "state_before_no_save": no_save_before, "state_after_save": runs[2]["state_after"]}


def _validate_c3(engine: str) -> dict:
    case_root = _root("c3", engine)
    output = case_root / "output" / "c3"
    ids = _ids(output, "order_id")
    _assert_no_duplicate(ids, "C3 output")
    if set(ids) != {2001, 2002, 2003}:
        raise AssertionError(f"C3 expected inclusive continuation IDs, got {ids}")
    runs = _receipts("c3", engine)
    labels = [run["label"] for run in runs]
    if labels != ["request_end_first", "request_end_second", "incomplete_pagination"]:
        raise AssertionError(f"C3 invocation labels drifted: {labels}")
    first_command = runs[0]["command"]
    second_command = runs[1]["command"]
    if "--replay-start" not in first_command or "--replay-end" not in first_command:
        raise AssertionError("C3 seed invocation must establish the request-end boundary with replay")
    if "--replay-start" in second_command or "--replay-end" in second_command:
        raise AssertionError("C3 continuation invocation must be an ordinary Driver run")
    first_state = _state_values(runs[0]["state_after"])
    if not first_state or not any(
        _same_datetime(item.get("covered_at"), "2024-01-16T00:00:00+00:00")
        for item in first_state
    ):
        raise AssertionError("C3 seed invocation did not persist the exact request-end U")
    if runs[2]["actual_exit_code"] == 0:
        raise AssertionError("C3 incomplete pagination unexpectedly succeeded")
    if runs[2]["state_before"]["sha256"] != runs[2]["state_after"]["sha256"]:
        raise AssertionError("C3 incomplete pagination advanced state")
    trace = _trace()["requests"]
    queries = [str(item["query"]) for item in trace if item["path"] == "/api/orders/surface-request-end"]
    if not any("covered_from=2024-01-16" in query for query in queries):
        raise AssertionError(f"C3 second run did not request inclusive U lower bound: {queries}")
    if not any("covered_to=2024-01-16" in query for query in queries):
        raise AssertionError(f"C3 first request missing exact request-end U: {queries}")
    return {"engine": engine, "ids": sorted(ids), "request_queries": queries, "runs": runs}


def _validate_c4(engine: str) -> dict:
    case_root = _root("c4", engine)
    for variant in ("ms", "iso"):
        output = case_root / "output" / variant
        ids = _ids(output, "order_id")
        if set(ids) != {2001, 2002}:
            raise AssertionError(f"C4 {variant} expected exact returned IDs, got {ids}")
    runs = _receipts("c4", engine)
    by_label = {run["label"]: run for run in runs}
    expected_bounds = {
        "ms": [
            ("1705319999123", "1705449600000"),
            ("1705319999123", "1705449600000"),
            ("1705449600000", "1705449600123"),
        ],
        "iso": [
            ("2024-01-15T11:59:59.123000+00:00", "2024-01-17T00:00:00+00:00"),
            ("2024-01-15T11:59:59.123000+00:00", "2024-01-17T00:00:00+00:00"),
            ("2024-01-17T00:00:00+00:00", "2024-01-17T00:00:00.123000+00:00"),
        ],
    }
    trace = _trace()["requests"]
    range_requests = [
        parse_qs(str(item["query"]), keep_blank_values=True)
        for item in trace
        if item["path"] == "/api/orders/surface-request-end"
    ]
    if len(range_requests) != 6:
        raise AssertionError(f"C4 expected six positive chunk/page requests, got {range_requests}")
    for variant, expected in expected_bounds.items():
        actual = [
            (query.get("from_time", [None])[0], query.get("to_time", [None])[0])
            for query in range_requests
            if query.get("variant") == [variant]
        ]
        # The endpoint carries the variant only on the first page; match the
        # remaining page/chunk requests by their exact wire bounds below.
        if not actual:
            raise AssertionError(f"C4 {variant} did not emit a traced positive request")
        if actual[0] != expected[0]:
            raise AssertionError(f"C4 {variant} first request bounds drifted: {actual[0]}")
    actual_bounds = [
        (query.get("from_time", [None])[0], query.get("to_time", [None])[0])
        for query in range_requests
    ]
    expected_all = expected_bounds["ms"][:2] + expected_bounds["ms"][2:] + expected_bounds["iso"]
    if actual_bounds != expected_all:
        raise AssertionError(f"C4 chunk/page wire bounds drifted: {actual_bounds}")
    for label in ("aligned_ms", "iso"):
        if by_label[label]["actual_exit_code"] != 0 or not by_label[label]["state_after"]["sha256"]:
            raise AssertionError(f"C4 {label} did not commit positive output/state")
    for label in ("invalid_precision", "invalid_observed"):
        if by_label[label]["actual_exit_code"] == 0:
            raise AssertionError(f"C4 {label} unexpectedly succeeded")
        if by_label[label]["state_before"]["sha256"] != by_label[label]["state_after"]["sha256"]:
            raise AssertionError(f"C4 {label} changed persisted state on pre-HTTP rejection")
        invalid_variant = "invalid_precision" if label == "invalid_precision" else "invalid_observed"
        if (case_root / "output" / invalid_variant / "_delta_log").exists():
            raise AssertionError(f"C4 {label} created target output on pre-HTTP rejection")
    invalid_queries = [str(item["query"]) for item in trace if "variant=invalid" in str(item["query"])]
    if invalid_queries:
        raise AssertionError(f"C4 invalid pre-HTTP case dispatched a request: {invalid_queries}")
    return {"engine": engine, "positive_ids": {v: _ids(case_root / "output" / v, "order_id") for v in ("ms", "iso")}, "request_count": len(trace), "runs": runs}


def _validate_c5(engine: str) -> dict:
    case_root = _root("c5", engine)
    for variant in ("opaque", "matching", "missing"):
        ids = _ids(case_root / "output" / variant, "order_id")
        if set(ids) != {2101, 2102}:
            raise AssertionError(f"C5 {variant} expected both pages, got {ids}")
    runs = _receipts("c5", engine)
    by_label = {run["label"]: run for run in runs}
    for label in ("conflict", "duplicate"):
        if by_label[label]["actual_exit_code"] == 0:
            raise AssertionError(f"C5 {label} unexpectedly succeeded")
        if by_label[label]["state_after"]["sha256"] is not None:
            raise AssertionError(f"C5 {label} created state despite pre-request rejection")
        if (case_root / "output" / label / "_delta_log").exists():
            raise AssertionError(f"C5 {label} created target output despite pre-request rejection")
    trace = _trace()["requests"]
    by_mode: dict[str, list[str]] = {}
    for item in trace:
        if item["path"] != "/api/orders/surface-next-link":
            continue
        query = str(item["query"])
        mode = next((part.split("=", 1)[1] for part in query.split("&") if part.startswith("mode=")), "")
        by_mode.setdefault(mode, []).append(query)
    expected_page2 = {
        "opaque": "page=2&mode=opaque&signed=token%2Bopaque",
        "matching": "page=2&mode=matching&from_time=2024-01-15T00%3A00%3A00%2B00%3A00&to_time=2024-01-16T00%3A00%3A00%2B00%3A00&signed=token%2Bmatching",
        "missing": "page=2&mode=missing&signed=token%2Bmissing&from_time=2024-01-15T00%3A00%3A00%2B00%3A00&to_time=2024-01-16T00%3A00%3A00%2B00%3A00",
    }
    for mode, page2 in expected_page2.items():
        queries = by_mode.get(mode, [])
        if len(queries) != 2 or queries[1] != page2:
            raise AssertionError(f"C5 {mode} continuation query drifted: {queries}")
    if len(by_mode.get("conflict", [])) != 1 or len(by_mode.get("duplicate", [])) != 1:
        raise AssertionError(f"C5 rejected continuations dispatched offending page: {by_mode}")
    return {"engine": engine, "modes": by_mode, "runs": runs}


def validate(case: str, engine: str) -> dict:
    if case == "c1":
        result = _validate_c1(engine)
    elif case == "c3":
        result = _validate_c3(engine)
    elif case == "c4":
        result = _validate_c4(engine)
    elif case == "c5":
        result = _validate_c5(engine)
    else:
        raise ValueError(f"unsupported validation case: {case}")
    receipt_path = _root(case, engine) / "validation_receipt.json"
    receipt = {
        "case": case,
        "engine": engine,
        "validated_at": datetime.utcnow().isoformat() + "Z",
        "script": str(SCRIPT),
        "script_sha256": hashlib.sha256(SCRIPT.read_bytes()).hexdigest(),
        "result": result,
    }
    receipt_path.write_text(json.dumps(receipt, indent=2, default=str) + "\n", encoding="utf-8")
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("c1", "c3", "c4", "c5"), required=True)
    parser.add_argument("--engine", choices=("polars", "spark"), required=True)
    args = parser.parse_args()
    print(json.dumps(validate(args.case, args.engine), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
