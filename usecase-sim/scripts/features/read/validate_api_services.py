"""Qualify the mock API service contracts for amendment 9 D1-D3.

The validator compiles one dataflow at a time into a fresh FileProvider
document and executes every phase through the normal Polars Driver.  Existing
auth flows are copied from the shared local fixture; the two focused JSON and
case-keyed 429 documents provide only the missing service activations.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import json
import sys
import uuid
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qs

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

# Importing the runner utility activates the simulator's local synthetic
# credential defaults.  Environment-provided values still take precedence.
from runner import _runner_utils as _runner_utils  # noqa: F401,E402

from _stateful_support import (  # noqa: E402
    GENERATED_COLUMNS,
    USECASE_SIM_DIR,
    CasePaths,
    DriverEvidence,
    assert_exact_rows,
    assert_schema,
    assert_successful_flow,
    business_rows,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_receipt,
)


SCRIPT = Path(__file__).resolve()
READ_METADATA_DIR = USECASE_SIM_DIR / "metadata" / "file" / "read"
SHARED_LOCAL_PATH = USECASE_SIM_DIR / "metadata" / "file" / "shared" / "local.json"
READ_LOCAL_PATH = READ_METADATA_DIR / "local.json"

D1_FLOWS = (
    ("api_auth__bearer", "/api/secure/bearer"),
    ("api_auth__api_key", "/api/secure/apikey"),
    ("api_auth__basic", "/api/secure/basic"),
)
D2_FLOWS = (
    ("api_auth__oauth2", "form-post"),
    ("api_auth__oauth2_basic_method", "form-basic"),
)
BEHAVIORS = (
    "all",
    "auth_bearer",
    "auth_apikey",
    "auth_basic",
    "oauth_form",
    "oauth_basic",
    "oauth_json",
    "ratelimit",
    "requestpost",
    "pagination_offset",
    "pagination_cursor",
    "pagination_next_link",
    "staticparams",
)
EXPECTED_SCHEMA = {
    "order_id": "Int64",
    "order_date": "String",
    "amount": "String",
    "quantity": "Int64",
    "region": "String",
    "modified_at": "String",
    "customer_name": "String",
    "status": "String",
}


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _mock_rows() -> list[dict[str, Any]]:
    """Load the embedded mock dataset as the exact service oracle."""
    path = USECASE_SIM_DIR / "docker" / "mock_api_server.py"
    spec = importlib.util.spec_from_file_location("a9_api_service_mock", path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load mock fixture: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return copy.deepcopy(module.ROWS)


def _shared_documents() -> tuple[dict[str, Any], dict[str, Any]]:
    return _read_json(READ_LOCAL_PATH), _read_json(SHARED_LOCAL_PATH)


def _find_flow(document: Mapping[str, Any], name: str) -> dict[str, Any]:
    for flow in document.get("dataflows", []):
        if isinstance(flow, dict) and flow.get("name") == name:
            return copy.deepcopy(flow)
    raise KeyError(f"missing authored dataflow: {name}")


def _find_connection(document: Mapping[str, Any], name: str) -> dict[str, Any]:
    for connection in document.get("connections", []):
        if isinstance(connection, dict) and connection.get("name") == name:
            return copy.deepcopy(connection)
    raise KeyError(f"missing authored connection: {name}")


def _opaque_id(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _runtime_identity(case: CasePaths, authored_name: str, authored_stage: str, label: str) -> tuple[str, str]:
    suffix = case.root.name.rsplit("_", 1)[-1]
    return (
        f"{authored_name}__{label}__{suffix}",
        f"{authored_stage}__{label}__{suffix}",
    )


def _existing_phase(
    name: str,
    *,
    case: CasePaths,
    stage: str,
    base_url: str,
    trace_token: str,
    runtime_label: str,
    destination_table: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    read_document, shared_document = _shared_documents()
    flow = _find_flow(read_document, name)
    runtime_name, runtime_stage = _runtime_identity(case, name, stage, runtime_label)
    flow["name"] = runtime_name
    flow["stage"] = runtime_stage
    if destination_table is not None:
        flow["destination"]["table"] = destination_table
    source_connection_name = flow["source"]["connection_name"]
    connections = [
        _find_connection(shared_document, source_connection_name),
        _find_connection(shared_document, "local_parquet_dest"),
    ]
    for connection in connections:
        configure = connection.setdefault("configure", {})
        if connection.get("connection_type") == "api":
            configure["base_url"] = base_url
            configure.setdefault("default_headers", {})["X-Sim-Trace-Run"] = trace_token
            if "token_url" in configure:
                configure["token_url"] = (
                    base_url.rstrip("/")
                    + "/oauth/token?trace_id="
                    + _opaque_id(trace_token)
                )
        elif connection.get("name") == "local_parquet_dest":
            configure["base_path"] = "__CASE_ROOT__/output/parquet"
    return flow, connections


def _focused_phase(
    fixture_name: str,
    *,
    case: CasePaths,
    base_url: str,
    trace_token: str,
    runtime_label: str,
    replacements: Mapping[str, str] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    text = (READ_METADATA_DIR / fixture_name).read_text(encoding="utf-8")
    values = {
        "__CASE_ROOT__": case.root.as_posix(),
        "__MOCK_BASE_URL__": base_url.rstrip("/"),
    }
    values.update(replacements or {})
    for marker, value in values.items():
        text = text.replace(marker, value)
    document = json.loads(text)
    if not isinstance(document, dict):
        raise ValueError(f"focused fixture is not an object: {fixture_name}")
    flows = document.get("dataflows")
    connections = document.get("connections")
    if not isinstance(flows, list) or len(flows) != 1:
        raise ValueError(f"focused fixture must contain one dataflow: {fixture_name}")
    if not isinstance(connections, list):
        raise ValueError(f"focused fixture must contain connections: {fixture_name}")
    authored_name = flows[0]["name"]
    authored_stage = flows[0]["stage"]
    runtime_name, runtime_stage = _runtime_identity(case, authored_name, authored_stage, runtime_label)
    flows[0]["name"] = runtime_name
    flows[0]["stage"] = runtime_stage
    for connection in connections:
        if connection.get("connection_type") == "api":
            connection.setdefault("configure", {}).setdefault("default_headers", {})[
                "X-Sim-Trace-Run"
            ] = trace_token
            configure = connection["configure"]
            if "token_url" in configure:
                configure["token_url"] = (
                    configure["token_url"]
                    + ("&" if "?" in configure["token_url"] else "?")
                    + "trace_id="
                    + _opaque_id(trace_token)
                )
    return copy.deepcopy(flows[0]), copy.deepcopy(connections)


def _write_document(
    case: CasePaths,
    flows: list[dict[str, Any]],
    connections: list[dict[str, Any]],
) -> None:
    document = {"connections": connections, "dataflows": flows}
    text = json.dumps(document, indent=2).replace("__CASE_ROOT__", case.root.as_posix())
    case.metadata.write_text(text + "\n", encoding="utf-8")


def _output_path(case: CasePaths, table: str) -> Path:
    return case.output / "parquet" / table


def _output_rows(
    case: CasePaths,
    table: str,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    import polars as pl

    target = _output_path(case, table)
    files = sorted(target.rglob("*.parquet")) if target.exists() else []
    if not files:
        raise AssertionError(f"no parquet output for {table}: {target}")
    frame = pl.concat([pl.read_parquet(path) for path in files], how="vertical_relaxed")
    rows = business_rows(frame.to_dicts())
    schema = {
        name: str(dtype)
        for name, dtype in frame.schema.items()
        if name not in GENERATED_COLUMNS
    }
    return rows, schema


def _checkpoint_snapshot(case: CasePaths) -> dict[str, bytes]:
    return {
        str(path.relative_to(case.watermarks)): path.read_bytes()
        for path in case.watermarks.rglob("watermark_value.json")
    }


def _assert_result_counts(evidence: DriverEvidence, expected: dict[str, int]) -> None:
    actual = {key: evidence.result.get(key) for key in expected}
    if actual != expected:
        raise AssertionError(f"unexpected Driver counts: actual={actual!r}, expected={expected!r}")


def _http_json(base_url: str, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    import httpx

    try:
        response = httpx.request(method, base_url.rstrip("/") + path, timeout=5, **kwargs)
    except httpx.HTTPError as exc:
        raise AssertionError(f"mock API control request failed: {type(exc).__name__}") from None
    if response.status_code >= 400:
        raise AssertionError(f"mock API control request returned HTTP {response.status_code}")
    value = response.json()
    if not isinstance(value, dict):
        raise AssertionError("mock API control response was not an object")
    return value


def _trace(
    base_url: str,
    *,
    trace_id: str | None = None,
    case_id: str | None = None,
) -> list[dict[str, Any]]:
    params = {}
    if trace_id is not None:
        params["trace_id"] = trace_id
    if case_id is not None:
        params["case_id"] = case_id
    value = _http_json(base_url, "GET", "/__sim/trace", params=params)
    requests = value.get("requests")
    if not isinstance(requests, list) or not all(isinstance(item, dict) for item in requests):
        raise AssertionError("mock API trace has an invalid request list")
    return requests


def _oauth_trace(base_url: str, *, trace_id: str | None = None) -> list[dict[str, Any]]:
    params = {} if trace_id is None else {"trace_id": trace_id}
    value = _http_json(base_url, "GET", "/__sim/oauth/trace", params=params)
    requests = value.get("requests")
    if not isinstance(requests, list) or not all(isinstance(item, dict) for item in requests):
        raise AssertionError("mock OAuth trace has an invalid request list")
    return requests


def _clear_oauth_cache() -> None:
    from datacoolie.sources._api import auth

    with auth._OAUTH2_TOKEN_CACHE_LOCK:
        auth._OAUTH2_TOKEN_CACHE.clear()


def _assert_single_route_trace(
    entries: list[dict[str, Any]],
    *,
    path: str,
    status: int,
    method: str = "GET",
) -> dict[str, Any]:
    matching = [entry for entry in entries if entry.get("path") == path]
    if len(matching) != 1:
        raise AssertionError(f"expected one {path} request, got {matching!r}")
    entry = matching[0]
    if entry.get("method") != method or entry.get("status") != status:
        raise AssertionError(f"unexpected {path} request trace: {entry!r}")
    return {
        key: entry.get(key)
        for key in (
            "ordinal",
            "method",
            "path",
            "query",
            "status",
            "retry_after",
            "case_id",
            "trace_id",
            "content_type",
            "body",
        )
    }


def _assert_output(
    case: CasePaths,
    table: str,
    expected: list[dict[str, Any]],
    *,
    ordered: bool = False,
) -> None:
    rows, schema = _output_rows(case, table)
    if ordered and [row.get("order_id") for row in rows] != [row.get("order_id") for row in expected]:
        raise AssertionError(f"{table} order mismatch")
    assert_exact_rows(rows, expected, key="order_id", label=table)
    assert_schema(schema, EXPECTED_SCHEMA, label=f"{table} schema")


def _negative_auth_connection(
    connection: dict[str, Any],
    *,
    mode: str,
) -> None:
    configure = connection.setdefault("configure", {})
    auth_type = configure.get("auth_type")
    connection.pop("secrets_ref", None)
    if mode == "missing":
        if auth_type == "bearer":
            configure["auth_token"] = ""
        elif auth_type == "api_key":
            configure["api_key_value"] = ""
        elif auth_type == "basic":
            configure["username"] = ""
            configure["password"] = ""
        else:
            raise ValueError(f"unsupported D1 auth type: {auth_type}")
        return
    if mode != "wrong":
        raise ValueError(f"unsupported negative auth mode: {mode}")
    if auth_type == "bearer":
        configure["auth_token"] = "invalid-a9-bearer"
    elif auth_type == "api_key":
        configure["api_key_value"] = "invalid-a9-api-key"
    elif auth_type == "basic":
        configure["username"] = "invalid-a9-user"
        configure["password"] = "invalid-a9-password"
    else:
        raise ValueError(f"unsupported D1 auth type: {auth_type}")


def _run_d1(
    case: CasePaths,
    base_url: str,
    expected: list[dict[str, Any]],
    selected: tuple[tuple[str, str], ...] = D1_FLOWS,
) -> tuple[list[DriverEvidence], dict[str, Any]]:
    runs: list[DriverEvidence] = []
    details: dict[str, Any] = {"positive": [], "negative": []}
    for index, (flow_name, route) in enumerate(selected, 1):
        stage = "api_service_auth"
        trace_token = f"a9-d1-positive-{flow_name}-{uuid.uuid4().hex}"
        flow, connections = _existing_phase(
            flow_name,
            case=case,
            stage=stage,
            base_url=base_url,
            trace_token=trace_token,
            runtime_label="positive",
        )
        _write_document(case, [flow], connections)
        evidence = run_driver(case, stage=flow["stage"], iteration=index)
        assert_successful_flow(evidence, flow["name"])
        runs.append(evidence)
        _assert_output(case, flow["destination"]["table"], expected)
        trace_entry = _assert_single_route_trace(
            _trace(base_url, trace_id=_opaque_id(trace_token)),
            path=route,
            status=200,
        )
        details["positive"].append({
            "authored_flow": flow_name,
            "runtime_flow": flow["name"],
            "runtime_stage": flow["stage"],
            "route_trace": trace_entry,
        })

        for offset, mode in enumerate(("wrong", "missing"), 1):
            negative_stage = "api_service_auth_negative"
            negative_flow, negative_connections = _existing_phase(
                flow_name,
                case=case,
                stage=negative_stage,
                base_url=base_url,
                trace_token=(negative_trace_token := f"a9-d1-{mode}-{flow_name}-{uuid.uuid4().hex}"),
                runtime_label=mode,
                destination_table=f"negative_{flow_name.replace('__', '_')}_{mode}",
            )
            source_name = negative_flow["source"]["connection_name"]
            source_connection = next(item for item in negative_connections if item["name"] == source_name)
            _negative_auth_connection(source_connection, mode=mode)
            _write_document(case, [negative_flow], negative_connections)
            before_checkpoints = _checkpoint_snapshot(case)
            failed = run_driver(case, stage=negative_flow["stage"], iteration=index * 2 + offset)
            _assert_result_counts(failed, {"succeeded": 0, "failed": 1, "skipped": 0})
            runs.append(failed)
            target = _output_path(case, negative_flow["destination"]["table"])
            if list(target.rglob("*.parquet")) if target.exists() else []:
                raise AssertionError(f"auth failure wrote business output: {target}")
            if _checkpoint_snapshot(case) != before_checkpoints:
                raise AssertionError("auth failure advanced a checkpoint")
            failed_trace = _assert_single_route_trace(
                _trace(base_url, trace_id=_opaque_id(negative_trace_token)),
                path=route,
                status=401,
            )
            details["negative"].append({
                "authored_flow": flow_name,
                "runtime_flow": negative_flow["name"],
                "runtime_stage": negative_flow["stage"],
                "mode": mode,
                "route_trace": failed_trace,
            })
    return runs, details


def _assert_oauth_shape(
    entries: list[dict[str, Any]],
    *,
    label: str,
    body_format: str,
    auth_method: str,
    scope: str,
    audience: bool,
) -> dict[str, Any]:
    if len(entries) != 1:
        raise AssertionError(f"{label} expected one token request, got {entries!r}")
    entry = entries[0]
    for key, expected in {
        "method": "POST",
        "path": "/oauth/token",
        "body_format": body_format,
        "auth_method": auth_method,
        "content_type": "application/json" if body_format == "json" else "application/x-www-form-urlencoded",
        "status": 200,
        "scope": scope,
    }.items():
        if entry.get(key) != expected:
            raise AssertionError(f"{label} OAuth trace field {key!r}: {entry!r}")
    body = entry.get("body")
    if not isinstance(body, dict) or ("audience" in body) != audience:
        raise AssertionError(f"{label} OAuth body extras mismatch: {entry!r}")
    if any(secret_key in body for secret_key in ("client_id", "client_secret")):
        raise AssertionError(f"{label} OAuth trace contains credential fields")
    return {
        key: entry.get(key)
        for key in ("ordinal", "method", "path", "content_type", "auth_method", "body_format", "scope", "status")
    }


def _run_d2(
    case: CasePaths,
    base_url: str,
    expected: list[dict[str, Any]],
    selected: tuple[str, ...] | None = None,
) -> tuple[list[DriverEvidence], dict[str, Any]]:
    runs: list[DriverEvidence] = []
    details: dict[str, Any] = {"positive": []}
    phases = [
        (D2_FLOWS[0][0], "form", "client_secret_post", ""),
        (D2_FLOWS[1][0], "form", "client_secret_basic", "read:orders"),
    ]
    if selected is not None:
        phases = [item for item in phases if item[0] in selected]
    for index, (flow_name, body_format, auth_method, scope) in enumerate(phases, 1):
        trace_token = f"a9-d2-{flow_name}-{uuid.uuid4().hex}"
        flow, connections = _existing_phase(
            flow_name,
            case=case,
            stage="api_service_oauth2",
            base_url=base_url,
            trace_token=trace_token,
            runtime_label="positive",
        )
        _write_document(case, [flow], connections)
        _clear_oauth_cache()
        evidence = run_driver(case, stage=flow["stage"], iteration=index)
        assert_successful_flow(evidence, flow["name"])
        runs.append(evidence)
        _assert_output(case, flow["destination"]["table"], expected)
        trace_id = _opaque_id(trace_token)
        route = _assert_single_route_trace(
            _trace(base_url, trace_id=trace_id),
            path="/api/secure/oauth2",
            status=200,
        )
        oauth = _assert_oauth_shape(
            _oauth_trace(base_url, trace_id=trace_id),
            label=flow_name,
            body_format=body_format,
            auth_method=auth_method,
            scope=scope,
            audience=False,
        )
        details["positive"].append({
            "authored_flow": flow_name,
            "runtime_flow": flow["name"],
            "runtime_stage": flow["stage"],
            "route_trace": route,
            "oauth_trace": oauth,
        })

    if selected is not None and "api_service__oauth2_json" not in selected:
        return runs, details

    json_flow, json_connections = _focused_phase(
        "a9_api_oauth_json.json",
        case=case,
        base_url=base_url,
        trace_token=(json_trace_token := f"a9-d2-json-{uuid.uuid4().hex}"),
        runtime_label="positive",
    )
    _write_document(case, [json_flow], json_connections)
    _clear_oauth_cache()
    json_evidence = run_driver(case, stage=json_flow["stage"], iteration=1)
    assert_successful_flow(json_evidence, json_flow["name"])
    runs.append(json_evidence)
    _assert_output(case, json_flow["destination"]["table"], expected)
    json_trace_id = _opaque_id(json_trace_token)
    route = _assert_single_route_trace(
        _trace(base_url, trace_id=json_trace_id),
        path="/api/secure/oauth2",
        status=200,
    )
    oauth = _assert_oauth_shape(
        _oauth_trace(base_url, trace_id=json_trace_id),
        label=json_flow["name"],
        body_format="json",
        auth_method="client_secret_post",
        scope="read:orders",
        audience=True,
    )
    details["positive"].append({
        "authored_flow": "api_service__oauth2_json",
        "runtime_flow": json_flow["name"],
        "runtime_stage": json_flow["stage"],
        "route_trace": route,
        "oauth_trace": oauth,
    })
    return runs, details


def _configure_rate_case(base_url: str, token: str, sequence: list[int]) -> str:
    result = _http_json(
        base_url,
        "POST",
        "/__sim/rate-limit/config",
        json={"case_token": token, "sequence": sequence, "retry_after": "0"},
    )
    case_id = result.get("case_id")
    expected = hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]
    if case_id != expected:
        raise AssertionError("mock rate case did not return the expected sanitized case id")
    return expected


def _run_d3(case: CasePaths, base_url: str, expected: list[dict[str, Any]]) -> tuple[list[DriverEvidence], dict[str, Any]]:
    token = "a9-rate-" + uuid.uuid4().hex
    case_id = _configure_rate_case(base_url, token, [429, 200])
    trace_token = f"a9-d3-positive-{uuid.uuid4().hex}"
    flow, connections = _focused_phase(
        "a9_api_rate_limit.json",
        case=case,
        base_url=base_url,
        trace_token=trace_token,
        runtime_label="positive",
        replacements={"__RATE_CASE_TOKEN__": token},
    )
    _write_document(case, [flow], connections)
    positive = run_driver(case, stage=flow["stage"], iteration=1)
    assert_successful_flow(positive, flow["name"])
    _assert_output(case, flow["destination"]["table"], expected)
    positive_trace = [
        item
        for item in _trace(base_url, case_id=case_id)
        if item.get("path") == "/api/orders/rate-limited"
    ]
    if [item.get("status") for item in positive_trace] != [429, 200]:
        raise AssertionError(f"unexpected 429 retry trace: {positive_trace!r}")
    if positive_trace[0].get("retry_after") != "0":
        raise AssertionError(f"Retry-After was not preserved in trace: {positive_trace!r}")

    exhausted_token = "a9-rate-exhausted-" + uuid.uuid4().hex
    exhausted_id = _configure_rate_case(base_url, exhausted_token, [429, 429])
    exhausted_trace_token = f"a9-d3-exhausted-{uuid.uuid4().hex}"
    exhausted_flow, exhausted_connections = _focused_phase(
        "a9_api_rate_limit.json",
        case=case,
        base_url=base_url,
        trace_token=exhausted_trace_token,
        runtime_label="exhausted",
        replacements={"__RATE_CASE_TOKEN__": exhausted_token},
    )
    exhausted_flow["source"]["configure"]["max_retries"] = 1
    exhausted_flow["destination"]["table"] = "negative_api_service_rate_limit_exhausted"
    _write_document(case, [exhausted_flow], exhausted_connections)
    before_checkpoints = _checkpoint_snapshot(case)
    failed = run_driver(case, stage=exhausted_flow["stage"], iteration=2)
    _assert_result_counts(failed, {"succeeded": 0, "failed": 1, "skipped": 0})
    target = _output_path(case, exhausted_flow["destination"]["table"])
    if list(target.rglob("*.parquet")) if target.exists() else []:
        raise AssertionError(f"exhausted 429 sequence wrote business output: {target}")
    if _checkpoint_snapshot(case) != before_checkpoints:
        raise AssertionError("exhausted 429 sequence advanced a checkpoint")
    exhausted_trace = [
        item
        for item in _trace(base_url, case_id=exhausted_id)
        if item.get("path") == "/api/orders/rate-limited"
    ]
    if [item.get("status") for item in exhausted_trace] != [429, 429]:
        raise AssertionError(f"unexpected exhausted 429 trace: {exhausted_trace!r}")

    return [positive, failed], {
        "positive": {
            "authored_flow": "api_service__rate_limit_case",
            "runtime_flow": flow["name"],
            "runtime_stage": flow["stage"],
            "case_id": case_id,
            "statuses": [item.get("status") for item in positive_trace],
            "retry_after": positive_trace[0].get("retry_after"),
            "trace": positive_trace,
        },
        "exhausted": {
            "authored_flow": "api_service__rate_limit_case",
            "runtime_flow": exhausted_flow["name"],
            "runtime_stage": exhausted_flow["stage"],
            "case_id": exhausted_id,
            "statuses": [item.get("status") for item in exhausted_trace],
            "trace": exhausted_trace,
        },
    }


def _run_o6(case: CasePaths, base_url: str, expected: list[dict[str, Any]]) -> tuple[list[DriverEvidence], dict[str, Any]]:
    trace_token = f"a9-o6-post-{uuid.uuid4().hex}"
    flow, connections = _existing_phase(
        "api_request__post_body",
        case=case,
        stage="api_service_request_post",
        base_url=base_url,
        trace_token=trace_token,
        runtime_label="primary",
    )
    _write_document(case, [flow], connections)
    evidence = run_driver(case, stage=flow["stage"], iteration=1)
    assert_successful_flow(evidence, flow["name"])
    expected_rows = [row for row in expected if row.get("status") == "completed"]
    _assert_output(case, flow["destination"]["table"], expected_rows)
    trace_id = _opaque_id(trace_token)
    entry = _assert_single_route_trace(
        _trace(base_url, trace_id=trace_id),
        path="/api/orders/search",
        status=200,
        method="POST",
    )
    if entry.get("method") != "POST":
        raise AssertionError(f"POST fixture dispatched the wrong method: {entry!r}")
    if entry.get("content_type") != "application/json":
        raise AssertionError(f"POST fixture content type mismatch: {entry!r}")
    if entry.get("body") != {"status": "completed"}:
        raise AssertionError(f"POST fixture body/precedence mismatch: {entry!r}")
    return [evidence], {
        "authored_flow": "api_request__post_body",
        "runtime_flow": flow["name"],
        "runtime_stage": flow["stage"],
        "expected_status": "completed",
        "route_trace": entry,
    }


def _pagination_query_contract(kind: str, entries: list[dict[str, Any]]) -> None:
    queries = [parse_qs(entry.get("query") or "") for entry in entries]
    if kind == "offset":
        offsets = [int(query.get("offset", ["0"])[0]) for query in queries]
        if offsets != [0, 5, 10, 15, 20, 25, 30]:
            raise AssertionError(f"offset page sequence mismatch: {offsets!r}")
        if any(query.get("limit") != ["5"] for query in queries):
            raise AssertionError(f"offset page size mismatch: {queries!r}")
    elif kind == "cursor":
        cursors = [query.get("cursor", [None])[0] for query in queries]
        if cursors[0] is not None or len(set(cursors[1:])) != 2:
            raise AssertionError(f"cursor continuation mismatch: {queries!r}")
    elif kind == "next_link":
        pages = [query.get("page", [None])[0] for query in queries]
        if pages[0] not in (None, "1") or pages[1:] != ["2", "3"]:
            raise AssertionError(f"next-link page sequence mismatch: {pages!r}")
    else:
        raise ValueError(f"unknown pagination kind: {kind}")


def _run_o7(
    case: CasePaths,
    base_url: str,
    expected: list[dict[str, Any]],
    *,
    selected: tuple[str, str, str] | None = None,
) -> tuple[list[DriverEvidence], dict[str, Any]]:
    flows = {
        "pagination_offset": ("api_page__offset", "/api/orders/offset", "offset"),
        "pagination_cursor": ("api_page__cursor", "/api/orders/cursor", "cursor"),
        "pagination_next_link": ("api_page__next_link", "/api/orders/next-link", "next_link"),
    }
    selected_items = [selected] if selected is not None else list(flows.values())
    runs: list[DriverEvidence] = []
    details: list[dict[str, Any]] = []
    for index, (flow_name, route, kind) in enumerate(selected_items, 1):
        trace_token = f"a9-o7-{kind}-{uuid.uuid4().hex}"
        flow, connections = _existing_phase(
            flow_name,
            case=case,
            stage="api_service_pagination",
            base_url=base_url,
            trace_token=trace_token,
            runtime_label=kind,
        )
        _write_document(case, [flow], connections)
        evidence = run_driver(case, stage=flow["stage"], iteration=index)
        assert_successful_flow(evidence, flow["name"])
        runs.append(evidence)
        _assert_output(case, flow["destination"]["table"], expected, ordered=True)
        entries = [
            item
            for item in _trace(base_url, trace_id=_opaque_id(trace_token))
            if item.get("path") == route
        ]
        expected_requests = 7 if kind == "offset" else 3
        if len(entries) != expected_requests or any(item.get("status") != 200 for item in entries):
            raise AssertionError(f"{kind} request count/status mismatch: {entries!r}")
        _pagination_query_contract(kind, entries)
        details.append({
            "authored_flow": flow_name,
            "runtime_flow": flow["name"],
            "runtime_stage": flow["stage"],
            "route": route,
            "kind": kind,
            "trace": entries,
        })
    return runs, {"flows": details}


def _run_o8(case: CasePaths, base_url: str, expected: list[dict[str, Any]]) -> tuple[list[DriverEvidence], dict[str, Any]]:
    trace_token = f"a9-o8-static-{uuid.uuid4().hex}"
    flow, connections = _existing_phase(
        "api_request__static_params",
        case=case,
        stage="api_service_static_params",
        base_url=base_url,
        trace_token=trace_token,
        runtime_label="primary",
    )
    _write_document(case, [flow], connections)
    evidence = run_driver(case, stage=flow["stage"], iteration=1)
    assert_successful_flow(evidence, flow["name"])
    _assert_output(case, flow["destination"]["table"], expected, ordered=True)
    entries = [
        item
        for item in _trace(base_url, trace_id=_opaque_id(trace_token))
        if item.get("path") == "/api/orders/with-params"
    ]
    if len(entries) != 7 or any(item.get("status") != 200 for item in entries):
        raise AssertionError(f"static-parameter request count/status mismatch: {entries!r}")
    queries = [parse_qs(entry.get("query") or "") for entry in entries]
    if any(query.get("api_version") != ["v2"] for query in queries):
        raise AssertionError(f"static query parameter was not preserved: {queries!r}")
    offsets = [int(query.get("offset", ["0"])[0]) for query in queries]
    if offsets != [0, 5, 10, 15, 20, 25, 30]:
        raise AssertionError(f"static parameter pagination precedence mismatch: {offsets!r}")
    return [evidence], {
        "authored_flow": "api_request__static_params",
        "runtime_flow": flow["name"],
        "runtime_stage": flow["stage"],
        "static_params": {"api_version": "v2"},
        "trace": entries,
    }


def validate(
    root: Path | None = None,
    base_url: str = "http://127.0.0.1:8082",
    behavior: str = "all",
) -> Path:
    if behavior not in BEHAVIORS:
        raise ValueError(f"unknown API service behavior: {behavior}")
    case = resolve_case_root("read", root)
    all_rows = _mock_rows()
    # Auth and rate fixtures intentionally exercise the route's first page;
    # their existing source declarations do not opt into pagination.
    page_rows = all_rows[:10]

    # This is the existing delay-only fixture.  Assert its authored controls
    # remain unchanged while D3 uses the dedicated rate-limited route.
    read_document, _ = _shared_documents()
    delay_only = _find_flow(read_document, "api_request__rate_limit")
    delay_config = delay_only["source"]["configure"]
    if delay_config.get("endpoint") != "/api/orders/offset" or delay_config.get("rate_limit_delay") != 0.5:
        raise AssertionError("existing delay-only API fixture changed")

    base_url = base_url.rstrip("/")
    details: dict[str, Any] = {}
    runs: list[DriverEvidence] = []
    if behavior == "all":
        d1_runs, details["d1"] = _run_d1(case, base_url, page_rows)
        d2_runs, details["d2"] = _run_d2(case, base_url, page_rows)
        d3_runs, details["d3"] = _run_d3(case, base_url, page_rows)
        o6_runs, details["o6"] = _run_o6(case, base_url, all_rows)
        o7_runs, details["o7"] = _run_o7(case, base_url, all_rows)
        o8_runs, details["o8"] = _run_o8(case, base_url, all_rows)
        runs.extend(d1_runs + d2_runs + d3_runs + o6_runs + o7_runs + o8_runs)
    elif behavior == "auth_bearer":
        run, detail = _run_d1(case, base_url, page_rows, selected=(D1_FLOWS[0],))
        runs.extend(run)
        details["d1"] = detail
    elif behavior == "auth_apikey":
        run, detail = _run_d1(case, base_url, page_rows, selected=(D1_FLOWS[1],))
        runs.extend(run)
        details["d1"] = detail
    elif behavior == "auth_basic":
        run, detail = _run_d1(case, base_url, page_rows, selected=(D1_FLOWS[2],))
        runs.extend(run)
        details["d1"] = detail
    elif behavior == "oauth_form":
        run, detail = _run_d2(case, base_url, page_rows, selected=(D2_FLOWS[0][0],))
        runs.extend(run)
        details["d2"] = detail
    elif behavior == "oauth_basic":
        run, detail = _run_d2(case, base_url, page_rows, selected=(D2_FLOWS[1][0],))
        runs.extend(run)
        details["d2"] = detail
    elif behavior == "oauth_json":
        run, detail = _run_d2(case, base_url, page_rows, selected=("api_service__oauth2_json",))
        runs.extend(run)
        details["d2"] = detail
    elif behavior == "ratelimit":
        run, detail = _run_d3(case, base_url, page_rows)
        runs.extend(run)
        details["d3"] = detail
    elif behavior == "requestpost":
        run, detail = _run_o6(case, base_url, all_rows)
        runs.extend(run)
        details["o6"] = detail
    elif behavior == "pagination_offset":
        run, detail = _run_o7(case, base_url, all_rows, selected=("api_page__offset", "/api/orders/offset", "offset"))
        runs.extend(run)
        details["o7"] = detail
    elif behavior == "pagination_cursor":
        run, detail = _run_o7(case, base_url, all_rows, selected=("api_page__cursor", "/api/orders/cursor", "cursor"))
        runs.extend(run)
        details["o7"] = detail
    elif behavior == "pagination_next_link":
        run, detail = _run_o7(case, base_url, all_rows, selected=("api_page__next_link", "/api/orders/next-link", "next_link"))
        runs.extend(run)
        details["o7"] = detail
    elif behavior == "staticparams":
        run, detail = _run_o8(case, base_url, all_rows)
        runs.extend(run)
        details["o8"] = detail
    payload = receipt_payload(
        case,
        runs=runs,
        script=SCRIPT,
        details={
            "behavior": behavior,
            **details,
            "expected_rows": page_rows if behavior in {"auth_bearer", "auth_apikey", "auth_basic", "oauth_form", "oauth_basic", "oauth_json", "ratelimit"} else all_rows,
            "expected_schema": EXPECTED_SCHEMA,
            "trace_contract": "method/path/query/status/retry_after plus hashed case_id/trace_id; no headers, bodies, or credentials",
            "existing_delay_only": {
                "flow": "api_request__rate_limit",
                "endpoint": "/api/orders/offset",
                "rate_limit_delay": 0.5,
            },
        },
        qualification_gaps=(
            "This receipt qualifies the selected loopback mock API behavior with normal Polars Driver execution; cloud OAuth/API identities remain deferred.",
        ),
        references=(
            "metadata/file/shared/local.json",
            "metadata/file/read/local.json",
            "metadata/file/read/a9_api_rate_limit.json",
            "metadata/file/read/a9_api_oauth_json.json",
            "docker/mock_api_server.py",
            "tests/unit/sources/test_api_auth_guards.py",
        ),
    )
    payload["profile"].update(
        destination_format="parquet",
        qualification_scope=f"host-polars-docker-mock-api-{behavior}",
    )
    return write_receipt(case, payload)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8082",
        help="loopback mock API base URL (default: http://127.0.0.1:8082)",
    )
    parser.add_argument("--behavior", choices=BEHAVIORS, default="all")
    args = parser.parse_args()
    receipt = validate(args.root, args.base_url, args.behavior)
    print(receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
