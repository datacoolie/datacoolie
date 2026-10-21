"""Meaningful guards for existing fixture oracles and runtime compilation."""

import copy
import contextlib
import importlib.util
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SIM = Path(__file__).resolve().parents[3] / "usecase-sim"
sys.path.insert(0, str(SIM / "scripts/features"))
from _fixture_support import compile_existing  # noqa: E402
from _stateful_support import assert_exact_rows  # noqa: E402
from datacoolie.utils.identity import name_to_uuid  # noqa: E402


def _load_sigv4_oracle():
    path = SIM / "scripts/features/read/validate_minio_sigv4.py"
    spec = importlib.util.spec_from_file_location("a9_sigv4_oracle", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_existing_compile_preserves_authored_contract_and_isolates_ids(tmp_path):
    original = (SIM / "metadata/file/transform.json").read_bytes()
    roots = []
    for name in ["a", "b"]:
        root = tmp_path / name
        root.mkdir()
        case = SimpleNamespace(
            input=root / "input",
            output=root / "output",
        )
        doc, identity = compile_existing(
            case, "transform", "row_filter__on_computed_col"
        )
        authored = next(
            f
            for f in json.loads(original)["dataflows"]
            if f["name"] == "row_filter__on_computed_col"
        )
        assert doc["dataflows"][0]["transform"] == authored["transform"]
        assert doc["dataflows"][0]["source"] == authored["source"]
        assert identity["authored_flow"] == "row_filter__on_computed_col"
        assert identity["runtime_flow"] == "row_filter__on_computed_col"
        assert identity["runtime_stage"] == authored["stage"]
        roots.append(identity["runtime_flow"])
    assert roots == ["row_filter__on_computed_col"] * 2
    assert (SIM / "metadata/file/transform.json").read_bytes() == original


@pytest.mark.parametrize(
    "mutant", ["lost-id", "wrong-value", "duplicate", "extra-field"]
)
def test_exact_existing_fixture_rows_reject_mutants(mutant):
    expected = [{"id": 1, "value": "eligible"}, {"id": 2, "value": "newest"}]
    actual = copy.deepcopy(expected)
    if mutant == "lost-id":
        actual.pop()
    elif mutant == "wrong-value":
        actual[0]["value"] = "excluded"
    elif mutant == "duplicate":
        actual.append(actual[0])
    else:
        actual[0]["unexpected"] = "lost-schema-boundary"
    with pytest.raises(AssertionError):
        assert_exact_rows(actual, expected, key="id")


def test_sigv4_negative_controls_start_from_pristine_signed_document(
    tmp_path, monkeypatch
):
    """Exercise both denial configurations without MinIO or Driver I/O."""

    oracle = _load_sigv4_oracle()
    flow_name = "minio-sigv4-private-read"
    seen: list[tuple[int, str, str]] = []
    active_trace: list[list[dict[str, object]]] = []
    correct_secret = "fixture-correct-secret"
    monkeypatch.setenv("A9_MINIO_SECRET_KEY", correct_secret)

    document = {
        "connections": [
            {
                "name": "minio_sigv4_source",
                "configure": {
                    "base_url": "http://127.0.0.1:9000",
                    "auth_type": "aws_sigv4",
                    "aws_service": "s3",
                    "aws_region": "us-east-1",
                    "aws_access_key_id": "A9_MINIO_ACCESS_KEY",
                    "aws_secret_access_key": "A9_MINIO_SECRET_KEY",
                    "aws_session_token": "unused-session-token",
                },
            },
            {
                "name": "minio_sigv4_destination",
                "configure": {"base_path": "__CASE_ROOT__/output"},
            },
        ],
        "dataflows": [{"name": flow_name, "stage": "minio_sigv4"}],
    }
    case = SimpleNamespace(
        root=tmp_path / "case",
        output=tmp_path / "case" / "output",
        watermarks=tmp_path / "case" / "watermarks",
        document=None,
    )

    def materialize(case_value, _fixture_name):
        case_value.document = copy.deepcopy(document)

    def fake_run_driver(case_value, *, stage, iteration=1, **_kwargs):
        config = case_value.document["connections"][0]["configure"]
        auth_type = config.get("auth_type", "")
        secret = os.environ["A9_MINIO_SECRET_KEY"]
        seen.append((iteration, auth_type, secret))
        if auth_type == "none":
            status, error_code = 403, "AccessDenied"
        elif secret == correct_secret:
            status, error_code = 200, None
        else:
            status, error_code = 403, "SignatureDoesNotMatch"
        if active_trace:
            active_trace[-1].append(
                {"method": "GET", "path": "/private/records.json", "status": status, "error_code": error_code}
            )
        succeeded = status == 200
        return SimpleNamespace(
            result={"succeeded": int(succeeded), "failed": int(not succeeded), "skipped": 0},
            records=[
                {
                    "record_type": "dataflow_run_log",
                    "dataflow_id": name_to_uuid(flow_name),
                    "status": "succeeded" if succeeded else "failed",
                }
            ],
        )

    @contextlib.contextmanager
    def fake_trace(_endpoint):
        trace: list[dict[str, object]] = []
        active_trace.append(trace)
        try:
            yield trace
        finally:
            active_trace.pop()

    class FakeS3:
        def create_bucket(self, **_kwargs):
            return None

        def put_object(self, **_kwargs):
            return None

    class FakeHttpx:
        @staticmethod
        def get(*_args, **_kwargs):
            return SimpleNamespace(status_code=403, text="<Code>AccessDenied</Code>")

    monkeypatch.setattr(oracle, "resolve_case_root", lambda *_args, **_kwargs: case)
    monkeypatch.setattr(oracle, "materialize_fixture", materialize)
    monkeypatch.setattr(oracle, "run_driver", fake_run_driver)
    monkeypatch.setattr(oracle, "driver_http_trace", fake_trace)
    monkeypatch.setattr(oracle, "PolarsEngine", lambda: object())
    monkeypatch.setattr(oracle, "assert_successful_flow", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(oracle, "assert_output", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(oracle, "assert_denied_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(oracle, "receipt_payload", lambda *_args, **_kwargs: {"profile": {}})
    monkeypatch.setattr(oracle, "write_receipt", lambda _case, payload: payload)
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=lambda *_args, **_kwargs: FakeS3()))
    monkeypatch.setitem(sys.modules, "httpx", FakeHttpx())
    monkeypatch.setenv("A9_MINIO_ACCESS_KEY", "fixture-correct-access")

    result = oracle._phases(None)

    assert seen == [
        (1, "aws_sigv4", correct_secret),
        (2, "none", correct_secret),
        (3, "aws_sigv4", "invalid-development-signature"),
    ]
    assert result["profile"]["qualification_scope"] == "host-polars-docker-minio-api-sigv4"
