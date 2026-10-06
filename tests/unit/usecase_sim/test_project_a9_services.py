"""Unit guards for live service oracle and credential discovery isolation."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
import pytest

SIM = Path(__file__).resolve().parents[3] / "usecase-sim"


def _load(group, name):
    path = SIM / "scripts/features" / group / name
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_api_checkpoint_rejects_wrong_history():
    oracle = _load("workflows", "validate_api_watermark_lifecycle.py")
    expected = {"modified_at": {"__datetime__": "2024-01-02T10:00:00"}}
    with pytest.raises(AssertionError, match="previous_value"):
        oracle.assert_watermark(
            {"current_value": expected, "previous_value": expected}, expected, None
        )


def test_api_checkpoint_rejects_lost_typed_value():
    oracle = _load("workflows", "validate_api_watermark_lifecycle.py")
    expected = {"modified_at": {"__datetime__": "2024-01-02T10:00:00"}}
    with pytest.raises(AssertionError, match="current_value"):
        oracle.assert_watermark(
            {"current_value": {"modified_at": "2024-01-02T10:00:00"}}, expected, None
        )


@pytest.mark.parametrize(
    "value",
    [
        {"modified_at": "2024-01-02T10:00:00"},
        {"other": {"__datetime__": "2024-01-02T10:00:00"}},
        {"modified_at": {"__datetime__": "2024-01-01T10:00:00"}},
    ],
)
def test_database_checkpoint_rejects_untyped_wrong_column_or_stale_value(value):
    oracle = _load("read", "validate_database_profiles.py")
    with pytest.raises(AssertionError, match="database checkpoint"):
        oracle.assert_typed_checkpoint(value, "modified_at")


def test_sigv4_child_cannot_inherit_cloud_discovery(tmp_path, monkeypatch):
    oracle = _load("read", "validate_minio_sigv4.py")
    for key in [
        "AWS_PROFILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_SESSION_TOKEN",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        "AWS_ENDPOINT_URL",
    ]:
        monkeypatch.setenv(key, "inherited-must-not-reach-child")
    env = oracle._isolated_environment(
        SimpleNamespace(root=tmp_path), "fake-dev-key", "fake-dev-secret"
    )
    assert "AWS_PROFILE" not in env and "AWS_SESSION_TOKEN" not in env
    assert not any(
        "inherited-must-not-reach-child" == v
        for k, v in env.items()
        if k.startswith("AWS_")
    )
    assert env["AWS_EC2_METADATA_DISABLED"] == "true"
    assert env["AWS_ACCESS_KEY_ID"] == "fake-dev-key"
    assert env["AWS_SECRET_ACCESS_KEY"] == "fake-dev-secret"
    for key in ["AWS_SHARED_CREDENTIALS_FILE", "AWS_CONFIG_FILE", "BOTO_CONFIG"]:
        path = Path(env[key])
        assert path.parent == tmp_path and path.read_bytes() == b""


@pytest.mark.parametrize(
    "status,code", [(200, None), (503, "InternalError"), (403, "AccessDenied")]
)
def test_sigv4_wrong_secret_oracle_rejects_other_backend_outcomes(status, code):
    oracle = _load("read", "validate_minio_sigv4.py")
    failed = SimpleNamespace(result={"succeeded": 0, "failed": 1, "skipped": 0})
    with pytest.raises(AssertionError):
        oracle.assert_denial(
            failed, [{"status": status, "error_code": code}], "SignatureDoesNotMatch"
        )


@pytest.mark.parametrize(
    "counts",
    [
        {"succeeded": 1, "failed": 0, "skipped": 0},
        {"succeeded": 0, "failed": 0, "skipped": 1},
    ],
)
def test_sigv4_denial_oracle_rejects_success_or_skip(counts):
    oracle = _load("read", "validate_minio_sigv4.py")
    with pytest.raises(AssertionError):
        oracle.assert_denial(
            SimpleNamespace(result=counts),
            [{"status": 403, "error_code": "AccessDenied"}],
            "AccessDenied",
        )


def test_sigv4_denial_requires_driver_http_response():
    oracle = _load("read", "validate_minio_sigv4.py")
    failed = SimpleNamespace(result={"succeeded": 0, "failed": 1, "skipped": 0})
    with pytest.raises(AssertionError, match="before a backend response"):
        oracle.assert_denial(failed, [], "AccessDenied")


def test_sigv4_trace_uses_actual_http_response_and_omits_credentials():
    import httpx

    oracle = _load("read", "validate_minio_sigv4.py")
    with oracle.driver_http_trace("/private/test.json") as trace:
        with httpx.Client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    403, content=b"<Error><Code>AccessDenied</Code></Error>"
                )
            )
        ) as client:
            client.get(
                "https://example.test/private/test.json",
                headers={"Authorization": "synthetic-secret"},
            )
    assert trace == [
        {
            "method": "GET",
            "path": "/private/test.json",
            "status": 403,
            "error_code": "AccessDenied",
        }
    ]


@pytest.mark.parametrize("mutation", ["value", "missing_id", "type"])
def test_sigv4_output_oracle_rejects_wrong_rows_and_types(monkeypatch, mutation):
    oracle = _load("read", "validate_minio_sigv4.py")
    expected = [{"id": 1, "value": "first"}, {"id": 2, "value": "second"}]
    actual = [dict(row) for row in expected]
    schema = dict(oracle.EXPECTED_SCHEMA)
    if mutation == "value":
        actual[1]["value"] = "wrong"
    elif mutation == "missing_id":
        actual.pop()
    else:
        schema["id"] = "String"
    monkeypatch.setattr(oracle, "read_delta_rows", lambda *_: actual)
    monkeypatch.setattr(oracle, "read_delta_schema", lambda *_: schema)
    with pytest.raises(AssertionError):
        oracle.assert_output(None, None, expected)


@pytest.mark.parametrize("artifact", ["destination", "checkpoint"])
def test_sigv4_denial_oracle_rejects_visible_output_or_checkpoint(tmp_path, artifact):
    oracle = _load("read", "validate_minio_sigv4.py")
    target, watermarks = tmp_path / "target", tmp_path / "watermarks"
    if artifact == "destination":
        (target / "records").mkdir(parents=True)
    else:
        watermarks.mkdir()
        (watermarks / "watermark_value.json").write_text("{}")
    with pytest.raises(AssertionError):
        oracle.assert_denied_artifacts(target, watermarks)


@pytest.mark.parametrize(
    "column,value",
    [
        ("__created_at", None),
        ("__updated_at", None),
        ("__updated_by", "wrong"),
        ("__dataflow_run_id", ""),
    ],
)
def test_sigv4_output_oracle_rejects_invalid_generated_columns(
    monkeypatch, column, value
):
    from datetime import datetime, timezone

    oracle = _load("read", "validate_minio_sigv4.py")
    expected = [{"id": 1, "value": "first"}]
    actual = [
        {
            **expected[0],
            "__created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "__updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "__updated_by": "DataCoolie",
            "__dataflow_run_id": "11111111-1111-1111-1111-111111111111",
        }
    ]
    actual[0][column] = value
    monkeypatch.setattr(oracle, "read_delta_rows", lambda *_: actual)
    monkeypatch.setattr(
        oracle, "read_delta_schema", lambda *_: dict(oracle.EXPECTED_SCHEMA)
    )
    with pytest.raises((AssertionError, ValueError)):
        oracle.assert_output(None, None, expected)
