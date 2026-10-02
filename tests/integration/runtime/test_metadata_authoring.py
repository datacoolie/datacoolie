"""Local authoring boundary: metadata file -> CLI -> schema/service, without execution."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from datacoolie.cli.main import main
from datacoolie.core.secrets.resolver import EnvResolver
from datacoolie.orchestration.driver import DataCoolieDriver


pytestmark = pytest.mark.integration


@pytest.mark.parametrize("configure,expected", [
    ({}, 0),
    ({"next_link_bound_mode": "opaque"}, 0),
    ({"next_link_bound_mode": "repeat_query_bounds"}, 0),
    ({"next_link_bound_mode": "invalid"}, 1),
    ({"next_link_bound_mode": None}, 1),
    ({"next_link_bound_mode": 1}, 1),
    ({"next_link_bound_mode": True}, 1),
    ({"next_link_bound_mode": {}}, 1),
])
def test_api_mode_authoring_reports_path_without_starting_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], configure: dict, expected: int,
) -> None:
    def forbidden(*args, **kwargs):
        raise AssertionError("Authoring validation must not start runtime I/O")

    monkeypatch.setattr("httpx.Client", forbidden)
    monkeypatch.setattr(DataCoolieDriver, "__init__", forbidden)
    monkeypatch.setattr(EnvResolver, "resolve", forbidden)
    document = {
        "connections": [
            {"name": "api", "connection_type": "api", "format": "api",
             "configure": {"base_url": "https://unused.invalid", "token": "UNRESOLVED_TOKEN"},
             "secrets_ref": {"env:": ["token"]}},
            {"name": "target", "connection_type": "file", "format": "parquet"},
        ],
        "dataflows": [{
            "name": "orders",
            "source": {"connection_name": "api", "configure": configure},
            "destination": {"connection_name": "target", "table": "orders"},
        }],
    }
    path = tmp_path / "metadata.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    original = path.read_bytes()

    assert main(["--format", "json", "validate", "--metadata-path", str(path)]) == expected

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is (expected == 0)
    assert path.read_bytes() == original
    if expected:
        assert payload["error"]["code"] == "validation.failed"
        assert "/dataflows/0/source/configure/next_link_bound_mode" in json.dumps(payload)
