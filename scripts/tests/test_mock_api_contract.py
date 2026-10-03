"""The simulator must preserve UTC instants for canonical API ranges."""

from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path

import pytest

from datacoolie.sources._api.ranges import ResidualFilter, filter_records_by_ranges


@pytest.fixture
def mock_api():
    pytest.importorskip("flask", reason="Mock API qualification requires Flask")
    path = Path(__file__).resolve().parents[2] / "usecase-sim/docker/mock_api_server.py"
    spec = importlib.util.spec_from_file_location("range_contract_mock_api", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_mock_parser_preserves_offset_instant(mock_api):
    expected = datetime(2024, 1, 15, 10)
    for value in ("2024-01-15T10:00:00Z", "2024-01-15T17:00:00+07:00"):
        assert mock_api._parse_datetime(value) == expected
    assert mock_api._parse_datetime("2024-01-15T10:00:00") == expected


@pytest.mark.parametrize(
    "path",
    [
        "/api/orders/ranged",
        "/api/orders/ranged-inclusive",
        "/api/secure/bearer-ranged",
        "/api/orders/wm-body",
    ],
)
def test_range_routes_use_utc_rows_and_equivalent_offset_bounds(mock_api, path):
    client = mock_api.app.test_client()
    headers = {"Authorization": f"Bearer {mock_api.MOCK_BEARER_TOKEN}"}

    def fetch(lower, upper):
        bounds = {"modified_since": lower, "modified_until": upper}
        if path.endswith("wm-body"):
            response = client.post(path, json=bounds, headers=headers)
        else:
            response = client.get(path, query_string=bounds, headers=headers)
        assert response.status_code == 200
        return response.get_json()["data"]

    utc_rows = fetch("2024-01-15T10:00:00Z", "2024-01-15T10:05:00Z")
    offset_rows = fetch("2024-01-15T17:00:00+07:00", "2024-01-15T17:05:00+07:00")
    assert offset_rows == utc_rows
    expected = [1001, 1002] if path.endswith("ranged-inclusive") else [1001]
    assert [row["order_id"] for row in utc_rows] == expected
    assert all(row["modified_at"].endswith("+00:00") for row in utc_rows)
    residual = ResidualFilter(
        field="modified_at",
        response_column="modified_at",
        lower=datetime(2024, 1, 15, 10, tzinfo=timezone.utc),
        upper=datetime(2024, 1, 15, 10, 5, tzinfo=timezone.utc),
        lower_operator=">=",
        upper_operator="<",
    )
    selected = filter_records_by_ranges(utc_rows, [residual])
    assert [row["order_id"] for row in selected] == [1001]
    assert mock_api.ROWS[0]["modified_at"] == "2024-01-15T10:00:00"
