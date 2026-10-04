"""Canonical source request/result and partial-runtime evidence tests."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.exceptions import SourceError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.source import Source
from datacoolie.sources.api_reader import APIReader
from datacoolie.sources.contracts import (
    SourceReadRange,
    SourceReadRequest,
    SourceReadUpperOrigin,
)
from datacoolie.sources.delta_reader import DeltaReader
from datacoolie.watermark.contracts import (
    WatermarkCandidate,
    WatermarkCandidateKind,
)
from tests.unit.sources.support import (
    FailingSourceReader,
    MockEngine,
    StatefulSourceReader,
    read_request,
)
from tests.unit.sources.test_api_reader import FakeEngine, _make_source, _mock_response


def _source() -> Source:
    return Source(
        connection=Connection(name="c", format="delta"),
        table="in",
        watermark_columns=["id"],
    )


def test_builtin_reader_rejects_unencodable_candidate():
    engine = MockEngine()
    engine.set_data({"id": [float("inf")]}, row_count=1)
    engine.set_max_values({"id": float("inf")})
    with pytest.raises(SourceError, match="finite") as caught:
        DeltaReader(engine).read(_source(), request=read_request())
    assert caught.value.runtime_info.rows_read == 1


def test_request_rejects_conflicting_range_bounds_before_reader():
    with pytest.raises(ValueError, match="cannot be combined"):
        SourceReadRequest(
            reference_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            read_range=SourceReadRange("id", 1, 5),
            watermark_start={"id": 0},
        )


def test_read_returns_detached_source_control_and_runtime():
    engine = MockEngine()
    engine.set_max_values({"id": 3})
    reader = StatefulSourceReader(engine)
    first = reader.read(_source(), request=read_request(watermark_start={"id": 0}))

    assert first.rows_read == 3
    assert first.selection.lower == {"id": 0}
    assert first.candidate == WatermarkCandidate(
        values={"id": 3}, kind=WatermarkCandidateKind.OBSERVED_MAX
    )
    assert first.runtime is not None
    assert first.runtime.rows_read == 3
    assert first.runtime.selection == first.selection

    first.selection.lower["id"] = 999
    first.candidate.values["id"] = 999
    assert first.runtime.selection.lower == {"id": 0}
    assert first.candidate.values == {"id": 999}

    second = reader.read(_source(), request=read_request())
    assert second.frame is None
    assert second.rows_read == 0
    assert second.candidate is None
    assert second.runtime is not None
    assert second.runtime.rows_read == 0


def test_failed_read_reports_unknown_count_when_no_count_was_measured():
    error = None
    reader = FailingSourceReader(MockEngine())
    try:
        reader.read(_source(), request=read_request())
    except SourceError as exc:
        error = exc
    assert error is not None
    assert error.runtime_info is not None
    assert error.runtime_info.rows_read is None
    assert error.runtime_info.status == "failed"


def test_failed_read_preserves_count_measured_before_later_failure():
    class FailsAfterCount(StatefulSourceReader):
        def _read_internal(self, source, request):
            self._set_rows_read(2)
            raise SourceError("after count")

    reader = FailsAfterCount(MockEngine())
    with pytest.raises(SourceError) as raised:
        reader.read(_source(), request=read_request())
    assert raised.value.runtime_info is not None
    assert raised.value.runtime_info.rows_read == 2


def test_public_validate_and_merge_use_typed_candidate():
    reader = DeltaReader(MockEngine())
    reader.validate_watermark({"id": 1}, boundary="lower")
    with pytest.raises(SourceError, match="Binary watermark comparison"):
        reader.validate_watermark({"id": b"opaque"}, boundary="lower")

    merged = reader.merge_watermark(
        {"id": 3, "cursor": "old"},
        WatermarkCandidate(
            values={"id": 2, "cursor": "new"},
            kind=WatermarkCandidateKind.OPAQUE,
        ),
    )
    assert merged == {"id": 2, "cursor": "new"}


def test_api_observed_max_does_not_invent_a_request_ceiling():
    source = _make_source(
        src_cfg={
            "range_param_mapping": {
                "observed_at": {
                    "lower": {"name": "from", "operator": ">"},
                    "upper": {"name": "to", "operator": "<"},
                    "format": "iso",
                    "response_column": "observed_at",
                    "watermark_value": "observed_max",
                }
            }
        },
        watermark_columns=["observed_at"],
    )
    reference = datetime(2026, 1, 3, 4, 5, 6, tzinfo=timezone.utc)
    start = {"observed_at": "2026-01-01T00:00:00+00:00"}
    with patch("datacoolie.sources.api_reader.httpx") as http:
        client = MagicMock()
        http.Client.return_value.__enter__.return_value = client
        http.Client.return_value.__exit__.return_value = False
        http.HTTPError = Exception
        client.request.return_value = _mock_response(
            [{"observed_at": "2026-01-02T00:00:00+00:00"}]
        )
        result = APIReader(FakeEngine()).read(
            source,
            request=SourceReadRequest(
                reference_time=reference,
                watermark_start=start,
            ),
        )

    assert result.candidate is not None
    assert result.candidate.kind is WatermarkCandidateKind.OBSERVED_MAX
    assert result.selection.upper_origin is SourceReadUpperOrigin.NONE
    assert result.selection.upper is None
