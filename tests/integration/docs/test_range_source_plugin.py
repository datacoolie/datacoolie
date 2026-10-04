"""Verify the public custom source example at its real reader boundary."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = (
    ROOT
    / "docs"
    / "examples"
    / "files"
    / "projects"
    / "function"
    / "functions"
    / "range_source.py"
)


def _load_example_module():
    spec = importlib.util.spec_from_file_location("public_range_source", EXAMPLE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.integration
def test_public_range_source_plugin_filters_ordinary_and_bounded_reads() -> None:
    """The example applies ordinary and SourceReadRange filters to real frames."""

    pytest.importorskip("polars")
    from datacoolie import source_registry
    from datacoolie.core.models.connection import Connection
    from datacoolie.core.models.source import Source
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.platforms.local_platform import LocalPlatform
    from datacoolie.sources.contracts import SourceReadRange, SourceReadRequest
    from datacoolie.utils.time import utc_now

    module = _load_example_module()
    source_registry.register("public_range_example", module.RangeExampleReader)
    try:
        source = Source(
            connection=Connection(
                name="range_example",
                connection_type="function",
                format="function",
                configure={},
            ),
            table="events",
            watermark_columns=["sequence"],
            configure={
                "records": [
                    {"sequence": 1, "payload": "a"},
                    {"sequence": 2, "payload": "b"},
                    {"sequence": 3, "payload": "c"},
                    {"sequence": 4, "payload": "d"},
                    {"sequence": 5, "payload": "e"},
                ]
            },
        )
        reader = source_registry.get(
            "public_range_example",
            engine=PolarsEngine(platform=LocalPlatform()),
        )

        ordinary = reader.read(source, request=SourceReadRequest(
            reference_time=utc_now(), watermark_start={"sequence": 2},
        ))
        assert ordinary.frame is not None
        assert ordinary.frame.collect().get_column("sequence").to_list() == [3, 4, 5]
        assert ordinary.candidate.values == {"sequence": 5}

        bounded = reader.read(
            source,
            request=SourceReadRequest(reference_time=utc_now(),
                                      read_range=SourceReadRange("sequence", 2, 5)),
        )
        assert bounded.frame is not None
        assert bounded.frame.collect().get_column("sequence").to_list() == [2, 3, 4]
        assert bounded.candidate.values == {"sequence": 4}
        assert reader.merge_watermark(
            {"sequence": 5}, bounded.candidate
        ) == {"sequence": 5}
    finally:
        source_registry.unregister("public_range_example")
