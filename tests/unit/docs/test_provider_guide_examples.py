"""Executable checks for the metadata-provider guide payloads."""

from __future__ import annotations

import json
from pathlib import Path
import re

import pytest

from datacoolie.core.exceptions import MetadataError
from datacoolie.metadata.api_provider import APIProvider
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.platforms.local_platform import LocalPlatform


ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs"


def _json_blocks(relative_path: str) -> list[dict]:
    content = (DOCS / relative_path).read_text(encoding="utf-8")
    blocks = re.findall(r"```json\n(.*?)\n```", content, flags=re.DOTALL)
    assert blocks, relative_path
    return [json.loads(block) for block in blocks]


def _table_rows(markdown: str, heading: str) -> list[list[str]]:
    section = markdown.split(heading, 1)[1].lstrip().split("\n\n", 1)[0]
    lines = [line for line in section.splitlines() if line.startswith("|")]
    return [
        [cell.strip().strip("`") for cell in line.strip("|").split("|")]
        for line in (lines[:1] + lines[2:])
    ]


def test_file_guide_minimal_json_initializes_with_file_provider(tmp_path: Path) -> None:
    """The first copyable FileProvider payload remains a valid metadata document."""
    payload = _json_blocks("guide/providers/file.md")[0]
    config_path = tmp_path / "metadata.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    provider = FileProvider(config_path=str(config_path), platform=LocalPlatform())
    try:
        provider.initialize()
        assert len(provider.get_connections(active_only=False)) == 2
        dataflows = provider.get_dataflows(
            stage="bronze2silver",
            active_only=False,
            attach_schema_hints=False,
        )
        assert len(dataflows) == 1
        assert dataflows[0].destination.load_type == "overwrite"
    finally:
        provider.close()


def test_file_guide_yaml_matches_json_example() -> None:
    """The YAML variant describes the same connections and dataflow as JSON."""
    yaml = pytest.importorskip("yaml")
    content = (DOCS / "guide/providers/file.md").read_text(encoding="utf-8")
    block = re.search(r"```yaml\n(.*?)\n```", content, flags=re.DOTALL)
    assert block is not None
    assert yaml.safe_load(block.group(1)) == _json_blocks("guide/providers/file.md")[0]


def test_file_guide_excel_table_initializes_with_file_provider(tmp_path: Path) -> None:
    """The copyable workbook tables carry a valid one-flow metadata scope."""
    openpyxl = pytest.importorskip("openpyxl")
    content = (DOCS / "guide/providers/file.md").read_text(encoding="utf-8")
    workbook = openpyxl.Workbook()
    for index, heading in enumerate(("`connections` sheet:", "`dataflows` sheet:")):
        worksheet = workbook.active if index == 0 else workbook.create_sheet()
        worksheet.title = heading.split("`")[1]
        for row in _table_rows(content, heading):
            worksheet.append(row)
    path = tmp_path / "orders.xlsx"
    workbook.save(path)
    workbook.close()

    provider = FileProvider(config_path=str(path), platform=LocalPlatform())
    try:
        provider.initialize()
        assert len(provider.get_connections(active_only=False)) == 2
        assert [flow.name for flow in provider.get_dataflows(stage="bronze2silver")] == [
            "orders_to_parquet"
        ]
    finally:
        provider.close()


@pytest.mark.integration
def test_file_guide_orders_example_runs_locally(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The guide's CSV-to-Parquet document executes with the stated Polars extra."""
    import polars as pl

    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.orchestration.driver import DataCoolieDriver

    monkeypatch.chdir(tmp_path)
    metadata_path = tmp_path / "metadata" / "orders.json"
    metadata_path.parent.mkdir()
    metadata_path.write_text(
        json.dumps(_json_blocks("guide/providers/file.md")[0]), encoding="utf-8"
    )
    input_path = tmp_path / "data" / "input" / "orders" / "orders.csv"
    input_path.parent.mkdir(parents=True)
    input_path.write_text("order_id,amount\n1,19.99\n2,29.00\n3,5.50\n", encoding="utf-8")

    platform = LocalPlatform()
    provider = FileProvider(config_path=str(metadata_path), platform=platform)
    try:
        with DataCoolieDriver(
            engine=PolarsEngine(platform=platform),
            platform=platform,
            metadata_provider=provider,
            state_base_path=str(tmp_path / "state"),
        ) as driver:
            result = driver.run(stage="bronze2silver")
        assert result.succeeded == 1 and result.failed == 0
        output = tmp_path / "data" / "output" / "orders" / "orders.parquet"
        assert pl.read_parquet(output).sort("order_id").get_column("order_id").to_list() == [
            "1", "2", "3"
        ]
    finally:
        provider.close()


def test_file_provider_no_cache_keeps_source_snapshot_until_recreated(tmp_path: Path) -> None:
    """Disabling the cache does not turn a FileProvider into a file watcher."""
    payload = {"connections": [], "dataflows": []}
    config_path = tmp_path / "metadata.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    provider = FileProvider(
        config_path=str(config_path),
        platform=LocalPlatform(),
        enable_cache=False,
    )
    try:
        provider.initialize()
        assert provider.get_connections(active_only=False) == []
        config_path.write_text(
            json.dumps(
                {
                    "connections": [
                        {
                            "connection_id": "c-1",
                            "name": "new",
                            "connection_type": "file",
                            "format": "parquet",
                            "configure": {},
                        }
                    ],
                    "dataflows": [],
                }
            ),
            encoding="utf-8",
        )
        assert provider.get_connections(active_only=False) == []
    finally:
        provider.close()

    refreshed = FileProvider(config_path=str(config_path), platform=LocalPlatform())
    try:
        assert [connection.name for connection in refreshed.get_connections(active_only=False)] == [
            "new"
        ]
    finally:
        refreshed.close()


def test_api_guide_envelopes_and_dataflow_map_to_provider_contract() -> None:
    """The documented API envelopes can be consumed by the current mapper."""
    payloads = _json_blocks("guide/providers/api.md")
    collection = next(payload for payload in payloads if "pagination" in payload)
    dataflow = next(payload for payload in payloads if payload.get("dataflow_id"))
    empty_watermark = next(payload for payload in payloads if "current_value" in payload)

    provider = APIProvider(
        base_url="http://127.0.0.1:1",
        api_key="test-key",
        workspace_id="your-workspace-id",
        max_retries=0,
    )
    try:
        assert provider._collect_pages("GET", "/connections", {}, collection) == []
        assert provider._collect_pages("GET", "/connections", {}, {}) == []
        assert provider._collect_pages("GET", "/connections", {}, {"data": []}) == []
        mapped = provider._dict_to_dataflow(dataflow)
        assert mapped.source.connection.connection_type == "file"
        assert mapped.destination.connection.connection_type == "file"
        assert mapped.destination.load_type == "overwrite"
        assert empty_watermark == {"current_value": None}
    finally:
        provider.close()


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ([{}], "must be an object"),
        ({"data": {}}, "data must be a list"),
        ({"data": [], "pagination": []}, "pagination must be an object"),
    ],
)
def test_api_guide_rejects_malformed_collection_shapes(body: object, message: str) -> None:
    """Documented collection responses preserve the provider's actual reject cases."""
    provider = APIProvider(
        base_url="http://127.0.0.1:1",
        api_key="test-key",
        workspace_id="workspace",
        max_retries=0,
    )
    try:
        with pytest.raises(MetadataError, match=message):
            provider._collect_pages("GET", "/connections", {}, body)  # type: ignore[arg-type]
    finally:
        provider.close()
