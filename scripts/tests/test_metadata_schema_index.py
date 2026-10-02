from __future__ import annotations

from pathlib import Path

from packaging.version import Version

from scripts.generate_metadata_schema_index import (
    LATEST_SCHEMA_PATH,
    LATEST_SCHEMA_URL,
    build_index,
    build_public_index,
    check_index,
    index_bytes,
    public_index_bytes,
)


def test_bundled_metadata_schema_index_is_current() -> None:
    root = Path(__file__).resolve().parents[2] / "src" / "datacoolie" / "project" / "schemas"

    check_index(root)
    index = build_index(root)

    expected_versions = sorted(
        (path.parent.name for path in root.glob("*/metadata.schema.json")),
        key=Version,
        reverse=True,
    )
    assert [entry["version"] for entry in index["schemas"]] == expected_versions
    assert (root / "index.json").read_bytes() == index_bytes(root)


def test_public_index_points_latest_to_the_highest_stable_schema() -> None:
    root = Path(__file__).resolve().parents[2] / "src" / "datacoolie" / "project" / "schemas"

    index = build_public_index(root)
    latest = index["latest"]
    source = index["schemas"][0]

    assert latest["version"] == source["version"]
    assert latest["path"] == LATEST_SCHEMA_PATH
    assert latest["url"] == LATEST_SCHEMA_URL
    assert latest["source_path"] == source["path"]
    assert latest["source_url"] == source["url"]
    assert latest["sha256"] == source["sha256"]
    assert public_index_bytes(root).decode("utf-8").count(LATEST_SCHEMA_URL) == 1


def test_public_index_does_not_promote_a_prerelease_to_latest(tmp_path: Path) -> None:
    source_root = Path(__file__).resolve().parents[2] / "src" / "datacoolie" / "project" / "schemas"
    stable = tmp_path / "0.2.0"
    preview = tmp_path / "0.3.0a1"
    stable.mkdir()
    preview.mkdir()

    stable_schema = source_root / "0.2.0" / "metadata.schema.json"
    stable_payload = stable_schema.read_text(encoding="utf-8")
    preview_payload = stable_payload.replace(
        "schema/0.2.0/metadata.schema.json",
        "schema/0.3.0a1/metadata.schema.json",
    )
    (stable / "metadata.schema.json").write_text(stable_payload, encoding="utf-8")
    (preview / "metadata.schema.json").write_text(preview_payload, encoding="utf-8")

    index = build_public_index(tmp_path)

    assert index["latest"]["version"] == "0.2.0"
    assert index["latest"]["source_path"] == "0.2.0/metadata.schema.json"
