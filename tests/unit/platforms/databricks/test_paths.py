"""Databricks portable path contract tests."""

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._databricks.paths import (
    canonicalize_output_path,
    ensure_mutable_path,
    parent_path,
    parse_databricks_path,
)


def test_volume_path_and_alias_have_one_canonical_form() -> None:
    direct = parse_databricks_path(
        "/Volumes/main/default/logs/a.json", allow_cloud=False
    )
    alias = parse_databricks_path(
        "dbfs:/Volumes/main/default/logs/a.json", allow_cloud=False
    )
    assert direct == alias
    assert direct.canonical_path == "/Volumes/main/default/logs/a.json"
    assert direct.volume_identity == ("main", "default", "logs")
    assert parent_path(direct) == "/Volumes/main/default/logs"


@pytest.mark.parametrize(
    "path",
    [
        "/Volumes",
        "/Volumes/catalog",
        "/Volumes/catalog/schema",
        "dbfs:/FileStore/a.txt",
        "dbfs:/mnt/legacy/a.txt",
        "/dbfs/Volumes/catalog/schema/volume/a.txt",
        "/volumes/catalog/schema/volume/a.txt",
        "/Volumes/catalog/schema/volume/a/../b",
        "/Volumes/catalog/schema/volume/a/%2F/b",
        "/Volumes/catalog/schema/volume/a/%252F/b",
        "/Volumes/catalog/schema/volume//a",
        "/Volumes/catalog/schema/volume/a?token=value",
        " relative/path",
    ],
)
def test_rejects_deprecated_ambiguous_or_unsafe_paths(path: str) -> None:
    with pytest.raises(PlatformError):
        parse_databricks_path(path, allow_cloud=False)


@pytest.mark.parametrize(
    "path",
    [
        "s3://bucket/key/file.json",
        "abfss://container@account.dfs.core.windows.net/key/file.json",
        "gs://bucket/key/file.json",
    ],
)
def test_raw_cloud_uris_are_native_only(path: str) -> None:
    parsed = parse_databricks_path(path, allow_cloud=True)
    assert parsed.kind == "cloud"
    with pytest.raises(PlatformError, match="only with runtime='databricks'"):
        parse_databricks_path(path, allow_cloud=False)


@pytest.mark.parametrize(
    "path",
    [
        "s3://user@bucket/key",
        "abfss://container:password@account.dfs.core.windows.net/key",
    ],
)
def test_native_raw_cloud_uris_reject_user_credentials(path: str) -> None:
    with pytest.raises(PlatformError, match="user information"):
        parse_databricks_path(path, allow_cloud=True)


def test_mutation_requires_a_child_below_volume_or_cloud_root() -> None:
    root = parse_databricks_path("/Volumes/main/default/logs", allow_cloud=False)
    with pytest.raises(PlatformError, match="Volume root"):
        ensure_mutable_path(root)
    ensure_mutable_path(
        parse_databricks_path("/Volumes/main/default/logs/a.json", allow_cloud=False)
    )

    cloud_root = parse_databricks_path("s3://bucket", allow_cloud=True)
    with pytest.raises(PlatformError, match="storage root"):
        ensure_mutable_path(cloud_root)


def test_output_alias_is_canonicalized() -> None:
    assert (
        canonicalize_output_path("dbfs:/Volumes/main/default/logs/a.json/")
        == "/Volumes/main/default/logs/a.json"
    )
