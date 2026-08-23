"""Qualified OneLake and ADLS Gen2 path contract tests."""

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._fabric.paths import (
    ensure_mutable_path,
    parse_azure_datalake_path,
)


def test_parse_onelake_abfss_path() -> None:
    value = parse_azure_datalake_path(
        "abfss://workspace@onelake.dfs.fabric.microsoft.com/"
        "sales.Lakehouse/Files/raw/a.csv"
    )
    assert value.provider == "onelake"
    assert value.account_url == "https://onelake.dfs.fabric.microsoft.com"
    assert value.file_system == "workspace"
    assert value.path == "sales.Lakehouse/Files/raw/a.csv"
    assert value.canonical_uri.endswith("/sales.Lakehouse/Files/raw/a.csv")


def test_parse_onelake_guid_https_path() -> None:
    value = parse_azure_datalake_path(
        "https://onelake.dfs.fabric.microsoft.com/"
        "11111111-1111-1111-1111-111111111111/"
        "22222222-2222-2222-2222-222222222222/Files/a.csv"
    )
    assert value.file_system == "11111111-1111-1111-1111-111111111111"
    assert value.path.startswith("22222222-2222-2222-2222-222222222222/Files")


@pytest.mark.parametrize(
    "uri,account_url,file_system,path",
    [
        (
            "abfs://raw@account.dfs.core.windows.net/bronze/a.parquet",
            "https://account.dfs.core.windows.net",
            "raw",
            "bronze/a.parquet",
        ),
        (
            "https://account.dfs.core.windows.net/raw/bronze/a.parquet",
            "https://account.dfs.core.windows.net",
            "raw",
            "bronze/a.parquet",
        ),
    ],
)
def test_parse_adls_forms(
    uri: str,
    account_url: str,
    file_system: str,
    path: str,
) -> None:
    value = parse_azure_datalake_path(uri)
    assert value.provider == "adls"
    assert value.account_url == account_url
    assert value.file_system == file_system
    assert value.path == path
    assert value.canonical_uri.startswith("abfss://")


def test_encoded_spaces_are_decoded_for_sdk_and_encoded_canonically() -> None:
    value = parse_azure_datalake_path(
        "https://onelake.dfs.fabric.microsoft.com/My%20Workspace/"
        "Sales%20Lakehouse.Lakehouse/Files/a%20b.csv"
    )
    assert value.file_system == "My Workspace"
    assert value.path == "Sales Lakehouse.Lakehouse/Files/a b.csv"
    assert "My%20Workspace" in value.canonical_uri
    assert "a%20b.csv" in value.canonical_uri


@pytest.mark.parametrize(
    "uri",
    [
        "relative/path",
        "s3://bucket/key",
        "abfss://raw@evil.example.com/path",
        "abfss://raw@account.dfs.core.windows.net/a/../b",
        "abfss://raw@account.dfs.core.windows.net/a/%2F/b",
        "abfss://raw@account.dfs.core.windows.net/a/%252F/b",
        "abfss://raw%40other@account.dfs.core.windows.net/a",
        "abfss://raw@account.dfs.core.windows.net/a/%00/b",
        "abfss://raw@account.dfs.core.windows.net//a",
        "abfss://raw@account.dfs.core.windows.net/a?sig=secret",
        "abfss://raw@account.dfs.core.windows.net/a#fragment",
        "https://user@account.dfs.core.windows.net/raw/a",
        "https://account.dfs.core.windows.net/",
        "https://account.dfs.core.windows.net:invalid/raw/a",
        "https://[invalid/raw/a",
    ],
)
def test_reject_invalid_or_ambiguous_paths(uri: str) -> None:
    with pytest.raises(PlatformError):
        parse_azure_datalake_path(uri)


def test_onelake_mutation_requires_path_below_files_or_tables() -> None:
    for uri in (
        "abfss://ws@onelake.dfs.fabric.microsoft.com",
        "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse",
        "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files",
        "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Other/a",
    ):
        with pytest.raises(PlatformError):
            ensure_mutable_path(parse_azure_datalake_path(uri))

    ensure_mutable_path(
        parse_azure_datalake_path(
            "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/a"
        )
    )


def test_adls_mutation_rejects_only_filesystem_root() -> None:
    with pytest.raises(PlatformError):
        ensure_mutable_path(parse_azure_datalake_path("abfss://raw@a.dfs.core.windows.net"))
    ensure_mutable_path(
        parse_azure_datalake_path("abfss://raw@a.dfs.core.windows.net/path")
    )
