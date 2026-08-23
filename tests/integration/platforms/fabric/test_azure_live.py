"""Opt-in live contract tests for external OneLake access."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from datacoolie.platforms.fabric_platform import FabricPlatform
from tests.integration.cloud_config import FabricIntegrationConfig

pytestmark = [
    pytest.mark.integration,
    pytest.mark.cloud_integration,
    pytest.mark.cloud_platform("fabric"),
]


def test_external_azure_file_contract(
    fabric_integration_config: FabricIntegrationConfig,
    tmp_path: Path,
) -> None:
    platform = FabricPlatform(runtime="external")
    test_root = (
        f"{fabric_integration_config.onelake_root}/datacoolie-live-{uuid4().hex}"
    )
    source = f"{test_root}/source.txt"
    copied = f"{test_root}/copied.txt"
    moved = f"{test_root}/nested/deep/moved.txt"
    uploaded = f"{test_root}/uploaded.bin"
    local_source = tmp_path / "source.bin"
    local_download = tmp_path / "download.bin"
    local_source.write_bytes(b"binary-payload")
    created = False

    try:
        platform.create_folder(test_root)
        created = True
        platform.write_file(source, "onelake-")
        platform.append_file(source, "payload")
        assert platform.read_file(source) == "onelake-payload"

        platform.copy_file(source, copied)
        platform.move_file(copied, moved)
        assert platform.file_exists(copied) is False
        assert platform.read_file(moved) == "onelake-payload"

        platform.write_file(moved, "previous-value", overwrite=True)
        platform.copy_file(source, copied)
        platform.move_file(copied, moved, overwrite=True)
        assert platform.file_exists(copied) is False
        assert platform.read_file(moved) == "onelake-payload"

        platform.upload_file(str(local_source), uploaded)
        platform.download_file(uploaded, str(local_download))
        assert local_download.read_bytes() == b"binary-payload"

        listed = platform.list_files(test_root)
        assert {item.name for item in listed} == {"source.txt", "uploaded.bin"}
        assert {
            item.name for item in platform.list_files(test_root, recursive=True)
        } == {
            "source.txt",
            "moved.txt",
            "uploaded.bin",
        }
        assert platform.get_file_info(source).size == len(b"onelake-payload")
    finally:
        # The configured root is never removed; cleanup is scoped to this unique child.
        if created:
            platform.delete_folder(test_root, recursive=True)


def test_external_key_vault_secret_contract(
    fabric_integration_config: FabricIntegrationConfig,
) -> None:
    vault_url = fabric_integration_config.key_vault_url
    secret_name = fabric_integration_config.key_vault_secret
    if not vault_url or not secret_name:
        pytest.skip("Key Vault live test variables are not configured")

    value = FabricPlatform(runtime="external").get_secret(secret_name, vault_url)
    assert isinstance(value, str)
    assert len(value) > 0
