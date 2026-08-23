"""Opt-in live S3 contract tests for AWS S3 and MinIO."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from datacoolie.platforms.aws_platform import AWSPlatform
from tests.integration.cloud_config import AwsIntegrationConfig

pytestmark = [
    pytest.mark.integration,
    pytest.mark.cloud_integration,
    pytest.mark.cloud_platform("aws"),
]


def test_s3_compatible_file_contract(
    aws_integration_config: AwsIntegrationConfig,
    tmp_path: Path,
) -> None:
    platform = AWSPlatform(
        bucket=aws_integration_config.bucket,
        region=aws_integration_config.region,
        endpoint_url=aws_integration_config.endpoint_url,
    )
    prefix = aws_integration_config.prefix.strip("/")
    test_root = f"s3://{aws_integration_config.bucket}/{prefix}/aws-live-{uuid4().hex}"
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
        platform.write_file(source, "s3-")
        platform.append_file(source, "payload")
        assert platform.read_file(source) == "s3-payload"

        platform.copy_file(source, copied)
        platform.move_file(copied, moved)
        assert platform.file_exists(copied) is False
        assert platform.read_file(moved) == "s3-payload"

        platform.write_file(moved, "previous-value", overwrite=True)
        platform.copy_file(source, copied)
        platform.move_file(copied, moved, overwrite=True)
        assert platform.file_exists(copied) is False
        assert platform.read_file(moved) == "s3-payload"

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
        assert platform.get_file_info(source).size == len(b"s3-payload")
    finally:
        if created:
            platform.delete_folder(test_root, recursive=True)
