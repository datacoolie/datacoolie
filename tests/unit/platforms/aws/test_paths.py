from __future__ import annotations

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._aws.paths import ensure_bucket, parse_path


def test_parse_path_normalizes_s3_schemes() -> None:
    assert parse_path("s3://other/key.txt", "default") == ("other", "key.txt")
    assert parse_path("s3a://other/key.txt", "default") == ("other", "key.txt")


def test_parse_path_uses_default_bucket_for_plain_key() -> None:
    assert parse_path("folder/key.txt", "default") == ("default", "folder/key.txt")


def test_ensure_bucket_reports_missing_bucket() -> None:
    with pytest.raises(PlatformError, match="No bucket"):
        ensure_bucket("")
