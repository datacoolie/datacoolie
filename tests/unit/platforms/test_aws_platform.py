"""Tests for datacoolie.platforms.aws_platform — AWSPlatform.

All tests mock ``boto3`` since the real SDK requires AWS credentials
and network access.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms.aws_platform import AWSPlatform


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture()
def mock_client() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def platform(mock_client: MagicMock) -> AWSPlatform:
    p = AWSPlatform(bucket="test-bucket")
    p._client = mock_client
    return p


# =====================================================================
# Path parsing
# =====================================================================


class TestPathParsing:
    def test_s3_uri(self) -> None:
        p = AWSPlatform(bucket="default")
        bucket, key = p._parse_path("s3://other-bucket/some/key.txt")
        assert bucket == "other-bucket"
        assert key == "some/key.txt"

    def test_plain_key(self) -> None:
        p = AWSPlatform(bucket="mybucket")
        bucket, key = p._parse_path("data/file.txt")
        assert bucket == "mybucket"
        assert key == "data/file.txt"

    def test_no_bucket_raises(self) -> None:
        p = AWSPlatform()
        p._client = MagicMock()  # skip boto3 import
        with pytest.raises(PlatformError, match="No bucket"):
            p.read_file("some/key")


# =====================================================================
# File I/O
# =====================================================================


class TestReadFile:
    def test_read(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        body = MagicMock()
        body.read.return_value = b"hello"
        mock_client.get_object.return_value = {"Body": body}
        assert platform.read_file("data/file.txt") == "hello"

    def test_read_failure(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.get_object.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="read file"):
            platform.read_file("data/file.txt")


class TestWriteFile:
    def test_write_new(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.side_effect = Exception("not found")
        platform.write_file("data/file.txt", "content")
        mock_client.put_object.assert_called_once()

    def test_write_exists_no_overwrite(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.return_value = {}
        with pytest.raises(PlatformError, match="already exists"):
            platform.write_file("data/file.txt", "content")


class TestAppendFile:
    def test_append_new(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.side_effect = Exception("not found")
        platform.append_file("data/file.txt", "new")
        mock_client.put_object.assert_called_once()

    def test_append_existing(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.return_value = {}
        body = MagicMock()
        body.read.return_value = b"old"
        mock_client.get_object.return_value = {"Body": body}
        platform.append_file("data/file.txt", "new")
        call_args = mock_client.put_object.call_args
        assert b"oldnew" in call_args.kwargs.get("Body", b"") or call_args[1].get("Body", b"") == b"oldnew"


class TestDeleteFile:
    def test_delete(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        platform.delete_file("data/file.txt")
        mock_client.delete_object.assert_called_once()


# =====================================================================
# Directory operations
# =====================================================================


class TestCreateFolder:
    def test_create(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        platform.create_folder("data/subfolder")
        mock_client.put_object.assert_called_once()


class TestDeleteFolder:
    def test_delete_recursive(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "data/folder/a.txt"}, {"Key": "data/folder/b.txt"}]}
        ]
        mock_client.get_paginator.return_value = paginator
        mock_client.delete_objects.return_value = {}
        platform.delete_folder("data/folder", recursive=True)
        mock_client.delete_objects.assert_called_once()

    def test_delete_two_pages(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        """Each page batch is processed sequentially via the paginator."""
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "data/folder/a.txt"}]},
            {"Contents": [{"Key": "data/folder/b.txt"}]},
        ]
        mock_client.get_paginator.return_value = paginator
        mock_client.delete_objects.return_value = {}
        platform.delete_folder("data/folder", recursive=True)
        assert mock_client.delete_objects.call_count == 2

    def test_delete_partial_failure_raises(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        """Partial failures from delete_objects are surfaced as PlatformError."""
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "data/folder/a.txt"}]}
        ]
        mock_client.get_paginator.return_value = paginator
        mock_client.delete_objects.return_value = {
            "Errors": [{"Key": "data/folder/a.txt", "Code": "AccessDenied", "Message": "Access Denied"}]
        }
        with pytest.raises(PlatformError, match="Failed to delete 1 object"):
            platform.delete_folder("data/folder", recursive=True)


class TestListFiles:
    def test_list(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "data/a.parquet", "Size": 100, "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc)},
                    {"Key": "data/b.csv", "Size": 200, "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc)},
                ]
            }
        ]
        mock_client.get_paginator.return_value = paginator
        result = platform.list_files("data")
        assert len(result) == 2

    def test_extension_filter(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "data/a.parquet", "Size": 100, "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc)},
                    {"Key": "data/b.csv", "Size": 200, "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc)},
                ]
            }
        ]
        mock_client.get_paginator.return_value = paginator
        result = platform.list_files("data", extension=".parquet")
        assert len(result) == 1

    def test_list_recursive_parallel(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        """Recursive listing uses the eager per-page parallel tree-walk."""
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "data/a.parquet", "Size": 100, "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc)},
                ],
                "CommonPrefixes": [],
            }
        ]
        mock_client.get_paginator.return_value = paginator
        result = platform.list_files("data", recursive=True)
        assert len(result) == 1
        assert result[0].name == "a.parquet"


class TestListFolders:
    def test_list(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"CommonPrefixes": [{"Prefix": "data/sub1/"}, {"Prefix": "data/sub2/"}]}
        ]
        mock_client.get_paginator.return_value = paginator
        result = platform.list_folders("data")
        assert len(result) == 2


# =====================================================================
# Existence checks
# =====================================================================


class TestExistence:
    def test_file_exists(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.return_value = {}
        assert platform.file_exists("data/file.txt") is True

    def test_file_not_exists(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.side_effect = Exception("not found")
        assert platform.file_exists("data/file.txt") is False

    def test_folder_exists(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.list_objects_v2.return_value = {"KeyCount": 1}
        assert platform.folder_exists("data/folder") is True

    def test_folder_not_exists(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.list_objects_v2.return_value = {"KeyCount": 0}
        assert platform.folder_exists("data/folder") is False


# =====================================================================
# File management
# =====================================================================


class TestUploadFile:
    def test_upload(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.side_effect = Exception("not found")
        platform.upload_file("/local/file.txt", "data/file.txt")
        mock_client.upload_file.assert_called_once()


class TestDownloadFile:
    def test_download(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        platform.download_file("s3://test-bucket/data/file.txt", "/tmp/local.txt")
        mock_client.download_file.assert_called_once_with("test-bucket", "data/file.txt", "/tmp/local.txt")

    def test_download_plain_key(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        platform.download_file("data/file.txt", "/tmp/local.txt")
        mock_client.download_file.assert_called_once_with("test-bucket", "data/file.txt", "/tmp/local.txt")

    def test_download_failure(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.download_file.side_effect = Exception("no access")
        with pytest.raises(PlatformError, match="download"):
            platform.download_file("data/file.txt", "/tmp/local.txt")


class TestCopyFile:
    def test_copy(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.side_effect = Exception("not found")
        platform.copy_file("s3://test-bucket/src.txt", "s3://test-bucket/dest.txt")
        mock_client.copy_object.assert_called_once()


class TestMoveFile:
    def test_move(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.side_effect = [Exception("not found"), None]
        platform.move_file("s3://test-bucket/src.txt", "s3://test-bucket/dest.txt")
        mock_client.copy_object.assert_called_once()
        mock_client.delete_object.assert_called_once()


class TestGetFileInfo:
    def test_info(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.return_value = {
            "ContentLength": 1024,
            "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc),
        }
        info = platform.get_file_info("data/file.txt")
        assert info.name == "file.txt"
        assert info.size == 1024


# =====================================================================
# Registry
# =====================================================================


class TestRegistry:
    def test_aws_in_registry(self) -> None:
        from datacoolie import platform_registry
        assert platform_registry.is_available("aws")


# =====================================================================
# Session caching
# =====================================================================


class TestSession:
    def test_session_reused_across_calls(self) -> None:
        """boto3.Session is constructed once and cached per platform instance."""
        with patch("boto3.Session") as mock_session_cls:
            mock_session_cls.return_value = MagicMock()
            p = AWSPlatform(region="us-east-1")
            p.boto3_client("sqs")
            p.boto3_client("sns")
            mock_session_cls.assert_called_once()

    def test_separate_instances_have_separate_sessions(self) -> None:
        """Each AWSPlatform instance gets its own session."""
        with patch("boto3.Session") as mock_session_cls:
            mock_session_cls.return_value = MagicMock()
            p1 = AWSPlatform(region="us-east-1")
            p2 = AWSPlatform(region="eu-west-1")
            p1.boto3_client("s3")
            p2.boto3_client("s3")
            assert mock_session_cls.call_count == 2


# =====================================================================
# boto3_client factory
# =====================================================================


class TestBoto3Client:
    def test_returns_service_client(self) -> None:
        with patch("boto3.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            p = AWSPlatform(region="us-east-1")
            client = p.boto3_client("sqs")
            mock_session.client.assert_called_once_with("sqs")
            assert client is mock_session.client.return_value

    def test_passes_endpoint_url_for_s3(self) -> None:
        with patch("boto3.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            p = AWSPlatform(endpoint_url="http://localstack:4566")
            p.boto3_client("s3")
            call_kwargs = mock_session.client.call_args.kwargs
            assert call_kwargs.get("endpoint_url") == "http://localstack:4566"

    def test_does_not_inject_endpoint_url_for_non_s3(self) -> None:
        """endpoint_url must not bleed into non-storage services (e.g. secretsmanager)."""
        with patch("boto3.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            p = AWSPlatform(endpoint_url="http://minio:9000")
            p.boto3_client("secretsmanager")
            call_kwargs = mock_session.client.call_args.kwargs
            assert "endpoint_url" not in call_kwargs

    def test_kwarg_overrides_endpoint_url(self) -> None:
        with patch("boto3.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            p = AWSPlatform(endpoint_url="http://default:4566")
            p.boto3_client("sqs", endpoint_url="http://custom:4567")
            call_kwargs = mock_session.client.call_args.kwargs
            assert call_kwargs.get("endpoint_url") == "http://custom:4567"


# =====================================================================
# Secrets
# =====================================================================


class TestGetSecret:
    def _make_sm_client(self, secret_value: str) -> MagicMock:
        client = MagicMock()
        client.get_secret_value.return_value = {"SecretString": secret_value}
        return client

    def test_fetch_json_secret_extracts_key(self) -> None:
        import json
        with patch("boto3.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            mock_session.client.return_value = self._make_sm_client(
                json.dumps({"username": "admin", "password": "s3cr3t"})
            )
            p = AWSPlatform(region="us-east-1")
            assert p.get_secret("password", "prod/database/credentials") == "s3cr3t"
            mock_session.client.assert_called_once_with("secretsmanager")
            mock_session.client.return_value.get_secret_value.assert_called_once_with(
                SecretId="prod/database/credentials"
            )

    def test_fetch_plain_string_secret(self) -> None:
        with patch("boto3.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            mock_session.client.return_value = self._make_sm_client("tok3n")
            p = AWSPlatform(region="us-east-1")
            # plain string (not JSON) — returned as-is regardless of key
            assert p.get_secret("api_key", "prod/api-token") == "tok3n"

    def test_missing_json_key_raises(self) -> None:
        import json
        with patch("boto3.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            mock_session.client.return_value = self._make_sm_client(
                json.dumps({"username": "admin"})
            )
            p = AWSPlatform(region="us-east-1")
            with pytest.raises(PlatformError, match="'password'.*not found"):
                p._fetch_secret("password", "prod/database/credentials")

    def test_missing_source_raises(self) -> None:
        p = AWSPlatform(region="us-east-1")
        with pytest.raises(PlatformError, match="secret name"):
            p._fetch_secret("password", "")

    def test_boto3_failure_raises(self) -> None:
        with patch("boto3.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            mock_session.client.return_value = MagicMock(
                get_secret_value=MagicMock(side_effect=Exception("access denied"))
            )
            p = AWSPlatform(region="us-east-1")
            with pytest.raises(PlatformError, match="Failed to fetch secret"):
                p._fetch_secret("password", "prod/database/credentials")

    def test_region_from_platform_not_source(self) -> None:
        """Region always comes from AWSPlatform, never from source."""
        import json
        with patch("boto3.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            mock_session.client.return_value = self._make_sm_client(
                json.dumps({"token": "abc123"})
            )
            p = AWSPlatform(region="ap-southeast-1")
            p.get_secret("token", "prod/api-token")
            mock_session_cls.assert_called_once_with(region_name="ap-southeast-1")

    def test_fetch_binary_secret(self) -> None:
        with patch("boto3.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            mock_session.client.return_value = MagicMock(
                get_secret_value=MagicMock(return_value={"SecretBinary": b"binary-value"})
            )
            p = AWSPlatform(region="us-east-1")
            assert p.get_secret("any_key", "prod/binary-secret") == "binary-value"

    def test_is_base_secret_provider(self) -> None:
        from datacoolie.core.secret_provider import BaseSecretProvider
        assert isinstance(AWSPlatform(), BaseSecretProvider)


class TestAWSAdvancedPaths:
    def test_parse_path_s3_without_key(self) -> None:
        p = AWSPlatform(bucket="default")
        bucket, key = p._parse_path("s3://only-bucket")
        assert bucket == "only-bucket"
        assert key == ""

    def test_ensure_bucket_raises_on_empty(self) -> None:
        p = AWSPlatform()
        with pytest.raises(PlatformError, match="No bucket"):
            p._ensure_bucket("")

    def test_boto3_client_injects_only_for_s3(self) -> None:
        with patch("boto3.Session") as mock_session_cls:
            sess = MagicMock()
            mock_session_cls.return_value = sess
            p = AWSPlatform(endpoint_url="http://s3-local")
            p.boto3_client("s3")
            p.boto3_client("secretsmanager")
            first = sess.client.call_args_list[0].kwargs
            second = sess.client.call_args_list[1].kwargs
            assert first.get("endpoint_url") == "http://s3-local"
            assert "endpoint_url" not in second

    def test_session_import_error_wrapped(self) -> None:
        p = AWSPlatform()
        with patch.dict("sys.modules", {"boto3": None}):
            with pytest.raises(PlatformError, match="boto3 is not available"):
                _ = p._session

    def test_delete_folder_non_recursive_calls_delete_object(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        platform.delete_folder("data/folder", recursive=False)
        mock_client.delete_object.assert_called_once()

    def test_delete_folder_non_recursive_with_trailing_slash(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        platform.delete_folder("data/folder/", recursive=False)
        mock_client.delete_object.assert_called_once_with(Bucket="test-bucket", Key="data/folder/")

    def test_delete_folder_recursive_with_no_contents(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": []}]
        mock_client.get_paginator.return_value = paginator
        platform.delete_folder("data/folder", recursive=True)
        mock_client.delete_objects.assert_not_called()

    def test_delete_folder_recursive_outer_exception_wrapped(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.get_paginator.side_effect = RuntimeError("boom")
        with pytest.raises(PlatformError, match="Failed to delete folder"):
            platform.delete_folder("data/folder", recursive=True)

    def test_list_files_skips_folder_marker_and_empty_name(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        paginator = MagicMock()
        paginator.paginate.return_value = [{
            "Contents": [
                {"Key": "data/", "Size": 0, "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc)},
                {"Key": "data/a.txt", "Size": 1, "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc)},
            ]
        }]
        mock_client.get_paginator.return_value = paginator
        result = platform.list_files("data")
        assert [f.name for f in result] == ["a.txt"]

    def test_list_files_error_wrapped(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.get_paginator.side_effect = RuntimeError("boom")
        with pytest.raises(PlatformError, match="Failed to list files"):
            platform.list_files("data")

    def test_list_files_with_root_prefix_key_empty(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        paginator = MagicMock()
        paginator.paginate.return_value = [{
            "Contents": [
                {"Key": "a.txt", "Size": 1, "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc)},
            ]
        }]
        mock_client.get_paginator.return_value = paginator
        files = platform.list_files("")
        assert len(files) == 1
        assert files[0].name == "a.txt"

    def test_list_files_reraises_platform_error(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.get_paginator.side_effect = PlatformError("boom")
        with pytest.raises(PlatformError, match="boom"):
            platform.list_files("data")

    def test_list_folders_recursive_derives_intermediate_paths(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        paginator = MagicMock()
        paginator.paginate.return_value = [{
            "Contents": [{"Key": "data/a/b/file.txt"}]
        }]
        mock_client.get_paginator.return_value = paginator
        folders = platform.list_folders("data", recursive=True)
        assert "s3://test-bucket/data/a/" in folders
        assert "s3://test-bucket/data/a/b/" in folders

    def test_list_folders_error_wrapped(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.get_paginator.side_effect = RuntimeError("boom")
        with pytest.raises(PlatformError, match="Failed to list folders"):
            platform.list_folders("data")

    def test_list_folders_reraises_platform_error(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.get_paginator.side_effect = PlatformError("boom")
        with pytest.raises(PlatformError, match="boom"):
            platform.list_folders("data")

    def test_list_folders_with_empty_key_prefix(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        paginator = MagicMock()
        paginator.paginate.return_value = [{"CommonPrefixes": [{"Prefix": "a/"}]}]
        mock_client.get_paginator.return_value = paginator
        result = platform.list_folders("")
        assert result == ["s3://test-bucket/a/"]

    def test_file_folder_exists_false_when_bucket_missing(self) -> None:
        p = AWSPlatform(bucket="")
        p._client = MagicMock()
        assert p.file_exists("x") is False
        assert p.folder_exists("x") is False

    def test_upload_conflict_and_error(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.return_value = {}
        with pytest.raises(PlatformError, match="already exists"):
            platform.upload_file("/tmp/a", "data/a")

        mock_client.head_object.side_effect = Exception("not found")
        mock_client.upload_file.side_effect = RuntimeError("boom")
        with pytest.raises(PlatformError, match="Failed to upload"):
            platform.upload_file("/tmp/a", "data/a")

    def test_copy_conflict_and_error(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.return_value = {}
        with pytest.raises(PlatformError, match="already exists"):
            platform.copy_file("data/src", "data/dst")

        mock_client.head_object.side_effect = Exception("not found")
        mock_client.copy_object.side_effect = RuntimeError("boom")
        with pytest.raises(PlatformError, match="Failed to copy"):
            platform.copy_file("data/src", "data/dst")

    def test_get_file_info_error_wrapped(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.side_effect = RuntimeError("boom")
        with pytest.raises(PlatformError, match="Failed to get file info"):
            platform.get_file_info("data/x")

    def test_fetch_secret_platform_error_is_rewrapped(self) -> None:
        p = AWSPlatform(region="us-east-1")
        with patch.object(AWSPlatform, "boto3_client", side_effect=PlatformError("boto3 not installed")):
            with pytest.raises(Exception, match="boto3 not installed"):
                p._fetch_secret("key", "source")

    def test_fetch_secret_json_non_dict_returns_plain(self) -> None:
        with patch("boto3.Session") as mock_session_cls:
            sess = MagicMock()
            mock_session_cls.return_value = sess
            sm = MagicMock()
            sm.get_secret_value.return_value = {"SecretString": "[1,2,3]"}
            sess.client.return_value = sm
            p = AWSPlatform(region="us-east-1")
            assert p._fetch_secret("x", "secret-id") == "[1,2,3]"

    def test_session_uses_profile_name_when_configured(self) -> None:
        with patch("boto3.Session") as mock_session_cls:
            mock_session_cls.return_value = MagicMock()
            p = AWSPlatform(region="us-east-1", profile="dev")
            _ = p._session
            mock_session_cls.assert_called_once_with(region_name="us-east-1", profile_name="dev")

    def test_s3_property_initializes_client_once(self) -> None:
        p = AWSPlatform(bucket="b")
        with patch.object(AWSPlatform, "boto3_client", return_value=MagicMock()) as mk:
            _ = p.s3
            _ = p.s3
            mk.assert_called_once_with("s3")

    def test_write_append_create_error_wrapped(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.side_effect = Exception("missing")
        mock_client.put_object.side_effect = RuntimeError("boom")
        with pytest.raises(PlatformError, match="Failed to write file"):
            platform.write_file("data/f.txt", "x")

        mock_client.put_object.side_effect = RuntimeError("boom")
        with pytest.raises(PlatformError, match="Failed to append"):
            platform.append_file("data/f.txt", "x")

        mock_client.put_object.side_effect = RuntimeError("boom")
        with pytest.raises(PlatformError, match="Failed to create folder"):
            platform.create_folder("data/folder")


# =====================================================================
# Athena / Glue Catalog
# =====================================================================


class TestExecuteAthenaDDL:
    """Tests for AWSPlatform.execute_athena_ddl."""

    def _make_platform(self) -> tuple[AWSPlatform, MagicMock]:
        p = AWSPlatform(bucket="test-bucket", region="us-east-1")
        mock_athena = MagicMock()
        p.boto3_client = MagicMock(return_value=mock_athena)
        return p, mock_athena

    def test_requires_output_location(self) -> None:
        p, _ = self._make_platform()
        with pytest.raises(PlatformError, match="athena_output_location is required"):
            p.execute_athena_ddl("CREATE TABLE foo", output_location="")

    def test_succeeds_on_first_poll(self) -> None:
        p, athena = self._make_platform()
        athena.start_query_execution.return_value = {"QueryExecutionId": "q-123"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        qid = p.execute_athena_ddl(
            "CREATE TABLE foo", output_location="s3://bucket/results/"
        )
        assert qid == "q-123"
        athena.start_query_execution.assert_called_once()
        call_kwargs = athena.start_query_execution.call_args[1]
        assert call_kwargs["QueryString"] == "CREATE TABLE foo"
        assert call_kwargs["ResultConfiguration"]["OutputLocation"] == "s3://bucket/results/"

    def test_passes_database_context(self) -> None:
        p, athena = self._make_platform()
        athena.start_query_execution.return_value = {"QueryExecutionId": "q-456"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        p.execute_athena_ddl(
            "SELECT 1", database="mydb", output_location="s3://b/r/"
        )
        call_kwargs = athena.start_query_execution.call_args[1]
        assert call_kwargs["QueryExecutionContext"]["Database"] == "mydb"

    def test_raises_on_failed(self) -> None:
        p, athena = self._make_platform()
        athena.start_query_execution.return_value = {"QueryExecutionId": "q-fail"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "FAILED", "StateChangeReason": "syntax error"}
            }
        }
        with pytest.raises(PlatformError, match="FAILED.*syntax error"):
            p.execute_athena_ddl("BAD SQL", output_location="s3://b/r/")

    def test_raises_on_cancelled(self) -> None:
        p, athena = self._make_platform()
        athena.start_query_execution.return_value = {"QueryExecutionId": "q-cancel"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "CANCELLED", "StateChangeReason": "user cancelled"}
            }
        }
        with pytest.raises(PlatformError, match="CANCELLED"):
            p.execute_athena_ddl("SELECT 1", output_location="s3://b/r/")

    def test_raises_on_start_failure(self) -> None:
        p, athena = self._make_platform()
        athena.start_query_execution.side_effect = RuntimeError("network error")
        with pytest.raises(PlatformError, match="Failed to start Athena query"):
            p.execute_athena_ddl("SELECT 1", output_location="s3://b/r/")

    def test_raises_on_poll_failure(self) -> None:
        p, athena = self._make_platform()
        athena.start_query_execution.return_value = {"QueryExecutionId": "q-poll-err"}
        athena.get_query_execution.side_effect = RuntimeError("api error")
        with pytest.raises(PlatformError, match="Failed to poll Athena"):
            p.execute_athena_ddl("SELECT 1", output_location="s3://b/r/")


class TestRegisterDeltaTable:
    """Tests for AWSPlatform.register_delta_table."""

    def test_calls_execute_athena_ddl_with_drop_and_create(self) -> None:
        p = AWSPlatform(bucket="b", region="us-east-1")
        p.execute_athena_ddl = MagicMock(return_value="q-1")
        p.register_delta_table(
            "my_table", "s3://bucket/data/my_table",
            database="mydb", output_location="s3://b/r/",
        )
        # DROP + CREATE = 2 calls
        assert p.execute_athena_ddl.call_count == 2

        drop_sql = p.execute_athena_ddl.call_args_list[0][0][0]
        assert "DROP TABLE IF EXISTS" in drop_sql
        assert "`my_table`" in drop_sql

        create_sql = p.execute_athena_ddl.call_args_list[1][0][0]
        assert "CREATE EXTERNAL TABLE IF NOT EXISTS" in create_sql
        assert "`my_table`" in create_sql
        assert "LOCATION 's3://bucket/data/my_table'" in create_sql
        assert "'table_type'='DELTA'" in create_sql
        assert p.execute_athena_ddl.call_args_list[1][1]["database"] == "mydb"


class TestRegisterSymlinkTable:
    """Tests for AWSPlatform.register_symlink_table."""

    def test_creates_symlink_table_without_partitions(self) -> None:
        p = AWSPlatform(bucket="b", region="us-east-1")
        p.execute_athena_ddl = MagicMock(return_value="q-1")
        p.register_symlink_table(
            "my_table", "s3://bucket/data/my_table",
            database="symlink_mydb", output_location="s3://b/r/",
            schema_ddl="`id` INT, `name` STRING",
        )
        # Should call DROP then CREATE (2 calls)
        assert p.execute_athena_ddl.call_count == 2
        drop_sql = p.execute_athena_ddl.call_args_list[0][0][0]
        create_sql = p.execute_athena_ddl.call_args_list[1][0][0]
        assert "DROP TABLE IF EXISTS" in drop_sql
        assert "SymlinkTextInputFormat" in create_sql
        assert "ParquetHiveSerDe" in create_sql
        assert "_symlink_format_manifest/" in create_sql

    def test_creates_symlink_table_with_partitions(self) -> None:
        p = AWSPlatform(bucket="b", region="us-east-1")
        p.execute_athena_ddl = MagicMock(return_value="q-1")
        p.register_symlink_table(
            "my_table", "s3://bucket/data/my_table",
            database="symlink_mydb", output_location="s3://b/r/",
            schema_ddl="`id` INT, `name` STRING",
            partition_ddl="PARTITIONED BY (`year` STRING)",
        )
        # DROP + CREATE + MSCK REPAIR = 3 calls
        assert p.execute_athena_ddl.call_count == 3
        repair_sql = p.execute_athena_ddl.call_args_list[2][0][0]
        assert "MSCK REPAIR TABLE" in repair_sql

    def test_delete_file_exception_is_idempotent(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.delete_object.side_effect = RuntimeError("boom")
        platform.delete_file("data/f.txt")

    def test_delete_folder_recursive_platform_error_reraised(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "data/a.txt"}]}]
        mock_client.get_paginator.return_value = paginator
        mock_client.delete_objects.return_value = {"Errors": [{"Key": "data/a.txt"}]}
        with pytest.raises(PlatformError, match="Failed to delete"):
            platform.delete_folder("data", recursive=True)

    def test_list_files_recursive_keeps_nested_items(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        paginator = MagicMock()
        paginator.paginate.return_value = [{
            "Contents": [
                {"Key": "data/x/y.txt", "Size": 2, "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc)},
            ]
        }]
        mock_client.get_paginator.return_value = paginator
        files = platform.list_files("data", recursive=True)
        assert len(files) == 1
        assert files[0].name == "y.txt"

    def test_list_folders_recursive_error_wrapped(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        paginator = MagicMock()
        paginator.paginate.side_effect = RuntimeError("boom")
        mock_client.get_paginator.return_value = paginator
        with pytest.raises(PlatformError, match="Failed to list folders"):
            platform.list_folders("data", recursive=True)

    def test_folder_exists_with_trailing_slash_path(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.list_objects_v2.return_value = {"KeyCount": 1}
        assert platform.folder_exists("data/folder/") is True

    def test_folder_exists_exception_returns_false(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.list_objects_v2.side_effect = RuntimeError("boom")
        assert platform.folder_exists("data/folder") is False

    def test_copy_download_failures_wrapped(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        mock_client.head_object.side_effect = Exception("missing")
        mock_client.copy_object.side_effect = RuntimeError("boom")
        with pytest.raises(PlatformError, match="Failed to copy"):
            platform.copy_file("data/src.txt", "data/dst.txt")

        mock_client.download_file.side_effect = RuntimeError("boom")
        with pytest.raises(PlatformError, match="Failed to download"):
            platform.download_file("data/src.txt", "/tmp/out.txt")

    def test_create_folder_keeps_trailing_slash(self, platform: AWSPlatform, mock_client: MagicMock) -> None:
        platform.create_folder("data/folder/")
        mock_client.put_object.assert_called_once_with(Bucket="test-bucket", Key="data/folder/", Body=b"")

    def test_fetch_secret_with_empty_payload_returns_empty_string(self) -> None:
        with patch("boto3.Session") as mock_session_cls:
            sess = MagicMock()
            mock_session_cls.return_value = sess
            sm = MagicMock()
            sm.get_secret_value.return_value = {}
            sess.client.return_value = sm
            p = AWSPlatform(region="us-east-1")
            assert p._fetch_secret("x", "secret-id") == ""

    def test_fetch_secret_platform_error_propagates(self) -> None:
        p = AWSPlatform(region="us-east-1")
        with patch.object(AWSPlatform, "boto3_client", side_effect=PlatformError("boto3 missing")):
            with pytest.raises(PlatformError, match="boto3 missing"):
                p._fetch_secret("k", "src")
