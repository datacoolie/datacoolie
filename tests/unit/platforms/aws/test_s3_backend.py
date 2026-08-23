from __future__ import annotations

from unittest.mock import MagicMock

from botocore.exceptions import ClientError
import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._aws.s3_backend import S3Backend


def test_s3_backend_reads_small_bytes_without_temp_file() -> None:
    client = MagicMock()
    client.head_object.return_value = {"ContentLength": 3}
    client.get_object.return_value = {"Body": MagicMock()}
    client.get_object.return_value["Body"].read.return_value = b"abc"
    backend = S3Backend("bucket", lambda: client)

    assert backend.read_bytes("s3://bucket/file.bin") == b"abc"
    client.download_fileobj.assert_not_called()


def test_s3_backend_keeps_source_when_copy_fails() -> None:
    client = MagicMock()
    client.head_object.side_effect = Exception("missing")
    client.copy_object.side_effect = Exception("copy failed")
    backend = S3Backend("bucket", lambda: client)

    with pytest.raises(PlatformError, match="Failed to copy"):
        backend.move_file("source.txt", "destination.txt")

    client.delete_object.assert_not_called()


def _client_error(code: str, status: int) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "operation",
    )


def test_small_read_is_one_get_without_head() -> None:
    client = MagicMock()
    body = MagicMock()
    body.read.return_value = b"payload"
    client.get_object.return_value = {"Body": body}
    backend = S3Backend("bucket", lambda: client)

    assert backend.read_bytes("key") == b"payload"
    client.head_object.assert_not_called()
    client.get_object.assert_called_once_with(Bucket="bucket", Key="key")


def test_aws_small_create_uses_conditional_put() -> None:
    client = MagicMock()
    backend = S3Backend("bucket", lambda: client, native_aws=True)

    backend.write_file("key", "value")

    client.head_object.assert_not_called()
    assert client.put_object.call_args.kwargs["IfNoneMatch"] == "*"


def test_custom_endpoint_keeps_exact_preflight() -> None:
    client = MagicMock()
    client.head_object.side_effect = _client_error("NotFound", 404)
    backend = S3Backend("bucket", lambda: client, native_aws=False)

    backend.write_file("key", "value")

    client.head_object.assert_called_once_with(Bucket="bucket", Key="key")
    assert "IfNoneMatch" not in client.put_object.call_args.kwargs


def test_conditional_put_conflict_is_destination_error() -> None:
    client = MagicMock()
    client.put_object.side_effect = _client_error("PreconditionFailed", 412)
    backend = S3Backend("bucket", lambda: client, native_aws=True)

    with pytest.raises(PlatformError, match="already exists"):
        backend.write_file("key", "value")


def test_append_existing_is_get_then_put_without_head() -> None:
    client = MagicMock()
    body = MagicMock()
    body.read.return_value = b"old"
    client.get_object.return_value = {"Body": body}
    backend = S3Backend("bucket", lambda: client)

    backend.append_file("key", "new")

    client.head_object.assert_not_called()
    client.get_object.assert_called_once_with(Bucket="bucket", Key="key")
    assert client.put_object.call_args.kwargs["Body"] == b"oldnew"


def test_nonrecursive_delete_preserves_nonempty_folder() -> None:
    client = MagicMock()
    client.list_objects_v2.return_value = {
        "KeyCount": 2,
        "Contents": [{"Key": "folder/"}, {"Key": "folder/file"}],
    }
    backend = S3Backend("bucket", lambda: client)

    with pytest.raises(PlatformError, match="not empty"):
        backend.delete_folder("folder")
    client.delete_object.assert_not_called()


def test_recursive_bucket_root_delete_is_rejected() -> None:
    client = MagicMock()
    backend = S3Backend("bucket", lambda: client)

    with pytest.raises(PlatformError, match="bucket root"):
        backend.delete_folder("s3://bucket", recursive=True)
    client.assert_not_called()


def test_folder_file_info_is_inferred_from_descendant() -> None:
    client = MagicMock()
    client.head_object.side_effect = _client_error("NotFound", 404)
    client.list_objects_v2.return_value = {
        "KeyCount": 1,
        "Contents": [{"Key": "folder/file.txt", "Size": 1}],
    }
    backend = S3Backend("bucket", lambda: client)

    info = backend.get_file_info("folder")
    assert info.is_dir is True
    assert info.name == "folder"
    assert info.size == 0


def test_same_path_copy_and_move_do_not_issue_requests() -> None:
    client = MagicMock()
    backend = S3Backend("bucket", lambda: client)

    backend.copy_file("s3://bucket/folder/key", "folder/key")
    backend.move_file("folder/key", "s3://bucket/folder/key")

    client.assert_not_called()


def test_entity_too_large_falls_back_to_managed_copy() -> None:
    client = MagicMock()
    client.copy_object.side_effect = _client_error("EntityTooLarge", 400)
    backend = S3Backend("bucket", lambda: client)

    backend.copy_file("source", "destination")

    client.copy.assert_called_once_with(
        {"Bucket": "bucket", "Key": "source"},
        "bucket",
        "destination",
    )
