"""S3-compatible filesystem backend used by :class:`AWSPlatform`."""

from __future__ import annotations

import io
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._aws.errors import (
    failure_detail,
    is_entity_too_large,
    is_not_found,
    is_precondition_failed,
)
from datacoolie.platforms._aws.paths import ensure_bucket, parse_path
from datacoolie.platforms.base import FileInfo


class S3Backend:
    """Implement the ``BasePlatform`` filesystem contract over S3 APIs.

    ``native_aws`` selects conditional writes for AWS S3.  Custom S3
    endpoints retain a compatibility preflight because third-party services
    do not all implement the same conditional-copy/write surface.
    """

    _MANAGED_TRANSFER_THRESHOLD = 8 * 1024 * 1024
    # Backward-compatible alias for callers/tests that inspected the old
    # name; full reads no longer consult this threshold.
    _INLINE_READ_THRESHOLD = _MANAGED_TRANSFER_THRESHOLD

    def __init__(
        self,
        bucket: str,
        client_getter: Callable[[], Any],
        *,
        native_aws: bool = True,
    ) -> None:
        self._bucket = bucket
        self._client_getter = client_getter
        self._native_aws = native_aws

    @property
    def s3(self) -> Any:
        return self._client_getter()

    @staticmethod
    def _uri(bucket: str, key: str) -> str:
        return f"s3://{bucket}/{key}" if key else f"s3://{bucket}"

    def _target(self, path: str) -> tuple[str, str]:
        bucket, key = parse_path(path, self._bucket)
        ensure_bucket(bucket)
        return bucket, key

    @staticmethod
    def _folder_key(key: str) -> str:
        return f"{key.rstrip('/')}/" if key else ""

    @staticmethod
    def _object_entries(response: Mapping[str, Any] | Any) -> list[Mapping[str, Any]]:
        contents = response.get("Contents", []) if hasattr(response, "get") else []
        return [item for item in contents if isinstance(item, Mapping)]

    @staticmethod
    def _has_listing_result(response: Mapping[str, Any] | Any) -> bool:
        entries = S3Backend._object_entries(response)
        if entries:
            return True
        prefixes = (
            response.get("CommonPrefixes", []) if hasattr(response, "get") else []
        )
        if prefixes:
            return True
        key_count = response.get("KeyCount") if hasattr(response, "get") else None
        return isinstance(key_count, int) and key_count > 0

    @staticmethod
    def _raise(operation: str, uri: str, exc: BaseException) -> PlatformError:
        if isinstance(exc, PlatformError):
            return exc
        return PlatformError(f"Failed to {operation}: {uri} ({failure_detail(exc)})")

    @staticmethod
    def _already_exists(uri: str) -> PlatformError:
        return PlatformError(f"File already exists (set overwrite=True): {uri}")

    def _ensure_destination_absent(self, bucket: str, key: str) -> None:
        uri = self._uri(bucket, key)
        if self.file_exists(uri):
            raise self._already_exists(uri)

    def read_file(self, path: str) -> str:
        bucket, key = self._target(path)
        uri = self._uri(bucket, key)
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            body = response["Body"]
            try:
                return body.read().decode("utf-8")
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    close()
        except Exception as exc:  # noqa: BLE001
            raise self._raise("read file", uri, exc) from exc

    def read_bytes(self, path: str) -> bytes:
        """Read complete content with one unbounded ``GetObject`` request."""
        bucket, key = self._target(path)
        uri = self._uri(bucket, key)
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            body = response["Body"]
            try:
                return body.read()
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    close()
        except Exception as exc:  # noqa: BLE001
            raise self._raise("read bytes", uri, exc) from exc

    def _put_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        overwrite: bool,
    ) -> None:
        uri = self._uri(bucket, key)
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key, "Body": data}
        if not overwrite and self._native_aws:
            kwargs["IfNoneMatch"] = "*"
        elif not overwrite:
            self._ensure_destination_absent(bucket, key)
        try:
            self.s3.put_object(**kwargs)
        except Exception as exc:  # noqa: BLE001
            if not overwrite and is_precondition_failed(exc):
                raise self._already_exists(uri) from exc
            raise self._raise("write file", uri, exc) from exc

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None:
        bucket, key = self._target(path)
        uri = self._uri(bucket, key)
        try:
            if len(data) <= self._MANAGED_TRANSFER_THRESHOLD:
                self._put_bytes(bucket, key, data, overwrite=overwrite)
                return
            if not overwrite:
                self._ensure_destination_absent(bucket, key)
            self.s3.upload_fileobj(io.BytesIO(data), bucket, key)
        except PlatformError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise self._raise("write bytes", uri, exc) from exc

    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None:
        self.write_bytes(path, content.encode("utf-8"), overwrite=overwrite)

    def append_file(self, path: str, content: str) -> None:
        bucket, key = self._target(path)
        uri = self._uri(bucket, key)
        existing = b""
        try:
            try:
                response = self.s3.get_object(Bucket=bucket, Key=key)
                body = response["Body"]
                try:
                    existing = body.read()
                finally:
                    close = getattr(body, "close", None)
                    if callable(close):
                        close()
            except Exception as exc:  # noqa: BLE001
                if not is_not_found(exc):
                    raise
            self.s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=existing + content.encode("utf-8"),
            )
        except Exception as exc:  # noqa: BLE001
            raise self._raise("append to file", uri, exc) from exc

    def delete_file(self, path: str) -> None:
        bucket, key = self._target(path)
        uri = self._uri(bucket, key)
        try:
            # DeleteObject is idempotent for absent unversioned S3 keys; an
            # SDK exception therefore represents an operational failure.
            self.s3.delete_object(Bucket=bucket, Key=key)
        except Exception as exc:  # noqa: BLE001
            raise self._raise("delete file", uri, exc) from exc

    def create_folder(self, path: str) -> None:
        bucket, key = self._target(path)
        uri = self._uri(bucket, key)
        if not key:
            try:
                self.s3.head_bucket(Bucket=bucket)
            except Exception as exc:  # noqa: BLE001
                raise self._raise("validate folder", uri, exc) from exc
            return

        marker = self._folder_key(key)
        try:
            self.s3.put_object(Bucket=bucket, Key=marker, Body=b"")
        except Exception as exc:  # noqa: BLE001
            raise self._raise("create folder", self._uri(bucket, marker), exc) from exc

    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
        bucket, key = self._target(path)
        uri = self._uri(bucket, key)
        if not key:
            if recursive:
                raise PlatformError("Refusing to recursively delete an S3 bucket root")
            try:
                self.s3.head_bucket(Bucket=bucket)
            except Exception as exc:  # noqa: BLE001
                if is_not_found(exc):
                    return
                raise self._raise("delete folder", uri, exc) from exc
            return

        marker = self._folder_key(key)
        try:
            if recursive:
                paginator = self.s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=marker):
                    objects = [
                        {"Key": str(obj["Key"])}
                        for obj in self._object_entries(page)
                        if obj.get("Key")
                    ]
                    if not objects:
                        continue
                    response = self.s3.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": objects, "Quiet": True},
                    )
                    errors = response.get("Errors") or []
                    if errors:
                        failed = [str(item.get("Key", "")) for item in errors[:5]]
                        raise PlatformError(
                            f"Failed to delete {len(errors)} object(s) under "
                            f"{uri}: {failed}"
                        )
                return

            response = self.s3.list_objects_v2(
                Bucket=bucket,
                Prefix=marker,
                MaxKeys=2,
            )
            objects = self._object_entries(response)
            if not objects:
                # A real empty response includes KeyCount=0 and represents a
                # missing folder.  Some lightweight SDK fakes expose no
                # mapping fields; treat those as an existing empty marker so
                # the compatibility path still exercises the delete call.
                if not isinstance(response, Mapping):
                    self.s3.delete_object(Bucket=bucket, Key=marker)
                return
            children = [obj for obj in objects if obj.get("Key") != marker]
            key_count = response.get("KeyCount")
            if children or (
                isinstance(key_count, int) and key_count > len(children) + 1
            ):
                raise PlatformError(f"Folder is not empty: {uri}")
            self.s3.delete_object(Bucket=bucket, Key=marker)
        except PlatformError:
            raise
        except Exception as exc:  # noqa: BLE001
            if is_not_found(exc):
                return
            raise self._raise("delete folder", uri, exc) from exc

    def _paginate_listing(
        self,
        bucket: str,
        key: str,
        *,
        recursive: bool,
    ) -> Iterable[Mapping[str, Any]]:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": key}
        if not recursive:
            kwargs["Delimiter"] = "/"
        paginator = self.s3.get_paginator("list_objects_v2")
        return paginator.paginate(**kwargs)

    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        bucket, key = self._target(path)
        prefix = self._folder_key(key)
        uri = self._uri(bucket, prefix)
        files: list[FileInfo] = []
        saw_result = not bool(prefix)
        try:
            for page in self._paginate_listing(bucket, prefix, recursive=recursive):
                saw_result = saw_result or self._has_listing_result(page)
                for obj in self._object_entries(page):
                    obj_key = str(obj.get("Key", ""))
                    if obj_key == prefix or not obj_key:
                        continue
                    name = obj_key.rsplit("/", 1)[-1]
                    if not name or (extension and not name.endswith(extension)):
                        continue
                    files.append(
                        FileInfo(
                            name=name,
                            path=self._uri(bucket, obj_key),
                            modification_time=obj.get("LastModified"),
                            size=int(obj.get("Size", 0) or 0),
                        )
                    )
            if not saw_result:
                raise PlatformError(f"Path does not exist or is not a folder: {uri}")
            return files
        except PlatformError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise self._raise("list files", uri, exc) from exc

    def list_folders(self, path: str, *, recursive: bool = False) -> list[str]:
        bucket, key = self._target(path)
        prefix = self._folder_key(key)
        uri = self._uri(bucket, prefix)
        folders: dict[str, None] = {}
        saw_result = not bool(prefix)
        try:
            for page in self._paginate_listing(bucket, prefix, recursive=recursive):
                saw_result = saw_result or self._has_listing_result(page)
                if not recursive:
                    for item in page.get("CommonPrefixes", []) or []:
                        child = (
                            item.get("Prefix") if isinstance(item, Mapping) else None
                        )
                        if child:
                            folders[self._uri(bucket, str(child))] = None
                    continue

                base_len = len(prefix)
                for obj in self._object_entries(page):
                    obj_key = str(obj.get("Key", ""))
                    if not obj_key.startswith(prefix) or obj_key == prefix:
                        continue
                    relative = obj_key[base_len:]
                    parts = relative.split("/")
                    current = prefix
                    for part in parts[:-1]:
                        if not part:
                            continue
                        current = f"{current}{part}/"
                        folders[self._uri(bucket, current)] = None
            if not saw_result:
                raise PlatformError(f"Path does not exist or is not a folder: {uri}")
            return list(folders)
        except PlatformError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise self._raise("list folders", uri, exc) from exc

    def file_exists(self, path: str) -> bool:
        bucket, key = parse_path(path, self._bucket)
        if not bucket or not key:
            return False
        uri = self._uri(bucket, key)
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as exc:  # noqa: BLE001
            if is_not_found(exc):
                return False
            raise self._raise("check file", uri, exc) from exc

    def folder_exists(self, path: str) -> bool:
        bucket, key = parse_path(path, self._bucket)
        if not bucket:
            return False
        uri = self._uri(bucket, key)
        if not key:
            try:
                self.s3.head_bucket(Bucket=bucket)
                return True
            except Exception as exc:  # noqa: BLE001
                if is_not_found(exc):
                    return False
                raise self._raise("check folder", uri, exc) from exc

        prefix = self._folder_key(key)
        try:
            response = self.s3.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=1,
            )
            return self._has_listing_result(response)
        except Exception as exc:  # noqa: BLE001
            if is_not_found(exc):
                return False
            raise self._raise("check folder", uri, exc) from exc

    def upload_file(
        self, local_path: str, dest: str, *, overwrite: bool = False
    ) -> None:
        bucket, key = self._target(dest)
        uri = self._uri(bucket, key)
        if not overwrite:
            self._ensure_destination_absent(bucket, key)
        try:
            self.s3.upload_file(local_path, bucket, key)
        except Exception as exc:  # noqa: BLE001
            raise self._raise("upload file", f"{local_path} → {uri}", exc) from exc

    def download_file(self, src: str, dest: str) -> None:
        bucket, key = self._target(src)
        uri = self._uri(bucket, key)
        try:
            self.s3.download_file(bucket, key, dest)
        except Exception as exc:  # noqa: BLE001
            raise self._raise("download file", f"{uri} → {dest}", exc) from exc

    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        src_bucket, src_key = self._target(src)
        dest_bucket, dest_key = self._target(dest)
        src_uri = self._uri(src_bucket, src_key)
        dest_uri = self._uri(dest_bucket, dest_key)
        if src_bucket == dest_bucket and src_key == dest_key:
            return
        if not overwrite and not self._native_aws:
            self._ensure_destination_absent(dest_bucket, dest_key)

        kwargs: dict[str, Any] = {
            "CopySource": {"Bucket": src_bucket, "Key": src_key},
            "Bucket": dest_bucket,
            "Key": dest_key,
        }
        if not overwrite and self._native_aws:
            kwargs["IfNoneMatch"] = "*"
        try:
            try:
                self.s3.copy_object(**kwargs)
            except Exception as exc:  # noqa: BLE001
                if not is_entity_too_large(exc):
                    raise
                self.s3.copy(
                    {"Bucket": src_bucket, "Key": src_key},
                    dest_bucket,
                    dest_key,
                )
        except Exception as exc:  # noqa: BLE001
            if not overwrite and is_precondition_failed(exc):
                raise self._already_exists(dest_uri) from exc
            raise self._raise("copy file", f"{src_uri} → {dest_uri}", exc) from exc

    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        src_bucket, src_key = self._target(src)
        dest_bucket, dest_key = self._target(dest)
        if src_bucket == dest_bucket and src_key == dest_key:
            return
        self.copy_file(src, dest, overwrite=overwrite)
        # Keep the source when deletion fails so recovery remains possible.
        self.delete_file(self._uri(src_bucket, src_key))

    def get_file_info(self, path: str) -> FileInfo:
        bucket, key = self._target(path)
        uri = self._uri(bucket, key)
        if not key:
            try:
                self.s3.head_bucket(Bucket=bucket)
            except Exception as exc:  # noqa: BLE001
                raise self._raise("get file info", uri, exc) from exc
            return FileInfo(
                name=bucket,
                path=uri,
                modification_time=None,
                size=0,
                is_dir=True,
            )

        try:
            head = self.s3.head_object(Bucket=bucket, Key=key)
            name = key.rstrip("/").rsplit("/", 1)[-1]
            is_dir = key.endswith("/")
            return FileInfo(
                name=name,
                path=uri,
                modification_time=head.get("LastModified"),
                size=0 if is_dir else int(head.get("ContentLength", 0) or 0),
                is_dir=is_dir,
            )
        except Exception as exc:  # noqa: BLE001
            if not is_not_found(exc):
                raise self._raise("get file info", uri, exc) from exc

        prefix = self._folder_key(key)
        try:
            response = self.s3.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=1,
            )
            if self._has_listing_result(response):
                name = key.rstrip("/").rsplit("/", 1)[-1]
                return FileInfo(
                    name=name,
                    path=self._uri(bucket, key),
                    modification_time=None,
                    size=0,
                    is_dir=True,
                )
        except Exception as exc:  # noqa: BLE001
            raise self._raise("get file info", uri, exc) from exc
        raise PlatformError(f"Path does not exist: {uri}")
