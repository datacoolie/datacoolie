"""AWS-first platform facade backed by boto3 and S3-compatible storage.

The implementation is split into private components under ``platforms._aws``.
The public class remains stable for AWS S3, while ``endpoint_url`` allows the
same S3 filesystem contract to be used with MinIO and LocalStack.
"""

from __future__ import annotations

import threading
from typing import Any

from datacoolie.platforms._aws.catalog import CatalogBackend
from datacoolie.platforms._aws.paths import ensure_bucket, parse_path
from datacoolie.platforms._aws.s3_backend import S3Backend
from datacoolie.platforms._aws.secrets import SecretsBackend
from datacoolie.platforms._aws.session import create_client, create_session
from datacoolie.platforms.base import BasePlatform, FileInfo


class AWSPlatform(BasePlatform):
    """Platform backed by AWS S3 via ``boto3``.

    The storage portion also works with S3-compatible endpoints such as MinIO
    when ``endpoint_url`` is supplied. Secrets Manager, Glue, and Athena remain
    AWS service integrations and use their normal AWS endpoints by default.

    Args:
        bucket: Default S3 bucket name.
        region: AWS region for S3 and AWS service operations.
        profile: Named AWS profile from ``~/.aws/credentials``.
        endpoint_url: Custom S3 endpoint for MinIO, LocalStack, or similar.
        cache_ttl: Secret cache time-to-live in seconds (default 300). Pass ``0``
            to disable caching.
    """

    def __init__(
        self,
        bucket: str = "",
        region: str | None = None,
        profile: str | None = None,
        endpoint_url: str | None = None,
        cache_ttl: int = 300,
        **kwargs: Any,
    ) -> None:
        super().__init__(cache_ttl=cache_ttl)
        self._bucket = bucket
        self._region = region
        self._profile = profile
        self._endpoint_url = endpoint_url
        self._client: Any | None = None
        self._boto3_session: Any | None = None
        self._session_lock = threading.RLock()
        self._client_lock = threading.RLock()
        self._service_clients: dict[tuple[str, tuple[tuple[str, str], ...]], Any] = {}
        self._s3_backend: S3Backend | None = None
        self._secrets_backend: SecretsBackend | None = None
        self._catalog_backend: CatalogBackend | None = None

    # ------------------------------------------------------------------
    # Internal component wiring
    # ------------------------------------------------------------------

    def _get_s3_backend(self) -> S3Backend:
        if self._s3_backend is None:
            self._s3_backend = S3Backend(
                self._bucket,
                lambda: self.s3,
                native_aws=not bool(self._endpoint_url),
            )
        return self._s3_backend

    def _get_secrets_backend(self) -> SecretsBackend:
        if self._secrets_backend is None:
            self._secrets_backend = SecretsBackend(
                lambda service, **kwargs: self._cached_client(service, **kwargs),
                cache_ttl=self._cache_ttl,
            )
        return self._secrets_backend

    def _get_catalog_backend(self) -> CatalogBackend:
        if self._catalog_backend is None:
            self._catalog_backend = CatalogBackend(
                lambda service, **kwargs: self._cached_client(service, **kwargs),
                delete_table=lambda database, table_name: self.delete_glue_table(
                    database, table_name
                ),
                execute_ddl=lambda sql, **kwargs: self.execute_athena_ddl(
                    sql, **kwargs
                ),
                repair_partitions=lambda table_name, **kwargs: (
                    self.repair_table_partitions(table_name, **kwargs)
                ),
            )
        return self._catalog_backend

    # ------------------------------------------------------------------
    # Secrets
    # ------------------------------------------------------------------

    def _fetch_secret(self, key: str, source: str) -> str:
        return self._get_secrets_backend().fetch_secret(key, source)

    # ------------------------------------------------------------------
    # SDK access
    # ------------------------------------------------------------------

    def boto3_client(self, service: str, **kwargs: Any) -> Any:
        """Create a boto3 service client using the platform's credentials.

        The platform ``endpoint_url`` is a storage override and is injected
        automatically only for S3. Callers may pass an explicit endpoint for any
        service through ``kwargs``.
        """
        with self._client_lock:
            return create_client(
                self._session,
                service,
                storage_endpoint_url=self._endpoint_url,
                **kwargs,
            )

    def _cached_client(self, service: str, **kwargs: Any) -> Any:
        """Return one fully constructed internal client per service/options.

        The public :meth:`boto3_client` method deliberately remains a fresh
        client factory for compatibility.  Platform-owned backends use this
        private path so repeated metadata operations do not rebuild clients.
        """
        cache_items = tuple(
            sorted((str(key), repr(value)) for key, value in kwargs.items())
        )
        cache_key = (service, cache_items)
        with self._client_lock:
            cached = self._service_clients.get(cache_key)
            if cached is not None:
                return cached
            # Use the public factory for construction so integrations that
            # inject a test client continue to observe the same seam.  The
            # private cache controls reuse; the public method itself remains
            # a fresh-client factory when called directly.
            client = self.boto3_client(service, **kwargs)
            self._service_clients[cache_key] = client
            if service == "s3" and not kwargs:
                self._client = client
            return client

    @property
    def _session(self) -> Any:
        """Return the lazily created, per-instance boto3 Session."""
        with self._session_lock:
            if self._boto3_session is None:
                self._boto3_session = create_session(self._region, self._profile)
            return self._boto3_session

    @property
    def s3(self) -> Any:
        """Return the cached boto3 S3 client."""
        if self._client is not None:
            return self._client
        return self._cached_client("s3")

    def clear_cache(self) -> None:
        """Clear secret field and source-payload caches."""
        super().clear_cache()
        if self._secrets_backend is not None:
            self._secrets_backend.clear_cache()

    # ------------------------------------------------------------------
    # Path helpers retained as compatibility surfaces
    # ------------------------------------------------------------------

    def _parse_path(self, path: str) -> tuple[str, str]:
        return parse_path(path, self._bucket)

    @staticmethod
    def _ensure_bucket(bucket: str) -> None:
        ensure_bucket(bucket)

    # ------------------------------------------------------------------
    # File and directory operations
    # ------------------------------------------------------------------

    def read_file(self, path: str) -> str:
        return self._get_s3_backend().read_file(path)

    def read_bytes(self, path: str) -> bytes:
        return self._get_s3_backend().read_bytes(path)

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None:
        self._get_s3_backend().write_bytes(path, data, overwrite=overwrite)

    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None:
        self._get_s3_backend().write_file(path, content, overwrite=overwrite)

    def append_file(self, path: str, content: str) -> None:
        self._get_s3_backend().append_file(path, content)

    def delete_file(self, path: str) -> None:
        self._get_s3_backend().delete_file(path)

    def create_folder(self, path: str) -> None:
        self._get_s3_backend().create_folder(path)

    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
        self._get_s3_backend().delete_folder(path, recursive=recursive)

    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        return self._get_s3_backend().list_files(
            path,
            recursive=recursive,
            extension=extension,
        )

    def list_folders(self, path: str, *, recursive: bool = False) -> list[str]:
        return self._get_s3_backend().list_folders(path, recursive=recursive)

    def file_exists(self, path: str) -> bool:
        return self._get_s3_backend().file_exists(path)

    def folder_exists(self, path: str) -> bool:
        return self._get_s3_backend().folder_exists(path)

    def upload_file(
        self, local_path: str, dest: str, *, overwrite: bool = False
    ) -> None:
        self._get_s3_backend().upload_file(local_path, dest, overwrite=overwrite)

    def download_file(self, src: str, dest: str) -> None:
        self._get_s3_backend().download_file(src, dest)

    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        self._get_s3_backend().copy_file(src, dest, overwrite=overwrite)

    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        self._get_s3_backend().move_file(src, dest, overwrite=overwrite)

    def get_file_info(self, path: str) -> FileInfo:
        return self._get_s3_backend().get_file_info(path)

    # ------------------------------------------------------------------
    # Glue / Athena catalog operations
    # ------------------------------------------------------------------

    def delete_glue_table(self, database: str, table_name: str) -> None:
        self._get_catalog_backend().delete_glue_table(database, table_name)

    def glue_table_exists(self, database: str, table_name: str) -> bool:
        return self._get_catalog_backend().glue_table_exists(database, table_name)

    def execute_athena_ddl(
        self,
        sql: str,
        *,
        database: str | None = None,
        output_location: str = "",
    ) -> str:
        return self._get_catalog_backend().execute_athena_ddl(
            sql,
            database=database,
            output_location=output_location,
        )

    def register_delta_table(
        self,
        table_name: str,
        path: str,
        *,
        database: str,
        output_location: str = "",
        recreate: bool = False,
    ) -> None:
        self._get_catalog_backend().register_delta_table(
            table_name,
            path,
            database=database,
            output_location=output_location,
            recreate=recreate,
        )

    def register_symlink_table(
        self,
        table_name: str,
        path: str,
        *,
        database: str,
        output_location: str = "",
        schema_ddl: str = "",
        partition_ddl: str = "",
        recreate: bool = False,
        run_msck: bool = True,
    ) -> None:
        self._get_catalog_backend().register_symlink_table(
            table_name,
            path,
            database=database,
            output_location=output_location,
            schema_ddl=schema_ddl,
            partition_ddl=partition_ddl,
            recreate=recreate,
            run_msck=run_msck,
        )

    def repair_table_partitions(
        self,
        table_name: str,
        *,
        database: str,
        output_location: str = "",
    ) -> None:
        self._get_catalog_backend().repair_table_partitions(
            table_name,
            database=database,
            output_location=output_location,
        )
