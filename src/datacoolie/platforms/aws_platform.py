"""AWS S3 platform implementation.

Uses ``boto3`` for all file-system operations.  The SDK is lazily
imported so the class can be *defined* without ``boto3`` installed
(e.g. for registry registration).
"""

from __future__ import annotations

import io
import json
from typing import Any

from datacoolie.core.exceptions import PlatformError
from datacoolie.logging.base import get_logger
from datacoolie.platforms.base import BasePlatform, FileInfo

logger = get_logger(__name__)


class AWSPlatform(BasePlatform):
    """Platform backed by AWS S3 via ``boto3``.

    Also implements :meth:`_fetch_secret` via AWS Secrets Manager, so it
    can serve as its own secret provider.

    Args:
        bucket: Default S3 bucket name.
        region: AWS region for both S3 and Secrets Manager operations
            (e.g. ``"us-east-1"``).
        profile: Named AWS profile from ``~/.aws/credentials``.
        endpoint_url: Custom endpoint (for MinIO, LocalStack, etc.).
        cache_ttl: Secret cache time-to-live in seconds (default 300).
            Pass ``0`` to disable caching.
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

    # ------------------------------------------------------------------
    # Secrets
    # ------------------------------------------------------------------

    def _fetch_secret(self, key: str, source: str) -> str:
        """Fetch a value from an AWS Secrets Manager secret.

        Args:
            key: JSON field name to extract from the secret's
                ``SecretString``.  When ``SecretString`` is a plain (non-JSON)
                string, this argument is ignored and the raw value is returned.
            source: Secret name or ARN passed as ``SecretId`` to Secrets
                Manager (e.g. ``"prod/database/credentials"``).  This is the
                outer key in ``secrets_ref``.

        Raises:
            PlatformError: If *source* is empty, boto3 is unavailable,
                the call fails, or *key* is not found in the JSON secret.
        """
        if not source:
            raise PlatformError(
                "secret name (source) is required for AWSPlatform secret fetching. "
                'Pass it via secrets_ref as the outer key, e.g. '
                '{"prod/db/creds": ["key_1", "key_2"]}.'
            )
        try:
            sm_client = self.boto3_client("secretsmanager")
            response = sm_client.get_secret_value(SecretId=source)
            secret_string: str = response.get("SecretString", "") or ""

            # Fall back to SecretBinary when SecretString is absent/empty
            if not secret_string:
                secret_binary = response.get("SecretBinary")
                if secret_binary is not None:
                    secret_string = secret_binary.decode("utf-8")

            # Try to parse as JSON and extract the named field
            try:
                parsed = json.loads(secret_string)
                if isinstance(parsed, dict):
                    if key not in parsed:
                        raise PlatformError(
                            f"Key '{key}' not found in secret '{source}'. "
                            f"Available keys: {list(parsed.keys())}"
                        )
                    return str(parsed[key])
            except (json.JSONDecodeError, TypeError):
                pass

            # Plain-string secret — return as-is
            return secret_string
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(
                f"Failed to fetch secret '{source}' from AWS Secrets Manager: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # SDK access
    # ------------------------------------------------------------------

    def boto3_client(self, service: str, **kwargs: Any) -> Any:
        """Create a ``boto3`` service client using the platform's credentials.

        Useful for accessing any AWS service (SQS, SNS, Glue, etc.) with
        the same profile/region/endpoint configuration as the platform.
        The underlying :pyattr:`_session` is created once and reused.

        Args:
            service: AWS service name (e.g. ``"sqs"``, ``"glue"``, ``"sns"``).
            **kwargs: Extra keyword arguments forwarded to ``boto3.client()``.
                Pass ``endpoint_url`` here to override the endpoint for any
                service.  For S3, the platform-level ``endpoint_url`` is used
                automatically; for all other services it is ignored so that
                they always reach the real AWS endpoint.

        Returns:
            A new boto3 service client.

        Raises:
            PlatformError: If boto3 is unavailable.
        """
        client_kwargs: dict[str, Any] = {**kwargs}
        # _endpoint_url is a storage override (MinIO, LocalStack S3, etc.).
        # Apply it automatically only for S3; other services (e.g. secretsmanager)
        # must always reach the real AWS endpoint unless the caller explicitly
        # passes endpoint_url= in kwargs.
        if service == "s3" and self._endpoint_url and "endpoint_url" not in client_kwargs:
            client_kwargs["endpoint_url"] = self._endpoint_url
        return self._session.client(service, **client_kwargs)

    @property
    def _session(self) -> Any:
        """Return a cached ``boto3`` Session built from the platform credentials.

        Lazily created on first access and shared by :meth:`boto3_client` and
        :meth:`_fetch_secret` so that session construction overhead is paid
        only once per platform instance.
        """
        if self._boto3_session is None:
            try:
                import boto3  # type: ignore[import-untyped]
            except ImportError as exc:
                raise PlatformError(
                    "boto3 is not available. Install it with: pip install boto3"
                ) from exc
            session_kwargs: dict[str, Any] = {}
            if self._region:
                session_kwargs["region_name"] = self._region
            if self._profile:
                session_kwargs["profile_name"] = self._profile
            self._boto3_session = boto3.Session(**session_kwargs)
        return self._boto3_session

    @property
    def s3(self) -> Any:
        """Return a cached ``boto3`` S3 client."""
        if self._client is None:
            self._client = self.boto3_client("s3")
        return self._client

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def _parse_path(self, path: str) -> tuple[str, str]:
        """Split *path* into ``(bucket, key)``.

        Accepts ``s3://bucket/key``, ``s3a://bucket/key`` (Hadoop scheme),
        or plain ``key`` (uses default bucket).  Both ``s3://`` and ``s3a://``
        are normalised — the same bucket/key is returned regardless of scheme.
        """
        if path.startswith("s3://"):
            rest = path[5:]
        elif path.startswith("s3a://"):
            rest = path[6:]
        else:
            return self._bucket, path
        parts = rest.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key

    def _ensure_bucket(self, bucket: str) -> None:
        if not bucket:
            raise PlatformError(
                "No bucket specified. Provide an s3:// URI or set 'bucket' in the constructor."
            )

    # ------------------------------------------------------------------
    # File I/O
    # ------------------------------------------------------------------

    def read_file(self, path: str) -> str:
        bucket, key = self._parse_path(path)
        self._ensure_bucket(bucket)
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            return response["Body"].read().decode("utf-8")
        except Exception as exc:
            raise PlatformError(f"Failed to read file: s3://{bucket}/{key}") from exc

    # Files at or below this size are fetched in a single GetObject call.
    # Larger files use boto3's threaded multipart download for parallelism.
    _INLINE_READ_THRESHOLD = 8 * 1024 * 1024  # 8 MiB

    def read_bytes(self, path: str) -> bytes:
        """In-memory read — single GET for small files, multipart for large.

        Avoids the temp-file round-trip of the base implementation.
        """
        bucket, key = self._parse_path(path)
        self._ensure_bucket(bucket)
        try:
            head = self.s3.head_object(Bucket=bucket, Key=key)
            size = int(head.get("ContentLength", 0))
            if size <= self._INLINE_READ_THRESHOLD:
                return self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            buf = io.BytesIO()
            self.s3.download_fileobj(bucket, key, buf)
            return buf.getvalue()
        except Exception as exc:
            raise PlatformError(f"Failed to read bytes: s3://{bucket}/{key}") from exc

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None:
        """Native single-call upload — no temp file, no extra disk write.

        Uses ``put_object`` for payloads at or below the inline threshold and
        ``upload_fileobj`` (managed multipart) for larger payloads.
        """
        bucket, key = self._parse_path(path)
        self._ensure_bucket(bucket)
        if not overwrite and self.file_exists(path):
            raise PlatformError(f"File already exists (set overwrite=True): s3://{bucket}/{key}")
        try:
            if len(data) <= self._INLINE_READ_THRESHOLD:
                self.s3.put_object(Bucket=bucket, Key=key, Body=data)
            else:
                self.s3.upload_fileobj(io.BytesIO(data), bucket, key)
        except Exception as exc:
            raise PlatformError(f"Failed to write bytes: s3://{bucket}/{key}") from exc

    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None:
        bucket, key = self._parse_path(path)
        self._ensure_bucket(bucket)
        if not overwrite and self.file_exists(path):
            raise PlatformError(f"File already exists (set overwrite=True): s3://{bucket}/{key}")
        try:
            self.s3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
        except Exception as exc:
            raise PlatformError(f"Failed to write file: s3://{bucket}/{key}") from exc

    def append_file(self, path: str, content: str) -> None:
        bucket, key = self._parse_path(path)
        self._ensure_bucket(bucket)
        existing = ""
        if self.file_exists(path):
            existing = self.read_file(path)
        try:
            self.s3.put_object(Bucket=bucket, Key=key, Body=(existing + content).encode("utf-8"))
        except Exception as exc:
            raise PlatformError(f"Failed to append to file: s3://{bucket}/{key}") from exc

    def delete_file(self, path: str) -> None:
        bucket, key = self._parse_path(path)
        self._ensure_bucket(bucket)
        try:
            self.s3.delete_object(Bucket=bucket, Key=key)
        except Exception:  # noqa: BLE001
            pass  # idempotent

    # ------------------------------------------------------------------
    # Directory operations
    # ------------------------------------------------------------------

    def create_folder(self, path: str) -> None:
        bucket, key = self._parse_path(path)
        self._ensure_bucket(bucket)
        # S3 has no real folders — create a zero-byte marker
        if not key.endswith("/"):
            key += "/"
        try:
            self.s3.put_object(Bucket=bucket, Key=key, Body=b"")
        except Exception as exc:
            raise PlatformError(f"Failed to create folder: s3://{bucket}/{key}") from exc

    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
        bucket, key = self._parse_path(path)
        self._ensure_bucket(bucket)
        if not key.endswith("/"):
            key += "/"
        try:
            if recursive:
                paginator = self.s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=key):
                    objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                    if objects:
                        response = self.s3.delete_objects(
                            Bucket=bucket,
                            Delete={"Objects": objects, "Quiet": True},
                        )
                        errors = response.get("Errors") or []
                        if errors:
                            failed = [e.get("Key", "") for e in errors[:5]]
                            raise PlatformError(
                                f"Failed to delete {len(errors)} object(s) under "
                                f"s3://{bucket}/{key}: {failed}"
                            )
            else:
                self.s3.delete_object(Bucket=bucket, Key=key)
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(f"Failed to delete folder: s3://{bucket}/{key}") from exc

    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        bucket, key = self._parse_path(path)
        self._ensure_bucket(bucket)
        if key and not key.endswith("/"):
            key += "/"

        # Non-recursive: Delimiter="/" keeps listing at one level.
        # Recursive:     no Delimiter, S3 returns all objects flat.
        paginate_kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": key}
        if not recursive:
            paginate_kwargs["Delimiter"] = "/"
        try:
            files: list[dict[str, Any]] = []
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(**paginate_kwargs):
                for obj in page.get("Contents", []):
                    obj_key = obj["Key"]
                    if obj_key == key:
                        continue  # skip folder marker
                    name = obj_key.rsplit("/", 1)[-1]
                    if not name or (extension and not name.endswith(extension)):
                        continue
                    files.append(
                        FileInfo(
                            name=name,
                            path=f"s3://{bucket}/{obj_key}",
                            modification_time=obj.get("LastModified"),
                            size=obj.get("Size", 0),
                        )
                    )
            return files
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(f"Failed to list files: s3://{bucket}/{key}") from exc

    def list_folders(
        self,
        path: str,
        *,
        recursive: bool = False,
    ) -> list[str]:
        bucket, key = self._parse_path(path)
        self._ensure_bucket(bucket)
        if key and not key.endswith("/"):
            key += "/"

        paginate_kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": key}
        if not recursive:
            paginate_kwargs["Delimiter"] = "/"
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            if not recursive:
                folders: list[str] = []
                for page in paginator.paginate(**paginate_kwargs):
                    for cp in page.get("CommonPrefixes", []):
                        folders.append(f"s3://{bucket}/{cp['Prefix']}")
                return folders

            # Recursive: derive all intermediate directory levels from flat keys.
            base_len = len(key)
            seen: dict[str, None] = {}
            for page in paginator.paginate(**paginate_kwargs):
                for obj in page.get("Contents", []):
                    relative = obj["Key"][base_len:]
                    parts = relative.split("/")
                    current = key
                    for part in parts[:-1]:
                        current = f"{current}{part}/"
                        seen[f"s3://{bucket}/{current}"] = None
            return list(seen)
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(f"Failed to list folders: s3://{bucket}/{key}") from exc

    # ------------------------------------------------------------------
    # Existence checks
    # ------------------------------------------------------------------

    def file_exists(self, path: str) -> bool:
        bucket, key = self._parse_path(path)
        if not bucket:
            return False
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:  # noqa: BLE001
            return False

    def folder_exists(self, path: str) -> bool:
        bucket, key = self._parse_path(path)
        if not bucket:
            return False
        if not key.endswith("/"):
            key += "/"
        try:
            response = self.s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
            return response.get("KeyCount", 0) > 0
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------
    # File management
    # ------------------------------------------------------------------

    def upload_file(self, local_path: str, dest: str, *, overwrite: bool = False) -> None:
        bucket, key = self._parse_path(dest)
        self._ensure_bucket(bucket)
        if not overwrite and self.file_exists(dest):
            raise PlatformError(f"Destination already exists (set overwrite=True): s3://{bucket}/{key}")
        try:
            self.s3.upload_file(local_path, bucket, key)
        except Exception as exc:
            raise PlatformError(f"Failed to upload file: {local_path} → s3://{bucket}/{key}") from exc

    def download_file(self, src: str, dest: str) -> None:
        """Download a file from S3 to the local filesystem.

        Args:
            src: S3 URI (``s3://bucket/key``) or plain key using the default bucket.
            dest: Absolute local filesystem path to write to.

        Raises:
            PlatformError: On download failure.
        """
        bucket, key = self._parse_path(src)
        self._ensure_bucket(bucket)
        try:
            self.s3.download_file(bucket, key, dest)
        except Exception as exc:
            raise PlatformError(f"Failed to download file: {src} → {dest}") from exc

    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        src_bucket, src_key = self._parse_path(src)
        dest_bucket, dest_key = self._parse_path(dest)
        self._ensure_bucket(src_bucket)
        self._ensure_bucket(dest_bucket)
        if not overwrite and self.file_exists(dest):
            raise PlatformError(
                f"Destination already exists (set overwrite=True): s3://{dest_bucket}/{dest_key}"
            )
        try:
            self.s3.copy_object(
                CopySource={"Bucket": src_bucket, "Key": src_key},
                Bucket=dest_bucket,
                Key=dest_key,
            )
        except Exception as exc:
            raise PlatformError(f"Failed to copy file: {src} → {dest}") from exc

    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        self.copy_file(src, dest, overwrite=overwrite)
        self.delete_file(src)

    def get_file_info(self, path: str) -> FileInfo:
        bucket, key = self._parse_path(path)
        self._ensure_bucket(bucket)
        try:
            head = self.s3.head_object(Bucket=bucket, Key=key)
            name = key.rsplit("/", 1)[-1]
            return FileInfo(
                name=name,
                path=f"s3://{bucket}/{key}",
                modification_time=head.get("LastModified"),
                size=head.get("ContentLength", 0),
            )
        except Exception as exc:
            raise PlatformError(f"Failed to get file info: s3://{bucket}/{key}") from exc

    # ------------------------------------------------------------------
    # Athena / Glue Catalog
    # ------------------------------------------------------------------

    def execute_athena_ddl(
        self,
        sql: str,
        *,
        database: str | None = None,
        output_location: str = "",
    ) -> str:
        """Execute a DDL statement via Athena and wait for completion.

        Args:
            sql: The SQL/DDL statement to execute.
            database: Optional Athena database context.
            output_location: S3 location for query results
                (e.g. ``"s3://bucket/athena-results/"``).

        Returns:
            The Athena query execution ID.

        Raises:
            PlatformError: If the query fails, is cancelled, or times out.
        """
        if not output_location:
            raise PlatformError(
                "athena_output_location is required for execute_athena_ddl. "
                "Set athena_output_location in configure or pass output_location=."
            )
        athena = self.boto3_client("athena")
        start_kwargs: dict[str, Any] = {
            "QueryString": sql,
            "ResultConfiguration": {"OutputLocation": output_location},
        }
        if database:
            start_kwargs["QueryExecutionContext"] = {"Database": database}

        try:
            response = athena.start_query_execution(**start_kwargs)
            query_id: str = response["QueryExecutionId"]
        except Exception as exc:
            raise PlatformError(f"Failed to start Athena query: {exc}") from exc

        # Poll until terminal state
        import time

        max_attempts = 60
        for _ in range(max_attempts):
            try:
                status_resp = athena.get_query_execution(QueryExecutionId=query_id)
                state = status_resp["QueryExecution"]["Status"]["State"]
            except Exception as exc:
                raise PlatformError(
                    f"Failed to poll Athena query {query_id}: {exc}"
                ) from exc

            if state == "SUCCEEDED":
                logger.info("Athena query %s succeeded", query_id)
                return query_id
            if state in ("FAILED", "CANCELLED"):
                reason = (
                    status_resp["QueryExecution"]["Status"]
                    .get("StateChangeReason", "unknown")
                )
                raise PlatformError(
                    f"Athena query {query_id} {state}: {reason}"
                )
            time.sleep(1)

        raise PlatformError(
            f"Athena query {query_id} timed out after {max_attempts}s"
        )

    def register_delta_table(
        self,
        table_name: str,
        path: str,
        *,
        database: str | None = None,
        output_location: str = "",
    ) -> None:
        """Register a native Delta table in the Glue Catalog via Athena.

        Creates an external table with ``TBLPROPERTIES ('table_type'='DELTA')``
        so Athena v3 can query it natively.

        Args:
            table_name: Table name (without database prefix).
            path: S3 path to the Delta table root.
            database: Athena/Glue database.
            output_location: S3 location for Athena query results.
        """
        # DROP + CREATE pattern — ensures the Glue Catalog entry always has
        # the correct TBLPROPERTIES, even if a stale Spark-created entry existed.
        drop_sql = f"DROP TABLE IF EXISTS `{table_name}`"
        logger.info("Dropping existing native table (if any): %s", table_name)
        self.execute_athena_ddl(drop_sql, database=database, output_location=output_location)

        create_sql = (
            f"CREATE EXTERNAL TABLE IF NOT EXISTS `{table_name}` "
            f"LOCATION '{path}' "
            f"TBLPROPERTIES ('table_type'='DELTA')"
        )
        logger.info("Registering native Delta table: %s", table_name)
        self.execute_athena_ddl(create_sql, database=database, output_location=output_location)

    def register_symlink_table(
        self,
        table_name: str,
        path: str,
        *,
        database: str | None = None,
        output_location: str = "",
        schema_ddl: str = "",
        partition_ddl: str = "",
    ) -> None:
        """Register a symlink-based table in the Glue Catalog.

        Creates a table using ``SymlinkTextInputFormat`` pointing at the
        ``_symlink_format_manifest/`` directory of a Delta table, for use
        with Redshift Spectrum.

        Args:
            table_name: Table name.
            path: S3 path to the Delta table root (``/_symlink_format_manifest/``
                is appended automatically).
            database: Athena/Glue database.
            output_location: S3 location for Athena query results.
            schema_ddl: Column definitions, e.g.
                ``"id INT, name STRING, event_date STRING"``.
            partition_ddl: ``PARTITIONED BY`` clause, e.g.
                ``"PARTITIONED BY (year STRING, month STRING)"``.
        """
        manifest_location = path.rstrip("/") + "/_symlink_format_manifest/"

        # DROP + CREATE pattern so schema changes are picked up
        drop_sql = f"DROP TABLE IF EXISTS `{table_name}`"
        logger.info("Dropping existing symlink table (if any): %s", table_name)
        self.execute_athena_ddl(drop_sql, database=database, output_location=output_location)

        partitioned_by = f"\n{partition_ddl}" if partition_ddl else ""
        create_sql = (
            f"CREATE EXTERNAL TABLE `{table_name}` (\n"
            f"  {schema_ddl}\n"
            f")\n"
            f"{partitioned_by}"
            f"ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\n"
            f"STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'\n"
            f"OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n"
            f"LOCATION '{manifest_location}'"
        )
        logger.info("Registering symlink table: %s", table_name)
        self.execute_athena_ddl(create_sql, database=database, output_location=output_location)

        # Repair partitions if table is partitioned
        if partition_ddl:
            repair_sql = f"MSCK REPAIR TABLE `{table_name}`"
            logger.info("Repairing partitions for symlink table: %s", table_name)
            self.execute_athena_ddl(repair_sql, database=database, output_location=output_location)
