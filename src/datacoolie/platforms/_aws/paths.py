"""Pure S3 path parsing helpers."""

from __future__ import annotations

from datacoolie.core.exceptions import PlatformError


def parse_path(path: str, default_bucket: str) -> tuple[str, str]:
    """Split an S3 URI or plain key into ``(bucket, key)``."""
    if path.startswith("s3://"):
        rest = path[5:]
    elif path.startswith("s3a://"):
        rest = path[6:]
    else:
        return default_bucket, path

    parts = rest.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def ensure_bucket(bucket: str) -> None:
    """Raise the platform contract error when no bucket is available."""
    if not bucket:
        raise PlatformError(
            "No bucket specified. Provide an s3:// URI or set 'bucket' in the constructor."
        )
