"""File-based introspection — scan directories and extract schema from data files.

Supports: CSV, TSV, JSON, JSONL, Parquet, Excel, Avro.
Storage: local filesystem + cloud backends via fsspec (S3, ADLS, GCS, HDFS) + OneLake DFS SDK.

Cloud path examples:
    s3://bucket/prefix/
    abfs://container@account.dfs.core.windows.net/path/
    abfss://Workspace@onelake.dfs.fabric.microsoft.com/Lakehouse/Files/path/
    gs://bucket/prefix/
Pass storage_options dict for credentials:
    {"key": "...", "secret": "...", "endpoint_url": "http://localhost:9000"}  # S3/MinIO
    {"credential": "default"}                                                  # Azure DefaultAzureCredential
    {"token": "..."}                                                           # GCS service account
"""

from __future__ import annotations

import os
import posixpath
import sys
from pathlib import Path

from _schema_parsers import schema_from_bytes, schema_from_cloud_file, schema_from_local_file
from _types import Column, SourceResult, Table


SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".json", ".jsonl", ".parquet", ".avro", ".xlsx", ".xls"}

DEFAULT_MAX_FILES = 1000

# Protocols handled by fsspec cloud backends (not local pathlib)
_CLOUD_PROTOCOLS = ("s3://", "s3a://", "abfs://", "abfss://", "gs://", "gcs://",
                    "hdfs://", "dbfs://", "az://")

# OneLake DFS host — adlfs routes some ops to the Blob endpoint (absent on Fabric),
# so OneLake gets a dedicated code path using DataLakeServiceClient.
_ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"


# ---------------------------------------------------------------------------
# Path classification
# ---------------------------------------------------------------------------

def _is_cloud_path(path: str) -> bool:
    return any(path.startswith(p) for p in _CLOUD_PROTOCOLS)


def _is_onelake_path(path: str) -> bool:
    return _ONELAKE_HOST in path


# ---------------------------------------------------------------------------
# Storage options resolution
# ---------------------------------------------------------------------------

def _resolve_storage_options(opts: dict | None) -> dict:
    """Resolve $ENV_VAR values and special credential shortcuts."""
    if not opts:
        return {}
    resolved = {}
    for k, v in opts.items():
        if isinstance(v, str) and v.startswith("$"):
            env_name = v[1:]
            env_val = os.environ.get(env_name)
            if not env_val:
                print(f"WARN: env var '{env_name}' not set (from storage_options.{k})", file=sys.stderr)
                resolved[k] = v
            else:
                resolved[k] = env_val
        elif k == "credential" and v == "default":
            try:
                from azure.identity import DefaultAzureCredential
                resolved[k] = DefaultAzureCredential()
            except ImportError:
                print("ERROR: azure-identity required for credential='default' — pip install azure-identity", file=sys.stderr)
                sys.exit(1)
        elif isinstance(v, dict):
            resolved[k] = _resolve_storage_options(v)
        else:
            resolved[k] = v
    return resolved


def _get_filesystem(path: str, storage_options: dict | None):
    """Return (fsspec filesystem, normalised root path) for cloud paths.

    For local paths returns (None, path) — callers use pathlib directly.
    """
    if not _is_cloud_path(path):
        return None, path

    try:
        import fsspec
    except ImportError:
        print("ERROR: fsspec is required for cloud paths — pip install fsspec", file=sys.stderr)
        sys.exit(1)

    opts = _resolve_storage_options(storage_options)
    fs, root = fsspec.core.url_to_fs(path, **opts)
    return fs, root


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def introspect_files(dir_path: str, storage_options: dict | None = None,
                     max_files: int = DEFAULT_MAX_FILES) -> SourceResult:
    """Scan a directory (local or cloud) and extract schema from data files."""
    # OneLake requires the Azure DFS SDK — adlfs routes some ops to the Blob endpoint
    # which Fabric does not expose, causing IncorrectEndpointError on ls/glob.
    if _is_onelake_path(dir_path):
        opts = _resolve_storage_options(storage_options)
        result = SourceResult(source_type="file", path=dir_path, storage_backend="onelake")
        _introspect_onelake(dir_path, opts, result, max_files)
        result.total_columns = sum(len(t.columns) for t in result.tables)
        return result

    fs, root = _get_filesystem(dir_path, storage_options)

    result = SourceResult(
        source_type="file",
        path=dir_path,
        storage_backend="local" if fs is None else type(fs).__name__.replace("FileSystem", "").lower(),
    )

    if fs is None:
        _introspect_local(root, result, max_files)
    else:
        _introspect_cloud(fs, root, result, max_files)

    result.total_columns = sum(len(t.columns) for t in result.tables)
    return result


# ---------------------------------------------------------------------------
# Local implementation (pathlib)
# ---------------------------------------------------------------------------

def _introspect_local(dir_path: str, result: SourceResult, max_files: int = DEFAULT_MAX_FILES) -> None:
    path = Path(dir_path)
    if not path.exists():
        print(f"ERROR: Path does not exist: {dir_path}", file=sys.stderr)
        sys.exit(2)

    files: list[Path] = []
    for ext in SUPPORTED_EXTENSIONS:
        files.extend(path.glob(f"**/*{ext}"))
        if len(files) > max_files:
            break

    if len(files) > max_files:
        print(f"WARN: Found {len(files)} files, limiting to {max_files}. Use --max-files to increase.", file=sys.stderr)
        files = sorted(files)[:max_files]

    grouped: dict[str, list[Path]] = {}
    for f in files:
        rel = f.relative_to(path)
        key = str(rel.parent) if rel.parent != Path(".") else f.stem
        grouped.setdefault(key, []).append(f)

    for name, file_list in sorted(grouped.items()):
        representative = sorted(file_list)[0]
        columns_raw = schema_from_local_file(representative)
        _append_table(result, name, columns_raw, len(file_list),
                      representative.suffix.lstrip("."), representative.name)


# ---------------------------------------------------------------------------
# Cloud implementation (fsspec — S3, ADLS Gen2, GCS, HDFS)
# ---------------------------------------------------------------------------

def _introspect_cloud(fs, root: str, result: SourceResult, max_files: int = DEFAULT_MAX_FILES) -> None:
    """Enumerate files on a cloud filesystem and extract schema."""
    try:
        all_paths: list[str] = fs.glob(root.rstrip("/") + "/**")
    except Exception as e:
        print(f"ERROR: Cannot list cloud path '{root}': {e}", file=sys.stderr)
        sys.exit(1)

    # Filter to supported extensions
    supported = [p for p in all_paths if posixpath.splitext(p)[1].lower() in SUPPORTED_EXTENSIONS]
    if not supported:
        return

    if len(supported) > max_files:
        print(f"WARN: Found {len(supported)} files, limiting to {max_files}. Use --max-files to increase.", file=sys.stderr)
        supported = sorted(supported)[:max_files]

    # Group by "directory stem" relative to root
    root_stripped = root.rstrip("/")
    grouped: dict[str, list[str]] = {}
    for p in supported:
        rel = p[len(root_stripped):].lstrip("/")
        parts = rel.split("/")
        key = "/".join(parts[:-1]) if len(parts) > 1 else posixpath.splitext(parts[-1])[0]
        grouped.setdefault(key, []).append(p)

    for name, file_list in sorted(grouped.items()):
        representative = sorted(file_list)[0]
        ext = posixpath.splitext(representative)[1].lower()
        columns_raw = schema_from_cloud_file(fs, representative, ext)
        _append_table(result, name, columns_raw, len(file_list),
                      ext.lstrip("."), posixpath.basename(representative))


# ---------------------------------------------------------------------------
# OneLake implementation (azure-storage-file-datalake DFS SDK)
# ---------------------------------------------------------------------------

def _introspect_onelake(path: str, opts: dict, result: SourceResult,
                        max_files: int = DEFAULT_MAX_FILES) -> None:
    """Introspect OneLake using DataLakeServiceClient (DFS SDK).

    adlfs routes ls/exists to the Blob endpoint which Fabric does not expose,
    causing IncorrectEndpointError. The DFS SDK only talks to the DFS endpoint.
    """
    import re
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        print("ERROR: azure-storage-file-datalake required — pip install azure-storage-file-datalake",
              file=sys.stderr)
        sys.exit(1)

    # Parse abfss://WORKSPACE@onelake.dfs.fabric.microsoft.com/LAKEHOUSE/Files/...
    m = re.match(r"abfss://([^@]+)@onelake\.dfs\.fabric\.microsoft\.com/(.*)", path)
    if not m:
        raise ValueError(f"Cannot parse OneLake path: {path}")
    workspace = m.group(1)
    dir_path = m.group(2).rstrip("/")

    credential = opts.get("credential")
    service_client = DataLakeServiceClient(
        account_url=f"https://{_ONELAKE_HOST}",
        credential=credential,
    )
    fs_client = service_client.get_file_system_client(workspace)

    try:
        all_paths = [
            item.name for item in fs_client.get_paths(dir_path, recursive=True)
            if not item.is_directory
        ]
    except Exception as e:
        print(f"ERROR: Cannot list OneLake path '{path}': {e}", file=sys.stderr)
        sys.exit(1)

    supported = [p for p in all_paths
                 if posixpath.splitext(p)[1].lower() in SUPPORTED_EXTENSIONS]
    if not supported:
        return

    if len(supported) > max_files:
        print(f"WARN: Found {len(supported)} files, limiting to {max_files}. Use --max-files to increase.",
              file=sys.stderr)
        supported = sorted(supported)[:max_files]

    # Group by directory relative to dir_path
    root_stripped = dir_path.rstrip("/")
    grouped: dict[str, list[str]] = {}
    for p in supported:
        rel = p[len(root_stripped):].lstrip("/")
        parts = rel.split("/")
        key = "/".join(parts[:-1]) if len(parts) > 1 else posixpath.splitext(parts[-1])[0]
        grouped.setdefault(key, []).append(p)

    for name, file_list in sorted(grouped.items()):
        representative = sorted(file_list)[0]
        ext = posixpath.splitext(representative)[1].lower()
        try:
            file_client = fs_client.get_file_client(representative)
            content = file_client.download_file().readall()
        except Exception:
            content = b""
        columns_raw = schema_from_bytes(content, ext)
        _append_table(result, name, columns_raw, len(file_list),
                      ext.lstrip("."), posixpath.basename(representative))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _append_table(result: SourceResult, name: str, columns_raw: list[dict],
                  file_count: int, fmt: str, rep_file: str) -> None:
    columns = [
        Column(name=c["name"], data_type=c["data_type"], ordinal=c.get("ordinal", 0), nullable=True)
        for c in columns_raw
        if "error" not in c
    ]
    result.tables.append(Table(
        table_name=name,
        columns=columns,
        file_count=file_count,
        file_format=fmt,
        representative_file=rep_file,
    ))
