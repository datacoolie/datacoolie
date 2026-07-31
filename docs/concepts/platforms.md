---
title: Platform Abstractions — DataCoolie Concepts
description: Learn how DataCoolie platforms abstract file I O and secret retrieval for local, Fabric, Databricks, and AWS environments.
---

# Platforms

**TL;DR** A platform abstracts *file I/O* and *secret retrieval* for a specific
environment: your laptop, Fabric, Databricks, or AWS. Engines and other
storage-facing components go through the platform for backend-specific file
operations, so cloud SDK and path-routing differences stay isolated in the
platform layer instead of leaking through readers and writers.

## Responsibilities

`BasePlatform` extends `BaseSecretProvider` and declares 18 abstract methods
across six groups:

| Group | Methods |
|---|---|
| File I/O | `read_file`, `write_file`, `append_file`, `delete_file` |
| Directory ops | `create_folder`, `delete_folder`, `list_files`, `list_folders` |
| Existence | `file_exists`, `folder_exists` |
| File mgmt | `upload_file`, `download_file`, `copy_file`, `move_file`, `get_file_info` |
| Binary I/O | `read_bytes`, `write_bytes` |
| Secrets | `_fetch_secret` (inherited from `BaseSecretProvider`) |

Platforms also expose `FileInfo` (frozen dataclass: `name`, `path`,
`modification_time`, `size`, `is_dir`). `FileInfo` normalises its `path` when
the value object is created.

## Built-in platforms

| Platform | File backend | Secret backend | Install via |
|---|---|---|---|
| `LocalPlatform` | local FS via `pathlib` | environment variables | `datacoolie` (no extra) |
| `AWSPlatform` | S3 via `boto3` | AWS Secrets Manager | `datacoolie[aws]` |
| `FabricPlatform` | OneLake via `notebookutils` | Key Vault via `notebookutils.credentials` | `datacoolie[fabric]` |
| `DatabricksPlatform` | Unity Catalog Volumes via Python I/O; other paths via `dbutils.fs` | Databricks secrets via `dbutils.secrets` | `datacoolie[databricks]` |

All four are available as built-ins on `import datacoolie`, and the same names
are also published in the `datacoolie.platforms` entry-point group for plugin
discovery.

## Why platform-as-secret-provider?

`BasePlatform` subclasses `BaseSecretProvider` so that every platform
*automatically* serves as its own secret backend without a separate wiring
step. When you construct a driver:

```python
DataCoolieDriver(engine=engine, metadata_provider=metadata)
# no secret_provider= → engine.platform is used as the default.
```

If you need a different vault (e.g. HashiCorp, 1Password) write a dedicated
`BaseSecretProvider` and pass it as `secret_provider=`.

See [Secrets](secrets.md) and [ADR-0002](../adr/0002-secret-provider-resolver-split.md).

## Path normalisation

`FileInfo.__post_init__` runs paths through `normalize_path`:

- Forward slashes on all OSes
- No trailing slash on directories
- Scheme preserved (`abfss://`, `s3://`, `file://`)

This gives `FileInfo.path` a consistent form. Other path-bearing models
normalise their own fields; arbitrary caller-created strings are not
automatically canonicalised.

## Concurrency notes

- `AWSPlatform` lazily caches one `boto3.Session` and one S3 client on the
  platform instance.
- The driver can share one platform instance across worker threads. Backend SDK
  behaviour still applies, and callers must coordinate conflicting writes to
  the same path.
- No platform API promises that a multi-step operation is atomic.

## Related

- [Secrets](secrets.md)
- Custom platforms are possible through `BasePlatform` plus the `datacoolie.platforms` entry-point group.
- [`reference/api/platforms`](../reference/api/platforms.md)
- Blog: [Cloud-Agnostic Data Pipelines in Python](../blog/posts/2026-05-28-cloud-agnostic-data-pipelines-python.md)
