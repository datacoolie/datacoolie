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
| `LocalPlatform` | local FS via `os.scandir`, `pathlib`, and `shutil` | environment variables | `datacoolie` (no extra) |
| `AWSPlatform` | S3 via `boto3` | AWS Secrets Manager | `datacoolie[aws]` |
| `FabricPlatform` | OneLake/ADLS via `notebookutils` in Fabric or Azure Data Lake SDK outside Fabric | Key Vault via NotebookUtils or Azure SDK | base package in Fabric; `datacoolie[fabric-external]` outside Fabric |
| `DatabricksPlatform` | UC Volumes via native `dbutils`/POSIX I/O or `WorkspaceClient.files` outside Databricks | Databricks secrets via native or SDK-backed `dbutils.secrets` | base package in Databricks; `datacoolie[databricks-external]` outside Databricks |

All four are available as built-ins on `import datacoolie`, and the same names
are also published in the `datacoolie.platforms` entry-point group for plugin
discovery.

Platform extras describe only dependencies that are not supplied by the native
runtime. Install the base package inside Fabric or Databricks, where
`notebookutils`, `dbutils`, Spark, and cloud connectors are host-provided. Use
`fabric-external` or `databricks-external` when the same public platform API is
run from a laptop, function, CI worker, or another external Python process.
`aws` is shared by AWS and S3-compatible endpoints such as MinIO; compose it
with an engine or source profile instead of selecting a platform matrix bundle.

### Local filesystem platform

`LocalPlatform` keeps the standard-library implementation in one module. Reads,
writes, appends, and file management use native local filesystem operations.
Recursive and non-recursive listings use iterative `os.scandir()` traversal,
reuse directory-entry metadata when constructing `FileInfo`, and leave result
ordering unspecified. Directory symlinks are reported as direct entries but are
not followed during recursive traversal, which prevents cycles and keeps a
listing bounded by the requested tree. Callers that need deterministic order
should sort the returned paths at their own boundary.

### AWS S3 and S3-compatible endpoints

`AWSPlatform` uses the standard boto3 credential chain (environment/profile,
shared credentials, and workload or instance roles). Its public client factory
remains available, while platform-owned S3, Secrets Manager, Glue, and Athena
clients are constructed once per platform instance and safely reused.

AWS S3 is the optimized route: small non-overwrite writes and copies use
conditional requests, full in-memory reads use one unbounded `GetObject`, and
large local transfers retain boto3's managed transfer implementation. A
configured `endpoint_url` keeps the same filesystem contract for MinIO or
LocalStack and uses compatibility preflight checks where S3-compatible
implementations differ. The AWS extra pins `boto3>=1.43.2`.

### Portable Fabric runtime

`FabricPlatform` keeps one public API across Fabric notebooks, local laptops,
Azure Functions, CI, and other Python runtimes:

```python
from datacoolie.platforms.fabric_platform import FabricPlatform

platform = FabricPlatform()  # runtime="auto"
```

`auto` selects the native backend when `notebookutils` imports successfully;
it does not probe methods or make a service call. Outside Fabric it uses
`azure-storage-file-datalake` and `azure-keyvault-secrets`. Pass
`runtime="fabric"` or `runtime="external"` when execution must be
deterministic, including environments that happen to install `notebookutils`
but should use Azure SDK clients.

External mode requires a qualified OneLake or ADLS Gen2 `abfs://`,
`abfss://`, or HTTPS URI. The URI already supplies the workspace/container,
item, account, and path, so the platform has no default workspace or item.
Relative paths such as `Files/output.json` remain available only through the
native NotebookUtils backend.

Most external callers do not pass credentials. The platform creates
`DefaultAzureCredential` lazily, allowing Azure CLI/developer credentials on a
laptop and managed or workload identity in Azure-hosted runtimes. To choose a
specific identity explicitly, inject any Azure `TokenCredential`:

```python
from azure.identity import ManagedIdentityCredential

platform = FabricPlatform(
    runtime="external",
    azure_credential=ManagedIdentityCredential(client_id="<client-id>"),
)
```

The platform translates ABFS(S) URIs to the HTTPS endpoint, filesystem, and
SDK-relative path expected by Azure clients. Engine-specific storage setup,
such as Spark Hadoop configuration or Polars `storage_options`, remains an
engine concern.

`read_file()` and `read_bytes()` always return the complete file; they do not
use NotebookUtils preview APIs. Recursive native listing is iterative and uses
bounded parallel directory requests, while external Azure listing uses one
server-side paged traversal. List order is unspecified on every backend, so a
consumer that needs ordering must sort at its own boundary.

`move_file()` creates missing destination parents and removes the source only
after the destination operation succeeds. External Azure cross-filesystem
moves verify both SHA-256 and byte length before promotion and source deletion;
overwrite promotion protects and restores the previous destination on failure.

### Portable Databricks runtime

`DatabricksPlatform` keeps the same public file and secret API in a Databricks
notebook/job, on a laptop, or in CI:

```python
from datacoolie.platforms.databricks_platform import DatabricksPlatform

platform = DatabricksPlatform()  # runtime="auto"
```

`auto` chooses native `dbutils` when it can be resolved without a service
call. Otherwise it creates a Databricks `WorkspaceClient` lazily and uses
unified authentication. Pass `runtime="databricks"` or `runtime="external"`
when backend selection must be deterministic. Tests and applications that
already own a client can inject it with `workspace_client=`.

The portable path contract is
`/Volumes/<catalog>/<schema>/<volume>/...`. The non-deprecated
`dbfs:/Volumes/...` alias is accepted and canonicalized back to `/Volumes/...`
in returned metadata. DBFS root, DBFS mounts, incomplete Volume roots, and
mutations of the exact managed Volume root are rejected.

Inside Databricks, validated `s3://`, `abfss://`, and `gs://` paths can still
be handled by `dbutils.fs`. Those raw cloud URIs are not portable and are
rejected in external mode. Workspace Files are outside this platform contract.

Volume text and binary reads always return the complete file. Native execution
uses direct POSIX I/O for content and `dbutils` for filesystem management;
external execution uses the Files API and consumes every listing page.
Native Volume appends use an explicit read-modify-write sequence because
serverless Volume FUSE does not support Python's POSIX append mode reliably;
this preserves DataCoolie's existing single-writer append contract.
Recursive traversal is iterative with bounded directory-request concurrency,
and platform results are not sorted. The external SDK backend defaults to 16
listing workers after live validation on the configured metadata tree; mounted
native Volumes use the benchmark-winning POSIX traversal with 8 workers, while
`dbutils.fs.ls` remains the explicit baseline/fallback for non-FUSE runtimes.

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

- `AWSPlatform` lazily caches one `boto3.Session` and one internal low-level
  client per AWS service on the platform instance; client construction is
  serialized, while completed low-level clients may be shared across threads.
- External `FabricPlatform` instances cache one Azure credential and SDK
  clients by account/filesystem or vault URL.
- External `DatabricksPlatform` instances cache one `WorkspaceClient`; its
  authentication is resolved by the Databricks unified authentication chain.
- The driver can share one platform instance across worker threads. Backend SDK
  behaviour still applies, and callers must coordinate conflicting writes to
  the same path.
- No platform API promises that a multi-step operation is atomic.

## Related

- [Secrets](secrets.md)
- Custom platforms are possible through `BasePlatform` plus the `datacoolie.platforms` entry-point group.
- [`reference/api/platforms`](../reference/api/platforms.md)
- Blog: [Cloud-Agnostic Data Pipelines in Python](../blog/posts/2026-05-28-cloud-agnostic-data-pipelines-python.md)
