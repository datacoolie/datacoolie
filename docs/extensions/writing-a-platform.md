---
title: Write a Platform Plugin — DataCoolie
description: Build a DataCoolie platform plugin for a storage and secret backend, register it with the runtime, and wire one shared instance into an execution.
---

# Write a platform

**Prerequisites** · You own a storage environment with file, directory, and
secret operations that DataCoolie does not already support.
**End state** · A package that implements `BasePlatform`, is discoverable as a
`datacoolie.platforms` entry point, and is tested against the backend it owns.

`BasePlatform` is the control-plane I/O boundary. It supplies the operations
used for metadata files, SQL and function artifacts, logs, state, and
watermarks. A platform does not make a new DataFrame engine understand a data
format, and its paths and credentials do not configure Spark, Polars, Delta, or
Iceberg connectors automatically. See [Platforms](../reference/concepts/platforms.md)
for that boundary and [the public API reference](../reference/api/platforms.md)
for exact Python signatures.

## Implement the contract

The base class lists required methods for text files, directories, existence
checks, file management, binary files, and secrets. The
[`BasePlatform` API reference](../reference/api/platforms.md) is the source of
truth for the complete signatures.

| Capability | Required methods |
|---|---|
| Text files | `read_file`, `write_file`, `append_file`, `delete_file` |
| Directories | `create_folder`, `delete_folder`, `list_files`, `list_folders` |
| Existence | `file_exists`, `folder_exists` |
| File management | `upload_file`, `download_file`, `copy_file`, `move_file`, `get_file_info` |
| Binary files | `read_bytes`, `write_bytes` |
| Secrets | `_fetch_secret` |

Keep the public method signatures unchanged. A minimal class shape is:

```python
from datacoolie.platforms.base import BasePlatform, FileInfo


class MyPlatform(BasePlatform):
    def __init__(self, endpoint: str):
        super().__init__()
        self._endpoint = endpoint

    def read_file(self, path: str) -> str: ...
    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None: ...
    def append_file(self, path: str, content: str) -> None: ...
    def delete_file(self, path: str) -> None: ...

    def create_folder(self, path: str) -> None: ...
    def delete_folder(self, path: str, *, recursive: bool = False) -> None: ...
    def list_files(
        self, path: str, *, recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]: ...
    def list_folders(self, path: str, *, recursive: bool = False) -> list[str]: ...

    def file_exists(self, path: str) -> bool: ...
    def folder_exists(self, path: str) -> bool: ...
    def upload_file(self, local_path: str, dest: str, *, overwrite: bool = False) -> None: ...
    def download_file(self, src: str, dest: str) -> None: ...
    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None: ...
    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None: ...
    def get_file_info(self, path: str) -> FileInfo: ...
    def read_bytes(self, path: str) -> bytes: ...
    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None: ...

    def _fetch_secret(self, key: str, source: str) -> str: ...
```

!!! warning "Skeleton only"
    Implement every ellipsis before registering this class or using it for
    I/O. These placeholder bodies satisfy Python's abstract-method checks but
    supply no backend behavior. Keep `super().__init__()` when adding a
    constructor so inherited secret caching is initialized.

`FileInfo` is a frozen value object with `name`, normalized `path`,
`modification_time`, `size`, and `is_dir`. `read_file` and `read_bytes` return
the complete file. Writes refuse an existing destination unless
`overwrite=True`; `append_file` creates a missing file; `delete_file` is
idempotent; `create_folder` creates missing parents. Listings return
`FileInfo` objects for files, with ordering left to the backend contract.

For `copy_file` and `move_file`, a normalized source and destination that
identify the same file are an idempotent no-op. `move_file` creates missing
destination parents and must leave the source in place when the move fails.
Wrap backend failures in `PlatformError` with the path or operation that
failed. Do not truncate large files with a preview or bounded `head` API.

### Keep paths inside a configured base

When the platform resolves a user-controlled relative resource, use the
base-class helpers so normalization and escape checks happen before I/O:

```python
def read_metadata(self, base_path: str, relative_path: str) -> str:
    return self.read_file_under_base(base_path, relative_path)


def read_binary_metadata(self, base_path: str, relative_path: str) -> bytes:
    return self.read_bytes_under_base(base_path, relative_path)
```

`relative_path_under_base` applies the same boundary to paths returned by a
listing. Reject traversal and absolute paths that escape the configured root;
do not rely on a string prefix check in the backend adapter.

### Implement secret retrieval separately from data access

`BasePlatform` inherits `BaseSecretProvider`, so the platform can be the
driver's native secret provider. Implement `_fetch_secret(key, source)` with
both arguments. The inherited `get_secret(key, source="")` caches by the
`(source, key)` pair and delegates to the protected method. `key` is the
secret name or field, while `source` is the backend-specific scope, vault,
environment prefix, or secret identifier. Do not replace `get_secret` with a
different public signature unless the platform has a documented reason.

## Register and activate the package

Declare the entry point in the package that contains the platform:

```toml
[project.entry-points."datacoolie.platforms"]
mycloud = "mypkg.platforms:MyPlatform"
```

Install that package in the same Python environment as the DataCoolie process.
The runtime discovers entry points lazily, and `create_platform` forwards its
keyword arguments to the registered class:

```python
from datacoolie import create_platform

platform = create_platform("mycloud", endpoint="https://storage.example")
```

If discovery reports that `mycloud` is missing, check the installed package,
the group name, the entry-point name, and whether the target module imports in
that environment. A package import alone does not register a platform.

## Activate in a runner

The engine and Driver must use the same platform object. Platform instances
can carry different roots, credentials, and client state, so passing two
instances of the same class is rejected. Either construct the engine with the
platform and omit `platform=` from the Driver, or let the Driver attach this
same object:

```python
from datacoolie import create_platform
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.orchestration.driver import DataCoolieDriver

platform = create_platform("mycloud", endpoint="https://storage.example")
engine = PolarsEngine(platform=platform)

driver = DataCoolieDriver(
    engine=engine,
    # The same object is valid; a second create_platform(...) call is not.
    platform=platform,
    metadata_provider=metadata_provider,
)
```

`create_driver` also accepts a platform object, not a platform name. If the
engine already owns the instance, the shorter form is:

```python
driver = create_driver(engine=engine, metadata_provider=metadata_provider)
```

If you pass `secret_provider=...` to the Driver, that explicit provider takes
precedence. When it is omitted, the resolved platform supplies the native
secret-provider fallback through `BasePlatform`.

The project configuration has a separate meaning. In `datacoolie.yml`,
`environments.<name>.platform` is recorded as build intent; the CLI does not
instantiate or discover that class. Install and activate the platform package
in the runtime process, then call `create_platform` or inject an explicit
instance as above. See the [project configuration keys](../guide/cli/project.md#keys).

## Keep the engine connector boundary explicit

Platform methods are suitable for control-plane files and secrets. A source or
destination that reads or writes business data must still use the active
engine's connector boundary and its supported format contract. For example,
providing an S3-like `read_file` method does not make a Polars engine support a
new table format, and platform credentials do not become Spark Hadoop or
Polars `storage_options`. Put format-specific behavior in a source,
destination, or engine plugin and test that component at its own boundary.

The platform API does not promise multi-step atomicity. Coordinate concurrent
writes to the same path at the application or backend boundary, and document
whether the backend provides stronger guarantees.

## Test the backend you own

There is no generic platform conformance suite that qualifies an arbitrary
backend. Write package-owned tests using an in-memory or temporary backend for
the complete contract, then add opt-in live tests for service behavior:

- construct the platform through `create_platform` after installing the test
  package and verify the entry-point name;
- cover complete text and binary reads, overwrite and append behavior,
  idempotent delete, recursive listings, `FileInfo`, and copy/move failure
  semantics;
- test traversal and absolute-path rejection through the `*_under_base`
  helpers;
- call `_fetch_secret(key, source)` through `get_secret` with at least two
  source values so source/key ordering and cache isolation are exercised;
- construct an engine and Driver with one platform instance and assert that a
  distinct instance is rejected;
- mark cloud or service tests as live and keep credentials and network access
  out of the default unit run.

The existing base-platform test is an abstract smoke check, not evidence that
a new backend implements the full storage or secret behavior. Use the
built-in platform implementations as behavioral references, then qualify the
specific SDK and failure modes of your backend.

## Troubleshooting

- **No plugin registered** — verify the installed distribution exposes the
  `datacoolie.platforms` group and that the entry-point target imports.
- **Driver rejects the platform** — reuse the exact object passed to the
  engine; matching class names are not enough.
- **A path escape is accepted** — route relative resources through
  `read_file_under_base`, `read_bytes_under_base`, or
  `relative_path_under_base` before calling backend I/O.
- **Secret lookup misses a value** — check both the `key` and `source` passed
  to `_fetch_secret`; the inherited cache treats `(source, key)` as distinct.
- **Business-data reads fail after platform setup** — configure the engine,
  source, or destination connector separately; platform activation does not
  add format support.

## Related

- [Platform concepts](../reference/concepts/platforms.md)
- [Platform API reference](../reference/api/platforms.md)
- [Engine/platform attachment](../reference/concepts/engines.md#platform-attachment)
- [Secret providers and resolvers](../reference/concepts/secrets.md)
- [Project configuration](../guide/cli/project.md#keys)
