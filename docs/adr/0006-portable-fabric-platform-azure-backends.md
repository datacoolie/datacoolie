---
title: ADR-0006 — Portable FabricPlatform Azure Backends | DataCoolie
description: Why FabricPlatform prefers NotebookUtils in Fabric and uses Azure SDK clients with Microsoft Entra credentials elsewhere.
---

# ADR-0006 — Portable `FabricPlatform` Azure backends

**Status** · Accepted

## Context

`FabricPlatform` originally required `notebookutils`, so it could be imported
but could not perform file or secret operations on a laptop, in CI, or in an
Azure-hosted Python runtime. OneLake exposes ADLS-compatible DFS APIs, while
qualified ABFS(S) paths already carry the workspace/container, item, account,
and relative path needed for access.

The public platform contract must remain stable. Engine storage configuration
is a separate concern and must not be inferred or mutated by the platform.

## Decision

- Keep `datacoolie.platforms.fabric_platform:FabricPlatform` as the only public
  class and existing plugin entry point.
- Add `runtime="auto" | "fabric" | "external"`. Automatic selection chooses
  NotebookUtils after a successful import, without probing methods or making
  filesystem or authentication calls.
- Use NotebookUtils for native Fabric file operations and Key Vault access.
- Outside Fabric, parse qualified OneLake and ADLS Gen2 ABFS(S)/HTTPS paths and
  call `azure-storage-file-datalake` over HTTPS.
- Use an injected Azure `TokenCredential` when supplied; otherwise create one
  `DefaultAzureCredential` lazily and reuse it for Storage and Key Vault.
- Keep Azure packages optional under the `fabric-external` extra and import them lazily.
- Group implementation details in the private `_fabric/` package. Do not
  export backend classes or introduce a shared Fabric/Databricks hierarchy
  before Databricks requirements demonstrate stable duplication.
- Reject SAS tokens, account keys, default workspace/item state, relative
  external paths, traversal, ambiguous encoding, and mutations of managed
  OneLake roots.

## Alternatives considered

- Separate OneLake and ADLS platforms would split one caller contract by
  endpoint even though both use the same Azure Data Lake SDK surface.
- Azure SDK everywhere would discard Fabric-native identity and filesystem
  context.
- Flat `_fabric_*.py` modules would make the public platform directory harder
  to navigate as runtime-specific implementations grow.
- A generic Fabric/Databricks backend hierarchy would encode unverified
  similarities in path routing and secret behavior.

## Consequences

- Existing native callers continue using `FabricPlatform()` and relative or
  qualified paths without Azure packages.
- External callers install `datacoolie[fabric-external]` and use qualified cloud URIs.
- Authentication or authorization failures remain visible and never cause an
  identity/backend fallback.
- Exact Azure existence and metadata operations use exact SDK clients rather
  than enumerating a parent directory. Azure listings retain service order.
- Non-overwrite Azure uploads atomically create and append/flush the new file;
  overwrite uploads retain the SDK `upload_data(overwrite=True)` fast path.
  This avoids OneLake's observed `upload_data(overwrite=False)` append failure
  without adding an existence preflight or weakening conflict behavior.
- Cross-filesystem moves stage a destination-side copy, verify its SHA-256 and
  byte length, safely promote it, and delete the source only after successful
  verification and promotion.
- Platform moves create missing destination parents. Azure overwrite promotion
  preserves the previous destination and restores it when promotion fails.
- Read methods retain whole-file semantics. Append relies on DataCoolie's
  invariant that one file has no concurrent append writers.
- Spark Hadoop settings, Polars `storage_options`, and table-format connectors
  remain owned by their respective engines.

## Verification

- Pure parser and runtime-selection tests cover accepted paths, invalid hosts,
  traversal, encoded separators, managed roots, and explicit overrides.
- Fake Azure SDK contract tests cover credential/client reuse, file and
  directory operations, Key Vault, redaction, copy verification, and move
  failure safety.
- Optional live tests exercise writable OneLake and ADLS roots when their
  environment variables are configured.

## Related

- [ADR-0002 · Secret provider / resolver split](0002-secret-provider-resolver-split.md)
- [Platforms](../concepts/platforms.md)
