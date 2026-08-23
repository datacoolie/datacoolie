---
title: ADR-0008 — Portable DatabricksPlatform Backends | DataCoolie
description: Why DatabricksPlatform prefers native dbutils in Databricks and uses the Databricks SDK for Unity Catalog Volumes elsewhere.
---

# ADR-0008 — Portable `DatabricksPlatform` backends

**Status** · Accepted

## Context

`DatabricksPlatform` originally required an active Databricks runtime. That
prevented metadata and log file operations from using the same platform API on
a laptop, in CI, or in a function runtime. Databricks exposes Unity Catalog
Volume files through both native paths and the workspace Files API.

The platform contract must remain separate from engine-specific Spark, Polars,
Delta, and object-storage configuration. Databricks has deprecated DBFS root
and mounts, so a new portable contract must not perpetuate those paths.

## Decision

- Keep `datacoolie.platforms.databricks_platform:DatabricksPlatform` as the
  only public class and existing plugin entry point.
- Add `runtime="auto" | "databricks" | "external"`. Automatic selection
  prefers a resolvable native `dbutils` handle without making a service call.
- Use native `dbutils` for filesystem management and secrets inside
  Databricks. Use POSIX I/O for complete text/binary content on UC Volumes and
  for mounted-Volume traversal after the serverless performance gate; retain an
  explicit `dbutils.fs.ls` baseline/fallback for non-FUSE runtimes.
- Outside Databricks, use a lazy or injected `WorkspaceClient`, its Files API,
  and SDK-backed `dbutils.secrets` with unified authentication.
- Make `/Volumes/<catalog>/<schema>/<volume>/...` the portable path contract;
  accept `dbfs:/Volumes/...` only as its non-deprecated alias.
- Support validated raw `s3://`, `abfss://`, and `gs://` paths only on the
  native backend. Reject DBFS root, mounts, Workspace Files, incomplete Volume
  paths, and mutation of managed Volume roots.
- Keep implementation details in the private `_databricks/` package instead
  of adding a speculative Fabric/Databricks shared backend hierarchy.

## Alternatives considered

- SDK everywhere would discard native notebook/job identity and efficient
  Volume access.
- A separate external Databricks platform would split one caller contract by
  execution location.
- Supporting legacy DBFS paths would retain deprecated storage behavior in a
  new API contract.
- Treating raw cloud URIs as portable would require DataCoolie to own each
  cloud provider's credential and path semantics outside Databricks.

## Consequences

- Existing native Volume callers continue using `DatabricksPlatform()`.
- External callers install the SDK extra and rely on Databricks unified
  authentication unless they inject an existing `WorkspaceClient`.
- Full read methods never use bounded head/preview APIs.
- Exact external existence checks use metadata endpoints, while recursive
  listings consume all pages with bounded directory concurrency.
- External append follows DataCoolie's single-writer invariant. Copy and move
  stream through a spooled local file, verify SHA-256 and length, restore an
  overwritten destination on failure, and delete a move source last.
- Native Volume append uses read-modify-write because serverless Volume FUSE
  rejects Python append mode with `Illegal seek`; this remains within the
  existing single-writer invariant.
- Native and external append paths use bounded spools and treat only an exact
  missing-file response as a create path; existing external files do not incur
  a redundant metadata or parent-creation request.
- Native mounted-Volume listing defaults to iterative POSIX traversal after a
  serverless comparison on the 66-file metadata tree (99.0561% lower fastest
  p50 than `dbutils.fs.ls`, lower p95, identical path sets, and no failures or
  throttling). Raw cloud URI management remains on `dbutils.fs`.
- External recursive deletion removes files with bounded concurrency and
  removes directories deepest-first. The default of eight delete workers was
  selected after three UUID-scoped live repetitions without throttling or
  errors; the listing worker defaults remain independently benchmarked.
- Engine storage configuration remains owned by each engine.

## Verification

- Runtime and parser tests cover native priority, explicit overrides,
  canonical aliases, raw URI boundaries, traversal, and protected roots.
- Native and SDK backend contract tests cover full reads, CRUD, pagination,
  exact metadata, secrets, permission errors, verification mismatch, rollback,
  and move source safety.
- Opt-in live tests run the same UUID-scoped file contract externally and
  inside Databricks, with a read-only recursive-listing benchmark. The native
  serverless gate passed through Databricks CLI run `1077257588175694` (task
  run `1113446787434626`).
- The external and native listing benchmarks record repeated p50/p95 and
  directory-list call counts. The native POSIX `/Volumes` strategy is now the
  default for mounted Volumes; `volume_listing="dbutils"` remains an explicit
  internal baseline/fallback.

## Related

- [ADR-0002 · Secret provider / resolver split](0002-secret-provider-resolver-split.md)
- [ADR-0006 · Portable FabricPlatform Azure backends](0006-portable-fabric-platform-azure-backends.md)
- [Platforms](../concepts/platforms.md)
