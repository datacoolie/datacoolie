---
title: ADR-0008 — Portable DatabricksPlatform Backends | DataCoolie
description: Why DatabricksPlatform prefers native dbutils in Databricks and uses the Databricks SDK for Unity Catalog Volumes elsewhere.
---

# ADR-0008 — Portable `DatabricksPlatform` backends

**Status** · Accepted

**Backend defaults checked against source** · 2026-10-02. This is a local
source review, not a new execution of the historical live benchmarks below.

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
  historical serverless comparison on a 66-file metadata tree (99.0561% lower fastest
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
  inside Databricks, with a read-only recursive-listing benchmark. The recorded
  historical native serverless gate passed through Databricks CLI run `1077257588175694` (task
  run `1113446787434626`).
- The external and native listing benchmarks record repeated p50/p95 and
  directory-list call counts. The native POSIX `/Volumes` strategy is now the
  default for mounted Volumes; `volume_listing="dbutils"` remains an explicit
  internal baseline/fallback.

The percentages, file count and run IDs above describe that recorded workload;
they are not a performance guarantee for another Volume, tree or runtime.
This ADR does not include a dated, complete runtime/hardware manifest for the
historical run, so those results cannot establish current qualification.
The [listing benchmark owner](https://github.com/datacoolie/datacoolie/blob/main/tests/integration/platforms/databricks/test_listing_benchmark.py)
and [opt-in test instructions](../testing.md#real-cloud-integration-tests)
provide the reproducible checks. When reporting a new run, preserve its date,
runtime/SDK versions, tree shape, identity of the selected route and measured
samples together with the result. Current backend defaults are defined by
the [native backend](https://github.com/datacoolie/datacoolie/blob/main/src/datacoolie/platforms/_databricks/dbutils_backend.py)
and [traversal settings](https://github.com/datacoolie/datacoolie/blob/main/src/datacoolie/platforms/_databricks/traversal.py).

## Related

- [ADR-0002 · Secret provider / resolver split](0002-secret-provider-resolver-split.md)
- [ADR-0006 · Portable FabricPlatform Azure backends](0006-portable-fabric-platform-azure-backends.md)
- [Platforms](../../reference/concepts/platforms.md)
