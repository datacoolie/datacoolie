---
title: ADR-0007 — Private engine implementation packages | DataCoolie
description: Keep BaseEngine, PolarsEngine, and SparkEngine as stable public modules while organizing engine-owned helpers under private packages.
---

# ADR-0007 — Private engine implementation packages

**Status** · Accepted

## Context

`datacoolie.engines` originally kept public engine classes and engine-specific
support modules in one flat package. Qualified Polars SQL added independent
relation, parser, and discovery modules, while Spark session construction also
lived beside the public engine contracts. As these implementations grow, a
flat layout obscures ownership and makes implementation modules appear to be
supported public APIs.

The public class modules are already used by package entry points, registries,
documentation, and user code. Moving those classes would create unnecessary
import and class-identity risk.

## Decision

Keep these public modules at `datacoolie.engines` root:

- `base.py` with `BaseEngine`;
- `polars_engine.py` with `PolarsEngine`;
- `spark_engine.py` with `SparkEngine`.

Move Polars-owned support into `datacoolie.engines._polars` and Spark-owned
support into `datacoolie.engines._spark`. Capabilities implemented by both
engines use the same private filename or relative path; engine-only capabilities
remain only in their owning package rather than gaining placeholder mirrors.
Private package initializers do not eagerly import optional engine dependencies.

Dependencies flow from a public engine module to its private implementation.
Private modules do not import their public engine class, and the two private
packages do not import each other. Shared code moves to a neutral package only
after more than one engine requires an explicit common contract.

Only cohesive support modules move. Methods remain in their engine class when
they directly implement `BaseEngine`, coordinate multiple facilities, or
depend substantially on engine state. File length alone does not justify a
split.

For Polars, the private package uses a functional-core boundary. The public
facade retains runtime state, public `BaseEngine` methods, name resolution,
SQL coordination, and format routing. Stateless implementation lives in
cohesive modules for type mapping, database access, flat-file I/O and file
metadata, metrics, temporal/window semantics, relation registration, Delta path
operations, native transforms, and Iceberg schema/catalog operations.
These functions receive catalog, platform, storage, and resolved-column inputs
explicitly; they never receive or import `PolarsEngine`. Optional packages
remain imported only on the operation paths that require them.

Spark uses the same functional-core principle with Spark-specific capability
boundaries. `SparkEngine` remains the only owner of the active `SparkSession`
and platform reference and retains every public `BaseEngine` override plus
format routing. Stateless private modules own runtime compatibility, type
mapping, JDBC access, file I/O, native transforms, WriterV2/named-table
mutations, path-oriented Delta operations, metrics, temporal/window semantics,
and Iceberg behavior split between `iceberg/schema.py` and
`iceberg/operations.py`. They receive sessions, DataFrames, identifiers, options,
and platform dependencies explicitly and never import `SparkEngine`. Delta is
still imported lazily only by operations that require `DeltaTable`.

`spark_session_builder.py` is removed without a compatibility facade. All
DataCoolie-owned consumers use `_spark.session_builder`; end users construct
`SparkEngine` or supply its supported `spark_session` argument. Private module
paths are not supported external API.

## Compatibility

- Public `BaseEngine`, `PolarsEngine`, and `SparkEngine` import paths and
  package entry points are unchanged.
- Undocumented flat Polars helper imports are intentionally removed.
- The previously documented flat Spark session-builder import is intentionally
  removed without backward compatibility.
- Engine behavior, metadata, storage operations, and optional dependency
  selection are unchanged.

## Consequences

- The filesystem and import graph show clear engine ownership.
- Optional implementation modules no longer broaden the public engine API.
- Focused private-module tests can mirror the source package structure.
- Shared capability names make Polars/Spark parity review direct without
  pretending engine-specific SQL, registration, or runtime facilities are shared.
- Polars storage and transform behavior can be tested without constructing a
  second stateful service object or exposing implementation helpers publicly.
- Spark JDBC, file, transform, named-table, Delta, and Iceberg behavior can be
  tested independently while the facade preserves session ownership, action
  boundaries, cache lifecycle, and Spark 3/4 merge dispatch.
- Direct consumers of the removed Spark session-builder module must construct
  `SparkEngine` or manage their own Spark session.
- Future engine extraction requires a real composition boundary and
  behavior-equivalence tests rather than file-length-driven decomposition.

## Related

- [ADR-0005 · Qualified SQL relations in PolarsEngine](0005-polars-qualified-sql-relations.md)
- [API reference · Engines](../reference/api/engines.md)
