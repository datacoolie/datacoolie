---
title: Metadata — Python API Reference | DataCoolie
description: Python API reference for DataCoolie metadata packages covering file, database, and API providers plus related helpers.
---

# Metadata

::: datacoolie.metadata.base
    options:
      members:
        - BaseMetadataProvider
        - MetadataCache

::: datacoolie.metadata.contracts.context

::: datacoolie.metadata.file_provider
::: datacoolie.metadata.database_provider
::: datacoolie.metadata.api_provider

## Query references

`classify_query(...)` classifies a declared `Source.query` by string shape and does not probe
storage. Blank values, comments, and ordinary SQL are inline queries. A relative `.sql` value is
the shorthand for a file under the configured SQL base path; `artifact:/...` is an explicit artifact
reference. Absolute paths, URI-shaped paths, and query or fragment suffixes are rejected.

::: datacoolie.metadata.resolution.query
    options:
      members:
        - QueryReference
        - classify_query

## Metadata provider extension hooks

Metadata provider subclasses implement the selected protected fetch hooks below. The public
lookup methods normalize inputs and coordinate caching before calling these hooks; provider
implementations own only the underlying store access.

::: datacoolie.metadata.base.BaseMetadataProvider._fetch_connections
::: datacoolie.metadata.base.BaseMetadataProvider._fetch_connection_by_id
::: datacoolie.metadata.base.BaseMetadataProvider._fetch_connection_by_name
::: datacoolie.metadata.base.BaseMetadataProvider._fetch_dataflows
::: datacoolie.metadata.base.BaseMetadataProvider._fetch_dataflow_by_id
::: datacoolie.metadata.base.BaseMetadataProvider._fetch_schema_hints

Providers may also override these lifecycle and bulk-loading hooks. The base
implementation validates the complete scope and publishes one cache snapshot;
overrides must preserve that lifecycle boundary and release only resources the
provider created.

::: datacoolie.metadata.base.BaseMetadataProvider._bulk_load
::: datacoolie.metadata.base.BaseMetadataProvider._bulk_fetch_schema_hints
::: datacoolie.metadata.base.BaseMetadataProvider._initialize_metadata
::: datacoolie.metadata.base.BaseMetadataProvider._cleanup_failed_initialization
::: datacoolie.metadata.base.BaseMetadataProvider._close_resources

## Metadata cache publication

`MetadataCache` is published as a complete snapshot. Use
`publish_snapshot(...)`, the read-only getters, and `clear()`; the former
partial `set_connection*`, `set_dataflow*`, and `set_schema_hints*` mutation
methods have been removed. Code that owns metadata loading should publish one
validated snapshot rather than update individual cache sections.
