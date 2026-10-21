# Metadata authoring checklist

This file helps an agent route and verify metadata. It is not a second schema
or validator. The full contract is owned by the framework resources and the
published [metadata schema reference](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#metadata-document)
and [schema index](https://datacoolie.github.io/datacoolie/schema/index.json).
For current authoring and IDE discovery, use the stable [latest schema alias](https://datacoolie.github.io/datacoolie/schema/latest/metadata.schema.json).
Select the compatible public schema for the installed framework, run the
installed CLI validation, and record the effective version and scope reported
by that CLI. Version selection, resolution, and validation stages belong to the
public schema and CLI docs.

## Docs-first routing

Start with the public [Metadata Guide](https://datacoolie.github.io/datacoolie/guide/metadata/)
and its [first-file workflow](https://datacoolie.github.io/datacoolie/guide/metadata/first-metadata-file/).
Route topics directly to [connections](https://datacoolie.github.io/datacoolie/guide/metadata/connections/),
[dataflows](https://datacoolie.github.io/datacoolie/guide/metadata/dataflows/),
[source patterns](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/),
[transform patterns](https://datacoolie.github.io/datacoolie/guide/metadata/transform-patterns/),
[destination and load patterns](https://datacoolie.github.io/datacoolie/guide/metadata/destination-and-load-patterns/),
[datatypes and schema hints](https://datacoolie.github.io/datacoolie/guide/metadata/data-types/),
or the [validation checklist](https://datacoolie.github.io/datacoolie/guide/metadata/validation-checklist/).
For complete configuration, use the [metadata document guide](https://datacoolie.github.io/datacoolie/guide/metadata/#metadata-document),
[API source configuration](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/#api-source-configuration),
and [incremental windows](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/#incremental-windows-and-look-back).
For combined cases use [window replacement](https://datacoolie.github.io/datacoolie/guide/metadata/watermark-window-replacement/),
[paginated API](https://datacoolie.github.io/datacoolie/guide/metadata/api-advanced/),
[late files](https://datacoolie.github.io/datacoolie/guide/metadata/late-arriving-files/),
[protected keys](https://datacoolie.github.io/datacoolie/guide/metadata/stable-keys-and-protected-output/),
or [incremental SCD2](https://datacoolie.github.io/datacoolie/guide/metadata/merge-and-scd2/).
Use the exact [metadata schema reference](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#metadata-document)
for fields and anchors. This file keeps only agent gates, verification evidence, and project edge
cases; it does not repeat field definitions or defaults.

When the question is about one field, use its direct schema target:

- [Connection](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#connection),
  [Dataflow](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#dataflow),
  [Source](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#source),
  [Transform](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#transform),
  [Destination](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#destination),
  [Schema Hint](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#schema-hint),
  and [Shared Schema Hint](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#shared-schema-hint).
- [connection secrets_ref](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#connection-secrets-ref),
  [API auth_type](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#connection-configure-api-auth-type),
  [source query](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#source-query),
  [API pagination_type](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#source-configure-pagination-type),
  [source watermark_columns](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#source-watermark-columns),
  [transform schema_hints](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#transform-schema-hints),
  [destination load_type](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#destination-load-type),
  and [replace_by_watermark](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#destination-configure-replace-by-watermark).

## Authoring and verification sequence

1. Read the project `datacoolie.yml`, affected metadata, configured resource roots, and matching
   environment runner.
2. Select the compatible public schema route, then record the effective version reported by the
   installed CLI.
3. Follow the public metadata guide for section structure, identity, source/destination choices,
   transforms, overlays, and merge-key conditions; do not infer a fixed metadata root or field
   default from this reference.
4. Run `dc validate --format json` and follow the public CLI's reported diagnostics and scope. Do
   not treat a warning or partial result as proof that the requested validation succeeded.
5. If building, run the project CLI build and validate the exact artifact; if only authoring was
   requested, do not execute a workload.

`source.query` remains the authored declaration; the public source guide and runtime configuration
guide own inline/file/artifact resolution. Keep credentials and external scheduler identifiers out
of metadata; use the public runtime/provider contract for those boundaries.

## Environment and representation gates

Read the public [project workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/)
for metadata roots, overlays, and build projections. Treat conversion as a representation change;
do not use it as a substitute for validation, overlay selection, or query-resource resolution.
Project filenames, shard boundaries, and configured SQL/function roots are project choices. Preserve
them in handoff evidence rather than creating a Skill-owned layout.

## Agent handoff

Apply [Build's outcome checks](../SKILL.md#build-and-local-verification). Record the selected schema URL/version, SQL roots and overlay/provider assumptions. If a schema, resource or public sample is unavailable, report the blocker; do not switch to a newer schema or silently treat a missing `.sql` file as inline SQL.
