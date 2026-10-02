# Metadata authoring checklist

This file helps an agent draft metadata. It is not a second schema or a
validator. The full contract is owned by the framework resources and published
at the [metadata schema reference](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#metadata-document)
and [`schema/index.json`](https://datacoolie.github.io/datacoolie/schema/index.json).
For current authoring and IDE discovery, use the stable
[`latest` schema alias](https://datacoolie.github.io/datacoolie/schema/latest/metadata.schema.json).
For a target framework, select the greatest compatible version less than or
equal to the installed framework version, then run `dc validate --format json`.
Validation is offline and applies JSON Schema before runtime-model and resource
checks; a `latest` marker is resolved to the local framework-compatible schema,
not fetched from the public site. Pin a versioned URL for reproducible artifacts.

## Docs-first routing

Start with the public [Metadata Guide](https://datacoolie.github.io/datacoolie/guide/metadata/)
and follow its [first-file workflow](https://datacoolie.github.io/datacoolie/guide/metadata/first-metadata-file/).
For a topic, go directly to [connections](https://datacoolie.github.io/datacoolie/guide/metadata/connections/),
[dataflows](https://datacoolie.github.io/datacoolie/guide/metadata/dataflows/),
[source patterns](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/),
[transform patterns](https://datacoolie.github.io/datacoolie/guide/metadata/transform-patterns/),
[destination and load patterns](https://datacoolie.github.io/datacoolie/guide/metadata/destination-and-load-patterns/),
[datatypes and schema hints](https://datacoolie.github.io/datacoolie/guide/metadata/data-types/),
or the [validation checklist](https://datacoolie.github.io/datacoolie/guide/metadata/validation-checklist/).
For complete configuration, use the [Metadata guide](https://datacoolie.github.io/datacoolie/guide/metadata/#metadata-document),
[API source configuration](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/#api-source-configuration)
and [incremental windows](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/#incremental-windows-and-look-back).
For combined cases use [window replacement](https://datacoolie.github.io/datacoolie/guide/metadata/watermark-window-replacement/),
[paginated API](https://datacoolie.github.io/datacoolie/guide/metadata/api-advanced/),
[late files](https://datacoolie.github.io/datacoolie/guide/metadata/late-arriving-files/),
[protected keys](https://datacoolie.github.io/datacoolie/guide/metadata/stable-keys-and-protected-output/)
or [incremental SCD2](https://datacoolie.github.io/datacoolie/guide/metadata/merge-and-scd2/).
Use this file only for agent gates, verification evidence and project-specific
edge cases; the public guide and [exact schema reference](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#metadata-document)
remain authoritative.

For exact field contracts, jump directly to the schema anchors for
[Connection](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#connection),
[Dataflow](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#dataflow),
[Source](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#source),
[Transform](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#transform),
[Destination](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#destination),
[Schema Hint](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#schema-hint),
and [Shared Schema Hint](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#shared-schema-hint).

When the question is about one field, use its direct target rather than the
family heading: [connection secrets_ref](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#connection-secrets-ref),
[API auth_type](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#connection-configure-api-auth-type),
[source query](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#source-query),
[API pagination_type](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#source-configure-pagination-type),
[source watermark_columns](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#source-watermark-columns),
[transform schema_hints](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#transform-schema-hints),
[destination load_type](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#destination-load-type),
or [replace_by_watermark](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#destination-configure-replace-by-watermark).

## Authoring sequence

1. Read the project `datacoolie.yml` and the matching environment runner.
2. Select a compatible schema URL. Use the public `latest` alias for current
   authoring, or pin the selected versioned URL for reproducible artifacts;
   `dc validate` resolves either form locally against the framework version.
3. Draft section wrappers (`connections`, `dataflows`, and `schema_hints`).
   Filenames and shard boundaries are project choices; wrappers identify the
   section.
4. Give every connection and dataflow a stable unique `name`. Reference named
   connections with `connection_name`, or use an intentional inline connection.
5. For each dataflow, declare one source and destination, then add transforms
   only for business behavior that belongs in the dataflow contract.
6. Run `dc validate`; fix structural errors first, then model/semantic errors,
   then missing SQL resources. Do not treat a warning as proof that a failed
   later check is safe to ignore.

## Source and destination decisions

- Choose the source selector supported by the selected reader: use `table` for
  direct object/path reads, `query` for source-side SQL, and `python_function`
  for a metadata-addressed function. `source.query` remains the authored
  declaration: it may be inline SQL, a relative `.sql` path, or explicit
  `artifact:/...`; the runtime resolves precedence per reader.
- Query files are resolved during framework preparation, never by rewriting
  metadata. Configure one or more SQL roots in the project/runner and keep the
  path relative to the root or artifact. `sql/` is not a framework-fixed folder.
- Use `watermark_columns` only for an incremental source and verify that the
  selected provider can persist the required state. Keep API pagination and
  push-down settings under `source.configure`.
- A destination always has a table identity. Choose `load_type` deliberately:
  merge and SCD2 strategies require the applicable non-empty `merge_keys`.
  Partition expressions belong to destination configuration and should not be
  duplicated as business columns solely to create folders.
- Keep credentials and external scheduler identifiers out of metadata. Secrets
  are resolved by the runtime provider; external IDs belong in
  `DataCoolieRunConfig.run_attributes`.

## Transform checklist

- Use `value_rules` for typed normalization, `schema_hints` for intentional
  casts, and `additional_columns` for derived business values.
- `select_columns` and `drop_columns` are alternatives. Rename mappings are
  atomic. Do not recreate framework-owned audit, SCD2, or dataflow-run columns.
- Deduplication keys and latest-row columns must reflect the destination grain.
  A hash column is not inferred as a merge key; declare it explicitly when it
  is part of the business identity.
- Treat options such as `inferSchema` as engine-specific choices. The generic
  CLI does not promise a lint rule for them; verify the selected engine and
  runner instead.

## Environment overlays and representations

The project validator merges the authored metadata shards and the selected
`metadata/environments/<env>.json` overlay before model/resource checks. Keep
environment-specific paths, catalogs and credentials in the overlay or runner,
not duplicated in every dataflow. `dc metadata convert` changes one document's
encoding only; it does not merge overlays or resolve query files.

The CLI may build metadata as one file, split section files, or preserved source
boundaries according to `components.metadata.output`. These are preparation
choices. Runtime Providers hydrate typed `core.models` directly and do not load
the JSON Schema during Driver execution.

## Agent handoff

When handing metadata to the next step, report the selected schema URL/version,
the exact files changed, the `dc validate --format json` result, and any SQL
roots or external provider assumptions. If a schema is unavailable or the
public sample is unpublished, stop and report the blocker; never fall forward
to a newer schema or silently treat a missing `.sql` file as inline SQL.
