---
title: DataCoolie Project Configuration and Workflow
description: The datacoolie.yml contract and the portable project lifecycle.
---

# Project configuration and workflow

`datacoolie.yml` is the marker and source of truth for a DataCoolie project.
The same page describes the project lifecycle so configuration and command
usage stay together. Paths are project-relative directories. Metadata is
required; SQL and functions are optional, and each may contain one entry or a
list of component entries. `runners/<env>` is a fixed project tooling
directory and is not a `components` entry.

## `datacoolie.yml`

```yaml
schema_version: 1
project:
  name: orders
components:
  metadata:
    path: metadata
    output:
      layout: single
      format: json
  sql:
    path: sql
  functions:
    path: functions
    packaging: auto
environments:
  dev:
    platform: local
  prod:
    platform: fabric
    deployment_path: abfss://container/orders/prod
```

### Keys

| Key | Required | Values and behavior |
|---|---|---|
| `schema_version` | No | Currently `1`. Unsupported versions fail validation. |
| `project.name` | Yes | Non-empty project name. |
| `components.metadata.path` | No | Defaults to `metadata`. The root is scanned recursively; section wrappers in file content, not file names, identify metadata. |
| `components.metadata.output.layout` | No | `single` (default), `split`, or `preserve`. |
| `components.metadata.output.format` | No | `json` (default), `yaml`, `excel`, or `preserve` when the layout is `preserve`. |
| `components.sql.path` | No | One SQL root, or a list of entries with `path`. Each root is copied to every environment projection when configured. |
| `components.functions.path` | No | One Python/functions root, or a list of entries with `path`. |
| `components.functions.packaging` | No | Per functions entry: `auto` (default), `copy`, `wheel`, or `zip`. |
| `environments` | Yes | At least one named environment. Names match `[A-Za-z0-9][A-Za-z0-9_.-]*`. |
| `environments.<name>.platform` | No | Platform label, defaulting to `local`. It is recorded as build intent; the CLI does not instantiate the platform. |
| `environments.<name>.deployment_path` | No | Destination used by an explicit external release/upload workflow. `dc build` never uploads here. |

Component roots must be relative, must not contain a drive or URI scheme, must
not overlap one another, and must not start with `.builds`, `.runtime`,
`.releases`, or `runners` (these are reserved for generated/runtime state and
project tooling). Metadata files may be named freely, including
shards in nested folders, as long as their contents use the section wrappers
`{"connections": [...]}`, `{"dataflows": [...]}`, or
`{"schema_hints": [...]}`. `metadata/environments/<env>.json` is reserved for
the environment overlay and is not loaded as common metadata.

`dc init` declares `metadata`, `sql`, and `functions` with these defaults and
creates empty roots. A hand-written configuration may omit SQL or functions.

For independent SQL or function trees, use a list of entries. Each entry has
one singular `path`; function entries additionally own their packaging mode:

```yaml
components:
  metadata: {path: metadata}
  sql:
    - {path: sql/orders}
    - {path: sql/shared}
  functions:
    - {path: functions/ingest, packaging: auto}
    - {path: functions/quality, packaging: zip}
```

The roots are copied or packaged independently. A query reference still names
the root folder that contains it (`orders/orders.sql` or `shared/customers.sql`)
when more than one SQL root is configured; an ambiguous leaf name is rejected.

When more than one SQL root is configured, a file reference is qualified by
the root folder name (for example `sql1/orders.sql` or
`sql2/customers.sql`). With one root, both the qualified form and a path
relative to that root (for example `orders.sql`) are accepted. An explicit
`artifact:/sql/orders.sql` reference is rooted at the selected artifact and is
useful when the same metadata is run without project configuration. An artifact
whose selected root contains `sql/` may therefore author a reference such as
`sql/orders/incremental.sql`. Manifests are
project/tooling descriptors for validation and inspection; the Driver does
not read them at runtime. An artifact-only runner must preserve each SQL
reference relative to the artifact root (for example
`shared/sql2/customers.sql`), or pass explicit `sql_base_path` roots.

### Metadata output

The output setting can be committed in `datacoolie.yml` or overridden for one
`dc build`:

- `single` writes one `metadata.json` (or the selected format) per environment.
- `split` writes `connections`, `schema_hints`, and dataflow `metadata` files.
- `preserve` keeps authored document boundaries and writes environment-only
  additions below `_generated/`.

Supported formats are JSON, YAML, and XLSX. YAML output requires the `cli`
extra; XLSX output requires `datacoolie[metadata-excel]`.

### Environment overlays

An environment overlay is optional and lives at
`<configured metadata root>/environments/<env>.json`. It is loaded after the
common metadata snapshot and before metadata validation. A missing file means
that the environment uses the common snapshot unchanged. The overlay is not a
second project config and cannot add arbitrary top-level keys; its supported
shape is section arrays, an optional `$schema`, and an optional ordered
`patches` array.

Top-level section entries are matched by identity (`name`/ID for connections
and dataflows; connection/schema/table for schema-hint groups). An existing
entry is recursively object-merged and a new identity is appended. Arrays
nested inside an entry use ordinary replacement semantics, except schema-hint
groups and dataflow-local `transform.schema_hints`, which merge by
`column_name`. Duplicate identities are errors.

Patches run before section entries are merged. Each patch has exactly
`match: {type, where}` and a non-empty `patch`; `type` is one of
`connections`, `dataflows`, or `schema_hints`. `where` uses nested equality
with AND semantics and must not contain arrays or empty objects. Matching is
against a canonical pre-patch snapshot for each patch, patches are applied in
file order, and zero matches fail validation. Identity fields cannot be
changed, and there is no deletion operator.

For example, a common connection can keep its identity while changing only a
production database, and a dataflow can replace one nested filter:

```json
{
  "patches": [
    {
      "match": {"type": "connections", "where": {"name": "warehouse"}},
      "patch": {"configure": {"database": "orders_prod"}}
    },
    {
      "match": {"type": "dataflows", "where": {"name": "orders", "stage": "bronze2silver"}},
      "patch": {"source": {"filter_expression": "is_current = true"}}
    }
  ],
  "dataflows": [
    {
      "name": "new-report",
      "source": {"connection_name": "warehouse", "query": "SELECT 1"},
      "destination": {"connection_name": "warehouse", "table": "new_report"}
    }
  ]
}
```

The effective document is common metadata, then ordered selector patches, then
identity-based section merges. `dc validate --env prod` and `dc inspect
metadata --env prod` use that effective document; the Driver receives prepared
metadata rather than reading this overlay file itself. The example assumes the
common snapshot already contains a `warehouse` connection and an `orders`
dataflow with a destination. For a minimal valid fixture, `warehouse` uses
`connection_type: database`, `format: sql`, and
`configure: {database_type: postgresql, host: warehouse.example, database: orders}`.
The `orders` dataflow has a source query and a destination table. The added
`new-report` entry is a complete dataflow, so the effective document can pass
metadata schema and model checks.

## Project lifecycle

The normal lifecycle is preparation first and execution second:

```bash
pip install "datacoolie[cli]"
dc init orders
cd orders
dc validate
dc inspect metadata
dc build
```

`dc init` creates an empty project. Add connections and dataflows under the
configured metadata root, SQL files under the configured SQL root, and
optional Python functions under the configured functions root. Put project
execution scripts or notebooks under `runners/<env>/`; the build copies them
to the matching environment artifact without executing or rewriting them. The
default
metadata tree is:

```text
metadata/
  connections.json
  schema_hints.json
  dataflows/
  environments/
```

Runner files must be assigned directly below a configured environment folder:
`runners/dev/run.py` or `runners/prod/notebook.ipynb`. Unknown or
case-mismatched environment folders and files placed directly under
`runners/` fail validation. Empty runner folders are valid; `.gitkeep`,
`__pycache__`, `.pyc`, and `.pyo` files are ignored, while symlinks are
rejected.

The CLI does not create a sample dataflow. Keep execution in a project-owned
script or notebook so the project can construct its engine, register custom
tables (for example for Polars SQL), resolve runtime providers, and pass the
deployed artifact root explicitly to the Driver.

### Portable invocation

Commands can run from any working directory. Select the project explicitly
with a directory or its marker file:

```bash
dc --project-dir ./orders validate --format json
dc --project-dir ./orders inspect config --env prod --format json
dc --project-dir ./orders build --dry-run --format json
```

Standalone metadata and artifact operations do not need a project marker:

```bash
dc validate --metadata-path ./orders/metadata --format json
dc validate --artifact-path ./orders/.builds/current --format json
dc inspect metadata --metadata-path ./orders/metadata --section dataflows --full
```

Relative CLI path arguments are resolved from the caller's current working
directory. Configured component roots and overlay locations are resolved from
the selected project root. `--project-dir` selects the project but does not
rebase an independent `--metadata-path`, `--artifact-path`, or
`--sql-base-path`; use explicit paths when invoking from another directory.

Use `--env` to validate or inspect one declared environment. A standalone
metadata path can use an environment overlay only when `--project-dir` is also
provided, because the project configuration declares the environment name.

### Build and execute

`dc build` builds all environments together. The immutable result is written
to `.builds/artifacts/<build_id>/`; `.builds/current/` is replaced only after
the build passes validation and checksum verification. Inspect or validate an
older build by passing its artifact directory explicitly.

```text
.builds/
  artifacts/<build_id>/
    <env>/<configured metadata path>/metadata.json
    <env>/<configured sql path>/...
    <env>/<configured functions path>/...
    <env>/runners/...
    manifest.json
  current/
    manifest.json
    <same complete projection as artifacts/<build_id>>
```

The build ID is the deployable identity. `current` is only a convenient
projection of the latest successful build. A failed build leaves the previous
projection unchanged. The root manifest records component paths, environment
intent, input/content digests, and the artifact inventory. Each environment
also contains a `manifest.json` describing the build output for tooling;
`functions_artifact` is always an array when functions are configured, even
when there is only one functions root. Pass
that environment directory as `artifact_base_path` to a Driver when running
a deployed artifact; runtime uses `<environment>/metadata` unless an explicit
`metadata_base_path` is supplied and does not consume the manifest's component
declarations.

Packaging is decided per functions root. `auto` uses a valid `[build-system]`
backend as a wheel, a root-level `__init__.py` as a wrapped ZIP, and otherwise
copies the source tree. A nested package such as `loaders/__init__.py` does not
change the parent root's mode. Explicit `wheel` requires the backend/build
dependency; explicit `zip` requires a non-empty root and keeps the root's
import shape. Normal builds can execute that wheel backend, while
`dc build --dry-run` records the decision without invoking it.

The publisher uses the build ID plus input/layout/format and content digests as
collision protection. An existing ID is reused only when every recorded value
matches; a mismatch fails instead of overwriting the retained artifact. This
does not promise a global content cache or reproducible wheel bytes.

Publication uses the retained `.builds/.publish.lock` file as an OS-owned
interprocess lock; the file itself is harmless state and does not indicate a
stale lock after a process exits. A failed replacement keeps the previous
`current` or leaves a uniquely named recoverable backup.

The build command does not deploy or run a dataflow. A project-owned runner
chooses whether to load metadata from the authored tree, `current`, or a
deployed environment and then creates the appropriate `DataCoolieDriver`.

### Release handoff (external upload workflow)

There is intentionally no `dc release` command. A project-owned adapter, CI
job, or the release Skill may publish a **selected, already verified**
environment; this step is separate from `dc build` and does not rebuild it.

1. Run the project and artifact checks as separate commands:
   `dc validate --project-dir <project> --env <env> --format json` validates
   the selected environment overlay, and
   `dc validate --artifact-path <artifact-root> --format json` validates the
   retained `.builds/current` or `.builds/artifacts/<build_id>` directory.
   The artifact check has a different scope depending on the selected path:

   - For `.builds/current`, require `ok` and
     `data.details.current_comparison.performed == true` with
     `data.details.current_comparison.ok == true`. This proves the mutable
     projection matches its retained build.
   - For `.builds/artifacts/<build_id>`, require `ok` and the artifact
     inventory/hash checks reported for that retained build. A direct retained
     build does not need a comparison with `current`.
   - For `.builds/artifacts/<build_id>/<env>` or a manifest-less tree, require
     `ok` only for the checks it reports and keep
     `data.details.limited_scope == true`. It cannot certify parent build
     history or the root inventory.
2. Pin the exact `build_id` from the build manifest. Read the target only from
   `environments.<env>.deployment_path` in `datacoolie.yml`; do not duplicate it
   in a runner, manifest, or external prompt.
3. Upload the selected environment bytes to
   `<deployment_path>/artifacts/<build_id>/` first, preserving relative paths.
   After the artifact is complete and locally verified, mirror the same bytes
   to `<deployment_path>/current/`.
4. Overwrite objects with matching relative paths, but do not delete files that
   exist only at the target. A failure before the artifact is complete must not
   update `current`; a failure while mirroring `current` is a partial release
   and must be reported as such.

This handoff does not promise remote hash comparison, atomic activation,
package installation, job creation, or rollback. Keeping the immutable
`artifacts/<build_id>/` copy is what permits a later runner to select the same
verified build. Approval, target authentication, receipts, and platform SDK
adaptation remain responsibilities of the external release workflow.

For runtime state, use `.runtime/<env>/` when several environments share a
workspace or storage namespace. A single-environment project may use
`.runtime/` directly. The framework does not impose either layout: callers own
the explicit log, watermark, state, SQL, metadata, and artifact roots passed to
the Driver.

Use `dc build --dry-run --format json` to inspect the exact local decisions
before writing anything. The response includes the resolved metadata layout
and format, component mappings for every environment, SQL/function/runner
source counts, and effective function packaging. It also marks serialization,
packaging, artifact verification, and publication as `not_performed`; no
`.builds` directory, lock, staging tree, or package is created.
