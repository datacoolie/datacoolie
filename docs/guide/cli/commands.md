---
title: DataCoolie CLI Command Reference
description: Complete parameters and behavior for every DataCoolie project CLI command.
---

# Command reference

The CLI examples use `dc`. `datacoolie` is an equivalent executable alias:
it uses the same dispatcher, options, output contract, and exit codes.

Project-aware commands discover the nearest `datacoolie.yml` unless
`--project-dir` is supplied. The CLI does not execute a dataflow; use a
project-owned Python script or notebook for Driver and engine lifecycle.

Use `-h` or `--help` after any command or subcommand to print that parser's
usage and exit. Global `--version` is root-only; `dc --version` works, while a
command-local form such as `dc validate --version` is rejected. `--format` may
be placed before or after the command, but it controls only CLI rendering and
not metadata serialization.

## `dc init`

Create an empty DataCoolie project, its component roots, and the latest
canonical `AGENTS.md`.

```text
dc init [PATH] [--name NAME] [--env ENV]... [--config FILE]
        [--format text|json]
```

`datacoolie init` is identical.

| Parameter | Description |
|---|---|
| `PATH` | Directory to create. Defaults to `.`. It may be new or empty; a non-empty directory is rejected. |
| `--name NAME` | Project name. Defaults to the target directory name. |
| `--env ENV` | Declare an environment. Repeat for multiple environments. Defaults to `dev` when no seed is supplied. |
| `--config FILE` | Use a complete YAML configuration seed instead of the default configuration. The seed still must satisfy the `datacoolie.yml` contract. |
| `--format text\|json` | Select CLI result rendering. |

`--name` and `--env` must not conflict with values in `--config`. Component
paths in a seed are respected. SQL and functions may each be one entry or a
list of entries; every entry uses the singular `path` key.

With the default configuration, initialization creates:

```text
datacoolie.yml
AGENTS.md
README.md
.gitignore
metadata/connections.json
metadata/schema_hints.json
metadata/dataflows/.gitkeep
metadata/environments/.gitkeep
sql/.gitkeep
functions/.gitkeep
runners/dev/.gitkeep
```

The metadata marker files contain empty section wrappers. No sample dataflow,
SQL query, runnable function, or runner is generated. `runners/<env>` is a
fixed project-tooling directory and receives one empty marker per configured
environment. `AGENTS.md` is downloaded from
the [canonical repository source](https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/AGENTS.md).
The latest file is fetched before project files are written. A network failure
therefore leaves no newly initialized project; a later filesystem failure
triggers best-effort cleanup of files created by that invocation (pre-existing
files are never removed).

## `dc validate`

Validate a complete project, a metadata file/folder, or a built artifact. The
command is read-only and never starts a Driver, engine, platform SDK, secret
resolver, network connection, or user code.

```text
dc validate [--project-dir PATH] [--metadata-path PATH | --artifact-path PATH]
            [--env ENV]... [--only config|metadata|resources]...
            [--sql-base-path PATH] [--artifact-base-path PATH]
            [--format text|json]
```

`datacoolie validate` is identical.

| Parameter | Description |
|---|---|
| `--project-dir PATH` | Project directory or `datacoolie.yml` path. Without it, discover the nearest project. |
| `--metadata-path PATH` | Validate one metadata file or recursively scan one metadata directory. Mutually exclusive with `--artifact-path`. |
| `--artifact-path PATH` | Validate a build root, `current`, or an extracted environment artifact. Mutually exclusive with `--metadata-path`. |
| `--env ENV` | Select environments in project mode; repeatable. Standalone metadata/artifact validation accepts at most one. |
| `--only SCOPE` | Limit project checks to `config`, `metadata`, or `resources`; repeatable. |
| `--sql-base-path PATH` | Base directory for shorthand SQL file references when validating standalone metadata. Repeat for multiple roots; with multiple roots the query must be prefixed by the root folder name. |
| `--artifact-base-path PATH` | Artifact root for explicit `artifact:/...` references and, when no SQL roots are supplied, ordinary relative SQL references in standalone metadata. |
| `--format text\|json` | Select CLI result rendering. |

Without a standalone path, validation checks project configuration, configured
component directories, all selected environment overlays, metadata model
constraints, identities, and SQL file references when a corresponding base is
known. `--only config`, `--only metadata`, and `--only resources` can reduce
that scope. The target boundary is:

| Target | Checks performed | Explicitly out of scope |
|---|---|---|
| Project (default) | Config, selected overlays, metadata schema/models/identities, configured resources, runners, and query references | Driver/engine/provider startup, SQL execution, secrets, network, and unselected `--only` scopes |
| Standalone `--metadata-path` | Decode, schema, model/identity checks, and query-reference checks when a SQL/artifact root is supplied | Project config, resource existence, other environments, and runtime readiness (`data.details.limited_scope: true`) |
| Standalone `--artifact-path` at `.builds/current` | Build manifest, every selected environment, inventory/hash checks, and comparison with `.builds/artifacts/<build_id>` | Driver/engine/provider startup and runtime execution |
| Standalone `--artifact-path` at `.builds/artifacts/<build_id>` | Retained build manifest, environment metadata/resources, inventory and hashes | Comparison with `current`; Driver/engine/provider startup and runtime execution |
| Standalone `--artifact-path` at an environment directory or manifest-less tree | Environment metadata/model checks and safe tree checks where available | Parent build history and root inventory identity (`data.details.limited_scope: true`) |

`--only` is valid only for the default project target. Root overrides are valid
only with standalone metadata. A successful limited-scope report certifies only
the checks listed in `data.checks`; inspect `data.details.not_checked` before
treating it as a release gate.

An initialized project with no dataflows is valid but reports a warning. A
warning does not produce a non-zero result; malformed metadata, missing files,
unsafe paths, or a failed artifact checksum do.

When `--metadata-path` is used, the report appends `project-config`,
`resource-existence`, and `other-environments` to any service-level skipped
checks. An environment overlay requires both `--env` and `--project-dir`.

Metadata validation runs in a fixed order: decode and normalize supported
representations, validate the project-owned metadata JSON Schema, construct
runtime models and cross-entity references, then check SQL resources when a
base path is available. JSON output reports the selected `framework_version`,
effective `schema_version`, and immutable `schema_url`. A structural schema
failure stops model and resource checks for that document; the skipped stages
remain visible in `data.details.not_checked`. Query validation classifies a string
as a file reference by path shape, checks safe existence/readability and
non-empty content when a root is supplied, and emits a warning rather than
inventing a root when no base is available. It does not parse or execute SQL.
Validation never starts a Driver, engine, platform SDK, secret resolver,
network connection, user function, or SQL query.

## `dc inspect`

Produce compact, read-only reports. Inspection redacts sensitive-looking
values and does not resolve secrets or instantiate plugin SDKs.

```text
dc inspect [SUBCOMMAND] [--project-dir PATH] [--format text|json]
```

`datacoolie inspect` is identical. If no subcommand is supplied, the command
prints a project summary.

### `inspect config`

```text
dc inspect config [--project-dir PATH] [--env ENV] [--format text|json]
```

Shows the effective `datacoolie.yml`, its config path, resolved absolute
component roots, environment intent, and origin information. `--env` limits
the environment section.

### `inspect metadata`

```text
dc inspect metadata [--project-dir PATH] [--metadata-path PATH]
                    [--env ENV]
                    [--section connections|dataflows|schema_hints]
                    [--name NAME] [--stage STAGE] [--full]
                    [--format text|json]
```

The report lists discovered document paths, formats, section counts, and
optional filtered items. `--metadata-path` permits portable inspection without
a project. `--env` requires `--project-dir` when an overlay must be resolved.
Filters have these prerequisites:

| Filter | Allowed section | Effect |
|---|---|---|
| `--section` | `connections`, `dataflows`, or `schema_hints` | Select the inventory section |
| `--name NAME` | `connections` or `dataflows` only | Exact name match; requires `--section` |
| `--stage STAGE` | `dataflows` only | Exact stage match; requires `--section` |
| `--full` | Any selected section | Include the redacted item payload; requires `--section` |

Without `--full`, items are compact identity fields. Inspection is an inventory
view, not schema/model validation, query-resource verification, or runtime
readiness; key-based redaction is not a guarantee that every sensitive value is
recognized.

### `inspect capabilities`

```text
dc inspect capabilities [--format text|json]
```

Lists registered engines, platforms, sources, destinations, transformers, and
secret resolvers, plus the installed DataCoolie version. It reads registry
names only; it does not prove optional extras are installed, credentials are
valid, connectivity works, or a Driver can start.

### `inspect artifact`

```text
dc inspect artifact [--project-dir PATH] [--artifact-path PATH]
                    [--format text|json]
```

Reports build ID, content digest, project/environment records, function
packaging information, and artifact count from `manifest.json`. If
`--artifact-path` is omitted, the project `.builds/current` directory is used.
An extracted environment or artifact without a manifest is reported with
`limited_scope: true` and without an integrity identity. Inspection reads and
summarizes what is present; use `dc validate` for inventory/hash verification
and current/history comparison.

## `dc build`

Build every declared environment together. The command stages metadata,
copies SQL, packages functions, validates the result, writes an integrity
manifest, and publishes a `current` projection only after the build succeeds.

```text
dc build [--project-dir PATH]
         [--metadata-layout single|split|preserve]
         [--metadata-format json|yaml|excel|preserve]
         [--dry-run] [--format text|json]
```

`datacoolie build` is identical.

| Parameter | Description |
|---|---|
| `--project-dir PATH` | Project directory or `datacoolie.yml` path. Without it, discover the nearest project. |
| `--metadata-layout LAYOUT` | One-build override for `single`, `split`, or `preserve`. |
| `--metadata-format FORMAT` | One-build override for `json`, `yaml`, `excel`, or `preserve` (only with `preserve` layout). |
| `--dry-run` | Validate and calculate the input digest without packaging or publishing artifacts. |
| `--format text\|json` | Select CLI result rendering. |

The project configuration remains the source of truth after the command
returns; layout and format flags affect only that build. Each functions entry
uses `packaging`: `auto`, `copy`, `wheel`, or `zip`. `auto` selects a wheel when
the root declares a Python build backend, a wrapped ZIP when the root itself
has `__init__.py`, and a source-tree copy otherwise. Nested packages do not
implicitly change the packaging mode of their parent root. An explicit `wheel`
requires a valid backend and the build dependency; explicit `zip` requires a
non-empty root and preserves the root import shape. A root package is wrapped
so its parent directory is importable; a nested `loaders/__init__.py` does not
turn an otherwise source-tree root into a ZIP. A malformed or incomplete
`pyproject.toml` is an error; `auto` does not silently fall back to copying it.

Normal builds may execute the configured Python wheel backend (`python -m
build`) and therefore may resolve backend build dependencies. They still never
execute runners, user functions, a Driver, or a dataflow. `--dry-run` makes the
packaging decision and reports the backend-dependent step as
`not_performed`; it cannot guarantee a later backend or permission success.

Each environment has the configured metadata, SQL, functions paths, and (when
present) `runners/<env>` files plus its own `manifest.json` describing the
environment artifact. Runner files keep their names and bytes; they are never
executed or rewritten by the CLI. A runner root or environment may be absent.
The build root has one `manifest.json`; `current/` is an exact copy of the
selected build and has the same root manifest. All environments are included
in one build ID—there is no per-environment build mode. A failed build leaves
the previous `current` projection unchanged. If an existing build ID is found,
the publisher reuses it only when the recorded input digest, layout/format and
content digests match; a same-ID mismatch is an error. This is collision
protection, not a general content cache or a reproducible-wheel guarantee.
`deployment_path` is recorded for an explicit external upload workflow;
`dc build` does not upload. See the [release handoff](project.md#release-handoff-external-upload-workflow)
for the ordering and partial-failure boundary.

`--dry-run` performs the same local discovery, metadata/environment validation,
input digest calculation, output codec checks, and function packaging decision
as a normal build. Its result includes a secret-free `plan` with component
source paths, environment-relative output paths, source file/entity counts,
effective function modes, and the build/current destinations. It also includes
`not_performed` markers for metadata serialization/round-trip, function
packaging, assembled-artifact verification, and publication. It never creates
`.builds`, staging files, locks, package outputs, or runtime data. A successful
preview cannot guarantee a later backend build, permission, or changing-input
success.

## `dc metadata convert`

Convert one metadata document without loading a project, merging environment
overlays, resolving queries, or changing metadata identity.

```text
dc metadata convert --input FILE --output FILE
                    [--to json|yaml|excel] [--overwrite]
                    [--format text|json]
```

`datacoolie metadata convert` is identical.

| Parameter | Description |
|---|---|
| `--input FILE` | Required source document. The suffix must be `.json`, `.yaml`, `.yml`, or `.xlsx`. |
| `--output FILE` | Required destination. Its suffix must match `--to` when `--to` is supplied. |
| `--to FORMAT` | Destination format: `json`, `yaml`, or `excel`. If omitted, infer it from the output suffix. |
| `--overwrite` | Allow replacement of an existing output. Without it, the command fails before changing the file. |
| `--format text\|json` | Select CLI result rendering. This is separate from `--to`. |

The destination parent is created when needed. The command writes through a
temporary file, decodes the result again, and compares semantic content before
replacing the destination. Nested section wrappers and unknown top-level
metadata fields are retained. Install `datacoolie[cli]` for YAML and
`datacoolie[metadata-excel]` for Excel support. Conversion checks representation
round-trip semantics only; it does not run the full project/schema/model/query
validation. Follow it with `dc validate --metadata-path <output>` when the
converted document is intended for a runtime or build.

## `dc agents update`

Download the latest repository guidance into the project's root `AGENTS.md`.
This command is separate from metadata and artifact preparation so a project
can refresh its instructions without rebuilding anything.

```text
dc agents update [--project-dir PATH] [--format text|json]
```

`datacoolie agents update` is identical.

| Parameter | Description |
|---|---|
| `--project-dir PATH` | Project directory or `datacoolie.yml` path. Without it, discover the nearest project. |
| `--format text\|json` | Select CLI result rendering. |

The source is
[`datacoolie/datacoolie/main/ai/AGENTS.md`](https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/AGENTS.md).
If the local file is already identical, the command reports `unchanged`.
When content changes, the old file is kept as a timestamped
`AGENTS.md.bak-*` sibling before replacement. If no local file exists, it is
created. A network or filesystem failure returns exit code `1` and does not
silently replace the file.

## Response fields and scope

The shared JSON envelope, exit codes, error codes, text/JSON streams, and
versioning rules are owned by the [CLI overview](index.md#shared-options). The
command-specific `data` object is intentionally conditional:

| Field | Commands | Meaning |
|---|---|---|
| `data.warnings` | `validate`, `build` | Non-fatal diagnostics; they do not change exit code `0`. |
| `data.details.not_checked` | `validate` | Service checks skipped because of an earlier failure or target scope. |
| `data.details.limited_scope` | standalone `validate` | The metadata or artifact target cannot certify project-wide or integrity checks. |
| `data.limited_scope` | `inspect artifact` on an extracted environment or manifest-less tree | Inspection is inventory-only and has no full build identity. |
| `data.details.current_comparison` | `validate --artifact-path` at `.builds/current` | Whether comparison was attempted and whether current matches the retained build. |
| `data.plan`, `data.not_performed` | `build --dry-run` | Planned paths/decisions and intentionally skipped writes/package/verification steps. |
| `data.status`, `data.build_id`, `data.build_path`, `data.current_path` | `build` | Publication outcome and immutable build identity/paths. |

For automation, inspect both the process exit code and the envelope's `ok` field.
The paths in the table above begin at `data`. A successful operation with a
limited-scope marker or omitted checks is not a full release certification.
Representative recipes:

```bash
# Portable metadata validation with two SQL roots.
dc validate --metadata-path ./project/metadata \
  --sql-base-path ./project/sql1 --sql-base-path ./project/sql2 --format json

# Filtered, redacted inventory.
dc inspect metadata --metadata-path ./project/metadata \
  --section dataflows --stage bronze2silver --full --format json

# Validate the current projection, including its retained-build comparison.
dc validate --artifact-path ./project/.builds/current --format json

# Validate a retained build root without requiring a current comparison.
dc validate --artifact-path ./project/.builds/artifacts/20260917-120000-abc123 --format json

# An environment-only path is useful for a portable handoff, but remains limited.
dc validate --artifact-path ./project/.builds/artifacts/20260917-120000-abc123/dev --format json

# Preview a new build; no .builds directory is created.
dc --project-dir ./project build --dry-run --format json

# Convert, then apply full metadata validation.
dc metadata convert --input metadata.yaml --output metadata.json
dc validate --metadata-path metadata.json --format json
```
