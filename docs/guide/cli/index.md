---
title: DataCoolie CLI
description: Portable commands for initializing, validating, inspecting, and building DataCoolie projects.
---

# DataCoolie CLI

The DataCoolie CLI prepares project inputs and build artifacts. It does not
execute a dataflow or a project runner. Execution stays in a project-owned
Python script or notebook so each project can configure its engine, register
custom tables, and choose its runtime providers.

## Install and invoke

Install the optional project-tooling dependencies:

```bash
pip install "datacoolie[cli]"
```

Both executable names are supported and are equivalent:

```bash
dc --help
datacoolie --help
```

They use the same command dispatcher, options, output contract, and exit
codes. The examples in this section use `dc`; replace it with `datacoolie`
without changing the rest of the command. Environments without an installed
console script can use the equivalent module form, `python -m datacoolie`.

## Shared options

The following options are available globally and on the commands that use
them. They may be written before the command or after it. Every parser also
accepts `-h`/`--help` for command-local usage; help exits without running the
operation. `--version` is root-only and must appear with no command.

| Option | Meaning |
|---|---|
| `--version` | Print the installed DataCoolie version and exit. Root-only; `dc validate --version` is invalid. |
| `--format text\|json` | Choose human-readable or machine-readable result output. The default is text on a TTY and JSON when stdout is not a TTY. |
| `--project-dir PATH` | Select a project directory or a `datacoolie.yml` file for project-aware commands. Without it, those commands discover the nearest marker by walking upward from the current directory. `init`, `metadata convert`, and `inspect capabilities` reject it. |

`--format` controls the CLI response, not the metadata encoding used by
`build` or `metadata convert`. Every JSON response uses the same envelope:

```json
{"schema_version": 1, "datacoolie_version": "{{ datacoolie_version }}", "ok": true, "data": {}}
```

Validation report fields are carried inside `data`; the Python validation API
itself still exposes its native `schema_version` and `ok` fields.

`datacoolie_version` identifies the installed package that produced the response,
matching `dc --version`. It is included in successful and failed JSON responses.
`schema_version` identifies the envelope contract independently of the package
release. Consumers must ignore unknown fields; compatible optional additions do
not increment the schema version. Older responses may omit `datacoolie_version`.

## Commands

| Command | Purpose |
|---|---|
| [`dc init`](commands.md#dc-init) | Create an empty project and fetch the current `AGENTS.md`. |
| [`dc validate`](commands.md#dc-validate) | Check a project, metadata path, or build artifact. |
| [`dc inspect`](commands.md#dc-inspect) | Produce redacted, read-only configuration, metadata, capability, or artifact reports. |
| [`dc build`](commands.md#dc-build) | Build every configured environment into one immutable artifact and update `current`. |
| [`dc metadata convert`](commands.md#dc-metadata-convert) | Convert one JSON, YAML, or Excel metadata document. |
| [`dc agents update`](commands.md#dc-agents-update) | Refresh a project's canonical `AGENTS.md`. |

If this is your first CLI session, follow the [CLI preparation walkthrough](quickstart.md)
with the downloadable artifact project. It stops after the build artifact is
validated, then links to the runtime guide for executing a dataflow.

Project configuration and the end-to-end workflow are documented in
[Project configuration and workflow](project.md). Full command parameters are
in the [command reference](commands.md), including the fields returned by each
operation and the limits of each validation scope.

## Scope and safety boundaries

- There is intentionally no `dc run` command. A project runner owns Driver
  construction and execution.
- CLI commands do not resolve secrets, open database/API connections,
  instantiate engines, execute runner files, or upload a build to an environment's
  `deployment_path`. A normal build can invoke a Python packaging backend for a
  functions wheel; that is packaging work, not Driver or dataflow execution.
  The explicit external upload handoff is documented in
  [Project configuration and workflow](project.md#release-handoff-external-upload-workflow).
- `validate`, `inspect`, and `build --dry-run` are read-only with respect to
  authored project inputs. A normal build writes `.builds/` and may run the
  configured wheel backend; it never runs a runner.
- `init` refuses a non-empty target and needs network access to fetch the
  canonical `AGENTS.md`. `agents update` keeps a timestamped backup when it
  replaces an existing file.

## Exit codes

| Code | Meaning |
|---:|---|
| `0` | The requested operation completed. Warnings do not change success. |
| `1` | The operation ran but validation, conversion, download, or build failed. |
| `2` | The invocation is invalid or a required project prerequisite is missing. |

For CI and AI-agent callers, prefer `--format json` and treat the exit code as
the primary success signal. Help and version remain ordinary textual info
commands even when JSON was requested. Other JSON failures use:

```json
{
  "schema_version": 1,
  "datacoolie_version": "{{ datacoolie_version }}",
  "ok": false,
  "data": null,
  "error": {"code": "validation.failed", "message": "..."}
}
```

When diagnostics are available, `data` contains the complete validation
report. Stable error codes include `usage.invalid`, `project.config_invalid`,
`validation.failed`, `dependency.missing`, `operation.failed`, and
`internal.error`. Text failures, usage messages, and other error reports are
written to stderr; successful text output is written to stdout.

The [command reference](commands.md#response-fields-and-scope) documents the
conditional fields under the envelope's `data` object. Validation reports use
`data.warnings`, `data.details.not_checked`, `data.details.limited_scope`, and
`data.details.current_comparison`; `inspect artifact` uses `data.limited_scope`;
build previews and builds use `data.plan`, `data.not_performed`, `data.status`,
and the build path fields.
