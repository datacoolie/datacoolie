# Testing datacoolie-build

Run:

```bash
python ai/skills/tests/run_build.py
python -m pytest -o addopts='' ai/skills/tests/unit -q
```

For a fast build-owned selection, use `python ai/skills/tests/run_all.py build`. The selected command
includes shared workflow/harness tests but does not collect unrelated discover, provision, or release
unit modules.

Verify:

- Canonical modular JSON supports the five approved dataflow fragment layouts, requires explicit
  content `stage`, rejects duplicate names, and resolves environment overlays correctly.
- Environment overlays support ordered exact-subset selector patches over canonical connections,
  dataflows, and column-grain global schema hints. Selectors use the unchanged canonical snapshot,
  fail on zero matches, and exact keyed overrides win last.
- Global `schema_hints` selectors and dataflow-local `transform.schema_hints` patches remain
  isolated; local hints merge by `column_name` while other patched arrays replace.
- A supported path remains metadata-driven and calls `DataCoolieDriver.run(...)`.
- Polars qualified Delta/Iceberg SQL remains a normal metadata `source.query`; source-native table
  registration runs on the same engine before driver construction, indexes lazily by default, and
  is omitted from non-SQL Polars runners.
- Platform selection distinguishes the DataCoolie adapter from the execution host. Fixed native
  Fabric/Databricks notebooks use explicit native modes, while external Python templates use SDK
  modes, ambient/default authentication, and the matching platform extras.
- Platform path checks cover qualified Fabric ABFS(S)/HTTPS paths, portable Databricks Volumes,
  AWS/MinIO S3 addressing, and Local sandboxed paths without runner-owned parsing or normalization.
- Metadata-only checks use existence/stat/list operations; `read_file` and `read_bytes` remain
  full-content operations and are not used as head probes.
- One environment can materialize multiple engine-specific runners.
- Local, Databricks, Fabric, and Glue runners pass one optional stage value unchanged to one
  framework operation; no runner creates a stage plan or accepts repeated stage arguments.
- Platform parameters preserve generated notebook bytes; runners pass stage and path values to the
  framework without content validation or normalization.
- Executable notebooks never install packages or restart their runtime; provision/release attaches
  verified dependencies before execution.
- A build emits no Python-function artifact or exactly one architecture-selected WHL/ZIP artifact;
  it never emits both. ZIPs have one project-specific package root and wheels are pure Python.
- Function validation imports only the generated artifact, enforces the metadata prefix and
  callable signature, and cannot pass through workspace authoring source.
- Function-capable runners use the fixed manifest import prefix in `allowed_function_prefixes`;
  no-function runners render an empty allowlist.
- Replay templates pass one stage unchanged, preserve numeric boundary types lost by text-only
  transports, decode the serialized chunk value, call `load_dataflows`/`run_replay`, and require
  separate confirmation before saving watermarks. Framework execution owns replay interval and
  range validation.
- Maintenance templates call `run_maintenance` once after explicit confirmation, expose only
  framework inputs, delegate target selection, deduplication, dispatch, logging, connection, and
  numeric constraints to DataCoolie, and retain the at-least-one-operation guard. They do not add
  preview, inspection, scheduling, or `dry_run` behavior.
- Normal, replay, and maintenance entrypoints all materialize by fixed operation/platform/engine
  identity and appear in manifest checksums.
- Every materialization is time-addressed; only byte-identical same-second output may reuse an ID.
- Build IDs use UTC `YYMMDD-HHMMSS` plus 12 content-digest characters; manifests retain the full
  digest and verification rejects identity/date/time/collision mismatches.
- Materialization replaces `.builds/current` with a directly runnable, byte-verified projection of
  the whole selected build. `current/build.json` records its exact source ID; historical tests use
  an explicit artifact build ID.
- Checksums reject mutation and no generated file is a symlink.
- Build-tool dependencies are explicit, schema resolution is bundled-only, and generated automation
  carries its dependency manifest, build-owned schemas, and verification tooling without sibling
  skill resources.
- Capability inspection reports installed version, requirements, entry points, and all six registry
  groups without connection values or secrets.
- Every build validates and hashes all configured environment bindings and overlays, and both the
  immutable artifact and current projection contain every environment.
- A typed v4 receipt matches the exact generated environment, runner, metadata, singular optional
  function artifact, hashes, runtime paths, and timestamps. It requires artifact validation and
  artifact-only function import when applicable; Build-host runtime execution is optional evidence.
  Failed or mismatched receipts cannot satisfy release staging.
- Release consumes an exact build ID and explicit receipt path, never `current` or latest evidence.
- Integration tests execute `.builds/artifacts/{build_id}` while logs/watermarks remain under
  `.runtime/`; receipts remain under `.builds/evidence/`.
- `automation/` is rendered only for explicit CI/reproducible-build scope.
- Metadata lint covers clean, warning, and input-error exit paths; conversion covers native Excel
  round-trip and flattened transform fields.

## Unresolved questions

- None.
