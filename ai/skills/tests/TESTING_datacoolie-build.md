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
- A supported path remains metadata-driven and calls `DataCoolieDriver.run(...)`.
- One environment can materialize multiple engine-specific runners.
- Local, Databricks, Fabric, and Glue runners pass one optional stage value unchanged to one
  framework operation; no runner creates a stage plan or accepts repeated stage arguments.
- Platform parameters preserve generated notebook bytes; runners pass stage and path values to the
  framework without content validation or normalization.
- Executable notebooks never install packages or restart their runtime; provision/release attaches
  verified dependencies before execution.
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
- Subset builds validate and hash only selected environment bindings/overlays while retaining shared
  metadata, functions, runner, design, framework-version, and tooling identity.
- A typed receipt matches the exact generated environment, runner, metadata, optional functions,
  hashes, runtime paths, and timestamps; failed or mismatched receipts cannot satisfy release.
- Release consumes an exact build ID and explicit receipt path, never `current` or latest evidence.
- Integration tests execute `.builds/artifacts/{build_id}` while logs/watermarks remain under
  `.runtime/`; receipts remain under `.builds/evidence/`.
- `automation/` is rendered only for explicit CI/reproducible-build scope.
- Metadata lint covers clean, warning, and input-error exit paths; conversion covers native Excel
  round-trip and flattened transform fields.

## Unresolved questions

- None.
