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
- Repeated stage groups preserve order and stop after failure.
- Databricks widgets and Fabric/Glue JSON parameters preserve structured stage plans without
  editing generated notebook bytes; blank stages fail before driver construction.
- Executable notebooks never install packages or restart their runtime; provision/release attaches
  verified dependencies before execution.
- Replay templates preserve ordered stage selections, `[start, end)` and chunk parameters, call
  `load_dataflows`/`run_replay`, and require separate confirmation before saving watermarks.
- Maintenance templates use read-only target inspection, call `run_maintenance` only after explicit
  confirmation, expose exact non-secret physical targets and retention/operation/connection
  controls, and make no `dry_run` safety claim.
- Normal, replay, and maintenance entrypoints all materialize by fixed operation/platform/engine
  identity and appear in manifest checksums.
- Equal inputs reuse a verified build; changed inputs create a new build ID.
- New build IDs use a UTC `YYMMDD` prefix plus 12 content-digest characters; manifests retain the
  full digest and verification rejects identity/date/collision mismatches.
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
- Release consumes an explicit receipt path and never searches for latest evidence.
- Integration tests execute `.builds/{build_id}` while logs/watermarks remain under `.runtime/`.
- `automation/` is rendered only for explicit CI/reproducible-build scope.
- Metadata lint covers clean, warning, and input-error exit paths; conversion covers native Excel
  round-trip and flattened transform fields.

## Unresolved questions

- None.
