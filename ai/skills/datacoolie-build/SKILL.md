---
name: datacoolie-build
description: Build, modify, materialize, run, and verify DataCoolie projects. Use for workspace bootstrap, metadata authoring, environment overlays, capability checks, runners, notebooks, custom functions, narrow unsupported adapters, local tests, immutable builds, and project-owned build/CI automation. This is the sole implementation skill for pipeline and build artifacts; it does not discover sources, make material design decisions, provision infrastructure, or deploy releases.
---

# DataCoolie Build

## Outcome And Boundary

Turn current project intent into durable DataCoolie sources and an immutable
`.builds/artifacts/{build_id}` verified by executing the generated artifacts. Bootstrap only the
workspace structure required by the request; initialization is not a separate phase.

Own configuration, metadata, overlays, capability proof, runners/notebooks, functions, narrow
unsupported adapters, materialization, local execution, build evidence, and requested project-owned
automation. Return unknown source facts to discover, material decisions to design, missing resources
to provision with the exact requirements artifact and evidence, and deployment work to release.

Use the installed `datacoolie` package and public APIs. Resolve bundled resources relative to this
skill; generated projects must not depend on skill paths.

## Inputs And Gates

- Read the user request and only affected workspace sources.
- Use `architecture/current.md` when a new project or material contract requires it.
- When `architecture/current.md` exists, recompute its final-byte hash and reject a missing,
  malformed, or stale matching design receipt; reject misnamed receipts too. Architecture never
  self-declares an approval bypass.
- Require discovery evidence for every declared source in a new project. Use discovery artifacts
  only as authoring evidence; runtime code must not import them.
- Return to design before implementation if the requested change would alter a material contract.

## Resource Routing

| Need | Read or run |
|---|---|
| Build-tool dependencies | `scripts/requirements.txt`; add `requirements-excel.txt` only for Excel conversion |
| Workspace/config | `templates/project-structure.md`, `schemas/workspace-config.schema.json`, `scripts/validate_config.py` |
| Metadata fields and authoring | `references/schema-quick-reference.md`, `schemas/`, `scripts/validate.py` |
| Metadata import/merge/lint | `scripts/convert.py`, `scripts/merge.py`, `scripts/lint.py` |
| Built-in capability inventory | `scripts/inspect_capabilities.py`, `references/capability-catalog.md` |
| Native versus custom boundary | `references/framework-boundary.md` |
| Common entrypoint and normal run | `references/runner-contract.md`, `templates/runners/README.md`, matching template |
| Replay or maintenance extensions | load `references/runner-contract.md`, then `references/operations-contract.md` and matching templates |
| Immutable build, current pointer, and verification receipt | `scripts/materialize.py`, `scripts/validate_build.py`, `schemas/current-build.schema.json`, `schemas/build-verification-receipt.schema.json` |
| Requested project automation | `scripts/render_automation.py` |

Load only resources needed for the current outcome. Exact metadata layouts, runner names and
parameters, stage semantics, operation behavior, build identity, and manifest rules live in
the routed build resources rather than this prompt.

## Decision Workflow

### 1. Bind the environment

Keep `config.yaml` limited to project identity and environment-to-platform mapping. Validate it
against installed platform registrations. Engines, stages, runtime paths, secrets, and gate state
do not belong there.

### 2. Prove capability fit

Evaluate the installed combination of source, authentication, engine, transforms, destination,
load, platform, and dependencies. Inspect the installed registries before deciding; a missing
optional dependency is setup work, not evidence that a registered capability is unsupported. Use
metadata and `DataCoolieDriver.run(...)` for a supported path. Add custom code only around a
verified unsupported boundary, record the evidence, and leave the supported remainder native.

### 3. Author durable sources

Use the canonical metadata contract and environment overlays; do not clone full metadata per
environment. Create only required normal, replay, or maintenance entrypoints. The selected file
fixes platform, engine, provider, and operation; runtime inputs carry only values allowed by the
runner and operation contracts. Keep credentials in environment or platform secret services.

Author exact source-observed types once in `metadata/schema_hints.json`. Put a hint directly in a
dataflow transform only when it is an intentional dataflow-specific cast or override, not a copy of
the source schema. Select the simplest native source address using the framework-boundary order.

### 4. Run fast source checks

Validate config and resolved metadata, lint affected paths, parse/compile entrypoints, and unit-test
helpers directly. These checks give fast feedback but do not prove the generated build.

### 5. Materialize and verify

Run `scripts/materialize.py`; it validates its inputs, renders environment slices, packages optional
functions, writes the manifest and checksums under `.builds/artifacts/{build_id}`, verifies the
immutable bytes, and atomically updates `.builds/current/{env}.json` for each selected environment.
Never symlink or mutate build contents.

Execute the exact generated runner/notebook, resolved metadata, and functions artifact. Keep logs
and watermarks under persistent `.runtime/{env}/`. Apply the runner contract for normal runs and
the operations contract for replay or maintenance, including their mutation confirmations.

Resolve `current` for the normal latest-build test path or select an exact historical build ID, then
execute that generated runner/notebook, metadata, and functions artifact. Write a typed successful
or failed receipt under `.builds/evidence/{build_id}/{env}/{receipt_id}.json` and validate it against
the resolved build. Receipts and handoffs always contain the exact build ID. Release never consumes
the moving current pointer.

### 6. Add automation only when requested

Use `scripts/render_automation.py` only for requested reproducible project-owned build/CI entrypoints.
Generated automation works with the installed framework and project sources without installed
skills. Release owns consume-only deployment automation. Do not generate speculative automation.

## Output And Handoff

```text
{workspace}/config.yaml
{workspace}/metadata/
{workspace}/runners/
{workspace}/functions/                          # optional
{workspace}/automation/                         # optional
{workspace}/.builds/artifacts/{build_id}/manifest.json
{workspace}/.builds/artifacts/{build_id}/SHA256SUMS
{workspace}/.builds/artifacts/{build_id}/{env}/...
{workspace}/.builds/evidence/{build_id}/{env}/*.json
{workspace}/.builds/current/{env}.json
```

Release receives only the exact build ID, local build directory or immutable remote artifact
identity, manifest/checksums, target slice, and successful matching verification receipt. Build or
design approval never authorizes deployment. End with verification evidence, skipped checks, and
unresolved questions.
