---
name: datacoolie-build
description: Build, modify, run, and verify DataCoolie projects. The sole implementation skill for metadata, runners, functions, immutable builds, and requested build automation. Route source discovery, material design, infrastructure, and deployment to their owners.
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

Use installed `datacoolie` public APIs. Resolve bundled resources relative to this skill; generated projects must not depend on skill paths.

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
| Metadata fields, paths, hints, audit columns, or incremental/file routing | `references/schema-quick-reference.md` (matching section), `schemas/`, `scripts/validate.py` |
| Dataflow dependencies, combined stages, concurrency, or job scale-out | `references/orchestration-contract.md` |
| Generated metadata layout | `templates/project-structure.md`, `scripts/materialize.py` |
| Metadata import/merge/lint | `scripts/convert.py`, `scripts/merge.py`, `scripts/lint.py` |
| Built-in capability inventory | `scripts/inspect_capabilities.py`, `references/capability-catalog.md` |
| Platform runtime, path, credential, or extra | `references/platform-contract.md`, then the matching runner template |
| Native versus custom boundary or source expression choice | `references/framework-boundary.md` |
| Python-function source or artifact | `references/python-functions-contract.md`, `scripts/validate_functions.py` |
| Common entrypoint and normal run | `references/runner-contract.md`, `templates/runners/README.md`, matching template |
| Polars Delta/Iceberg `source.query` | `references/polars-qualified-sql.md`, then `references/runner-contract.md` |
| Replay or maintenance extensions | load `references/runner-contract.md`, then `references/operations-contract.md` and matching templates |
| Immutable build, runnable current projection, and verification receipt | `scripts/materialize.py`, `scripts/validate_build.py`, `schemas/current-build.schema.json`, `schemas/build-verification-receipt.schema.json` |
| Requested project automation | `scripts/render_automation.py` |

Load only resources needed for the current outcome. Exact metadata layouts, runner names and
parameters, stage semantics, operation behavior, build identity, and manifest rules live in
the routed build resources rather than this prompt.

## Decision Workflow

### 1. Bind the environment

Keep `config.yaml` limited to project identity and environment-to-platform mapping. Validate it
against installed platform registrations. Engines, stages, runtime paths, secrets, and gate state
do not belong there. Environment names are project-defined non-empty values, not a fixed
`dev/test/prod` vocabulary.
Materialization always produces one complete snapshot of every configured environment. Environment
selection belongs to run, test-receipt, and release slices, never to Build scope or build identity.

### 2. Prove capability fit

Evaluate the installed combination of source, authentication, engine, transforms, destination,
load, platform, and dependencies. Inspect the installed registries before deciding; a missing
optional dependency is setup work, not evidence that a registered capability is unsupported. Use
metadata and `DataCoolieDriver.run(...)` for a supported path. Add custom code only around a
verified unsupported boundary, record the evidence, and leave the supported remainder native.
When platform execution context, path, credentials, or dependencies affect the combination, load
`references/platform-contract.md`; platform is the adapter and does not imply the execution host.

### 3. Author durable sources

Use canonical metadata and environment overlays, not full per-environment clones. Read
`templates/project-structure.md` and the matching sections of `references/schema-quick-reference.md`
for layout, selector precedence, local/global hints, source addressing, audit columns, and
incremental/file routing. Choose direct source addressing, a bounded query, or a verified custom
edge using `references/framework-boundary.md`; do not rediscover these contracts by trial and error.
Create only required normal, replay, or maintenance entrypoints. Their files fix platform, engine,
provider, and operation; runtime inputs follow the routed runner/operation contracts.
Keep credentials in environment or platform secret services.

Resolve metadata, log, and watermark paths inside the environment's approved persistent control
namespace and pass them unchanged. Deployed metadata is a build-scoped immutable projection; logs
and watermarks remain mutable and outside build artifacts. For a cloud platform used by an
on-premises runner, select the external runtime explicitly and keep the actual execution host
separate from the platform adapter.
Assume source query and action text can appear in framework logs. Do not embed secret literals;
apply the approved log classification, access, and retention policy to generated runtime paths.

For Polars Delta/Iceberg SQL, read `references/polars-qualified-sql.md` before runner bootstrap.

### 4. Run fast source checks

Validate config and resolved metadata, lint affected paths, parse/compile entrypoints, and unit-test
helpers directly. These checks give fast feedback but do not prove the generated build.

### 5. Materialize and verify

Run `scripts/materialize.py` to validate inputs, create the all-environment immutable build, and
replace `.builds/current` with its verified runnable projection. `templates/project-structure.md`
owns metadata layouts, fixed components, and manifest/projection contents; do not reproduce them
in runner code. Never symlink or mutate immutable artifact contents.

Always validate the immutable build, resolved metadata, exact runner, and optional functions
artifact. Execute the generated runner on the Build host when that host is compatible and the
approved check is safe; record the result as useful Build-host evidence, not target qualification.
Do not block an artifact-qualified receipt solely because the runner requires staging on its target
execution host. Release always qualifies the exact staged runner slice before activation.

Keep any Build-host logs and watermarks under persistent `.runtime/{env}/` or another approved
isolated test namespace. Apply the runner contract for normal runs and the operations contract for
replay or maintenance, including their mutation confirmations.

Execute and validate `.builds/current` directly for the normal latest-build path. Select
`.builds/artifacts/{build_id}` only for a historical version. Write a typed successful or failed
receipt under `.builds/evidence/{build_id}/{env}/{receipt_id}.json`, using the exact ID from
`current/build.json` when current was tested. Release never consumes the moving projection.

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
{workspace}/.builds/current/build.json
{workspace}/.builds/current/{env}/...
{workspace}/.builds/current/functions/            # when functions were packaged
```

Release may receive `current` as a convenience selector, but resolves `current/build.json` once and
then consumes only the exact build ID, canonical local build directory or immutable remote artifact
identity, manifest/checksums, target slice, and successful matching artifact-verification receipt.
The bundled schemas and validators own manifest/receipt versions and required checks, including
`generated-artifact-validation`. Build-host runtime execution is optional and never authorizes
target activation. Build current is never a transfer source or authorization identity. Build or
design approval never authorizes deployment. End with verification evidence, skipped checks, and
unresolved questions.
