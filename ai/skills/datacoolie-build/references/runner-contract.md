# DataCoolie Runner Contract

## Scope

- Read whenever authoring or verifying a Python or notebook entrypoint.
- Owns common entrypoint identity, runtime parameters, construction, generated-source behavior, and
  normal `run` stage passthrough semantics.
- Does not decide framework support or define metadata fields. Replay and maintenance additionally
  load `references/operations-contract.md`, which owns only their operation-specific extensions.

## Common identity

Durable and generated entrypoints encode fixed implementation identity:

```text
{run|replay|maintenance}_{platform}_{engine}[_{provider}].py|ipynb
```

Add the provider suffix only when bootstrap, authentication, session, or lifecycle code differs.
Environment is resolved during build and is not a runner parameter. One compatible environment may
materialize multiple engine runners; orchestration selects the exact file or notebook.

Use a separate operation-specific entrypoint instead of a runtime mode selector. The remainder of
this reference defines common behavior and normal `run`; the operations reference adds replay and
maintenance semantics.

## Common runtime parameters

A file-provider entrypoint accepts:

- `metadata_path`.
- Persistent `watermark_base_path`.
- Persistent `base_log_path`.
- One optional stage value when the operation supports stage selection.
- Only additional options owned by its selected operation and installed runtime.

Pass metadata, log, and watermark paths unchanged to the selected framework constructors. The
framework and platform own path interpretation and validation. Workspace layout guidance and build
receipt validation remain separate concerns. Another metadata provider may use a provider-specific
entrypoint and omit irrelevant file-provider parameters.

Notebooks expose equivalent values through the platform's parameter transport:

| Runtime | Stage transport |
|---|---|
| Local Python | one optional `--stage` string |
| Databricks | one named `STAGE` widget string |
| Fabric | one `STAGE` parameter-cell string |
| AWS Glue | one optional named `STAGE` job argument |

Pass the stage value unchanged. Do not split comma strings, decode a stage list, accept repeated
stage arguments, or create a stage plan in the runner. Operation-specific complex values may still
use the documented `*_JSON` convention. Decode those before constructing DataCoolie components.
The file fixes platform, engine, provider, and operation; do not expose them again as runtime
selectors.

The platform environment or job must install DataCoolie and attach any functions artifact before
the runner starts. Executable runners may report the installed version but must not install or
restart their own runtime. Provision owns platform readiness; release owns artifact attachment and
deployment configuration.

## Construction boundary

Every concrete entrypoint performs only:

1. Platform parameter transport and operation-specific decoding only.
2. Metadata-provider construction with explicit persistent watermark state when relevant.
3. Fixed platform, engine, provider, and session bootstrap.
4. Explicit base-log configuration.
5. DataCoolie driver construction.
6. Calls to the selected framework operation.

Load environment variables or platform secrets before DataCoolie resolves secret references. Local
`.env` loading is an optional local-launcher concern, not a universal cloud dependency. Capability
selection and custom-edge decisions belong to `references/framework-boundary.md`.

## Normal run stage passthrough

Pass the one received value unchanged to one call:

```python
driver.run(stage=stage)
```

- `--stage stage1,stage2` remains one string and one driver call; the framework owns its meaning.
- With no stage, invoke once with the transport default (`None` or an empty scalar); the framework
  preserves run-all behavior.
- Pass blank and non-blank scalar content unchanged; the framework owns its meaning.
- Sequential stage invocations belong to the external orchestrator calling the runner again.

## Durable and generated sources

```text
runners/run_{platform}_{engine}[_{provider}].ext
.builds/artifacts/{build_id}/{env}/runners/run_{platform}_{engine}[_{provider}].ext
```

Build copies or renders durable sources; it never symlinks them. Correct generated behavior by
editing the durable source and materializing a new build ID.

## Common verification

- Filename platform matches the selected environment binding.
- Engine, provider, and operation are fixed by entrypoint identity.
- No runtime `--env`, platform, engine, provider, or operation selector exists.
- Notebook/job parameters are read through the named platform transport and stage is passed
  unchanged to one framework operation.
- The runner does not install packages or restart its runtime.
- Metadata/provider parameters are explicit and relevant.
- Metadata, log, watermark, and stage values reach framework APIs without runner-side validation.
- Supported paths construct DataCoolie components and call the selected driver API.
- The exact generated entrypoint and metadata are executed; hashes match the build manifest.
- Validate one explicitly supplied receipt with `scripts/validate_build.py`; receipt field semantics
  belong to its schema and validator, not this reference.
