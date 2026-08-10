# DataCoolie Runner Contract

## Scope

- Read whenever authoring or verifying a Python or notebook entrypoint.
- Owns common entrypoint identity, runtime parameters, construction, generated-source behavior, and
  normal `run` stage-plan semantics.
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
- Zero or more ordered stage groups when the operation supports stage selection.
- Only additional options owned by its selected operation and installed runtime.

Reject log or watermark paths inside `.builds/` because generated builds are immutable and
disposable. Another metadata provider may use a provider-specific entrypoint and omit irrelevant
file-provider parameters.

Notebooks expose equivalent values through the platform's parameter transport:

| Runtime | Scalars | Structured values |
|---|---|---|
| Local Python | `argparse` values | repeated `--stage` occurrences |
| Databricks | named `dbutils.widgets` strings | JSON strings decoded after widget reads |
| Fabric | tagged parameter-cell values | `*_JSON` strings decoded after the parameter cell |
| AWS Glue | named job arguments | optional `STAGE_GROUPS_JSON`, default `[]` |

Use `STAGE_GROUPS_JSON` for notebook/job stage plans. Operation-specific complex values follow the
same `*_JSON` convention. Decode and validate values before constructing DataCoolie components.
The file fixes platform, engine, provider, and operation; do not expose them again as runtime
selectors.

The platform environment or job must install DataCoolie and attach any functions artifact before
the runner starts. Executable runners may report the installed version but must not install or
restart their own runtime. Provision owns platform readiness; release owns artifact attachment and
deployment configuration.

## Construction boundary

Every concrete entrypoint performs only:

1. Parameter decoding and path validation.
2. Metadata-provider construction with explicit persistent watermark state when relevant.
3. Fixed platform, engine, provider, and session bootstrap.
4. Explicit base-log configuration.
5. DataCoolie driver construction.
6. Calls to the selected framework operation.

Load environment variables or platform secrets before DataCoolie resolves secret references. Local
`.env` loading is an optional local-launcher concern, not a universal cloud dependency. Capability
selection and custom-edge decisions belong to `references/framework-boundary.md`.

## Normal run stage plan

```text
StageGroup = str | list[str]
StagePlan  = list[StageGroup]
```

Pass each group unchanged to one call:

```python
for stage_group in stage_plan:
    driver.run(stage=stage_group)
```

- `--stage stage1,stage2` is one string group and one driver call.
- `--stage stage1 stage2` is one list group and one driver call.
- Repeating `--stage` creates sequential groups in occurrence order.
- Do not split comma strings, flatten nested groups, sort stages, or call list members separately.
- Reject blank strings, empty lists, and blank members before constructing the driver.
- Stop after a failed group; later groups must not start.
- With no group, call `driver.run(stage=None)` once to preserve run-all behavior.

Python may use `argparse` with `action="append"` and `nargs="+"`, normalizing a one-token occurrence
to `str` and a multi-token occurrence to `list[str]`.

Notebook equivalent:

```python
STAGE_GROUPS = [
    "stage1,stage2",
    "stage3",
    ["stage4", "stage5"],
]
```

## Durable and generated sources

```text
runners/run_{platform}_{engine}[_{provider}].ext
.builds/{build_id}/{env}/runners/run_{platform}_{engine}[_{provider}].ext
```

Build copies or renders durable sources; it never symlinks them. Correct generated behavior by
editing the durable source and materializing a new build ID.

## Common verification

- Filename platform matches the selected environment binding.
- Engine, provider, and operation are fixed by entrypoint identity.
- No runtime `--env`, platform, engine, provider, or operation selector exists.
- Notebook/job parameters are read through the named platform transport and complex values are
  decoded from JSON without changing stage grouping or order.
- The runner does not install packages or restart its runtime.
- Metadata/provider parameters are explicit and relevant.
- Log and watermark paths remain persistent and outside `.builds/`.
- Supported paths construct DataCoolie components and call the selected driver API.
- The exact generated entrypoint and metadata are executed; hashes match the build manifest.
- Validate one explicitly supplied receipt with `scripts/validate_build.py`; receipt field semantics
  belong to its schema and validator, not this reference.
