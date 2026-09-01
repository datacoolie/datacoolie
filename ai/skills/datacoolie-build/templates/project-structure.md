# Canonical DataCoolie Workspace

Create only directories needed by the active request. Workspace bootstrap is part of
`datacoolie-build`; do not create an initialization phase or project-management journal tree.

## Durable sources

```text
{workspace_name}/
├── AGENTS.md
├── config.yaml
├── architecture/
│   └── current.md
├── discover/                              # optional evidence; never runtime source
├── metadata/
│   ├── connections.json
│   ├── schema_hints.json                  # optional
│   ├── dataflows/
│   │   └── {stage}.json                   # default authoring partition
│   └── environments/
│       └── {env}.json                     # optional overlay
├── runners/
│   ├── run_{platform}_{engine}[_{provider}].py|ipynb
│   ├── replay_{platform}_{engine}[_{provider}].py|ipynb       # when needed
│   └── maintenance_{platform}_{engine}[_{provider}].py|ipynb  # when needed
├── functions/                             # optional authoring source for one WHL or ZIP
├── automation/                            # optional, only for CI/reproducible build
└── provision/                             # optional, only for infrastructure scope
```

`architecture/`, `discover/`, `functions/`, `automation/`, and `provision/` are conditional. Do not
create empty placeholder trees. Function authoring uses the single-format layout selected in
`references/python-functions-contract.md`; generated artifacts live only under build `functions/`.

## Derived and runtime state

```text
{workspace_name}/
├── .builds/
│   ├── artifacts/
│   │   └── {YYMMDD-HHMMSS}-{12-char-content-digest}/
│   │       ├── manifest.json
│   │       ├── SHA256SUMS
│   │       ├── {env}/
│   │       │   ├── metadata/             # selected one-, two-, or three-file projection
│   │       │   └── runners/
│   │       └── functions/                 # only when custom functions exist
│   ├── evidence/
│   │   └── {build_id}/{env}/{receipt_id}.json
│   └── current/
│       ├── build.json                  # exact source build ID
│       ├── {env}/
│       │   ├── metadata/
│       │   └── runners/
│       └── functions/                  # only when the build contains it
├── .runtime/
│   └── {env}/
│       ├── logs/
│       └── watermarks/
├── .approvals/                            # only required approvals
└── .releases/
```

`.builds/artifacts/` contains materialized immutable copies, never symlinks. `.builds/current/`
copies the selected build's runtime files with the same relative layout, omits `manifest.json` and
`SHA256SUMS`, and adds only `build.json` for provenance. It is disposable and regenerated as a
whole. Durable corrections always return to metadata, runners, functions, or automation source.
Mutable log and watermark state remains under `.runtime/`.

## Workspace AGENTS.md

Preserve an existing workspace `AGENTS.md` unless the user asks to update it. When creating an
entrypoint, use the version-pinned canonical DataCoolie workflow rather than a moving branch.

## config.yaml

```yaml
schema_version: 1

project:
  name: example
  workspace_name: example_dcws

environments:
  dev:
    platform: local
```

Only project identity and environment-to-platform mapping belong here. Validate with the bundled
`scripts/validate_config.py` and installed platform registry.

Do not add:

- Engine or stage routing.
- Metadata, log, watermark, or generated paths.
- Secrets or connection values.
- Approval/gate state.
- Artifact directory overrides.

## Canonical metadata authoring

Use modular JSON only:

```text
metadata/connections.json
metadata/schema_hints.json
metadata/dataflows.json
metadata/dataflows/{branch}.json
metadata/dataflows/{stage}.json            # default
metadata/dataflows/{branch}/{stage}.json
metadata/dataflows/{stage}/{dataflow}.json
metadata/environments/{env}.json
```

These are organizational partitions of one dataflow contract, not different metadata formats.
Every dataflow declares a non-empty `name` and `stage`; content `stage` is the sole runtime source
of truth because branch/stage and stage/dataflow paths are structurally ambiguous. The merger reads
optional `metadata/dataflows.json` plus JSON recursively under `metadata/dataflows/`, accepts one
object, an array, or an object containing `dataflows`, and rejects duplicate names globally.

Default to `dataflows/{stage}.json`. Use a branch file to group related stages, a branch/stage tree
for large multi-domain projects, or stage/dataflow sharding to reduce conflicts in a large stage.
Paths never infer or override runtime stage.

Environment files may contain ordered `patches` plus exact keyed overrides for `connections`,
`dataflows`, and global `schema_hints`. Do not maintain a full metadata clone per environment.

Use patches for one change shared by a selected group and exact keyed sections for one named
entity, additions, or final exceptions:

```json
{
  "patches": [
    {
      "match": {
        "type": "dataflows",
        "where": {"processing_mode": "batch", "source": {"connection_name": "source_a"}}
      },
      "patch": {"destination": {"connection_name": "target_a"}}
    }
  ],
  "dataflows": [
    {"name": "exception_flow", "destination": {"connection_name": "target_b"}}
  ]
}
```

Selector types are exactly `connections`, `dataflows`, and `schema_hints`. `where` is a non-empty
recursive exact-subset match with scalar or null leaves; arrays, regex, glob, expressions, and
deletion are unsupported. Empty nested objects are invalid. Every patch must match at least one
canonical entity. Selectors always inspect the unchanged canonical snapshot, so a prior patch and
an exact keyed addition cannot create targets for a later patch.

Resolution is deterministic:

```text
canonical < patches in array order < exact keyed overrides < resolved-metadata validation
```

Later patches win at the same leaf; exact keyed overrides always win last. JSON object key order
does not affect resolution. Patches cannot rename or create selected top-level entities. Exact
keyed sections retain their ability to add an entity.

`type: schema_hints` means global schema hints and selects flattened records at
connection/schema/table/column grain. To change local `transform.schema_hints`, select
`type: dataflows` and put the hints in that dataflow patch. Local hints merge by `column_name` so
unmentioned local hints remain; other arrays replace as a whole. See
`references/schema-quick-reference.md` for the schema-hint boundary.

Use the versioned schema bundled with the active build tooling for offline validation. Schema
updates arrive with refreshed build tooling; do not download a moving schema during a build.
Import/export tools may read other formats, but durable authoring resolves back to this layout.

## Runners and notebooks

Name each durable entrypoint by fixed implementation identity:

```text
runners/run_{platform}_{engine}.py
runners/run_{platform}_{engine}_{provider}.py
runners/replay_{platform}_{engine}[_{provider}].py|ipynb
runners/maintenance_{platform}_{engine}[_{provider}].py|ipynb
```

Add the provider suffix only when provider bootstrap changes code, authentication, session, or
lifecycle. Do not put environment in the name; environment binds to platform during build.
For example, external SDK variants may use `run_fabric_polars_azure_sdk.py` or
`run_databricks_polars_sdk.py`. The platform name identifies the DataCoolie adapter, not necessarily
the execution host; select runtime mode and parameter transport through
`references/platform-contract.md` and `references/runner-contract.md`.

Runtime parameters may include metadata/provider settings, base log path, watermark path, ordered
one stage value, and supported operational options. Do not accept environment, platform, or engine as
parameters of a concrete runner.

Keep normal execution, replay, and maintenance as separate entrypoints. Add only operations the
project uses. Apply the common `references/runner-contract.md` first, then the operation-specific
parameters and safety gates from `references/operations-contract.md`.

## Build materialization

Run the materializer resolved from the active `datacoolie-build` skill:

```text
python <datacoolie-build>/scripts/materialize.py \
  --workspace {workspace_name} \
  --metadata-layout single
```

Every materialization validates and emits all configured environments. Environment selection is a
runtime and release-slice concern, not a Build parameter. Use repeated `--runner-name` only when the
selected names provide at least one compatible runner for every configured environment; otherwise
materialize all compatible runners. Metadata layout is one explicit build input: `single` (default) writes
`metadata/metadata.json`; `split-connections` writes `metadata/dataflows.json` plus
`metadata/connections.json`; `split-all` also writes `metadata/schema_hints.json`. Generated
runners receive exactly the declared `FileProvider` paths.

The command returns a UTC creation-time-prefixed build ID such as
`260808-091011-a13f83c9d7e2`. The manifest keeps the full content digest. Each materialization creates
a time-addressed immutable folder; only byte-identical output colliding in the same second may be
reused. After artifact verification, materialization replaces `.builds/current` with the runnable
projection of that entire all-environment build. It never drops a configured environment based on
invocation parameters.

For the normal latest-build path, execute and validate `.builds/current` directly. Its
`.builds/current/build.json` identifies the canonical artifact used for drift checks and evidence
binding. To test an earlier
version, select `.builds/artifacts/{build_id}` directly. Both paths write
`.builds/evidence/{build_id}/{env}/{receipt_id}.json`; release consumes that exact ID, canonical
checksums, target slice, and successful receipt rather than the moving projection.

A release request may say `current`; Release validates `current/build.json`, pins that build ID,
and transfers from `.builds/artifacts/{build_id}`. It never uploads moving current bytes. The
deployment target's stable current identity is Release state and does not add a workspace
`deployments/` directory.

## Optional project-owned automation

When CI or reproducible project automation is required:

```text
python <datacoolie-build>/scripts/render_automation.py --workspace {workspace_name}
```

This creates a checked-in `automation/` copy with its build-tool dependency manifest. It runs with
the installed framework and without the skill. Do not create it for a project that directly
executes runners/notebooks and has no CI build requirement.

## Ignore rules

Add these workspace outputs to `.gitignore` when they are created:

```gitignore
.builds/
.runtime/
.approvals/
.releases/
provision/evidence/
__pycache__/
*.py[cod]
.env
.env.*
```

Do not ignore durable metadata, runners, functions, automation, architecture, or provision source.
