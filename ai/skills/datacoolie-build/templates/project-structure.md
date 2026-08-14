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
├── functions/                             # optional
├── automation/                            # optional, only for CI/reproducible build
└── provision/                             # optional, only for infrastructure scope
```

`architecture/`, `discover/`, `functions/`, `automation/`, and `provision/` are conditional. Do not
create empty placeholder trees.

## Derived and runtime state

```text
{workspace_name}/
├── .builds/
│   └── {YYMMDD}-{12-char-content-digest}/
│       ├── manifest.json
│       ├── SHA256SUMS
│       ├── {env}/
│       │   ├── metadata.json
│       │   └── runners/
│       └── dist/                          # only when custom functions exist
├── .runtime/
│   └── {env}/
│       ├── logs/
│       └── watermarks/
├── .evidence/
│   ├── builds/
│   └── provision/
├── .approvals/                            # only required approvals
└── .releases/
```

`.builds/` contains materialized immutable copies, never symlinks. Durable corrections always
return to metadata, runners, functions, or automation source. Mutable log and watermark state must
remain outside `.builds/`.

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

Environment files contain only overrides for `connections`, `dataflows`, and `schema_hints`, merged
by stable identity. Do not maintain a full metadata clone per environment.

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
  --environment dev
```

Omit `--environment` to build every configured environment. Use repeated `--runner-name` only when
the request intentionally selects a subset; otherwise materialize all runners compatible with each
environment platform.

The command returns a UTC-date-prefixed build ID such as `260808-a13f83c9d7e2`. The manifest keeps
the full content digest. Equal inputs reuse the valid existing build and retain its original date;
changed inputs create a new immutable folder. Integration/runtime tests execute files under that
build and write `.evidence/builds/{build_id}/{env}/{receipt_id}.json`. Validate the exact build and
explicit receipt with `scripts/validate_build.py`; release consumes that ID, checksums, target slice,
and successful receipt path.

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
.evidence/
.approvals/
.releases/
__pycache__/
*.py[cod]
.env
.env.*
```

Do not ignore durable metadata, runners, functions, automation, architecture, or provision source.
