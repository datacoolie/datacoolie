# DataCoolie AI Workflow

Public documentation is the source of truth for shared framework/project
contracts and recipes. This file owns agent routing, existing authorization
boundaries, sequencing, evidence and handoff.

## Start with the public contract

Read the smallest compatible public page before selecting or adapting a
framework capability. Use installed CLI/schema/API evidence to verify the
selected version. Investigate discrepancies; report the affected docs owner
and evidence instead of silently choosing a conflicting summary or behavior.

| Need | Public owner |
|---|---|
| Project layout, preparation and upload handoff | [Project workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/) |
| Commands, options, validation scope and responses | [CLI commands](https://datacoolie.github.io/datacoolie/guide/cli/commands/) |
| Define or change metadata | [Metadata guide](https://datacoolie.github.io/datacoolie/guide/metadata/) and [field reference](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#metadata-document) |
| Select a metadata schema | [Schema index](https://datacoolie.github.io/datacoolie/schema/index.json) and [current alias](https://datacoolie.github.io/datacoolie/schema/latest/metadata.schema.json); apply the documented version-selection policy |
| Provider, runtime paths and lifecycle | [Providers](https://datacoolie.github.io/datacoolie/guide/providers/) and [runtime configuration](https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/) |
| Stage selection, results and completion gates | [Run a stage](https://datacoolie.github.io/datacoolie/guide/operations/run-stage/) and [orchestration](https://datacoolie.github.io/datacoolie/reference/concepts/orchestration/) |
| Maintained projects and runtime runner source | [Examples catalog](https://datacoolie.github.io/datacoolie/examples/) and the selected entry's guide |

Keep shared technical explanations in their public owners. Skill references
add only contextual decisions, task sequencing, adaptation checks and evidence.

## Example retrieval protocol

Use the catalog to discover a sample, then follow its direct `guide`, `source`,
`raw`, `project-files` or `download` action for the requested purpose. Read the
recipe before adapting files; its prerequisites, commands, working directory,
expected artifacts and rerun behavior govern that example.

Pin the published compatible revision when reproducibility matters. Verify
retrieved content and archive membership against that revision; a successful
HTTP status or a local preview alone does not establish publication. If the
revision/file is unavailable, report the blocker rather than substituting
another revision. An explicitly supplied same-revision local checkout is an
offline retrieval source. Public examples and projects own runtime runner
source; Skills do not keep a second maintained runner tree.

## Agent-owned workflow

| Skill | Owns | Handoff boundary |
|---|---|---|
| `datacoolie-discover` | Source facts, bounded probes and evidence assessments | Source/IAM owner supplies read-only access; Design chooses strategy and Build authors metadata |
| `datacoolie-design` | Material project intent, trade-offs, acceptance criteria and approval | Public docs define available behavior; Build implements and proves it |
| `datacoolie-build` | Project metadata, runners, functions, CLI invocation and requested local verification | CLI prepares and validates artifacts; framework executes the documented capabilities |
| `datacoolie-provision` | Infrastructure source, exact plan/apply approval and resource readiness | Platform tools perform resource changes; Build and Release consume readiness evidence |
| `datacoolie-release` | Authorized upload, adapter choice and transfer evidence | CLI owns local artifact validation; target workflow owns activation and operation |

CLI owns schema/project/artifact validation, packaging and manifests. Framework
owns runtime execution semantics. Skills select documented capabilities, supply
project intent and interpret evidence; their helpers automate only skill-owned
probes, approvals, receipts or thin command wrappers. Do not reproduce CLI or
framework logic in skill scripts or project runners.

Route compatible project authoring directly to Build. Require relevant
source discovery for missing/new facts, Design approval for material changes,
Provision for evidenced resource gaps and Release for authorized uploads.
Reuse exact handoff artifacts; do not introduce a parallel workflow state store.

## Verification and handoff

Select checks from the requested outcome and compatible public docs:

- Author/validate: apply the metadata/CLI guidance and report the actual scope,
  warnings and skipped checks. Validation success certifies that scope.
- Build/artifact: follow the project recipe and verify the selected build and
  artifact; report identity and check scope separately from runtime evidence.
- Requested local runtime verification: use the matching project-owned runner,
  recipe and project acceptance criteria. Check selection/results and read
  output independently; inspect state/log evidence when relevant. Empty/skipped
  outcomes are interpreted against those criteria. Report unmet outcomes even
  when a command exits zero.

When a required host, dependency or resource is unavailable, report which
checks remain unverified and route the concrete gap to its owner.

Keep credentials and sensitive discovery values out of authored files,
artifacts and evidence. Preserve custom runner bytes during CLI validation/build.
For CLI automation use `--format json`, check exit status and `ok`, then interpret
the documented response scope. Reuse existing reports/manifests/receipts.

Return exact source/artifact paths or IDs, docs/example revision, performed and
skipped checks, outcome limitations, next owner and unresolved questions.
Local verification is separate from upload. Release remains upload-only;
activation, workload operation, monitoring and recovery after deployment belong
to the project's platform workflow.
