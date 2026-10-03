---
name: datacoolie-build
description: Build, modify, run, and verify DataCoolie projects through public docs, the CLI and framework APIs. Owns metadata, runners, functions, local verification and requested project automation; route discovery, material design, infrastructure and upload to their owning skills.
---

# DataCoolie Build

## Outcome and boundary

Turn project intent into the requested authored, validated, built or locally
verified outcome. Public docs own framework contracts and recipes; the installed
`dc`/`datacoolie` CLI owns preparation. This skill owns contextual decisions,
sequencing, adaptation, evidence and handoff around the `datacoolie.yml` project.

Route new/missing source facts to Discover, material decisions to Design,
resource gaps to Provision and authorized upload to Release. Runtime execution
uses a project-owned runner; Build does not invent a CLI workload command.

## Route only the needed resource

Read the smallest compatible public page and matching agent reference:

| Need | First action / owner |
|---|---|
| Bootstrap | Follow [project lifecycle](https://datacoolie.github.io/datacoolie/guide/cli/project/#project-lifecycle); `dc init [PATH]` |
| Define/change metadata | Read the public [Metadata Guide](https://datacoolie.github.io/datacoolie/guide/metadata/) and `references/schema-quick-reference.md` |
| CLI options, validation targets or response scope | [CLI commands](https://datacoolie.github.io/datacoolie/guide/cli/commands/); choose `dc validate --project-dir <PATH>`, `--metadata-path` or `--artifact-path` according to the request |
| Config/metadata/capability/artifact inventory | Same CLI docs; `dc inspect ... --format json` |
| Metadata conversion | Same CLI docs; `dc metadata convert ...` |
| Build or inspect packaging decisions | [Project workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/); `dc build --project-dir <PATH>` or its documented dry-run |
| Runner/provider/runtime paths | [Runtime configuration](https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/); `references/runner-contract.md` |
| Platform/host/dependencies | `references/platform-contract.md` and the selected public platform/runner guide |
| Functions packaging/import setup | `references/python-functions-contract.md` and the selected public project recipe |
| Normal stage selection, ordering, sharding or results | [Run a stage](https://datacoolie.github.io/datacoolie/guide/operations/run-stage/); `references/orchestration-contract.md` |
| Replay/maintenance authoring or local checks | `references/operations-contract.md` and the selected public recipe |
| Public project/runner examples | `references/public-examples.md`; follow the known entry directly or discover it in the catalog |
| Polars qualified SQL/table registration | `references/polars-qualified-sql.md` |
| Native capability/source choice or unsupported edge | `references/framework-boundary.md` |
| Material Design approval | Existing `datacoolie-design/scripts/design_approval.py` |
| Latest project agent guidance | CLI docs; `dc agents update --project-dir <PATH>` |

Public [field reference](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#metadata-document),
[schema index](https://datacoolie.github.io/datacoolie/schema/index.json) and
[current schema alias](https://datacoolie.github.io/datacoolie/schema/latest/metadata.schema.json)
own metadata syntax and version policy. Inspect compatible installed evidence
when selecting capabilities. Reconcile discrepancies before relying on a
stale summary; this Skill carries no competing schema or technical field catalog.

## Inputs and gates

- Read the project configuration, affected authored components and environment
  runner. Obtain compatible docs and the recipe for the requested adaptation.
- Require relevant source evidence for a new source and approved intent for
  material architecture/data-contract/platform/release changes. Compatible work
  with no material Design dependency may proceed directly.
- For material Design, verify the final candidate through the existing helper:
  `python <datacoolie-design>/scripts/design_approval.py verify --workspace <project>
  --architecture <project>/architecture/current.md`. A missing, stale or unavailable
  hash-matching receipt blocks that dependent work. Do not copy its hash/receipt
  validator into Build.
- Resolve authoring, query-reference and component-path questions through the
  relevant docs and CLI; keep declarations such as `source.query` in their owner.
  Investigate missing files or incompatible capability evidence before custom code.
- Keep credentials in the selected external mechanism and preserve authored
  runner bytes through CLI preparation. Do not import Skill helpers at runtime.

## Build and local verification

Agree the requested outcome from the task and use its documented checks:

| Outcome | Required evidence |
|---|---|
| Author/validate | Changed files, compatible schema/docs, CLI diagnostics and actual validation scope; fix failures and report warnings/skipped checks |
| Build/artifact | CLI build result, exact build/environment identity, artifact validation scope and documented integrity/comparison checks |
| Requested runtime verification | Matching recipe/runner on a compatible host, selected dataflows/stage, execution result and independent output/state checks derived from project criteria |

For ordinary project preparation, follow the CLI docs with:

```text
dc validate --project-dir <project> --format json
dc build --project-dir <project> --format json
dc validate --artifact-path <selected-artifact> --format json
```

Inspect process exit code and `ok`, then apply the documented response scope and
skipped-check semantics. Record the exact selected artifact from CLI output;
validation/build success does not establish runtime success. Runtime checks
use that artifact/entrypoint when the task requests artifact verification.

For local runtime verification, load `references/runner-contract.md` and
`references/orchestration-contract.md`. Apply the selected recipe's dependencies,
working directory, commands, output readback and rerun/state expectations.
Report mismatched results even with exit zero. Legitimate empty input or skipped
work is judged against the project criteria rather than a universal row-count rule.
A missing host/resource leaves those checks unverified; report and route the gap.

## Edit loop and completion boundary

Follow the public [build and execute workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/#build-and-execute)
and the selected recipe; those pages own the exact commands, paths and runtime
sequence. Build selects the requested outcome and interprets its evidence:
source checks certify only their changed scope, an artifact outcome requires
the CLI-selected retained build and artifact validation, and a runtime outcome
requires the recipe's independent selection/output/state evidence from its
fresh execution copy.

Run CLI and recipe commands from the user's existing `.venv` (or already active
Python environment), even when it lives outside the project. Environment
discovery is not a Build step. Do not create, bundle, install into or upgrade a
second environment. If a selected recipe cannot run because a dependency or
host capability is missing, report that gap and its owner. Fix source defects
in authored files and rebuild; route environment failures to the environment
owner. Build-only work ends at artifact validation unless runtime verification
was requested.

## Automation and handoff

Generate automation only when requested; use `scripts/render_automation.py`
for thin project-owned wrappers around the installed CLI. Do not duplicate
project configuration, manifest or metadata validation in Skill code.

Return exact changed files, docs/example revision, selected artifact/build ID,
environment coverage, commands/results, verification scope, skipped checks and
unresolved questions. Use existing CLI reports/manifests rather than a second
handoff envelope. Route resources to Provision and authorized uploads to Release;
post-deploy workload operation remains a project/platform responsibility.
