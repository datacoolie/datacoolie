# DataCoolie AI Workflow

## Purpose

This file is the canonical, runner-agnostic operating contract for DataCoolie project work.
It guides agents working inside a project-level `{project_name}_dcws/` folder.

Use this workflow for end-to-end data platform delivery: discover sources, design stage-level
architecture, scaffold the workspace, generate metadata, provision infrastructure, deploy to
dev/test, collect review evidence, and promote to production only after required gates are approved.

## Workspace Contract

All new DataCoolie project artifacts belong under `{project_name}_dcws/`.

## Workspace Naming

The DataCoolie workspace folder is derived from the project name:

```text
{workspace_name} = {normalized_project_name}_dcws
```

Rules:

- Normalize `{project_name}` to lowercase snake/kebab-safe text before adding `_dcws`.
- Example: `Sales Analytics` -> `sales_analytics_dcws`.
- If no workspace exists, the first lifecycle skill used, usually `datacoolie-discover`, must ask for the project name and create/use `{project_name}_dcws/`.
- If exactly one `*_dcws/` folder exists, use it as the current DataCoolie workspace.
- If multiple `*_dcws/` folders exist, ask the user which project workspace to use.
- Do not create a fixed `datacoolie_workspace/` folder for new projects.
- Existing legacy `datacoolie_workspace/` folders may be read for migration, but new artifacts should use `{project_name}_dcws/`.

```text
{project_name}_dcws/
  AGENTS.md
  config.yaml
  discover/
  architecture/
    current.md
    amendments/
  metadata/
  project_management/
    status.md
    phases/
      architecture/
        scope.md
        notes.md
        evidence.md
        gate-reviews/
      source2bronze/
        scope.md
        notes.md
        evidence.md
        gate-reviews/
      bronze2silver/
        scope.md
        notes.md
        evidence.md
        gate-reviews/
      silver2gold/
        scope.md
        notes.md
        evidence.md
        gate-reviews/
      production/
        scope.md
        notes.md
        evidence.md
        gate-reviews/
    decisions/
    risks.md
    changelog.md
  generated/
  provision/
  deploy/
  promote/
```

`config.yaml` is the workspace control file. It identifies the project, workspace name,
valid environments, target platform per environment, and generated artifact locations.
Follow the structure documented in this file and in `datacoolie-init/templates/project-structure.md`.

`config.yaml` must not contain dataflow definitions, runtime engine strategy, gate approval
state, passwords, API keys, or secret values. Those belong in `metadata/`, `architecture/`,
`project_management/`, generated runner/deploy artifacts, or the target platform's secret manager.

## Phase Routing

| User Intent | Phase | Skill |
|---|---|---|
| Discover, introspect, profile source, explore data | 1 - Discover | `datacoolie-discover` |
| Design architecture, medallion layers, engine/platform strategy | 2 - Architect | `datacoolie-architect` |
| Add or re-discover source | 1 then 2 amendment/update | `datacoolie-discover`, `datacoolie-architect` |
| Init, scaffold, create project structure | 3 - Init | `datacoolie-init` |
| Generate, validate, merge, or edit metadata | 4 - Metadata | `datacoolie-metadata` |
| Provision infra, Terraform, lakehouse, bucket, workspace | 5 - Provision | `datacoolie-provision` |
| Deploy, preflight, promote, generate runner, CI/CD | 6 - Deploy | `datacoolie-deploy` |

For implementation beyond lifecycle skills, use the relevant engineering skill:

| User Intent | Skill |
|---|---|
| Spark or PySpark work | `spark-development` |
| SQL authoring or optimization | `sql-authoring` |
| Facts, dimensions, SCD, semantic model | `data-modeling` |
| Assertions, contracts, quarantine, reconciliation | `data-quality` |
| Notebooks | `notebook-development` |
| CI/CD, operations, rollback, monitoring | `dataops` |

## State Detection

Start from the earliest incomplete required phase unless the user explicitly asks to inspect or amend a later phase.

```text
Phase 1 complete: {project_name}_dcws/discover/ has at least one source report and schema inventory.
Phase 2 complete: {project_name}_dcws/architecture/current.md exists and architecture gate is approved.
Phase 3 complete: {project_name}_dcws/config.yaml and {project_name}_dcws/metadata/ exist.
Phase 4 complete: {project_name}_dcws/metadata/ has validated metadata files.
Phase 5 complete: {project_name}_dcws/provision/ has a provision log or generated IaC/script.
Phase 6 complete: {project_name}_dcws/generated/ or {project_name}_dcws/deploy/ has deploy artifacts.
Promotion complete: {project_name}_dcws/promote/ has promotion log(s).
```

Gate approval is determined by the latest Markdown journal under:

```text
{project_name}_dcws/project_management/phases/{phase}/gate-reviews/
```

The journal must have YAML frontmatter with `status: approved`.
Phase notes, review evidence, and delivery state belong under
`{project_name}_dcws/project_management/phases/{phase}/`. Technical artifacts stay in
their technical folders such as `architecture/`, `metadata/`, `generated/`, `provision/`,
and `deploy/`.

## Architecture Contract

Architecture is a stage-level contract. It must be specific enough for downstream work, but it must not duplicate metadata implementation.

Architecture must define:

- Source domains and ingestion boundaries.
- Layer/stage sequence.
- Load pattern: full, append, merge, CDC, or replay-window.
- Change detection and backfill approach.
- Target grain and key strategy by stage.
- Freshness and runtime targets.
- Engine and platform strategy.
- File/table format and partitioning principles.
- Required quality gates and promotion criteria.
- Ownership, risks, rollback, and amendment rules.

Architecture must not define:

- Final dataflow object names.
- Full column-by-column metadata implementation.
- Platform deployment commands.
- Detailed transform code.

## Architecture Amendments

If implementation reveals that architecture needs to change, create an amendment:

```text
{project_name}_dcws/architecture/amendments/YYMMDD_<change>.md
```

Each amendment must record:

- Requested change.
- Reason and evidence.
- Impacted layer or phase.
- Whether the change is breaking.
- Required backfill, replay, or rollback.
- Approval decision.

Breaking amendments stop downstream work until approved. Non-breaking amendments require lightweight review before continuing.

## Gate Journals

Gate decisions are Markdown journals with YAML frontmatter.

```markdown
---
gate: source2bronze
status: approved
reviewer: reviewer-name
reviewed_at: 2026-06-05
next_allowed: bronze2silver
---

# Source2Bronze Gate Review

## Evidence

- Schema check: pass
- Row count: pass
- Freshness: pass
- Reconciliation: pass
- Quality checks: pass
- Dev/test deploy: pass

## Decision

Approved.

## Notes

- ...
```

Allowed statuses:

- `pending`
- `approved`
- `changes_required`
- `blocked`

## Gate Enforcement

Stage completion does not imply approval to continue.

| Gate | Required Evidence | Next Stage |
|---|---|---|
| `architecture` | Stage-level architecture reviewed; scope, load pattern, grain, keys, freshness, quality gates, platform, and engine are clear. | `source2bronze` |
| `source2bronze` | Ingestion contract, schema, row count, freshness, replay/idempotency, quarantine or fail-fast behavior, dev/test deploy evidence. | `bronze2silver` |
| `bronze2silver` | Deduplication, keys, business rules, schema validation, reconciliation to bronze, dev/test deploy evidence. | `silver2gold` |
| `silver2gold` | Metrics, aggregates, dimensional grain, semantic/consumer checks, reconciliation to silver, dev/test deploy evidence. | `production` |
| `production` | All required gates approved, prod preflight passed, rollback path recorded, explicit production approval captured. | Production deploy |

Do not start downstream implementation when a required gate is unresolved, except isolated scaffolding that cannot affect the gated output.

## Cross-Skill Context

Pass the relevant prior artifacts when activating a skill:

```text
Architect reads: {project_name}_dcws/discover/*
Init reads: {project_name}_dcws/architecture/current.md if present
Metadata reads: {project_name}_dcws/architecture/current.md and {project_name}_dcws/discover/*
Provision reads: {project_name}_dcws/architecture/current.md
Deploy reads: {project_name}_dcws/config.yaml, {project_name}_dcws/metadata/, {project_name}_dcws/provision/
Promotion reads: {project_name}_dcws/project_management/phases/*/gate-reviews/ and {project_name}_dcws/deploy/
```

## Error Recovery

When a phase fails:

1. Diagnose the failure from the concrete error output.
2. Suggest an actionable fix.
3. Do not skip the phase.
4. Do not advance to the next gate with failed evidence.
5. Record unresolved blockers in `{project_name}_dcws/project_management/status.md`.

Common recovery patterns:

| Error | Recovery |
|---|---|
| Preflight CLI not found | Show install command and stop. |
| Secrets unresolved | List missing secret references without revealing values and stop. |
| Provision resource already exists | Treat as idempotent skip and log it. |
| Provision permission denied | Show required IAM/RBAC permissions. |
| Deploy merge fails | Check `{project_name}_dcws/metadata/environments/`. |
| Metadata validation fails | Show field path, expected format, and fix before continuing. |

## Quick Start

For a new project:

1. Run `datacoolie-discover` for each source.
2. Run `datacoolie-architect` to create `{project_name}_dcws/architecture/current.md`.
3. Stop for architecture gate review.
4. Run `datacoolie-init` after approval.
5. Run `datacoolie-metadata`.
6. Build/test/deploy `source2bronze` to dev/test and stop for gate review.
7. Repeat for `bronze2silver` and `silver2gold`.
8. Run production deploy only after all required gates and production approval are recorded.

## Principles

- Keep one source of truth under `{project_name}_dcws/`.
- Never auto-approve gates.
- Never deploy production without explicit approval.
- Dry-run and preflight before cloud changes.
- Write dated, reviewable artifacts.
- Preserve user changes and do not overwrite existing workspace `AGENTS.md`.
