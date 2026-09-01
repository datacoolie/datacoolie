---
name: datacoolie-design
description: Design or materially revise a DataCoolie project's stage graph, transition contracts, model grain, keys, load strategy, platform intent, quality/recovery policy, infrastructure requirements, or release policy. Use for new projects and architecture-affecting changes; skip it for implementation-only metadata, runner, function, test, or release work that preserves the current design.
---

# DataCoolie Design

## Outcome And Boundary

Produce one coherent `architecture/current.md` that gives build enough intent to implement without
inventing contracts. Own material system behavior: stage graph, transition grain and keys,
load/change strategy, compatible platform intent, control-storage and recovery policy, resource
requirements, and release policy.

Design does not inspect sources, author exact metadata or code, create resources, verify runtime
behavior, or deploy. It may create only the workspace container, `architecture/`, and
`.approvals/design/`.

## Trigger Decision

Design is required when no architecture exists for a new project or a request changes a stage
boundary, data contract, model grain, key, load/CDC behavior, platform or security boundary,
resource requirement, or release policy. These decisions require approval.

Skip design for compatible metadata, runner, function, test, and release work. A wording or
traceability correction outside the material contract also skips design and leaves the approved
architecture bytes unchanged.

## Inputs And Resources

Use requirements, the current architecture when present, exact relevant discovery paths, and
installed DataCoolie capability evidence at design-level granularity. A new project requires
discovery evidence for every declared source before design; existing projects require only evidence
affected by the material change.

| Need | Resource |
|---|---|
| Author the canonical design | `templates/architecture.tpl.md` |
| Inspect approval shape | `templates/design-approval.json.example` |
| Validate approval contract | `schemas/design-approval.schema.json` |
| Hash, record, or verify approval | `scripts/design_approval.py` |
| Install helper dependencies | `scripts/requirements.txt` |

The helper exists only to make hashing and receipt validation deterministic. It makes no design
decision and is not runtime or package CLI functionality.

## Decision Workflow

1. Separate observed facts, desired behavior, assumptions, and unresolved facts.
2. Define the project-specific stage graph and one contract for every in-scope transition.
3. For each transition decide grain, keys, load/change behavior, replay, schema evolution,
   idempotency, deduplication, late data, backfill, storage intent, quality gates, operational
   targets, and recovery.
4. Describe capability intent across source, authentication, engine, transforms, destination,
   load, platform, and dependencies. Prefer a credible native DataCoolie path; identify only a
   suspected unsupported boundary for build-time proof.
5. When a verified unsupported boundary requires a Python function, select exactly one artifact
   format for the whole build: `wheel` by default, or `zip` only for a compatible pure-Python host.
   Record a project-specific import prefix, dependency strategy, compatible execution targets,
   rationale, and required build proof. Otherwise record `none`. Never request both formats.
6. Record required resources and release policy without provisioning or deploying. The policy
   defines each runner's deployment kind and native target identity, stable target current identity,
   candidate activation mechanism, in-flight execution
   behavior, non-atomic exposure/recovery when applicable, dependent-runner order, and canonical
   artifact retention needed for rollback. Define one
   environment-isolated DataCoolie control-storage boundary for immutable deployed metadata,
   mutable logs, and critical watermark state. Only target components `metadata` and optional
   `functions` have fixed names; all runner and runtime-state references remain target-defined.
   Prefer a dedicated persistent resource separate
   from business data; when policy requires sharing, require isolated paths and access controls.
7. List compatible engines/platforms without binding stages to engines. Record execution host and
   native/external platform runtime mode separately from platform intent when an adapter can run on
   multiple hosts. Runtime orchestration selects an exact runner and supplies the stage value.
   For external cloud adapters, identify the actual scheduler or host separately from the cloud
   platform/resource context.
8. Define retention and access for logs, backup/recovery and outage behavior for watermarks, and
   single-writer ownership for each environment/dataflow state key. Account for the possibility
   that target writes succeed before watermark persistence. Keep those mutable paths outside the
   candidate and stable target current replacement boundary.
9. Write the complete candidate to `architecture/current.md`. It is the only design source of
   truth; use Git and approval receipts for history rather than layer files or amendments. Do not
   embed its own hash or approval state in the file.
10. Stop for explicit current-session approval after the candidate bytes are final, then record and
   verify the hash-bound receipt. Never infer or pre-create approval. Any architecture creation or
   byte change requires a matching receipt before build.

Do not fabricate physical schemas, source facts, provider products, stage names, environments,
formats, or exact metadata. Route missing facts to discover and implementation detail to build.

## Output And Handoff

```text
{workspace}/architecture/current.md
{workspace}/.approvals/design/architecture-{architecture_sha256_prefix}.approved.json
```

Approval state remains outside the architecture. Any byte change invalidates its receipt, and
design approval authorizes neither provisioning nor deployment. The readable filename uses the
first 12 digest characters; the payload and validator remain bound to the full SHA-256. The schema
owns exact receipt fields.

After finalization, hand build the architecture path/hash and matching receipt, transition
contracts, environments, capability assumptions, and implementation questions. End with unresolved
questions; state `None` when there are none.
