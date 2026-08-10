---
name: datacoolie-provision
description: Plan, validate, create, update, reconcile, or remove infrastructure required by a DataCoolie environment. Use only for an explicit infrastructure request or an evidenced resource gap from build or release. Inventory and preview are allowed without mutation approval; every apply requires approval bound to the exact environment and persisted plan. Never use this skill to change pipeline behavior, build artifacts, or releases.
---

# DataCoolie Provision

## Outcome And Boundary

Resolve one environment-scoped infrastructure gap and return verifiable evidence to the blocked
build or release. Provision is a conditional dependency, not a mandatory lifecycle phase.

Own resource inventory, gap classification, infrastructure source, plan or preview, apply,
reconciliation, resource verification, and provision receipts. Do not inspect sources, choose
pipeline architecture, edit metadata/runners/functions/builds, or deploy a release.

## Inputs And Approval

Require an explicit target environment and platform plus either an exact requirements artifact or
an explicit infrastructure scope. The handoff supplies its path and SHA-256, blocked operation,
required capability, and observed missing or inaccessible resource evidence. Use approved material
requirements when they exist. Treat absent, stale, or ambiguous requirements as unresolved.

- Read-only inventory, validation, and a demonstrably non-mutating preview are allowed in scope.
- Persist the exact plan before requesting approval; approval is bound to its environment and
  SHA-256. Any changed plan requires renewed approval.
- Every create, update, import/adoption, replacement, or deletion requires explicit current-session
  apply approval. A data-bearing replacement or deletion requires separate destructive approval.
- Design, build, release, or earlier broad implementation approval never authorizes provision.

## Resource Routing

Load only the reference needed by the selected mechanism:

| Need | Resource | Owns |
|---|---|---|
| Existing or selected Terraform workflow | `references/terraform-contract.md` | State, saved-plan, import, apply, and output rules |
| Direct platform CLI or API | `references/platform-tooling.md` | Current documentation lookup and safe command evidence |
| Receipt verification | `scripts/validate_provision.py`, `schemas/provision-receipt.schema.json` | Receipt shape, exact hashes, authorization binding, and success gate |

References never select resources, naming, versions, or authentication for the project. Approved
requirements, existing project conventions, installed lockfiles, and current official
documentation remain authoritative.

## Decision Workflow

1. Resolve the exact environment/platform and requirements artifact.
2. Observe target state and classify every requirement as `present`, `missing`, `drifted`,
   `inaccessible`, or `unknown`. Provision only confirmed gaps or explicitly requested changes.
3. Reuse the project's infrastructure source of truth. Do not create parallel state or resources,
   change a backend/workspace, or import an existing resource implicitly.
4. Persist an idempotent, environment-scoped plan covering actions, data-bearing impact, cost,
   permissions, state, rollback, tool versions, and unresolved risks.
5. Validate or preview without mutation. If the tool has no trustworthy non-mutating preview,
   return the plan without executing it.
6. Obtain approval for the exact plan hash, then apply that plan only. Stop on changed actions,
   partial apply, drift, state locks, or inaccessible state and reconcile observable state.
7. Verify resource state and least-privilege access. Record successful or failed evidence; never
   convert an incomplete or failed apply into success.

## Evidence And Handoff

Keep project-owned infrastructure under `{workspace}/provision/`. Persist approval-bound plans and
receipts under:

```text
{workspace}/.evidence/provision/{env}/plans/{plan_id}.{format}
{workspace}/.evidence/provision/{env}/{receipt_id}.json
```

Use `templates/provision-receipt.json.example`, then validate the explicitly selected receipt:

```bash
python scripts/validate_provision.py --workspace <workspace> --receipt <receipt-path>
```

Build or release consumers that require completed provisioning add `--require-apply-success`.
Never search for `latest`, glob for receipts, or include secrets, sensitive outputs, or raw provider
responses. Return the exact receipt path and SHA-256 plus non-sensitive resource identifiers to the
blocked skill. End with actions, verification, skipped checks, and unresolved questions.
