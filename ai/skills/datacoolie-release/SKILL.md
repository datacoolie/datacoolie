---
name: datacoolie-release
description: Plan, preflight, deploy, promote, roll back, or author consume-only release automation for an exact verified DataCoolie build. Use for explicit deployment-lifecycle work; read-only planning may precede authorization, while target mutation requires exact authorization. This skill consumes immutable build artifacts and never authors metadata or pipeline code, generates runners, rebuilds functions, provisions resources, or changes pipeline behavior.
---

# DataCoolie Release

## Outcome And Boundary

Prepare or apply one deployment action for an exact verified `.builds/{build_id}` and produce a
durable receipt for every attempted mutation. Release never rebuilds or repairs the artifact.
Promotion reuses one build; rollback selects an explicit previous successful release and its build.

Own release preflight, artifact transport, target activation, deployment automation, promotion,
rollback, target observation, and release receipts. Return build defects or missing build
automation to build, missing resources to provision, and material contract changes to design.

## Inputs And Authorization

Require an explicit action, target environment, build ID, build directory, declared target slice,
and one explicitly selected successful build receipt. Promotion and rollback also require an exact
successful source release receipt. Consume an explicitly selected successful provision receipt only
when target prerequisites required provisioning.

- Read-only preflight and release planning do not mutate a target.
- Every deploy, promote, rollback, or activation follows the target's release policy and records
  authorization bound to the canonical digest of its exact build slice, target, action, activation
  mechanism, and source/provision evidence.
- Production always requires explicit current-session authorization. Other protected targets follow
  their configured release policy.
- Design, build, provision, source-environment, or earlier broad approval never authorizes release.

Check authorization immediately before target mutation. A changed build, target, action, source
release, or deployment plan invalidates the authorization.

## Resource Routing

Load only the resource required by the selected outcome:

| Need | Resource | Owns |
|---|---|---|
| Deploy, promote, or rollback | `references/deployment-contract.md` | Stage, verify, activate, observe, and rollback semantics |
| Consume-only release CI/CD | `references/automation-contract.md` | Build-run identity, protection gates, credential flow, and receipt persistence |
| Platform command or API | `references/platform-tooling.md` | Current official documentation lookup and command evidence |
| Release evidence | `scripts/validate_release.py`, `schemas/release-receipt.schema.json` | Exact hashes, receipt bindings, source-release rules, and success gate |

References never choose target resources, naming, tool versions, or authentication. Target policy,
project-owned automation, installed tooling, and current official documentation are authoritative.

## Preflight

1. Resolve the explicitly supplied build ID and receipts; never select `latest` or glob for them.
2. Run the bundled release consumer validator against the exact build and successful build receipt.
   Reject modified, incomplete, symlinked, undeclared, or insufficiently verified artifacts.
3. Confirm the build receipt, manifest, environment, platform, runner, metadata, and functions
   describe the same exact target slice.
4. When provisioning was required, validate the exact successful apply receipt, its plan-bound
   authorization, observed resources, and requirements hash that blocked this release.
5. For promotion or rollback, validate the exact successful source release. Promotion requires the
   same build and a declared target slice; rollback uses the candidate source release's build.
6. Confirm exact target identity and activation mechanism, least-privilege credentials, log paths,
   watermark paths, release policy, and current authorization intent digest.

Any mismatch stops release. Do not edit, regenerate, or rebuild artifacts here.

## Deployment Transaction

1. Transfer the declared slice to an immutable candidate location.
2. Verify candidate identity and observable target contents before activation.
3. Attach the exact functions artifact when present, bind deployment configuration to the selected
   runner and metadata, then activate using the narrowest target-supported operation.
4. Observe the active target and required health checks.
5. Persist a successful or failed receipt. Partial transfer or activation remains failed evidence
   and must be reconciled before retry.

Promotion applies this transaction to another declared target without rebuilding. Rollback
reactivates or redeploys the exact build from an explicit previous successful release; it never
edits current metadata to imitate that version. If atomic activation is unavailable, record the
strategy, partial-state risk, and recovery action before mutation.

## Release Automation

Release owns consume-only deployment automation; build owns automation that creates and verifies
builds. Keep generated release automation project-owned under `automation/release/`. It downloads
  one artifact from an explicit build run/source, verifies its provenance, exact build receipt, and
  release intent, then performs the deployment transaction. It never materializes or calls installed
  skill paths at runtime; vendor the deterministic release consumer validator into automation.

Use target protection gates and short-lived workload identity when supported. Pin third-party
automation dependencies according to project security policy. Persist receipts outside ephemeral
job storage so later promotion and rollback can address one exact release.

## Evidence And Handoff

Write receipts to `{workspace}/.releases/{env}/{release_id}.json` and validate the explicitly
selected path:

```bash
python scripts/validate_release.py --workspace <workspace> --receipt <receipt-path>
```

Consumers requiring a completed release add `--require-success`. Never include credentials,
tokens, secret values, raw provider responses, or sensitive target outputs. End with exact build and
release IDs, target state, verification, skipped checks, and unresolved questions.
