---
name: datacoolie-release
description: Deploy, promote, roll back, or plan release automation for an exact verified DataCoolie build. Use for explicit deployment work; mutation requires exact authorization. Consumes artifacts and never authors metadata, rebuilds pipelines, or provisions resources.
---

# DataCoolie Release

## Outcome And Boundary

Prepare or apply one deployment action for an exact verified
`.builds/artifacts/{build_id}` and produce a durable receipt for every attempted mutation.
Release never rebuilds or repairs the artifact.
Promotion reuses one build; rollback selects an explicit previous successful release and its build.

Own release preflight, artifact transport, target activation, deployment automation, promotion,
rollback, target observation, and release receipts. Return build defects or missing build
automation to build, missing resources to provision, and material contract changes to design.

## Inputs And Authorization

Require an explicit action, target environment, an exact build ID or build `current` selector,
declared runner slice,
one explicitly selected successful artifact-verification receipt, exact target log and watermark
paths, and one declared runtime-state action. Promotion and rollback also require an exact active
source release receipt. Consume an explicitly selected successful provision receipt only when
target prerequisites required provisioning.

- Read-only preflight and release planning do not mutate a target.
- Every deploy, promote, rollback, or activation follows the target's release policy and records
  authorization bound to the canonical digest of its exact build slice, target, action, activation
  mechanism, and source/provision evidence.
- Production always requires explicit current-session authorization. Other protected targets follow
  their configured release policy.
- Design, build, provision, source-environment, or earlier broad approval never authorizes release.

Resolve `current` once through its validated `build.json`, pin the exact immutable build, then
persist a `prepared` receipt and check authorization immediately before candidate staging. Recheck
the candidate, stable target current, deployment marker, resource/state gates, and authorization
immediately before activation. A changed build, runner, target, action, source release, runtime
path, qualification scope, state action/reference, marker, or deployment plan invalidates the
authorization.
State actions `migrate`, `reset`, and `replay` require current-session authorization.

## Resource Routing

Load only the resource required by the selected outcome:

| Need | Resource | Owns |
|---|---|---|
| Deploy, promote, or rollback | `references/deployment-contract.md` | Stage, verify, activate, observe, and rollback semantics |
| Function artifact present | `references/python-functions-deployment.md` | Exact-artifact attachment, import proof, activation, and rollback |
| Consume-only release CI/CD | `references/automation-contract.md` | Build-run identity, protection gates, credential flow, and receipt persistence |
| Platform operation | `references/platform-tooling.md` | Control-plane CLI selection, installation boundary, official documentation, fallback, and command evidence |
| Runner target mapping | `references/runner-deployment-mapping.md` | Platform-native runner resource and runtime identity |
| Release evidence | `scripts/validate_release.py`, `schemas/release-receipt.schema.json` | Exact hashes, receipt bindings, source-release rules, and success gate |

References never choose target resources, naming, tool versions, or authentication. Target policy,
project-owned automation, installed tooling, and current official documentation are authoritative.

## Preflight

1. Resolve the supplied source. An explicit build ID selects its immutable artifact directly;
   `current` is a convenience selector whose validated `current/build.json` is read once before
   mutation. Pin that exact build ID in the prepared receipt and use only its canonical artifact
   bytes afterward. Never transfer from moving current, re-resolve it during the attempt, or select
   `latest` or a glob.
2. Run the bundled release consumer validator against the exact build and successful Build
   artifact-verification receipt. Reject modified, incomplete, symlinked, undeclared, or
   insufficiently verified artifacts. Build-host runtime execution is optional evidence and never
   substitutes for target qualification.
3. Confirm the build receipt, manifest, environment, platform, runner, typed metadata set, and singular function artifact
   describe the same exact target slice.
4. Observe every required resource immediately before mutation. Continue when it is present,
   accessible, and policy-compliant. Route `missing`, `drifted`, `inaccessible`, or `unknown`
   requirements to Provision. When provisioning was required, validate the exact successful apply
   receipt, its plan-bound authorization, observed resources, and requirements hash that blocked
   this release.
5. For promotion or rollback, validate the exact active source release. Promotion requires the
   same build and a declared target slice; rollback uses the candidate source release's build.
6. Compare the active and candidate dataflow identity, watermark columns and known types,
   destination identity/load behavior, and declared target grain or keys. Use `initialize` only
   without active state and `preserve` only when compatible. Otherwise stop for an approved
   migrate, reset, or replay plan; never infer or silently execute it.
7. Confirm exact target identity, temporary release-addressed candidate reference, stable target
   current reference, deployment marker, and activation mechanism, plus least-privilege
   credentials, environment-isolated active and qualification log/watermark paths, release policy,
   and current authorization intent digest. Require passed `resource-readiness`,
   `environment-isolation`, and
   `runtime-state-preflight`, and `shared-component-compatibility` checks in addition to build and
   target checks. If runner slices share target `metadata` or `functions`, require the same exact
   component digest or an approved isolation/coordinated-activation boundary. For an external cloud
   adapter, target identity is the actual scheduler or execution host, not the cloud platform.
8. Preserve an approved project-owned deployment mechanism. Otherwise read
   `references/platform-tooling.md` for control-plane selection, safe tooling setup, and fallback.

Any mismatch stops release. Do not edit, regenerate, or rebuild artifacts here.

## Deployment Transaction

Read `references/deployment-contract.md` before staging or activation. It owns the ordered
transaction, phase evidence, candidate reuse, promotion/rollback, non-atomic exposure, and recovery.
Use `references/python-functions-deployment.md` when a function artifact is present. Persist the
attempt before mutation; only target-qualified, observed work may become an active release.
Validators check receipt/artifact consistency; they do not themselves execute deployment or
establish that a human gave approval. Preserve actual tool observations and the authorization source.

## Release Automation

For requested automation, read `references/automation-contract.md`. Keep it project-owned under
`automation/release/`; vendor the deterministic release consumer validator. It consumes verified
builds, never materializes, and never calls installed skill paths at runtime.

## Evidence And Handoff

Write receipts to `{workspace}/.releases/{env}/{release_id}.json` and validate the explicitly
selected path:

```bash
python scripts/validate_release.py --workspace <workspace> --receipt <receipt-path> [--build-selector current|<build-id>]
```

Consumers requiring a completed release add `--require-success`; this accepts only `active`
receipts valid against the bundled schema. Never include credentials,
tokens, secret values, raw provider responses, or sensitive target outputs. End with exact build and
release IDs, target state, verification, skipped checks, and unresolved questions.

`schemas/release-receipt.schema.json` owns the current receipt version; the validator rejects
unsupported versions. Earlier receipts remain audit evidence only. Establish a newly verified
baseline for each active runner slice before using current promotion/rollback; never merely relabel
an old receipt with the current version.
