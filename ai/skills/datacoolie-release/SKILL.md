---
name: datacoolie-release
description: Plan, preflight, deploy, promote, roll back, or author consume-only release automation for an exact verified DataCoolie build. Use for explicit deployment-lifecycle work; read-only planning may precede authorization, while target mutation requires exact authorization. This skill consumes immutable build artifacts and never authors metadata or pipeline code, generates runners, rebuilds functions, provisions resources, or changes pipeline behavior.
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
2. Run the bundled release consumer validator against the exact build and successful Build v4
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
8. Preserve a project-owned deployment mechanism when one is already approved. Otherwise follow
   `platform-tooling.md` and select the direct tool by control plane. For Fabric targets, use Azure
   CLI only for Azure/ARM readiness and Fabric CLI for Fabric-native transfer, qualification, and
   activation. Resolve only the selected tooling; if it is missing, install it within the safe
   tooling boundary or request installation, never in a runner or notebook.

Any mismatch stops release. Do not edit, regenerate, or rebuild artifacts here.

## Deployment Transaction

1. Persist the exact authorized attempt as `prepared` before mutation.
2. Map every declared metadata role to the fixed target `metadata` component, the optional function
   artifact to `functions`, and the runner to its declared native deployment target. Stage that
   complete slice and a non-secret deployment marker in an inactive candidate. Reuse an
   existing candidate only when its observable bytes match exactly; otherwise stop for
   reconciliation and record `failed`.
3. Verify candidate contents and marker identity and record `staged`.
4. Run the exact candidate with a target-policy-approved `isolated-smoke`, `representative-run`, or
   `full-run` method. When functions are present, attach the exact artifact and prove target import
   plus function-backed execution. Record `qualified` only after all required checks pass.
5. Recheck the candidate, stable target current, marker, resource/state gates, and authorization,
   then replace or associate target current with the complete qualified slice using the narrowest
   target-supported operation.
6. Observe target current and its exact build/release marker, then record `active`. A failed
   observation after activation records `failed` with the actual `active_unhealthy` or partial
   target state. Candidate cleanup follows target policy and is not an activation-success gate.

Update the same attempt receipt atomically after each phase. An environment with multiple runners
uses a separate ordered receipt and stable target current reference for each runner slice; report
mixed or partial state truthfully and do not claim atomicity across execution hosts.

Promotion applies this transaction to another declared target without rebuilding. Rollback stages,
qualifies, and activates the exact retained canonical build from an explicit previous successful
release; it does not require historical target folders, edit current metadata to imitate that
version, or assume the mutable watermark is compatible. If
atomic activation is unavailable, record the strategy, partial-state risk, and recovery action
before mutation.

## Release Automation

Release owns consume-only deployment automation; build owns automation that creates and verifies
builds. Keep generated release automation project-owned under `automation/release/`. It downloads
  one artifact from an explicit build run/source or resolves a verified build current selector once,
  verifies its provenance, exact build receipt, and
  release intent, then performs the deployment transaction. It never materializes or calls installed
  skill paths at runtime; vendor the deterministic release consumer validator into automation.

Use target protection gates and short-lived workload identity when supported. Pin third-party
automation dependencies according to project security policy. Persist receipts outside ephemeral
job storage so later promotion and rollback can address one exact release.

## Evidence And Handoff

Write receipts to `{workspace}/.releases/{env}/{release_id}.json` and validate the explicitly
selected path:

```bash
python scripts/validate_release.py --workspace <workspace> --receipt <receipt-path> [--build-selector current|<build-id>]
```

Consumers requiring a completed release add `--require-success`; this accepts only `active` v7
receipts. Never include credentials,
tokens, secret values, raw provider responses, or sensitive target outputs. End with exact build and
release IDs, target state, verification, skipped checks, and unresolved questions.

Release receipts use schema version 6. Earlier receipt schemas remain audit evidence only; create a
verified v6 baseline for each active runner slice before using the current promotion or rollback
flow.
