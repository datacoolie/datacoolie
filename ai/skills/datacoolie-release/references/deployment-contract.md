# Deployment Contract

## Scope

Use this reference for deploy, promotion, and rollback operations. It owns candidate transfer,
verification, activation, target observation, partial-state handling, and action-specific source
release semantics. It does not define build contents, provision resources, select platform tools,
or author pipeline behavior.

## Common Transaction

1. Resolve one build and exact runner slice from the manifest and successful Build v4
   artifact-verification receipt. `current` may select the build, but resolve its descriptor once,
   pin that exact build ID, and read or transfer only canonical immutable artifacts afterward.
   Build-host runtime execution is optional evidence, not a staging prerequisite.
   Observe required resources and stop for Provision when any requirement is missing, drifted,
   inaccessible, or unknown.
2. Bind exact authorization and persist the `prepared` attempt before mutation. Allocate a
   candidate uniquely addressed by this `release_id`, or use a target-assigned opaque identity
   reserved only for this release attempt. Map the full typed
   metadata set to the fixed target `metadata` component, the optional artifact to `functions`, and
   the runner through its declared native deployment kind. Transfer only those artifacts plus the
   non-secret deployment marker to an inactive candidate.
3. Verify candidate and marker identity at the target and record `staged`. Prefer target-side SHA-256;
   otherwise record the
   strongest observable comparison supported by the target.
4. Attach the exact functions artifact when present, bind the selected runner and metadata, and run
   the candidate with the target-policy-approved bounded qualification method. Function-backed
   slices prove target import and execution. Record `qualified`; import-only evidence never proves
   runner qualification.
5. Recheck candidate identity, stable target current, deployment marker, environment-isolated
   active and qualification log/watermark paths, runtime-state compatibility, and exact
   authorization immediately before activation.
6. Replace or associate the stable target current with the complete qualified candidate without
   mutating its bytes. Provision owns reusable platform readiness; Release owns this build-specific
   attachment and configuration.
7. Observe target current, the exact build/release marker, runner, metadata, optional singular
   function artifact, and required health signal. Record `active` only when observation passes.

Do not expose a partially transferred candidate as current. When the target lacks atomic
activation, document scheduler quiescence or execution snapshot behavior, ordered operations,
exposure window, failure boundary, and recovery action before applying them. A
partial transfer or activation produces a failed receipt and requires reconciliation before retry.
When the target cannot keep staged artifacts inactive, stop before mutation until its release policy
defines the exact in-place sequence, exposure risk, recovery action, and required current-session
authorization; never treat an in-place upload as ordinary candidate staging.

## Deploy

Deploy one declared environment slice from the supplied build. An existing target association may
be recorded as the previous active release, but it does not authorize replacement or mutation.

## Promote

Start from an explicit active source release receipt. Revalidate its build, then use the target
slice declared by that same build. Promotion changes target association; it never adds timestamps,
regenerates metadata, or rebuilds functions.

## Rollback

Use an explicit previous active release in the same target environment as the rollback
candidate. Revalidate the candidate receipt and build, record the currently active release being
replaced, obtain rollback authorization, and reactivate or redeploy the candidate. Never edit
current metadata to reconstruct a previous version. Compare the candidate's dataflow identities,
watermark columns/types, destination identity/load behavior, and declared keys/grain with active
runtime state. Stop for an explicitly approved migrate, reset, or replay action when compatibility
cannot be proven.

Build/addressed candidate state is temporary, not target version history. Successful observation
may clean it according to target policy; failed candidates follow the policy's retention and
reconciliation rules. Cleanup is not an activation-success gate. Canonical artifact retention must
cover every active release and approved rollback window because rollback does not depend on old
target folders.

## Runtime state and authorization

Release receipt schema v7 records exact active `runtime_paths`, isolated `qualification_scope`, a
`runtime_state` action/reference, candidate/current references, deployment marker, and the singular
function artifact/attachment when present.
They participate in the pre-mutation deployment-intent digest. Active actions require passed
`resource-readiness`, `environment-isolation`, `runtime-state-preflight`,
`shared-component-compatibility`,
`candidate-artifact-integrity`, `deployment-marker-integrity`,
`candidate-runtime-qualification`, `activation-preflight`, and `target-observation` checks. A migrate,
reset, or replay action requires current-session authorization and must never be inferred from a
failed compatibility check.

Persist one attempt before staging and update it atomically through `prepared`, `staged`,
`qualified`, `active`, or `failed`. A failed post-activation observation records the actual
`active_unhealthy` or partial state. Multiple runners in one environment use separate ordered
runner-slice receipts and stable target current references; mixed state is reported rather than
hidden behind an environment-wide success claim.

## Evidence

Record source artifact hashes, target references, observed hashes when available, activation
identity, previous active release, verification checks, and unresolved state. Store secret
references only; do not store secret values or raw provider responses.
