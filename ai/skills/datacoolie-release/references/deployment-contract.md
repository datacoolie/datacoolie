# Deployment Contract

## Scope

Use this reference for deploy, promotion, and rollback operations. It owns candidate transfer,
verification, activation, target observation, partial-state handling, and action-specific source
release semantics. It does not define build contents, provision resources, select platform tools,
or author pipeline behavior.

## Common Transaction

1. Resolve one build and declared environment slice from the manifest and successful build receipt.
2. Transfer only those artifacts to a build-addressed candidate location.
3. Verify candidate identity at the target. Prefer target-side SHA-256; otherwise record the
   strongest observable comparison supported by the target.
4. Recheck exact authorization immediately before activation.
5. Attach the exact functions artifact when present, bind the selected runner and metadata, then
   activate or associate the candidate without mutating its bytes. Provision owns reusable platform
   readiness; release owns this build-specific attachment and configuration.
6. Observe the active build, runner, metadata, optional functions, and required health signal.
7. Persist the receipt before treating the operation as complete.

Do not expose a partially transferred candidate as active. When the target lacks atomic activation,
document the ordered operations, failure boundary, and recovery action before applying them. A
partial transfer or activation produces a failed receipt and requires reconciliation before retry.

## Deploy

Deploy one declared environment slice from the supplied build. An existing target association may
be recorded as the previous active release, but it does not authorize replacement or mutation.

## Promote

Start from an explicit successful source release receipt. Revalidate its build, then use the target
slice declared by that same build. Promotion changes target association; it never adds timestamps,
regenerates metadata, or rebuilds functions.

## Rollback

Use an explicit previous successful release in the same target environment as the rollback
candidate. Revalidate the candidate receipt and build, record the currently active release being
replaced, obtain rollback authorization, and reactivate or redeploy the candidate. Never edit
current metadata to reconstruct a previous version.

## Evidence

Record source artifact hashes, target references, observed hashes when available, activation
identity, previous active release, verification checks, and unresolved state. Store secret
references only; do not store secret values or raw provider responses.
