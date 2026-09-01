# Release Automation Contract

## Scope

Use this reference only when authoring or validating consume-only release CI/CD. It owns explicit
build-run identity, artifact acquisition, target protection, credential flow, deployment steps, and
durable receipt persistence. It does not build/materialize artifacts, define platform commands, or
replace project security policy.

## Required Inputs

- Exact build source or workflow run identity and immutable build ID, or a verified workspace
  `current` selector that is resolved once before authorization.
- Exact artifact name/digest or provenance attestation supported by the artifact store.
- Exact successful Build v4 artifact-verification receipt path or transported receipt identity.
- Target environment, action, and source release receipt for promotion or rollback.
- Exact successful provision receipt when the target prerequisite required provisioning.
- Exact log and watermark paths plus the runtime-state action and compatibility/transition
  reference authorized for this target.

Do not search recent runs, artifacts, receipts, or release history. Reject `latest`, globbing, and
ambiguous selectors. When `current` is supplied, validate `current/build.json`, pin its exact build
ID, and acquire canonical immutable bytes rather than uploading the projection.

## Job Boundary

- Build automation creates, artifact-verifies, and publishes one immutable artifact plus its Build
  v3 receipt. Build-host runtime execution is optional and does not authorize activation.
- Release automation downloads that artifact from the explicit source run, verifies transport
  identity, runs its vendored release consumer validator, and never materializes.
- Keep release validators in project-owned automation. Runtime jobs must not reference installed
  skill directories.
- Treat manually supplied environment, runner, and path values as untrusted. Resolve the target
  slice from the validated manifest rather than constructing paths from unchecked input.
- Observe resource readiness and environment isolation, then bind exact runtime paths and state
  intent, qualification scope, release-addressed candidate/stable-current references, and
  deployment marker into the
  deployment digest before the protected-environment gate. Persist `prepared`
  before target mutation and update the same receipt atomically after staging, qualification,
  activation, and observation.

## Security And Persistence

Use the CI provider's protected environment and short-lived workload identity when supported.
Grant only artifact-read and target-specific deployment permissions. Pin third-party actions or
tasks according to project policy and record tool/action identities in evidence.

Write the prepared release receipt before mutation and preserve it on failure. Upload or store
`.releases/{env}/{release_id}.json` in a durable release-evidence location before the ephemeral job
ends. Later promotion and rollback must
address that exact receipt by path or immutable object identity and hash.
