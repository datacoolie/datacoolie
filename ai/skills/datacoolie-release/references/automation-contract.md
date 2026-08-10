# Release Automation Contract

## Scope

Use this reference only when authoring or validating consume-only release CI/CD. It owns explicit
build-run identity, artifact acquisition, target protection, credential flow, deployment steps, and
durable receipt persistence. It does not build/materialize artifacts, define platform commands, or
replace project security policy.

## Required Inputs

- Exact build source or workflow run identity and immutable build ID.
- Exact artifact name/digest or provenance attestation supported by the artifact store.
- Exact successful build receipt path or transported receipt identity.
- Target environment, action, and source release receipt for promotion or rollback.
- Exact successful provision receipt when the target prerequisite required provisioning.

Do not search recent runs, artifacts, receipts, or release history. Reject `latest`, globbing, and
ambiguous selectors.

## Job Boundary

- Build automation creates, verifies, and publishes one immutable artifact plus its build receipt.
- Release automation downloads that artifact from the explicit source run, verifies transport
  identity, runs its vendored release consumer validator, and never materializes.
- Keep release validators in project-owned automation. Runtime jobs must not reference installed
  skill directories.
- Treat manually supplied environment, runner, and path values as untrusted. Resolve the target
  slice from the validated manifest rather than constructing paths from unchecked input.

## Security And Persistence

Use the CI provider's protected environment and short-lived workload identity when supported.
Grant only artifact-read and target-specific deployment permissions. Pin third-party actions or
tasks according to project policy and record tool/action identities in evidence.

Write the release receipt even on failure. Upload or store `.releases/{env}/{release_id}.json` in a
durable release-evidence location before the ephemeral job ends. Later promotion and rollback must
address that exact receipt by path or immutable object identity and hash.
