# Python Function Deployment Contract

## Scope

Read only when the exact Build manifest has a non-null `functions_artifact`. This reference owns
build-specific transfer, attachment, target import proof, session activation, promotion, rollback,
and receipt evidence. It does not build or convert packages, provision reusable facilities, install
from runners, or select a different format.

## Transaction

1. Validate Build manifest v3, Build receipt v4, fixed `functions/` component path, format, import
   prefix, and SHA-256.
2. Confirm Provision has already made the selected target mechanism ready when infrastructure was
   missing; otherwise observe the existing readiness directly.
3. Transfer the exact bytes into the release-addressed candidate, or its target-assigned opaque
   identity, and verify integrity. Keep `build_id` as artifact provenance, not candidate identity.
4. Attach that target reference through the approved host mechanism before runner/notebook start.
5. Start or select a fresh session when the host caches libraries or imports.
6. Import the manifest prefix from the target runtime and execute the required function-backed
   candidate qualification without resolving workspace authoring source. Record separate
   `functions-target-import` and `functions-target-execution` checks; import-only evidence is
   insufficient.
7. Activate the function attachment, runner, metadata, and deployment marker as one complete target
   current association only after all checks pass. Persist the same artifact and attachment
   identity in the Release receipt and observe it at target current.

The deployment-intent digest binds format, SHA-256, import prefix, attachment method, candidate and
target current references, and deployment marker. An active function-backed release requires
`functions-artifact-integrity`,
`functions-attachment`, `functions-target-import`, and `functions-target-execution`; also require
`functions-session-activation` when `fresh_session_required` is true.

Promotion transfers and attaches the same build bytes. Rollback reactivates the function artifact,
runner, metadata, and compatible runtime state from one explicit previous successful release. Do
not rebuild a wheel, rewrite a ZIP, change a version, substitute a package, or fall back to another
format.

## Host defaults

| Execution host | Default artifact | Release attachment boundary |
|---|---|---|
| Local, CI, VM, container | Wheel | Prepared venv/image before runner start |
| Fabric native notebook or Spark job | Wheel | Selected published Fabric Environment |
| Databricks native notebook or job | Wheel | Job/compute library dependency from immutable storage |
| AWS Glue | Wheel | Selected Glue library mechanism; ZIP only when explicitly designed |
| ZIP-native deployment host | ZIP | Exact ZIP deployment package |
| External cloud-adapter process | Wheel | Its actual host venv/image, not the cloud adapter |

Resolve exact current commands only after the host and mechanism are known, using official
documentation through `platform-tooling.md`.

## Unresolved Questions

None.
