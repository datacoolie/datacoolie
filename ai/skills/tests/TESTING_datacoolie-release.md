# Testing datacoolie-release

Run:

```bash
python ai/skills/tests/run_release.py
```

Verify:

- Supported direct AWS and Databricks operations prefer their official CLIs while preserving an
  already-approved project deployment mechanism.
- Fabric selection follows the operation's control plane: Azure CLI performs Azure/ARM readiness
  observation and routes resource mutation to Provision; Microsoft Fabric CLI performs
  Fabric-native transfer, qualification, activation, observation, promotion, and rollback.
- A supported Fabric REST operation without a high-level command uses `fab api` before `az rest`,
  curl, or an official API/SDK. A fallback is limited to a verified capability or approved-mechanism
  gap and records that reason.
- Resolve and version only the CLI and extension required for the selected operation. Do not assume
  `az` and `fab` share identity, tenant, target, subscription, or API audience.
- Missing CLI installation stays in operator/CI tooling, is non-admin and reversible, and never
  appears inside runners, notebooks, function artifacts, or the framework package.
- Release requires an exact build ID or validated `current` selector, runner slice, checksums,
  explicitly supplied successful Build v4 artifact receipt path, and target authorization. It
  resolves current once to canonical immutable bytes and never selects latest evidence.
- Modified or incomplete builds and receipts without generated-artifact proof fail preflight through
  the release consumer validator. Build-host runtime execution is optional and never substitutes
  for target qualification.
- Deploy and promotion never invoke materialization or functions packaging.
- Build manifest v3, Build receipt v4, and Release receipt v7 use one nullable typed
  `functions_artifact`; legacy plural shapes fail closed.
- A function-backed release binds exact format, SHA-256, import prefix, attachment method, and
  immutable target reference. Activation requires integrity, attachment, target import, target
  execution, and a fresh session check when the host caches imports.
- A no-function build keeps both release function fields null and omits irrelevant checks.
- Promotion reuses the source build, while rollback selects an explicit prior verified release
  rather than editing current metadata.
- CI references download and verify immutable artifacts without installed-skill paths.
- Production authorization is distinct from design or implementation approval.
- Cross-workflow acquisition identifies the exact source run or immutable artifact, not only its
  name.
- Release persists `prepared` before mutation, then records `staged`, `qualified`, `active`, or
  `failed`. Candidate runtime qualification is mandatory before activation; a post-activation health
  failure preserves `active_unhealthy` or partial target state.
- Candidate and stable target current references are distinct. A candidate is unique to one
  release attempt (or has a target-assigned opaque identity reserved for it), while target current
  stays version-independent. Active success proves a deployment
  marker bound to exact build, release, runner, metadata, and optional function artifact identity.
- Qualification uses separate log and watermark paths. Target current replacement never includes
  mutable runtime state, and rollback redeploys a retained canonical artifact rather than depending
  on target version history.
- Release authorization is bound to the canonical exact deployment intent, including build slice,
  target identity, activation mechanism, runtime paths/state intent, and source/provision evidence.
- Provision handoff validation binds the exact requirements, plan approval, and resource observation.
- Active release requires resource-readiness, environment-isolation, runtime-state-preflight,
  candidate integrity/runtime qualification, activation-preflight, and target-observation evidence;
  migrate/reset/replay intent requires current-session approval.
- Promotion and rollback consume explicit active source receipts, repeat destination qualification,
  and reject latest selection.
- Multiple runners in one environment use separate ordered runner-slice receipts and active
  references rather than implied cross-host atomicity.
- Release receipts remain durable beyond an ephemeral CI job.

Behavioral cases are stored in `datacoolie-release/evals/evals.json`; the validator checks that the
eval contract remains present and machine-readable, including high-level `fab`, `fab api`, and
verified fallback cases. Receipt unit tests verify build-slice binding, authorization intent,
upstream receipt semantics, source-release chains, target digests, build integrity, singular
function attachment/import semantics, and failed-release success gates.
