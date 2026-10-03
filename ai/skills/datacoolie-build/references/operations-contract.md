# DataCoolie Operational Runner Extensions

## Scope

- Read together with `references/runner-contract.md` only when replay or maintenance is required.
- This reference owns agent authorization, persistent-state risk, operation separation, and
  evidence gates. The public operation guides own parameters, defaults, range semantics, and
  framework scheduling.

Read the public [replay and backfill guide](https://datacoolie.github.io/datacoolie/guide/operations/replay-and-backfill/),
[maintenance guide](https://datacoolie.github.io/datacoolie/guide/operations/maintenance/),
[runtime configuration](https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/),
and [run-stage guide](https://datacoolie.github.io/datacoolie/guide/operations/run-stage/)
before adapting a project runner. This reference adds only agent-facing gates and evidence.

## Entrypoint identity

For newly authored host-specific entrypoints, use the common runner naming policy. Reused public
examples follow the canonical identity rule in [runner-contract.md](runner-contract.md#identity-and-location).
Keep replay and maintenance separate from normal `run` and from each other. The caller selects the
exact entrypoint; do not add a runtime operation selector.

## Replay gate

- Follow the public replay recipe for accepted parameters, boundary types, chunking, and result
  handling; do not duplicate framework validation in the entrypoint.
- Preserve the caller's stage selection and use separate replay invocations for dependent stages;
  external orchestration owns repeated calls and completion gates.
- Watermark mutation is opt-in. Keep the safe no-mutation mode as the default and require separate
  explicit confirmation before enabling a persistent watermark advance.
- DataCoolie owns replay execution and result aggregation. The runner decodes only the selected
  host transport and must not recreate a scheduler.

## Persistent state safety

Use one active writer for each environment/dataflow watermark state key unless the project has a
separately verified coordination mechanism. This applies across normal runs, replay with watermark
mutation, and overlapping scheduler instances; the runner does not invent locking around the
framework.

Treat watermark storage as critical state. If target output succeeds but watermark persistence
fails, stop and retain the failure evidence. Retry only through the project's idempotent load or an
explicit replay/recovery action; never advance or reset the watermark merely to clear the error.

## Maintenance gate

- Read the public maintenance guide for supported target selection, retention, and operation inputs.
- Require explicit executable confirmation before a mutating maintenance call; a comment or
  Markdown warning is not a gate.
- Keep maintenance separate from normal execution and do not add runner-owned preview,
  target-description, scheduling, or `dry_run` behavior.
- DataCoolie owns supported-target selection, physical-target deduplication, dispatch, logging, and
  result aggregation. The runner makes one selected framework call after authorization.

## Outcome-scoped verification

For author/build-only requests, validate the runner/project and built artifact without executing a
workload. For an explicitly requested safe replay or maintenance check, derive expected selection,
output, and state changes from the matching public recipe and project acceptance criteria; inspect
the framework result/logs and independent output/state evidence. Identify legitimate empty or
skipped outcomes, and report performed checks, skipped host/dependency checks, and unresolved
questions. The CLI build manifest and local validation report remain artifact evidence; runtime
observations belong to the operation's normal logs and result.
