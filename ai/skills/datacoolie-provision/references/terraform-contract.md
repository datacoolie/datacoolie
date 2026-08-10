# Terraform Contract

## Scope

Use this reference only when the project already uses Terraform or Terraform is the approved
provisioning mechanism. It owns Terraform state, saved-plan, import, apply, and output rules. It
does not select resources, define naming, pin provider versions, or replace platform documentation.

## Source And State

- Reuse the existing root module, module boundaries, backend, workspace, lockfile, and provider
  constraints. Project files and `.terraform.lock.hcl` are the source of truth.
- Inspect state and observable resources before planning. An existing resource outside state is a
  reconciliation decision, not permission to create a duplicate.
- Do not switch or initialize a different backend/workspace, force-unlock remote state, migrate
  state, or import/adopt a resource implicitly. Include such an action in the exact plan and obtain
  apply approval first.
- Keep state, credentials, and variable secrets out of provision evidence.

## Plan And Apply

1. Initialize without upgrading providers unless the approved plan explicitly includes an upgrade.
2. Validate configuration and create a saved, environment-specific plan.
3. Persist the saved plan or its exact immutable export under the provision evidence plan path and
   calculate SHA-256 from its bytes.
4. Review replacements, deletions, imports, permissions, cost, and data-bearing impact before
   requesting approval.
5. Apply the saved approved plan rather than creating a new plan during apply. If the tool cannot
   apply the persisted plan exactly, compare the new preview with it and stop on any difference.
6. Refresh and verify observable state after apply. Record partial changes and reconcile before any
   retry.

## Outputs

Record only non-sensitive identifiers needed by build or release. Respect Terraform `sensitive`
marks and omit values that contain credentials, tokens, keys, connection strings, or private
endpoints not intended for the consumer. Evidence may reference a secret name; it must not contain
the secret value.
