# Testing datacoolie-provision

Run:

```bash
python ai/skills/tests/run_provision.py
```

Forward scenarios:

- Direct AWS and Databricks operations prefer their official CLIs unless established IaC owns the
  resource.
- Fabric selection follows the resource control plane: Azure/ARM resources such as Fabric Capacity
  use Azure CLI, while Fabric-native workspaces, items, OneLake content, jobs, and workspace
  settings use Microsoft Fabric CLI.
- A supported Fabric REST operation without a high-level command uses `fab api` before `az rest`,
  curl, or an official API/SDK. A fallback is limited to a verified capability or approved-mechanism
  gap and records that reason.
- Resolve and version only the CLI and extension required for the selected operation. Do not assume
  `az` and `fab` share identity, tenant, target, subscription, or API audience.
- A missing CLI may be installed only non-interactively and reversibly inside a current-user,
  project-tooling, or CI boundary without administrator privilege; otherwise Provision requests
  installation and performs no platform mutation.
- Existing infrastructure skips provisioning.
- Missing resources produce a plan receipt without applying changes.
- Apply stops for explicit approval tied to the plan and environment.
- Data-bearing replacement requires a separate destructive approval, and a materially changed plan
  requires renewed approval.
- Metadata defects route back to build instead of triggering speculative infrastructure.
- Plan approval is bound to the exact environment and persisted plan hash.
- Existing resources outside Terraform state require approved reconciliation instead of implicit
  import or duplicate creation.
- Failed or partial applies cannot satisfy a successful apply receipt gate.
- Provision evidence excludes secrets and sensitive outputs.
- Provision never changes metadata, builds, or releases.
- Build or release supplies one exact requirements artifact and hash instead of an undefined
  resource-gap receipt type.

Behavioral cases are stored in `datacoolie-provision/evals/evals.json`; the validator checks that
the eval contract remains present and machine-readable, including Fabric Capacity, Fabric-native
resource, and `fab api` routing cases. Receipt unit tests verify artifact hashes, authorization
binding, destructive approval, partial-state handling, and sensitive output rejection.
