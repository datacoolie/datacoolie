# Platform Tooling

## Scope

Use this reference only when direct platform operations are the selected mechanism. It owns
control-plane tool selection, tooling availability, current documentation lookup, preview
classification, fallback, command evidence, and result capture.
It does not select resources, define naming, prescribe versions, or override an existing IaC source
of truth.

## Control-plane selection

For direct AWS or Databricks inventory, provisioning, reconciliation, and verification, prefer the
official AWS CLI or Databricks CLI when it expresses the operation. Fabric spans two control planes;
classify the exact resource before selecting a tool:

| Operation boundary | Preferred direct tool |
|---|---|
| Azure/ARM resources, including Fabric Capacity, resource groups, Azure RBAC, identity, and related Azure infrastructure | Azure CLI; prefer a dedicated command such as `az fabric capacity` when supported |
| Fabric-native workspaces, items, OneLake content, jobs, and workspace-scoped settings | A high-level Microsoft Fabric CLI command |
| Fabric REST operation without a suitable high-level Fabric CLI command | `fab api`, including `--show_headers` when the API's LRO contract requires response headers |
| Verified remaining capability or approved-mechanism gap | `az rest`, curl, or an official API/SDK, limited to that gap |

An established approved IaC source of truth remains authoritative; do not bypass its state by
creating the same resource through a CLI. The ability of `fab api` to call Azure endpoints does not
make it the default for ARM resources, and the ability of `az rest` to call Fabric endpoints does
not make Azure CLI the Fabric-native item tool. Record the selected control plane, mechanism, and
any fallback reason in the plan and receipt.

Resolve only the executable and extension required by the selected operation. Capture their exact
versions and verify current capability; do not hardcode a minimum version. When required tooling is
absent:

1. Verify its current official installation method and compatibility from the documentation below.
2. Install it automatically only when installation is in scope, non-interactive, reversible, and
   confined to the current user, project tooling environment, or CI image without administrator
   privileges.
3. Otherwise request installation or approval and stop before platform mutation.
4. Record the installer source and resolved version; never install a CLI inside DataCoolie runners,
   notebooks, function artifacts, or the `datacoolie` package.

Do not assume `az` and `fab` share authentication state. Before mutation, verify the selected
identity, tenant, Azure subscription when applicable, Fabric target, and API audience without
recording tokens or secrets.

## Documentation Routing

Resolve installed tool versions first, then verify syntax, installation, and capability against current official
documentation. Record the exact versions in the provision receipt; do not copy commands or version
constraints from memory.

| Tool family | Official documentation |
|---|---|
| AWS CLI | https://docs.aws.amazon.com/cli/latest/reference/ |
| Azure CLI | https://learn.microsoft.com/cli/azure/reference-index |
| Azure CLI Fabric Capacity | https://learn.microsoft.com/cli/azure/fabric/capacity |
| Databricks CLI | https://docs.databricks.com/dev-tools/cli/index.html |
| Microsoft Fabric CLI | https://microsoft.github.io/fabric-cli/ |
| Microsoft Fabric CLI API command | https://microsoft.github.io/fabric-cli/commands/api/ |
| Microsoft Fabric REST APIs | https://learn.microsoft.com/rest/api/fabric/articles/ |
| Microsoft Fabric agent/API patterns (optional guidance, not a runtime dependency) | https://github.com/microsoft/skills-for-fabric |
| AWS Terraform provider | https://registry.terraform.io/providers/hashicorp/aws/latest/docs |
| AzureRM Terraform provider | https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs |
| Databricks Terraform provider | https://registry.terraform.io/providers/databricks/databricks/latest/docs |
| Microsoft Fabric Terraform provider | https://registry.terraform.io/providers/microsoft/fabric/latest/docs |

## Safe Execution

- Classify a command as preview only when current documentation guarantees it is non-mutating.
  Otherwise stop at inventory and a persisted plan.
- Capture the exact command intent, CLI and extension versions, control plane, target identity, plan
  artifact, exit status, and redacted result. Do not place credentials or raw provider responses in
  evidence.
- Prefer idempotent commands and stable resource identifiers. Re-read observable state after each
  mutation instead of treating command success as resource verification.
- Stop when the platform proposes actions that differ from the approved plan. Direct commands do
  not weaken approval, destructive-action, state-ownership, or receipt requirements.

## Control-resource verification

Use current official platform commands or APIs to verify the approved resource identity, location,
access policy, and persistence behavior. The normal control-resource choices are a persistent local
directory, AWS S3 bucket, Fabric Lakehouse, or Databricks governed Volume. These are defaults,
not permission to invent a resource outside approved requirements.

Verify access from the intended execution host. For an external cloud adapter running on premises,
include outbound network reachability, workload identity, and create/read/update behavior for the
exact environment-scoped metadata, log, and watermark paths. Capture only redacted observations.

For a selected function package, verify reusable host capability for its exact format, immutable
artifact storage, least-privilege access, supported library/environment mechanism, and whether a
fresh session is required. This is readiness evidence only. Per-build upload, attachment,
activation, import testing, and rollback belong to Release.
