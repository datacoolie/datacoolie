# Platform Tooling

## Scope

Use this reference only when direct platform CLI or API operations are the approved mechanism. It
owns current documentation lookup, preview classification, command evidence, and result capture.
It does not select resources, define naming, prescribe versions, or override an existing IaC source
of truth.

## Documentation Routing

Resolve installed tool versions first, then verify syntax and capability against current official
documentation. Record the exact versions in the provision receipt; do not copy commands or version
constraints from memory.

| Tool family | Official documentation |
|---|---|
| AWS CLI | https://docs.aws.amazon.com/cli/latest/reference/ |
| Azure CLI | https://learn.microsoft.com/cli/azure/reference-index |
| Databricks CLI | https://docs.databricks.com/dev-tools/cli/index.html |
| Microsoft Fabric CLI | https://microsoft.github.io/fabric-cli/ |
| AWS Terraform provider | https://registry.terraform.io/providers/hashicorp/aws/latest/docs |
| AzureRM Terraform provider | https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs |
| Databricks Terraform provider | https://registry.terraform.io/providers/databricks/databricks/latest/docs |
| Microsoft Fabric Terraform provider | https://registry.terraform.io/providers/microsoft/fabric/latest/docs |

## Safe Execution

- Classify a command as preview only when current documentation guarantees it is non-mutating.
  Otherwise stop at inventory and a persisted plan.
- Capture the exact command intent, tool version, target identity, plan artifact, exit status, and
  redacted result. Do not place credentials or raw provider responses in evidence.
- Prefer idempotent commands and stable resource identifiers. Re-read observable state after each
  mutation instead of treating command success as resource verification.
- Stop when the platform proposes actions that differ from the approved plan. Direct commands do
  not weaken approval, destructive-action, state-ownership, or receipt requirements.
