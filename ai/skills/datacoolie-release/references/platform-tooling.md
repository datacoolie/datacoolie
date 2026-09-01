# Platform Tooling

## Scope

Use this reference only after the target platform and deployment mechanism are selected. It owns
control-plane tool selection, tooling availability, current documentation lookup, installed-version
capture, fallback, command validation, and redacted command evidence. It does not choose resources,
naming, credentials, CI providers, or deployment policy.

## Control-plane execution

For supported direct AWS or Databricks release operations, prefer the official AWS CLI or Databricks
CLI. Keep an already-approved project deployment mechanism; do not replace working automation merely
to introduce another command layer.

Fabric spans two control planes. Classify the exact release operation before selecting a tool:

| Release boundary | Preferred direct tool |
|---|---|
| Read-only Azure/ARM readiness, including Fabric Capacity and related Azure resources | Azure CLI; route any required resource mutation to Provision |
| Fabric-native workspace/item transfer, OneLake content, jobs, candidate qualification, activation, observation, promotion, and rollback | A high-level Microsoft Fabric CLI command |
| Fabric REST operation without a suitable high-level Fabric CLI command | `fab api`, including `--show_headers` when the API's LRO contract requires response headers |
| Verified remaining capability or approved-mechanism gap | `az rest`, curl, or an official API/SDK, limited to that gap |

The ability of `fab api` to call Azure endpoints does not make it the default for ARM readiness, and
the ability of `az rest` to call Fabric endpoints does not make Azure CLI the Fabric-native release
tool. Record the selected control plane and any fallback reason in release evidence.

Resolve only the executable and extension required by the selected operation. Capture their exact
versions and verify current capability; do not hardcode a minimum version. When required tooling is
absent, verify the current official installation method, then install automatically only when the release scope permits a
non-interactive, reversible installation confined to the current user, project tooling environment,
or CI image and requiring no administrator privilege. Otherwise request installation or approval
and stop before target mutation. Record installer source and resolved version.

CLI preparation belongs to operator/CI tooling. Never install it from a generated runner or
notebook, bundle it in a function artifact, or add it to the `datacoolie` framework package.

Do not assume `az` and `fab` share authentication state. Before mutation, verify the selected
identity, tenant, Azure subscription when applicable, Fabric target, and API audience without
recording tokens or secrets.

## Documentation Routing

Resolve installed tool versions and verify installation and commands against current official documentation before
generating or running automation. Do not copy authentication or deployment syntax from memory.

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
| GitHub Actions deployments | https://docs.github.com/actions/deployment |
| GitHub Actions artifacts | https://docs.github.com/actions/using-workflows/storing-workflow-data-as-artifacts |

## Execution Evidence

Capture CLI and extension versions, control plane, target identity, operation intent, exit status,
and redacted result. Prefer short-lived workload identity over long-lived tokens when current
platform support and target policy allow it. A successful command is not sufficient verification;
observe the target state and active build after the operation.
