# Platform Tooling

## Scope

Use this reference only after the target platform and deployment mechanism are selected. It owns
current documentation lookup, installed-version capture, command validation, and redacted command
evidence. It does not choose resources, naming, credentials, CI providers, or deployment policy.

## Documentation Routing

Resolve installed tool versions and verify commands against current official documentation before
generating or running automation. Do not copy authentication or deployment syntax from memory.

| Tool family | Official documentation |
|---|---|
| AWS CLI | https://docs.aws.amazon.com/cli/latest/reference/ |
| Azure CLI | https://learn.microsoft.com/cli/azure/reference-index |
| Databricks CLI | https://docs.databricks.com/dev-tools/cli/index.html |
| Microsoft Fabric CLI | https://microsoft.github.io/fabric-cli/ |
| GitHub Actions deployments | https://docs.github.com/actions/deployment |
| GitHub Actions artifacts | https://docs.github.com/actions/using-workflows/storing-workflow-data-as-artifacts |

## Execution Evidence

Capture tool version, target identity, operation intent, exit status, and redacted result. Prefer
short-lived workload identity over long-lived tokens when current platform support and target
policy allow it. A successful command is not sufficient verification; observe the target state and
active build after the operation.
